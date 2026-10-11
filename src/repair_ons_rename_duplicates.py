"""Plan/apply/roll back removal of ONS rows left behind by republished files.

The ONS loader keyed rows on (hour, plant name, ONS ID) and never deletes. When
ONS republished 2026 files with changed names (capitalisation in January; a
new upper-case format from May), the weekly load inserted the renamed rows
beside the old ones, doubling those plant-hours (May 2026 coal stored twice).
Some aggregate rows were re-identified (e.g. 'PQU MGMG TERM - CARGA GLOBAL',
ID PQU_MGMG_T, became 'PQU MGMG TERM' without an ID) and some zero-output
plants were dropped; their earlier rows remain although the current files no
longer contain them ("withdrawn"). And plants now published with null
generation keep the zeros loaded from earlier file versions ("now_missing";
the null hours are recorded as source gaps by the all-fuel work, not stored).

plan    Read-only. For every (hour, ONS ID) stored more than once, keeps the row
        whose name matches the audited ONS source file for that hour and lists
        every other copy. In the audited months it also lists every stored row
        whose plant-hour (ONS ID, or name for rows without one) is absent from
        the source, or present only with null generation. Rows are listed with
        full before-images. Fails if a
        duplicate falls outside the audited files or no copy (or more than one)
        matches. Writes a plan file with a fingerprint.
        Refuses if stale rows exceed half of a month's stored rows (a sign the
        source and stored timestamps or IDs do not line up).
apply   Re-checks the plan against the audited manifest, then under a table
        lock re-checks every listed row is unchanged, records the repair in
        ingestion.ons_republication_repairs, archives the rows to
        ingestion.ons_rename_duplicate_backup, deletes them, and verifies that
        the audited months hold exactly one row per duplicated plant-hour and
        no duplicates. Needs the plan's fingerprint.
rollback Restores the archived rows of one fingerprint and marks the repair
        rolled back (the archive is kept). Refuses while migration 020's
        plant-hour key exists, or if a restored row would collide.

Run through run_in_environment.py with the owner role; the target must match
config/environments.json. Every production command, including the read-only
plan, requires --allow-production. The
plan step reads Parquet, so run it with `uv run --with pyarrow`. Views are not
refreshed here.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/repairs/ons-rename-duplicates-2026.json"
TABLE = "ingestion.ons_generation_data"
BACKUP = "ingestion.ons_rename_duplicate_backup"
LEDGER = "ingestion.ons_republication_repairs"
MAX_STALE_SHARE = 0.5
COLUMNS = (
    "id",
    "extraction_run_id",
    "created_at_ms",
    "plant",
    "ons_plant_id",
    "plant_type",
    "fuel_type",
    "subsystem_id",
    "subsystem",
    "state",
    "state_name",
    "operation_mode",
    "ceg",
    "timestamp_ms",
    "generation_mwh",
    "resolution_minutes",
)
DUPLICATES = f"""
    SELECT {", ".join(COLUMNS)} FROM {TABLE} g
    WHERE NULLIF(ons_plant_id, '') IS NOT NULL
      AND (timestamp_ms, ons_plant_id) IN (
        SELECT timestamp_ms, ons_plant_id FROM {TABLE}
        WHERE NULLIF(ons_plant_id, '') IS NOT NULL
        GROUP BY 1, 2 HAVING count(*) > 1)
    ORDER BY timestamp_ms, ons_plant_id, id
"""


def as_record(row):
    """One stored row as compared by plan and apply."""
    return dict(zip(COLUMNS, row), extraction_run_id=str(row[1]))


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def manifest():
    data = MANIFEST.read_bytes()
    return json.loads(data), hashlib.sha256(data).hexdigest()


def fingerprint_of(manifest_sha256, deletions):
    return digest({"manifest_sha256": manifest_sha256, "deletions": deletions})


def plant_hour(timestamp_ms, plant_id, plant):
    """The key of migration 020: ONS ID, or the name for rows without one."""
    return (timestamp_ms, plant_id if plant_id else f"name:{plant}")


def index_source(df, names, keys, missing):
    """Add one source file (din_instante, id_ons, nom_usina, val_geracao)."""
    import pandas as pd

    stamps = pd.to_datetime(df["din_instante"]).values.astype("datetime64[ms]")
    ids = df["id_ons"].fillna("")
    value = df["val_geracao"].astype("string").str.strip()
    null = (value.isna() | value.eq("")).tolist()
    for stamp, plant_id, name, is_null in zip(
        stamps.astype("int64").tolist(), ids, df["nom_usina"], null
    ):
        (missing if is_null else keys).add(plant_hour(stamp, plant_id, name))
        if plant_id and names.setdefault((stamp, plant_id), name) != name:
            raise ValueError(f"Source names {plant_id} twice at {stamp}")


def source_names(source_dir):
    """Audited source files -> ((hour, ONS ID) -> name, plant-hours with a value,
    plant-hours published as null, months)."""
    import pandas as pd

    names, keys, missing, months = {}, set(), set(), set()
    for item in manifest()[0]["source_files"]:
        path = source_dir / item["url"].rsplit("/", 1)[1]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"{path.name} differs from the audited source file")
        index_source(
            pd.read_parquet(
                path, columns=["din_instante", "id_ons", "nom_usina", "val_geracao"]
            ),
            names,
            keys,
            missing,
        )
        months.add(item["label"])
    return names, keys, missing, months


def month_of(timestamp_ms):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).strftime("%Y-%m")


def month_bounds(label):
    from datetime import UTC, datetime

    year, month = map(int, label.split("-"))
    start = datetime(year, month, 1, tzinfo=UTC)
    end = datetime(year + month // 12, month % 12 + 1, 1, tzinfo=UTC)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def plan(cursor, source_dir):
    names, keys, missing, months = source_names(source_dir)
    cursor.execute(DUPLICATES)
    groups = {}
    for row in cursor.fetchall():
        record = as_record(row)
        groups.setdefault((record["timestamp_ms"], record["ons_plant_id"]), []).append(
            record
        )
    deletions = []
    for (stamp, plant_id), rows in groups.items():
        if month_of(stamp) not in months:
            raise ValueError(f"Duplicate {plant_id} at {stamp} is outside the audit")
        current = names.get((stamp, plant_id))
        kept = [r for r in rows if r["plant"] == current]
        if len(kept) != 1:
            raise ValueError(
                f"{plant_id} at {stamp}: {len(kept)} copies match the source name"
            )
        deletions += [r for r in rows if r is not kept[0]]
    duplicates = len(deletions)
    reasons = {row["id"]: "duplicate" for row in deletions}
    for label in sorted(months):
        start, end = month_bounds(label)
        cursor.execute(
            f"SELECT {', '.join(COLUMNS)} FROM {TABLE} "
            "WHERE timestamp_ms >= %s AND timestamp_ms < %s ORDER BY id",
            (start, end),
        )
        stored = list(map(as_record, cursor.fetchall()))
        stale = 0
        for row in stored:
            key = plant_hour(row["timestamp_ms"], row["ons_plant_id"], row["plant"])
            if key not in keys and row["id"] not in reasons:
                deletions.append(row)
                reasons[row["id"]] = "now_missing" if key in missing else "withdrawn"
                stale += 1
        if stored and stale > MAX_STALE_SHARE * len(stored):
            raise ValueError(
                f"{stale} of {len(stored)} stored rows in {label} are absent from the "
                "source: timestamps or IDs probably do not line up"
            )
    summary = {}
    for row in deletions:
        bucket = summary.setdefault(
            f"{reasons[row['id']]} {month_of(row['timestamp_ms'])} {row['fuel_type']}",
            [0, 0.0],
        )
        bucket[0] += 1
        bucket[1] += row["generation_mwh"]
    manifest_sha256 = manifest()[1]
    return {
        "repair": "ons-rename-duplicates-2026",
        "manifest_sha256": manifest_sha256,
        "months": sorted(months),
        "duplicate_plant_hours": len(groups),
        "duplicate_rows": duplicates,
        "withdrawn_rows": sum(r == "withdrawn" for r in reasons.values()),
        "now_missing_rows": sum(r == "now_missing" for r in reasons.values()),
        "rows_to_delete": len(deletions),
        "summary": {
            k: {"rows": v[0], "mwh": round(v[1], 3)} for k, v in sorted(summary.items())
        },
        "fingerprint": fingerprint_of(manifest_sha256, deletions),
        "reasons": {str(k): v for k, v in reasons.items()},
        "deletions": deletions,
    }


def create_archive(cursor):
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {LEDGER} (fingerprint text PRIMARY KEY, "
        "manifest_sha256 text NOT NULL, rows integer NOT NULL, "
        "state text NOT NULL CHECK (state IN ('applied', 'rolled_back')), "
        "applied_at timestamptz NOT NULL DEFAULT now(), rolled_back_at timestamptz)"
    )
    # Column copies only: no defaults, so the archive is not tied to the
    # main table's id sequence.
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP} AS TABLE {TABLE} WITH NO DATA")
    for column in (
        "fingerprint text",
        "reason text",
        "archived_at timestamptz DEFAULT now()",
        "rolled_back_at timestamptz",
    ):
        cursor.execute(f"ALTER TABLE {BACKUP} ADD COLUMN IF NOT EXISTS {column}")
    cursor.execute(f"ALTER TABLE {BACKUP} ALTER COLUMN id DROP DEFAULT")
    # The archive is the only copy of removed rows: owner only.
    cursor.execute(f"REVOKE ALL ON {LEDGER}, {BACKUP} FROM PUBLIC")
    for role in ("etl_writer", "dashboard_ro"):
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cursor.fetchone():
            cursor.execute(f"REVOKE ALL ON {LEDGER}, {BACKUP} FROM {role}")


def apply(cursor, planned, fingerprint):
    manifest_data, manifest_sha256 = manifest()
    rows = planned["deletions"]
    if (
        planned["fingerprint"] != fingerprint
        or planned["manifest_sha256"] != manifest_sha256
        or fingerprint_of(manifest_sha256, rows) != fingerprint
    ):
        raise ValueError("Plan does not match the fingerprint and audited manifest")
    months = {item["label"] for item in manifest_data["source_files"]}
    if any(month_of(row["timestamp_ms"]) not in months for row in rows):
        raise ValueError("Plan deletes rows outside the audited months")
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
    ids = [row["id"] for row in rows]
    cursor.execute(
        f"SELECT {', '.join(COLUMNS)} FROM {TABLE} WHERE id = ANY(%s) ORDER BY id",
        (ids,),
    )
    if [as_record(r) for r in cursor.fetchall()] != sorted(rows, key=lambda r: r["id"]):
        raise ValueError("Rows changed since the plan; re-plan")
    create_archive(cursor)
    cursor.execute(
        f"INSERT INTO {LEDGER} (fingerprint, manifest_sha256, rows, state) "
        "VALUES (%s, %s, %s, 'applied') ON CONFLICT (fingerprint) DO UPDATE "
        "SET state = 'applied', applied_at = now(), rolled_back_at = NULL "
        f"WHERE {LEDGER}.state = 'rolled_back'",
        (fingerprint, manifest_sha256, len(rows)),
    )
    if cursor.rowcount != 1:
        raise ValueError("This repair is already applied")
    reasons = planned["reasons"]
    execute_values(
        cursor,
        f"INSERT INTO {BACKUP} ({', '.join(COLUMNS)}, fingerprint, reason) VALUES %s",
        [
            tuple(row[c] for c in COLUMNS) + (fingerprint, reasons[str(row["id"])])
            for row in rows
        ],
    )
    cursor.execute(f"DELETE FROM {TABLE} WHERE id = ANY(%s)", (ids,))
    if cursor.rowcount != len(rows):
        raise ValueError("Deleted row count differs from the plan")
    for label in sorted(months):
        start, end = month_bounds(label)
        cursor.execute(
            f"SELECT count(*) FROM {TABLE} WHERE timestamp_ms >= %s AND timestamp_ms < %s "
            "AND NULLIF(ons_plant_id, '') IS NOT NULL "
            "GROUP BY timestamp_ms, ons_plant_id HAVING count(*) > 1 LIMIT 1",
            (start, end),
        )
        if cursor.fetchone():
            raise ValueError(f"Duplicate plant-hours remain in {label}")
    survivors = {
        (row["timestamp_ms"], row["ons_plant_id"])
        for row in rows
        if reasons[str(row["id"])] == "duplicate"
    }
    if survivors:
        cursor.execute(
            f"SELECT count(*) FROM {TABLE} t JOIN unnest(%s::bigint[], %s::text[]) "
            "AS k(timestamp_ms, ons_plant_id) USING (timestamp_ms, ons_plant_id)",
            ([s[0] for s in survivors], [s[1] for s in survivors]),
        )
        if cursor.fetchone()[0] != len(survivors):
            raise ValueError("A duplicated plant-hour lost every copy")
    return {"deleted": len(rows), "fingerprint": fingerprint}


def rollback(cursor, fingerprint):
    cursor.execute("SELECT to_regclass('ingestion.uq_ons_plant_hour')")
    if cursor.fetchone()[0]:
        raise ValueError("Drop migration 020's uq_ons_plant_hour before rolling back")
    cursor.execute(f"SELECT to_regclass('{LEDGER}')")
    if not cursor.fetchone()[0]:
        raise ValueError("No repair ledger: nothing to roll back")
    cursor.execute(
        f"SELECT state, rows FROM {LEDGER} WHERE fingerprint = %s FOR UPDATE",
        (fingerprint,),
    )
    entry = cursor.fetchone()
    if entry is None or entry[0] != "applied":
        raise ValueError(f"Fingerprint is not an applied repair: {entry}")
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
    cursor.execute(
        f"SELECT count(*) FROM {BACKUP} b JOIN {TABLE} t ON t.timestamp_ms = b.timestamp_ms "
        "AND t.plant = b.plant AND COALESCE(t.ons_plant_id, '') = COALESCE(b.ons_plant_id, '') "
        "WHERE b.fingerprint = %s AND b.rolled_back_at IS NULL",
        (fingerprint,),
    )
    if cursor.fetchone()[0]:
        raise ValueError(
            "Archived rows would collide with rows loaded since the repair"
        )
    cursor.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
        f"SELECT {', '.join(COLUMNS)} FROM {BACKUP} "
        "WHERE fingerprint = %s AND rolled_back_at IS NULL",
        (fingerprint,),
    )
    restored = cursor.rowcount
    if restored != entry[1]:
        raise ValueError(f"Restored {restored} rows; the repair removed {entry[1]}")
    cursor.execute(
        f"UPDATE {BACKUP} SET rolled_back_at = now() WHERE fingerprint = %s",
        (fingerprint,),
    )
    cursor.execute(
        f"UPDATE {LEDGER} SET state = 'rolled_back', rolled_back_at = now() "
        "WHERE fingerprint = %s",
        (fingerprint,),
    )
    return {"restored": restored, "fingerprint": fingerprint}


def pinned_dsn(environment, allow_production):
    """Fail before connecting unless the owner wrapper pinned this target.

    Same rules as repair_entsoe_coal.checked_dsn.
    """
    if environment not in {"staging", "production"}:
        raise ValueError("Run through run_in_environment.py")
    if environment == "production" and not allow_production:
        raise ValueError("Production access requires --allow-production")
    config = json.loads((ROOT / "config/environments.json").read_text())
    target = config["environments"][environment]
    if (
        os.environ.get("NEON_ENDPOINT_ID") != target["endpoint_id"]
        or os.environ.get("NEON_BRANCH_ID") != target["branch_id"]
    ):
        raise ValueError("Use the pinned environment owner wrapper")
    dsn = os.environ.get("DATABASE_URL", "")
    parsed = parse_dsn(dsn)
    if (
        parsed.get("host") != f"{target['endpoint_id']}.{config['proxy_host']}"
        or parsed.get("user") != config["roles"]["owner"]
        or parsed.get("dbname") != config["database"]
        or parsed.get("port") != "5432"
        or parsed.get("sslmode") not in {"require", "verify-full"}
        or any(k in parsed for k in ("hostaddr", "service"))
        or any(os.environ.get(k) for k in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"))
    ):
        raise ValueError("Expected the pinned direct owner connection")
    return dsn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["plan", "apply", "rollback"])
    parser.add_argument("--source-dir", type=Path, help="audited Parquet files (plan)")
    parser.add_argument("--plan-file", type=Path, help="required for plan and apply")
    parser.add_argument("--fingerprint", help="required for apply and rollback")
    parser.add_argument("--allow-production", action="store_true")
    args = parser.parse_args()
    if args.command != "plan" and not args.fingerprint:
        parser.error("--fingerprint is required")
    if args.command != "rollback" and not args.plan_file:
        parser.error("--plan-file is required")
    if args.command == "plan" and not args.source_dir:
        parser.error("--source-dir is required for plan")
    environment = os.environ.get("ETL_ENVIRONMENT")
    dsn = pinned_dsn(environment, args.allow_production)
    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            if args.command == "plan":
                cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            if args.command == "plan":
                result = plan(cursor, args.source_dir)
                args.plan_file.write_text(json.dumps(result, indent=2) + "\n")
                result = {
                    k: v for k, v in result.items() if k not in {"deletions", "reasons"}
                }
            elif args.command == "apply":
                result = apply(
                    cursor, json.loads(args.plan_file.read_text()), args.fingerprint
                )
            else:
                result = rollback(cursor, args.fingerprint)
    print(json.dumps({"environment": environment, **result}, indent=2))


if __name__ == "__main__":
    main()
