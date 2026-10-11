"""Plan/apply/roll back removal of ONS rows left behind by plant renames.

The ONS loader keyed rows on (hour, plant name, ONS ID). When ONS republished
2026 files with changed names (capitalisation in January; a new upper-case
format from May), the weekly load inserted the renamed rows beside the old
ones, doubling those plant-hours (May 2026 coal stored twice).

plan    Read-only. Finds every (hour, ONS ID) stored more than once, and keeps
        the row whose name matches the audited ONS source file for that hour;
        every other copy is listed with its full before-image. Fails if a
        duplicate falls outside the audited files or no copy (or more than one)
        matches. Writes a plan file with a fingerprint.
apply   Under a table lock, re-checks every listed row is unchanged, archives
        it to ingestion.ons_rename_duplicate_backup, deletes it, and verifies
        no duplicate plant-hour remains. Needs the plan's fingerprint.
rollback Restores the archived rows of one fingerprint. Refuses while
        migration 020's plant-hour key exists (drop it first).

Use through run_in_environment.py. Production also requires
--allow-production. The plan step reads Parquet, so run it with
`uv run --with pyarrow`. Views are not refreshed here.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/repairs/ons-rename-duplicates-2026.json"
TABLE = "ingestion.ons_generation_data"
BACKUP = "ingestion.ons_rename_duplicate_backup"
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


def source_names(source_dir):
    """(timestamp_ms, ONS ID) -> plant name, from the audited source files."""
    import pandas as pd

    manifest = json.loads(MANIFEST.read_text())
    names, months = {}, set()
    for item in manifest["source_files"]:
        path = source_dir / item["url"].rsplit("/", 1)[1]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"{path.name} differs from the audited source file")
        df = pd.read_parquet(path, columns=["din_instante", "id_ons", "nom_usina"])
        df = df[df["id_ons"].notna() & (df["id_ons"] != "")]
        stamps = pd.to_datetime(df["din_instante"]).values.astype("datetime64[ms]")
        for stamp, plant_id, name in zip(
            stamps.astype("int64").tolist(), df["id_ons"], df["nom_usina"]
        ):
            if names.setdefault((stamp, plant_id), name) != name:
                raise ValueError(f"Source names {plant_id} twice at {stamp}")
        months.add(item["label"])
    return names, months


def month_of(timestamp_ms):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).strftime("%Y-%m")


def plan(cursor, source_dir):
    names, months = source_names(source_dir)
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
    summary = {}
    for row in deletions:
        bucket = summary.setdefault(
            f"{month_of(row['timestamp_ms'])} {row['fuel_type']}", [0, 0.0]
        )
        bucket[0] += 1
        bucket[1] += row["generation_mwh"]
    return {
        "repair": "ons-rename-duplicates-2026",
        "duplicate_plant_hours": len(groups),
        "rows_to_delete": len(deletions),
        "summary": {
            k: {"rows": v[0], "mwh": round(v[1], 3)} for k, v in sorted(summary.items())
        },
        "fingerprint": digest(deletions),
        "deletions": deletions,
    }


def apply(cursor, planned, fingerprint):
    if (
        digest(planned["deletions"]) != fingerprint
        or planned["fingerprint"] != fingerprint
    ):
        raise ValueError("Plan does not match the fingerprint")
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
    rows = planned["deletions"]
    ids = [row["id"] for row in rows]
    cursor.execute(
        f"SELECT {', '.join(COLUMNS)} FROM {TABLE} WHERE id = ANY(%s) ORDER BY id",
        (ids,),
    )
    current = [as_record(r) for r in cursor.fetchall()]
    if current != sorted(rows, key=lambda r: r["id"]):
        raise ValueError("Rows changed since the plan; re-plan")
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP} "
        f"(LIKE {TABLE} INCLUDING DEFAULTS, fingerprint TEXT NOT NULL, "
        "archived_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    execute_values(
        cursor,
        f"INSERT INTO {BACKUP} ({', '.join(COLUMNS)}, fingerprint) VALUES %s",
        [tuple(row[c] for c in COLUMNS) + (fingerprint,) for row in rows],
    )
    cursor.execute(f"DELETE FROM {TABLE} WHERE id = ANY(%s)", (ids,))
    if cursor.rowcount != len(rows):
        raise ValueError("Deleted row count differs from the plan")
    cursor.execute(f"SELECT count(*) FROM ({DUPLICATES}) d")
    if cursor.fetchone()[0]:
        raise ValueError("Duplicate plant-hours remain after the repair")
    return {"deleted": len(rows), "fingerprint": fingerprint}


def rollback(cursor, fingerprint):
    cursor.execute("SELECT to_regclass('ingestion.uq_ons_plant_hour')")
    if cursor.fetchone()[0]:
        raise ValueError("Drop migration 020's uq_ons_plant_hour before rolling back")
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
    cursor.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
        f"SELECT {', '.join(COLUMNS)} FROM {BACKUP} WHERE fingerprint = %s",
        (fingerprint,),
    )
    restored = cursor.rowcount
    if not restored:
        raise ValueError("No archived rows for this fingerprint")
    cursor.execute(f"DELETE FROM {BACKUP} WHERE fingerprint = %s", (fingerprint,))
    return {"restored": restored, "fingerprint": fingerprint}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["plan", "apply", "rollback"])
    parser.add_argument("--source-dir", type=Path, help="audited Parquet files (plan)")
    parser.add_argument("--plan-file", type=Path, help="required for plan and apply")
    parser.add_argument("--fingerprint", help="required for apply and rollback")
    parser.add_argument("--allow-production", action="store_true")
    args = parser.parse_args()
    environment = os.environ.get("ETL_ENVIRONMENT")
    if environment not in {"staging", "production"}:
        parser.error("Run through run_in_environment.py")
    if (
        environment == "production"
        and args.command != "plan"
        and not args.allow_production
    ):
        parser.error("Production apply/rollback requires --allow-production")
    if args.command != "plan" and not args.fingerprint:
        parser.error("--fingerprint is required")
    if args.command != "rollback" and not args.plan_file:
        parser.error("--plan-file is required")
    with psycopg2.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            if args.command == "plan":
                cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            if args.command == "plan":
                if not args.source_dir:
                    parser.error("--source-dir is required for plan")
                result = plan(cursor, args.source_dir)
                args.plan_file.write_text(json.dumps(result, indent=2) + "\n")
                result = {k: v for k, v in result.items() if k != "deletions"}
            elif args.command == "apply":
                result = apply(
                    cursor, json.loads(args.plan_file.read_text()), args.fingerprint
                )
            else:
                result = rollback(cursor, args.fingerprint)
    print(json.dumps({"environment": environment, **result}, indent=2))


if __name__ == "__main__":
    main()
