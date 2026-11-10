"""Plan/apply/roll back removal of ENTSO-E units stored twice under two names.

The natural key of ingestion.entsoe_generation_data includes plant_name, so one
source series loaded under two spellings of its name (an older extractor's
shortened label and the exact source name) is stored twice for the same
intervals, and every total over those intervals counts the unit twice. The
reviewed manifest config/repairs/entsoe-source-duplicates-2025.json names each
case: country, psr_type, the name to keep, the name to remove, a half-open UTC
range and the source evidence that both names are one unit.

plan     Read-only. Lists every row of the removed name in a case's range with
         its kept twin (same country, psr_type and timestamp under the kept
         name), both as full row images, and binds them into plan_sha256.
         Refuses if a row has no twin, or the twin differs in generation_mw,
         resolution_minutes or fuel_type, or the two carry different unit
         EICs. Writes a plan file.
apply    Needs --expect-plan-sha256. Under a table lock it rebuilds the plan
         and refuses unless it has that hash, so a changed or missing row to
         delete or kept twin stops it. Records the repair in
         ingestion.entsoe_source_duplicate_repairs, archives both images of
         every pair in ingestion.entsoe_source_duplicate_backup, deletes the
         rows, and verifies that exactly the kept row remains per interval.
rollback Needs --expect-plan-sha256. Restores the archived rows of that applied
         plan with their original ids. Refuses if a kept twin changed or
         disappeared since the apply, or a restored row would collide.

A refused command commits nothing. Run through run_in_environment.py with the
owner role; production also needs --allow-production (for the plan too). Rows
outside the manifest are never touched, and views are not refreshed here.
"""

import argparse
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

from run_in_environment import checked_dsn

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/repairs/entsoe-source-duplicates-2025.json"
TABLE = "ingestion.entsoe_generation_data"
LEDGER = "ingestion.entsoe_source_duplicate_repairs"
BACKUP = "ingestion.entsoe_source_duplicate_backup"
TWIN_COLUMNS = ("generation_mw", "resolution_minutes", "fuel_type")
PAIRS = f"""
    SELECT to_jsonb(r), to_jsonb(k) FROM {TABLE} r
    LEFT JOIN {TABLE} k ON k.country_code = r.country_code
        AND k.psr_type = r.psr_type AND k.timestamp_ms = r.timestamp_ms
        AND k.plant_name = %(keep)s
    WHERE r.country_code = %(country_code)s AND r.psr_type = %(psr_type)s
        AND r.plant_name = %(remove)s
        AND r.timestamp_ms >= %(start_ms)s AND r.timestamp_ms < %(end_ms)s
    ORDER BY r.timestamp_ms
"""


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def utc_ms(text):
    if not text.endswith("Z"):
        raise ValueError(f"Manifest ranges are UTC and end in Z: {text}")
    return int(datetime.fromisoformat(text[:-1]).replace(tzinfo=UTC).timestamp() * 1000)


def load_manifest(path=None):
    """The reviewed cases with their ranges in milliseconds, and the file's hash."""
    data = (path or MANIFEST).read_bytes()
    manifest = json.loads(data)
    cases = manifest["cases"]
    if not cases or len({case["id"] for case in cases}) != len(cases):
        raise ValueError("Manifest needs uniquely named cases")
    for case in cases:
        case["start_ms"], case["end_ms"] = (
            utc_ms(case["start_utc"]),
            utc_ms(case["end_utc"]),
        )
        if case["start_ms"] >= case["end_ms"] or case["keep"] == case["remove"]:
            raise ValueError(f"Case {case['id']} needs a range and two names")
        if not case["source"]["xml_sha256"]:
            raise ValueError(f"Case {case['id']} names no source evidence")
    return manifest, hashlib.sha256(data).hexdigest()


def month_of(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).strftime("%Y-%m")


def check_twin(case, row, twin):
    """The removed row may go only if the kept row carries the same observation."""
    where = f"{case['id']} at {row['timestamp_ms']}"
    if twin is None:
        raise ValueError(f"{where}: no row under the kept name {case['keep']}")
    for column in TWIN_COLUMNS:
        if row[column] != twin[column]:
            raise ValueError(
                f"{where}: {column} differs ({row[column]} removed, {twin[column]} kept)"
            )
    if "Actual Consumption" in (row["data_type"], twin["data_type"]):
        raise ValueError(f"{where}: a consumption row is not a generation duplicate")
    eics = {r["unit_eic"] for r in (row, twin)} - {None}
    if eics - {case["source"]["unit_eic"]}:
        raise ValueError(f"{where}: unit EIC {sorted(eics)} is not the reviewed unit")


def build_plan(cursor, manifest, manifest_sha256, target):
    pairs, cases = [], {}
    for case in manifest["cases"]:
        cursor.execute(PAIRS, case)
        months = {}
        for row, twin in cursor.fetchall():
            check_twin(case, row, twin)
            pairs.append({"case": case["id"], "delete": row, "keep": twin})
            months.setdefault(month_of(row["timestamp_ms"]), []).append(
                row["generation_mw"] * row["resolution_minutes"] / 60
            )
        cases[case["id"]] = {
            "keep": case["keep"],
            "remove": case["remove"],
            "rows_to_delete": sum(map(len, months.values())),
            "mwh_counted_twice": round(math.fsum(sum(months.values(), [])), 3),
            "by_month": {
                month: {"rows": len(mwh), "mwh": round(math.fsum(mwh), 3)}
                for month, mwh in sorted(months.items())
            },
        }
    plan = {
        "repair_id": manifest["repair_id"],
        "manifest_sha256": manifest_sha256,
        "target": target,
        "rows_to_delete": len(pairs),
        "cases": cases,
        "pairs_sha256": digest(pairs),
    }
    plan["plan_sha256"] = digest(plan)
    return plan, pairs


def create_archive(cursor):
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {LEDGER} ("
        "apply_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "plan_sha256 text NOT NULL, repair_id text NOT NULL, "
        "manifest_sha256 text NOT NULL, rows integer NOT NULL, "
        "state text NOT NULL CHECK (state IN ('applied', 'rolled_back')), "
        "applied_at timestamptz NOT NULL DEFAULT now(), rolled_back_at timestamptz)"
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP} ("
        f"apply_id bigint NOT NULL REFERENCES {LEDGER}(apply_id), "
        "id bigint NOT NULL, case_id text NOT NULL, deleted_row jsonb NOT NULL, "
        "kept_row jsonb NOT NULL, PRIMARY KEY (apply_id, id))"
    )
    # The archive is the only copy of removed rows: owner only. Neon default
    # privileges can grant DML on new tables; remove those grants.
    cursor.execute(f"REVOKE ALL ON {LEDGER}, {BACKUP} FROM PUBLIC")
    for role in ("etl_writer", "dashboard_ro"):
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cursor.fetchone():
            cursor.execute(f"REVOKE ALL ON {LEDGER}, {BACKUP} FROM {role}")


def applied_entry(cursor, plan_sha256):
    """The ledger row of this plan while it is applied, locked; else None."""
    cursor.execute("SELECT to_regclass(%s)", (LEDGER,))
    if cursor.fetchone()[0] is None:
        return None
    cursor.execute(
        f"SELECT apply_id, rows FROM {LEDGER} "
        "WHERE plan_sha256 = %s AND state = 'applied' FOR UPDATE",
        (plan_sha256,),
    )
    return cursor.fetchone()


def apply(cursor, manifest, plan, pairs):
    if not pairs:
        raise ValueError("Nothing to remove: no row of a removed name is in range")
    create_archive(cursor)
    if applied_entry(cursor, plan["plan_sha256"]):
        raise ValueError("This plan is already applied")
    cursor.execute(
        f"INSERT INTO {LEDGER} (plan_sha256, repair_id, manifest_sha256, rows, state) "
        "VALUES (%s, %s, %s, %s, 'applied') RETURNING apply_id",
        (
            plan["plan_sha256"],
            plan["repair_id"],
            plan["manifest_sha256"],
            len(pairs),
        ),
    )
    apply_id = cursor.fetchone()[0]
    execute_values(
        cursor,
        f"INSERT INTO {BACKUP} (apply_id, id, case_id, deleted_row, kept_row) VALUES %s",
        [
            (apply_id, p["delete"]["id"], p["case"], Json(p["delete"]), Json(p["keep"]))
            for p in pairs
        ],
        page_size=5000,
    )
    cursor.execute(
        f"DELETE FROM {TABLE} a USING {BACKUP} b "
        "WHERE b.apply_id = %s AND a.id = b.id AND to_jsonb(a) = b.deleted_row",
        (apply_id,),
    )
    if cursor.rowcount != len(pairs):
        raise ValueError("Deleted row count differs from the plan")
    for case in manifest["cases"]:
        # Every interval a row was deleted from holds its kept twin and no
        # other row under either name.
        cursor.execute(
            f"SELECT count(*), count(*) FILTER (WHERE to_jsonb(a) = b.kept_row) "
            f"FROM {BACKUP} b JOIN {TABLE} a ON a.country_code = %(country_code)s "
            "AND a.psr_type = %(psr_type)s AND a.plant_name IN (%(keep)s, %(remove)s) "
            "AND a.timestamp_ms = (b.deleted_row->>'timestamp_ms')::bigint "
            "WHERE b.apply_id = %(apply_id)s AND b.case_id = %(id)s",
            {**case, "apply_id": apply_id},
        )
        expected = plan["cases"][case["id"]]["rows_to_delete"]
        if cursor.fetchone() != (expected, expected):
            raise ValueError(f"{case['id']}: not exactly the kept row per interval")
    return {"status": "applied", "deleted": len(pairs)}


def rollback(cursor, plan_sha256):
    entry = applied_entry(cursor, plan_sha256)
    if entry is None:
        raise ValueError("No applied repair has this plan fingerprint")
    apply_id, rows = entry
    cursor.execute(
        f"SELECT count(*) FROM {BACKUP} b WHERE b.apply_id = %s AND NOT EXISTS ("
        f"SELECT 1 FROM {TABLE} a WHERE a.id = (b.kept_row->>'id')::bigint "
        "AND to_jsonb(a) = b.kept_row)",
        (apply_id,),
    )
    if cursor.fetchone()[0]:
        raise ValueError(
            "A kept twin changed or disappeared since the apply; "
            "rollback requires a new review"
        )
    for collision in (
        "a.id = b.id",
        "a.timestamp_ms = (b.deleted_row->>'timestamp_ms')::bigint "
        "AND a.country_code = b.deleted_row->>'country_code' "
        "AND a.psr_type = b.deleted_row->>'psr_type' "
        "AND a.plant_name = b.deleted_row->>'plant_name'",
    ):
        cursor.execute(
            f"SELECT count(*) FROM {BACKUP} b JOIN {TABLE} a ON {collision} "
            "WHERE b.apply_id = %s",
            (apply_id,),
        )
        if cursor.fetchone()[0]:
            raise ValueError(
                "Archived rows would collide with rows loaded since the apply"
            )
    cursor.execute(
        f"INSERT INTO {TABLE} "
        f"SELECT (jsonb_populate_record(NULL::{TABLE}, b.deleted_row)).* "
        f"FROM {BACKUP} b WHERE b.apply_id = %s",
        (apply_id,),
    )
    restored = cursor.rowcount
    cursor.execute(
        f"SELECT count(*) FROM {BACKUP} b JOIN {TABLE} a ON a.id = b.id "
        "AND to_jsonb(a) = b.deleted_row WHERE b.apply_id = %s",
        (apply_id,),
    )
    if not restored == rows == cursor.fetchone()[0]:
        raise ValueError(f"Restored {restored} rows; the repair removed {rows}")
    cursor.execute(
        f"UPDATE {LEDGER} SET state = 'rolled_back', rolled_back_at = now() "
        "WHERE apply_id = %s",
        (apply_id,),
    )
    return {"status": "rolled_back", "restored": restored}


def run(connection, command, target, expected_plan=None, manifest_path=None):
    """One transaction per command; an error leaves the database as it was.

    Returns (report, pairs); pairs is None for a rollback.
    """
    manifest, manifest_sha256 = load_manifest(manifest_path)
    previous = connection.readonly, connection.isolation_level, connection.autocommit
    connection.set_session(
        readonly=command == "plan", isolation_level="REPEATABLE READ", autocommit=False
    )
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '300s'")
            if command == "plan":
                return build_plan(cursor, manifest, manifest_sha256, target)
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
            if command == "rollback":
                result = rollback(cursor, expected_plan)
                return {"plan_sha256": expected_plan, "target": target, **result}, None
            plan, pairs = build_plan(cursor, manifest, manifest_sha256, target)
            if plan["plan_sha256"] != expected_plan:
                raise ValueError(
                    "Rows or manifest changed since the reviewed plan "
                    f"(now {plan['plan_sha256']}); re-plan"
                )
            return {**plan, **apply(cursor, manifest, plan, pairs)}, pairs
    finally:
        connection.rollback()
        # Assigning None restores the server default; set_session would keep it.
        connection.readonly, connection.isolation_level = previous[:2]
        connection.autocommit = previous[2]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "apply", "rollback"))
    parser.add_argument(
        "--environment", choices=("staging", "production"), required=True
    )
    parser.add_argument("--allow-production", action="store_true")
    parser.add_argument("--plan-file", type=Path, help="written by plan (required)")
    parser.add_argument("--expect-plan-sha256", help="required for apply and rollback")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan" and not args.plan_file:
            raise ValueError("plan needs --plan-file")
        if args.command != "plan" and not args.expect_plan_sha256:
            raise ValueError("A reviewed --expect-plan-sha256 is required")
        dsn, target = checked_dsn(args.environment, args.allow_production)
        connection = psycopg2.connect(dsn, connect_timeout=15)
        try:
            report, pairs = run(
                connection, args.command, target, args.expect_plan_sha256
            )
        finally:
            connection.close()
        if args.command == "plan":
            args.plan_file.parent.mkdir(parents=True, exist_ok=True)
            args.plan_file.write_text(
                json.dumps({**report, "pairs": pairs}, indent=2) + "\n"
            )
        print(json.dumps(report, indent=2))
        return 0
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection errors can contain credentials; do not echo DSNs.
        print(str(error) if isinstance(error, ValueError) else type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
