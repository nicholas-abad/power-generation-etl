"""Plan/apply/roll back moving stored ENTSO-E rows to their real fuel (count_as_other_fuel).

A count_as_other_fuel record in config/entsoe-fuel-exceptions.json says that a
unit's rows ENTSO-E labels coal are another fuel in a range of months, e.g.
Studstrup 3 (DK_CA, EIC 45W000000000036L) labelled B05 hard coal after its
conversion to biomass is B01 "Biomass". Every load moves new rows
(src/database.py); this tool moves the rows already stored, which a weekly
load never re-fetches. Example: a stored row (DK_CA, B05, "Fossil Hard coal",
2024-03-01 00:00, 120 MW) becomes (DK_CA, B01, "Biomass", same time, 120 MW),
same id.

plan     Read-only. Lists every stored row a record matches (area, source PSR,
         unit EIC, or one of the record's reviewed names for a row without an
         EIC, UTC month in the record's window) as full row images, with count
         and MWh per record and month, and binds them into plan_sha256.
         Refuses if a row would land on a stored row (same natural key under
         the counted PSR, or the same unit and time under another PSR).
apply    Needs --expect-plan-sha256. Under a table lock it rebuilds the plan
         and refuses unless it has that hash. Records the repair in
         ingestion.entsoe_fuel_move_repairs, archives every row image in
         ingestion.entsoe_fuel_move_backup, sets psr_type and fuel_type, and
         checks that coal fell and the counted fuels rose by the planned MWh
         and that no stored row matches a record any more.
rollback Needs --expect-plan-sha256. Sets the archived rows of that applied plan
         back, refusing if a moved row changed since (e.g. a later load
         rewrote it); such a rollback needs a new review.

A refused command commits nothing. Run through run_in_environment.py with the
owner role; production also needs --allow-production (for the plan too). Views
are not refreshed here.
"""

import argparse
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

import entsoe_fuel_exceptions
from entsoe_fuels import PSR_TO_FUEL_TYPE
from run_in_environment import checked_dsn

TABLE = "ingestion.entsoe_generation_data"
LEDGER = "ingestion.entsoe_fuel_move_repairs"
BACKUP = "ingestion.entsoe_fuel_move_backup"
ROWS = f"""
    SELECT to_jsonb(t) FROM {TABLE} t
    WHERE t.country_code = %(area)s AND t.psr_type = %(source_psr)s
        AND (t.unit_eic = %(unit_eic)s OR (t.unit_eic IS NULL AND t.plant_name = ANY(%(names)s)))
        AND t.timestamp_ms >= %(start_ms)s AND t.timestamp_ms < %(end_ms)s
    ORDER BY t.timestamp_ms, t.id
"""
# A moved row must not land on a stored row: the same natural key under the
# counted PSR, or (migration 018) the same unit and time under another PSR.
LANDINGS = f"""
    SELECT count(*) FROM {TABLE} t WHERE t.country_code = %(area)s AND t.id <> ALL(%(ids)s) AND (
        (t.psr_type = %(counted_psr)s AND (t.timestamp_ms, t.plant_name) IN (
            SELECT * FROM unnest(%(stamps)s::bigint[], %(names_of_rows)s::text[])))
        OR (t.unit_eic IS NOT NULL AND (t.unit_eic, t.timestamp_ms) IN (
            SELECT * FROM unnest(%(eics)s::text[], %(stamps)s::bigint[]))))
"""


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def month_start_ms(month):
    return int(datetime.strptime(month, "%Y-%m").replace(tzinfo=UTC).timestamp() * 1000)


def month_after(month):
    year, m = map(int, month.split("-"))
    return f"{year + m // 12}-{m % 12 + 1:02d}"


def window_ms(record):
    """[start, end) of the record's months in UTC milliseconds; open end: far future."""
    end = month_start_ms(month_after(record.to_month)) if record.to_month else 2**62
    return month_start_ms(record.from_month), end


def month_of(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).strftime("%Y-%m")


def mwh(row):
    return row["generation_mw"] * row["resolution_minutes"] / 60


def move_records(records=None):
    return [
        r
        for r in (entsoe_fuel_exceptions.exceptions() if records is None else records)
        if r.treatment == "count_as_other_fuel"
    ]


def build_plan(cursor, records, records_sha256, target):
    moves, per_record = [], {}
    for record in records:
        start_ms, end_ms = window_ms(record)
        params = {
            "area": record.area,
            "source_psr": record.source_psr,
            "unit_eic": record.unit_eic,
            "names": list(record.reviewed_names),
            "start_ms": start_ms,
            "end_ms": end_ms,
        }
        cursor.execute(ROWS, params)
        rows = [r for (r,) in cursor.fetchall()]
        if rows:
            cursor.execute(
                LANDINGS,
                {
                    "area": record.area,
                    "counted_psr": record.counted_psr,
                    "ids": [r["id"] for r in rows],
                    "stamps": [r["timestamp_ms"] for r in rows],
                    "names_of_rows": [r["plant_name"] for r in rows],
                    "eics": [r["unit_eic"] for r in rows],
                },
            )
            landed = cursor.fetchone()[0]
            if landed:
                raise ValueError(
                    f"{record.id}: {landed} stored rows already sit where moved rows would land; "
                    "ENTSO-E reports the unit under both fuels, review before moving"
                )
        months = {}
        for row in rows:
            moves.append({"exception": record.id, "row": row})
            months.setdefault(month_of(row["timestamp_ms"]), []).append(mwh(row))
        per_record[record.id] = {
            "area": record.area,
            "from": record.source_psr,
            "to": record.counted_psr,
            "rows": sum(map(len, months.values())),
            "mwh": round(math.fsum(sum(months.values(), [])), 3),
            "by_month": {
                month: {"rows": len(v), "mwh": round(math.fsum(v), 3)}
                for month, v in sorted(months.items())
            },
        }
    plan = {
        "records_sha256": records_sha256,
        "target": target,
        "rows_to_move": len(moves),
        "records": per_record,
        "rows_sha256": digest(moves),
    }
    plan["plan_sha256"] = digest(plan)
    return plan, moves


def create_archive(cursor):
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {LEDGER} ("
        "apply_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "plan_sha256 text NOT NULL, records_sha256 text NOT NULL, rows integer NOT NULL, "
        "state text NOT NULL CHECK (state IN ('applied', 'rolled_back')), "
        "applied_at timestamptz NOT NULL DEFAULT now(), rolled_back_at timestamptz)"
    )
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP} ("
        f"apply_id bigint NOT NULL REFERENCES {LEDGER}(apply_id), "
        "id bigint NOT NULL, exception_id text NOT NULL, counted_psr text NOT NULL, "
        "original_row jsonb NOT NULL, PRIMARY KEY (apply_id, id))"
    )
    # The archive is the only record of the original labels: owner only.
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
        f"SELECT apply_id, rows FROM {LEDGER} WHERE plan_sha256 = %s AND state = 'applied' FOR UPDATE",
        (plan_sha256,),
    )
    return cursor.fetchone()


def fuel_mwh(cursor, records):
    """MWh per (area, psr) for the records' source and counted PSRs, whole table."""
    pairs = sorted(
        {(r.area, p) for r in records for p in (r.source_psr, r.counted_psr)}
    )
    cursor.execute(
        f"SELECT country_code, psr_type, COALESCE(sum(generation_mw * resolution_minutes / 60.0), 0) "
        f"FROM {TABLE} WHERE (country_code, psr_type) IN (SELECT * FROM unnest(%s::text[], %s::text[])) "
        "GROUP BY 1, 2",
        ([a for a, _ in pairs], [p for _, p in pairs]),
    )
    found = {(a, p): float(v) for a, p, v in cursor.fetchall()}
    return {pair: found.get(pair, 0.0) for pair in pairs}


def apply(cursor, records, plan, moves):
    if not moves:
        raise ValueError(
            "Nothing to move: no stored row matches a count_as_other_fuel record"
        )
    create_archive(cursor)
    if applied_entry(cursor, plan["plan_sha256"]):
        raise ValueError("This plan is already applied")
    before = fuel_mwh(cursor, records)
    cursor.execute(
        f"INSERT INTO {LEDGER} (plan_sha256, records_sha256, rows, state) "
        "VALUES (%s, %s, %s, 'applied') RETURNING apply_id",
        (plan["plan_sha256"], plan["records_sha256"], len(moves)),
    )
    apply_id = cursor.fetchone()[0]
    counted = {r.id: r.counted_psr for r in records}
    execute_values(
        cursor,
        f"INSERT INTO {BACKUP} (apply_id, id, exception_id, counted_psr, original_row) VALUES %s",
        [
            (
                apply_id,
                m["row"]["id"],
                m["exception"],
                counted[m["exception"]],
                Json(m["row"]),
            )
            for m in moves
        ],
        page_size=5000,
    )
    cursor.execute(
        f"UPDATE {TABLE} a SET psr_type = b.counted_psr, fuel_type = f.fuel FROM {BACKUP} b "
        "JOIN unnest(%s::text[], %s::text[]) AS f(psr, fuel) ON f.psr = b.counted_psr "
        "WHERE b.apply_id = %s AND a.id = b.id AND to_jsonb(a) = b.original_row",
        (list(PSR_TO_FUEL_TYPE), list(PSR_TO_FUEL_TYPE.values()), apply_id),
    )
    if cursor.rowcount != len(moves):
        raise ValueError("Moved row count differs from the plan")
    after = fuel_mwh(cursor, records)
    expected = dict.fromkeys(before, 0.0)
    for m in moves:
        record = next(r for r in records if r.id == m["exception"])
        expected[(record.area, record.source_psr)] -= mwh(m["row"])
        expected[(record.area, record.counted_psr)] += mwh(m["row"])
    for pair, change in expected.items():
        if not math.isclose(after[pair] - before[pair], change, abs_tol=0.01):
            raise ValueError(
                f"{pair}: MWh changed by {after[pair] - before[pair]:.3f}, planned {change:.3f}"
            )
    remaining, _ = build_plan(cursor, records, plan["records_sha256"], plan["target"])
    if remaining["rows_to_move"]:
        raise ValueError(
            f"{remaining['rows_to_move']} stored rows still match a record after the move"
        )
    return {"status": "applied", "moved": len(moves)}


def rollback(cursor, plan_sha256):
    entry = applied_entry(cursor, plan_sha256)
    if entry is None:
        raise ValueError("No applied repair has this plan fingerprint")
    apply_id, rows = entry
    # Each moved row must still be exactly its original image with the new label.
    cursor.execute(
        f"UPDATE {TABLE} a SET psr_type = b.original_row->>'psr_type', "
        "fuel_type = b.original_row->>'fuel_type' "
        f"FROM {BACKUP} b WHERE b.apply_id = %s AND a.id = b.id AND a.psr_type = b.counted_psr "
        "AND to_jsonb(a) - 'psr_type' - 'fuel_type' = b.original_row - 'psr_type' - 'fuel_type'",
        (apply_id,),
    )
    if cursor.rowcount != rows:
        raise ValueError(
            f"Only {cursor.rowcount} of {rows} moved rows are unchanged since the apply; "
            "rollback requires a new review"
        )
    cursor.execute(
        f"UPDATE {LEDGER} SET state = 'rolled_back', rolled_back_at = now() WHERE apply_id = %s",
        (apply_id,),
    )
    return {"status": "rolled_back", "restored": rows}


def run(connection, command, target, expected_plan=None, records=None):
    """One transaction per command; an error leaves the database as it was.

    Returns (report, moves); moves is None for a rollback.
    """
    records = move_records(records)
    records_sha256 = entsoe_fuel_exceptions.sha256()
    previous = connection.readonly, connection.isolation_level, connection.autocommit
    connection.set_session(
        readonly=command == "plan", isolation_level="REPEATABLE READ", autocommit=False
    )
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '300s'")
            if command == "plan":
                return build_plan(cursor, records, records_sha256, target)
            cursor.execute("SET LOCAL lock_timeout = '10s'")
            cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
            if command == "rollback":
                return {
                    "plan_sha256": expected_plan,
                    "target": target,
                    **rollback(cursor, expected_plan),
                }, None
            plan, moves = build_plan(cursor, records, records_sha256, target)
            if plan["plan_sha256"] != expected_plan:
                raise ValueError(
                    f"Rows or records changed since the reviewed plan (now {plan['plan_sha256']}); re-plan"
                )
            return {**plan, **apply(cursor, records, plan, moves)}, moves
    finally:
        connection.rollback()
        connection.readonly, connection.isolation_level = previous[:2]
        connection.autocommit = previous[2]


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
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
            report, moves = run(
                connection, args.command, target, args.expect_plan_sha256
            )
        finally:
            connection.close()
        if args.command == "plan":
            args.plan_file.parent.mkdir(parents=True, exist_ok=True)
            args.plan_file.write_text(
                json.dumps({**report, "moves": moves}, indent=2) + "\n"
            )
        print(json.dumps(report, indent=2))
        return 0
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection errors can contain credentials; do not echo DSNs.
        print(str(error) if isinstance(error, ValueError) else type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
