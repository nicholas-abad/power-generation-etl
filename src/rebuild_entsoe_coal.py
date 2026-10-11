"""Plan, apply and roll back a certified ENTSO-E coal rebuild for one country.

Replaces a country's stored coal rows (the manifest's PSRs) with the payload
built from its certified source archive (scripts/build_entsoe_coal_payload.py),
one month per transaction, with backups and an exact rollback. Design: plan
step 4a/4b in ~/.claude/plans/entsoe-interval-fix-all-countries.md.

- plan (default, writes nothing persistent): per window, the stored rows'
  count, MWh and server-side hash; blocking findings (stored rows in coal PSRs
  outside the manifest, unit/time key collisions with retained rows). The
  report's plan_sha256 must be passed to apply.
- apply: per window, under the table lock, require the planned stored hash,
  move the stored rows to the backup, insert the payload, and prove before
  commit that the window equals the payload row for row. Resumable; bound to
  one manifest and plan per repair id.
- rollback: per window, under the same lock, only while nothing has written to
  the window since apply; restores the original rows and ids.
Views are refreshed after apply and rollback. Production requires
--allow-production and the pinned owner wrapper.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal as D
import gzip
import hashlib
import io
import json
from pathlib import Path

import psycopg2

from repair_entsoe_coal import checked_dsn

TABLE = "ingestion.entsoe_generation_data"
HEADER = "ingestion.entsoe_coal_rebuilds"
MONTHS = "ingestion.entsoe_coal_rebuild_months"
BACKUP = "ingestion.entsoe_coal_rebuild_backup"
COLUMNS = (
    "timestamp_ms",
    "country_code",
    "psr_type",
    "plant_name",
    "fuel_type",
    "data_type",
    "generation_mw",
    "resolution_minutes",
    "unit_eic",
    "production_unit_eic",
    "source_unit_name",
    "extraction_run_id",
    "created_at_ms",
)
VIEWS = ("mv_entsoe_plant_monthly", "mv_entsoe_unit_monthly", "mv_entsoe_row_counts")
LINE = (
    "concat_ws(E'\\t', "
    + ", ".join(f"coalesce({c}::text, '\\N')" for c in COLUMNS)
    + ")"
)
ORDER = (
    'timestamp_ms, psr_type COLLATE "C", plant_name COLLATE "C", unit_eic COLLATE "C"'
)
HASH = (
    "encode(sha256(convert_to(coalesce(string_agg({line}, E'\\n' ORDER BY {order}), ''),"
    " 'UTF8')), 'hex')"
).format(line=LINE, order=ORDER)
# Exact decimal energy: float8 text is the shortest round-trip form, i.e. the
# source decimal value the payload was built from.
MWH = "coalesce(sum(generation_mw::text::numeric * resolution_minutes / 60), 0)"
COLS = ", ".join(COLUMNS)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def month_ms(month):
    year, number = map(int, month.split("-"))
    return int(datetime(year, number, 1, tzinfo=timezone.utc).timestamp() * 1000)


def load_payload(payload_dir):
    """The manifest, its windows and each month's verified payload lines."""
    path = payload_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    require(
        tuple(manifest["columns"]) == COLUMNS, "Payload columns differ from the tool"
    )
    months = manifest["months"]
    require(months, "Payload has no months")
    names = [m["month"] for m in months]
    require(
        names == sorted(names) and len(set(names)) == len(names),
        "Payload months are not ordered and unique",
    )
    expected = []
    year, number = map(int, manifest["start"][:7].split("-"))
    end_month = manifest["end"][:7]
    while f"{year:04d}-{number:02d}" < end_month:
        expected.append(f"{year:04d}-{number:02d}")
        year, number = (year + 1, 1) if number == 12 else (year, number + 1)
    require(names == expected, "Payload months do not cover the certified window")
    require(manifest["end"][8:10] == "01", "Certified end must be a month boundary")
    windows = []
    for i, month in enumerate(months):
        data = (payload_dir / month["file"]).read_bytes()
        require(
            hashlib.sha256(data).hexdigest() == month["sha256"],
            f"Payload file {month['file']} changed",
        )
        lines = gzip.decompress(data).decode().split("\n")
        require(
            lines[0] == "\t".join(COLUMNS) and lines[-1] == "",
            f"Malformed payload {month['file']}",
        )
        body = lines[1:-1]
        require(len(body) == month["rows"], f"{month['month']}: row count differs")
        end = month_ms(names[i + 1]) if i + 1 < len(months) else month_ms(end_month)
        windows.append(
            {
                "month": month["month"],
                "start": month_ms(month["month"]),
                "end": end,
                "rows": month["rows"],
                "mwh": month["mwh"],
                "payload_sha256": month["sha256"],
                "lines_sha256": hashlib.sha256("\n".join(body).encode()).hexdigest(),
                "lines": body,
            }
        )
    # From the certified end on, stored rows are deleted and nothing is inserted;
    # the resumed weekly job refills it (plan 4b.1).
    windows.append(
        {
            "month": f"{end_month}-tail",
            "start": month_ms(end_month),
            "end": None,
            "rows": 0,
            "mwh": "0",
            "payload_sha256": None,
            "lines_sha256": hashlib.sha256(b"").hexdigest(),
            "lines": [],
        }
    )
    return manifest, hashlib.sha256(path.read_bytes()).hexdigest(), windows


def scope(manifest, window):
    """Parameters for the window predicate (see WHERE)."""
    return (
        manifest["country"],
        manifest["psrs"],
        window["start"],
        window["end"],
        window["end"],
    )


WHERE = (
    "country_code = %s AND psr_type = ANY(%s) AND timestamp_ms >= %s"
    " AND (%s::bigint IS NULL OR timestamp_ms < %s)"
)


def preflight(cursor):
    cursor.execute("SELECT to_regclass('ingestion.schema_migrations') IS NOT NULL")
    require(cursor.fetchone()[0], "ingestion.schema_migrations is missing")
    cursor.execute("SELECT 1 FROM ingestion.schema_migrations WHERE version='018'")
    require(cursor.fetchone(), "Migration 018 (unit identifiers) is not applied")
    cursor.execute(
        "SELECT array_agg(column_name::text ORDER BY column_name) FROM information_schema.columns"
        " WHERE table_schema='ingestion' AND table_name='entsoe_generation_data'"
    )
    require(
        sorted(cursor.fetchone()[0]) == sorted(("id",) + COLUMNS),
        "entsoe_generation_data does not have the expected columns",
    )


def stage(cursor, window):
    """Load a window's payload into a temp table and prove it matches the file."""
    cursor.execute("DROP TABLE IF EXISTS _rebuild_payload")
    cursor.execute(
        f"CREATE TEMP TABLE _rebuild_payload AS SELECT {COLS} FROM {TABLE} WITH NO DATA"
    )
    if window["lines"]:
        cursor.copy_expert(
            f"COPY _rebuild_payload ({COLS}) FROM STDIN WITH (FORMAT text)",
            io.StringIO("\n".join(window["lines"]) + "\n"),
        )
    cursor.execute(f"SELECT count(*), {HASH} FROM _rebuild_payload")
    count, staged = cursor.fetchone()
    require(count == window["rows"], f"{window['month']}: staged row count differs")
    require(
        staged == window["lines_sha256"],
        f"{window['month']}: staged rows differ from the payload file",
    )


def window_state(cursor, manifest, window):
    cursor.execute(
        f"SELECT count(*), {MWH}, {HASH} FROM {TABLE} WHERE {WHERE}",
        scope(manifest, window),
    )
    count, mwh, stored = cursor.fetchone()
    return {"rows": count, "mwh": format(D(mwh).normalize(), "f"), "sha256": stored}


def plan(connection, manifest, manifest_sha, windows, target):
    with connection:
        with connection.cursor() as cursor:
            preflight(cursor)
            country, psrs = manifest["country"], manifest["psrs"]
            cursor.execute(
                f"SELECT psr_type, count(*) FROM {TABLE} WHERE country_code=%s"
                " AND psr_type IN ('B02','B03','B05') AND NOT psr_type = ANY(%s)"
                " GROUP BY 1 ORDER BY 1",
                (country, psrs),
            )
            other_psrs = dict(cursor.fetchall())
            report_windows, collisions = [], []
            for window in windows:
                stage(cursor, window)
                state = window_state(cursor, manifest, window)
                # Unit/time keys of the payload held by rows this window keeps.
                cursor.execute(
                    f"SELECT count(*) FROM _rebuild_payload p JOIN {TABLE} t"
                    " ON t.country_code=p.country_code AND t.unit_eic=p.unit_eic"
                    " AND t.timestamp_ms=p.timestamp_ms"
                    f" WHERE NOT (t.psr_type = ANY(%s))",
                    (psrs,),
                )
                clash = cursor.fetchone()[0]
                if clash:
                    collisions.append({"month": window["month"], "rows": clash})
                report_windows.append(
                    {
                        "month": window["month"],
                        "start": window["start"],
                        "end": window["end"],
                        "stored_rows": state["rows"],
                        "stored_mwh": state["mwh"],
                        "stored_sha256": state["sha256"],
                        "payload_rows": window["rows"],
                        "payload_mwh": window["mwh"],
                        "payload_sha256": window["payload_sha256"],
                    }
                )
            connection.rollback()
    report = {
        "repair_id": manifest["repair_id"],
        "target": target,
        "country": country,
        "psrs": psrs,
        "manifest_sha256": manifest_sha,
        "inputs": manifest["inputs"],
        "blocking": {
            "stored_rows_in_other_coal_psrs": other_psrs,
            "unit_time_collisions": collisions,
        },
        "windows": report_windows,
    }
    report["plan_sha256"] = digest(report)
    return report


def create_ledger(cursor):
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {HEADER} (
        repair_id text PRIMARY KEY, manifest_sha256 text NOT NULL,
        plan_sha256 text NOT NULL, views_state text NOT NULL DEFAULT 'pending',
        created_at timestamptz NOT NULL DEFAULT now())""")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {MONTHS} (
        repair_id text NOT NULL REFERENCES {HEADER}(repair_id),
        window_start bigint NOT NULL, window_end bigint, month text NOT NULL,
        payload_sha256 text, stored_sha256 text NOT NULL,
        post_apply_sha256 text NOT NULL, state text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now(), rolled_back_at timestamptz,
        PRIMARY KEY (repair_id, window_start))""")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP} (LIKE {TABLE})")
    cursor.execute(f"ALTER TABLE {BACKUP} ADD COLUMN IF NOT EXISTS repair_id text")
    cursor.execute(f"ALTER TABLE {BACKUP} ADD COLUMN IF NOT EXISTS window_start bigint")
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS entsoe_coal_rebuild_backup_window"
        f" ON {BACKUP} (repair_id, window_start)"
    )
    for table in (HEADER, MONTHS, BACKUP):
        cursor.execute(f"REVOKE ALL ON {table} FROM PUBLIC")
        for role in ("etl_writer", "dashboard_ro"):
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
            if cursor.fetchone():
                cursor.execute(f"REVOKE ALL ON {table} FROM {role}")


def lock(cursor, lock_timeout, statement_timeout):
    cursor.execute("SET LOCAL lock_timeout = %s", (lock_timeout,))
    cursor.execute("SET LOCAL statement_timeout = %s", (statement_timeout,))
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")


def bind(connection, manifest, manifest_sha, approved):
    """One manifest and plan per repair id, recorded once."""
    with connection:
        with connection.cursor() as cursor:
            create_ledger(cursor)
            cursor.execute(
                f"SELECT manifest_sha256, plan_sha256 FROM {HEADER} WHERE repair_id=%s",
                (manifest["repair_id"],),
            )
            found = cursor.fetchone()
            if found:
                require(
                    found == (manifest_sha, approved["plan_sha256"]),
                    "This repair id is bound to another manifest or plan;"
                    " use a new repair id",
                )
            else:
                cursor.execute(
                    f"INSERT INTO {HEADER} (repair_id, manifest_sha256, plan_sha256)"
                    " VALUES (%s, %s, %s)",
                    (manifest["repair_id"], manifest_sha, approved["plan_sha256"]),
                )


def apply(
    connection,
    manifest,
    manifest_sha,
    windows,
    approved,
    only=None,
    lock_timeout="10s",
    statement_timeout="15min",
):
    require(
        approved["manifest_sha256"] == manifest_sha,
        "The approved plan is for another manifest",
    )
    blocking = approved["blocking"]
    require(
        not blocking["stored_rows_in_other_coal_psrs"]
        and not blocking["unit_time_collisions"],
        f"The approved plan has blocking findings: {blocking}",
    )
    planned = {w["month"]: w for w in approved["windows"]}
    require(
        set(planned) == {w["month"] for w in windows},
        "The approved plan covers other windows",
    )
    bind(connection, manifest, manifest_sha, approved)
    # Every pending month must still match the approved plan before the first
    # write, so drift cannot leave the country half rebuilt (rechecked per
    # month under the lock below).
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT window_start FROM {MONTHS} WHERE repair_id=%s",
                (manifest["repair_id"],),
            )
            done = {row[0] for row in cursor.fetchall()}
            drifted = [
                w["month"]
                for w in windows
                if (not only or w["month"] in only)
                and w["start"] not in done
                and window_state(cursor, manifest, w)["sha256"]
                != planned[w["month"]]["stored_sha256"]
            ]
    require(not drifted, f"Stored rows changed since the plan in {drifted}; re-plan")
    results = []
    for window in windows:
        if only and window["month"] not in only:
            continue
        expected = planned[window["month"]]
        with connection:
            with connection.cursor() as cursor:
                lock(cursor, lock_timeout, statement_timeout)
                cursor.execute(
                    f"SELECT state, payload_sha256, post_apply_sha256 FROM {MONTHS}"
                    " WHERE repair_id=%s AND window_start=%s",
                    (manifest["repair_id"], window["start"]),
                )
                ledger = cursor.fetchone()
                state = window_state(cursor, manifest, window)
                if ledger:
                    require(
                        ledger[0] == "applied",
                        f"{window['month']} was rolled back; use a new repair id",
                    )
                    require(
                        ledger[1] == window["payload_sha256"],
                        f"{window['month']} was applied from another payload",
                    )
                    require(
                        state["sha256"] == ledger[2],
                        f"{window['month']} changed after apply; not touched",
                    )
                    results.append(
                        {
                            "month": window["month"],
                            "action": "already applied",
                            "written": 0,
                        }
                    )
                    continue
                require(
                    state["sha256"] == expected["stored_sha256"]
                    and state["rows"] == expected["stored_rows"],
                    f"{window['month']}: stored rows changed since the plan",
                )
                stage(cursor, window)
                cursor.execute(
                    f"SELECT count(*) FROM _rebuild_payload p JOIN {TABLE} t"
                    " ON t.country_code=p.country_code AND t.unit_eic=p.unit_eic"
                    " AND t.timestamp_ms=p.timestamp_ms WHERE NOT (t.psr_type = ANY(%s))",
                    (manifest["psrs"],),
                )
                require(
                    cursor.fetchone()[0] == 0,
                    f"{window['month']}: unit/time key held by a retained row",
                )
                cursor.execute(
                    f"WITH moved AS (DELETE FROM {TABLE} WHERE {WHERE} RETURNING id, {COLS})"
                    f" INSERT INTO {BACKUP} (id, {COLS}, repair_id, window_start)"
                    f" SELECT id, {COLS}, %s, %s FROM moved",
                    scope(manifest, window) + (manifest["repair_id"], window["start"]),
                )
                require(
                    cursor.rowcount == expected["stored_rows"],
                    f"{window['month']}: deleted row count differs from the plan",
                )
                cursor.execute(
                    f"INSERT INTO {TABLE} ({COLS}) SELECT {COLS} FROM _rebuild_payload"
                )
                require(
                    cursor.rowcount == window["rows"],
                    f"{window['month']}: inserted row count differs",
                )
                cursor.execute(
                    f"SELECT (SELECT count(*) FROM (SELECT {COLS} FROM {TABLE} WHERE {WHERE}"
                    f" EXCEPT ALL SELECT {COLS} FROM _rebuild_payload) a),"
                    f" (SELECT count(*) FROM (SELECT {COLS} FROM _rebuild_payload"
                    f" EXCEPT ALL SELECT {COLS} FROM {TABLE} WHERE {WHERE}) b)",
                    scope(manifest, window) * 2,
                )
                require(
                    cursor.fetchone() == (0, 0),
                    f"{window['month']}: window differs from the payload after insert",
                )
                after = window_state(cursor, manifest, window)
                require(
                    D(after["mwh"]) == D(window["mwh"]),
                    f"{window['month']}: window MWh differs from the payload",
                )
                require(
                    after["sha256"] == window["lines_sha256"],
                    f"{window['month']}: window hash differs from the payload",
                )
                cursor.execute(
                    f"INSERT INTO {MONTHS} (repair_id, window_start, window_end, month,"
                    " payload_sha256, stored_sha256, post_apply_sha256, state)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, 'applied')",
                    (
                        manifest["repair_id"],
                        window["start"],
                        window["end"],
                        window["month"],
                        window["payload_sha256"],
                        expected["stored_sha256"],
                        after["sha256"],
                    ),
                )
                results.append(
                    {
                        "month": window["month"],
                        "action": "applied",
                        "deleted": expected["stored_rows"],
                        "inserted": window["rows"],
                    }
                )
    return results


def rollback(
    connection,
    manifest,
    windows,
    only=None,
    lock_timeout="10s",
    statement_timeout="15min",
):
    results = []
    for window in reversed(windows):
        if only and window["month"] not in only:
            continue
        with connection:
            with connection.cursor() as cursor:
                lock(cursor, lock_timeout, statement_timeout)
                cursor.execute(
                    f"SELECT state, stored_sha256, post_apply_sha256 FROM {MONTHS}"
                    " WHERE repair_id=%s AND window_start=%s FOR UPDATE",
                    (manifest["repair_id"], window["start"]),
                )
                ledger = cursor.fetchone()
                if not ledger or ledger[0] != "applied":
                    results.append({"month": window["month"], "action": "not applied"})
                    continue
                state = window_state(cursor, manifest, window)
                require(
                    state["sha256"] == ledger[2],
                    f"{window['month']} has later writes; rollback refused",
                )
                cursor.execute(
                    f"DELETE FROM {TABLE} WHERE {WHERE}", scope(manifest, window)
                )
                cursor.execute(
                    f"INSERT INTO {TABLE} (id, {COLS}) SELECT id, {COLS} FROM {BACKUP}"
                    " WHERE repair_id=%s AND window_start=%s",
                    (manifest["repair_id"], window["start"]),
                )
                restored = window_state(cursor, manifest, window)
                require(
                    restored["sha256"] == ledger[1],
                    f"{window['month']}: restored rows differ from the original",
                )
                cursor.execute(
                    f"UPDATE {MONTHS} SET state='rolled_back', rolled_back_at=now()"
                    " WHERE repair_id=%s AND window_start=%s",
                    (manifest["repair_id"], window["start"]),
                )
                results.append({"month": window["month"], "action": "rolled back"})
    return results


def refresh_views(connection, repair_id):
    """Refresh the ENTSO-E views; a failure leaves views_state 'pending' to retry."""
    previous = connection.autocommit
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for view in VIEWS:
                cursor.execute(f"REFRESH MATERIALIZED VIEW public.{view}")
            cursor.execute(
                f"UPDATE {HEADER} SET views_state='refreshed' WHERE repair_id=%s",
                (repair_id,),
            )
    finally:
        connection.autocommit = previous


def months_arg(value):
    if not value:
        return None
    first, _, last = value.partition("..")
    return {"first": first, "last": last or first}


def select(windows, wanted):
    if not wanted:
        return None
    return {
        w["month"]
        for w in windows
        if wanted["first"] <= w["month"][:7] <= wanted["last"]
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--environment", choices=("staging", "production"), required=True
    )
    parser.add_argument("--allow-production", action="store_true")
    parser.add_argument("--payload", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    mode.add_argument("--refresh-views", action="store_true")
    parser.add_argument(
        "--approved-plan", type=Path, help="The plan report reviewed before apply"
    )
    parser.add_argument("--expect-plan-sha256")
    parser.add_argument("--months", type=months_arg, help="YYYY-MM or YYYY-MM..YYYY-MM")
    parser.add_argument("--lock-timeout", default="10s")
    parser.add_argument("--statement-timeout", default="15min")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        dsn, target = checked_dsn(args.environment, args.allow_production)
        manifest, manifest_sha, windows = load_payload(args.payload)
        connection = psycopg2.connect(dsn, connect_timeout=15)
        try:
            if args.apply or args.rollback:
                require(
                    args.approved_plan and args.expect_plan_sha256,
                    "Mutations need --approved-plan and --expect-plan-sha256",
                )
                approved = json.loads(args.approved_plan.read_text())
                require(
                    approved["plan_sha256"]
                    == args.expect_plan_sha256
                    == digest(
                        {k: v for k, v in approved.items() if k != "plan_sha256"}
                    ),
                    "The approved plan does not match --expect-plan-sha256",
                )
                require(
                    approved["target"] == target,
                    "The approved plan is for another target",
                )
            wanted = select(windows, args.months)
            if args.apply:
                report = {
                    "applied": apply(
                        connection,
                        manifest,
                        manifest_sha,
                        windows,
                        approved,
                        wanted,
                        args.lock_timeout,
                        args.statement_timeout,
                    )
                }
                refresh_views(connection, manifest["repair_id"])
            elif args.rollback:
                report = {
                    "rolled_back": rollback(
                        connection,
                        manifest,
                        windows,
                        wanted,
                        args.lock_timeout,
                        args.statement_timeout,
                    )
                }
                refresh_views(connection, manifest["repair_id"])
            elif args.refresh_views:
                refresh_views(connection, manifest["repair_id"])
                report = {"views": "refreshed"}
            else:
                report = plan(connection, manifest, manifest_sha, windows, target)
        finally:
            connection.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: v for k, v in report.items() if k != "windows"}, indent=2))
        return 0
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection errors can contain credentials; do not echo DSNs.
        print(str(error) if isinstance(error, ValueError) else type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
