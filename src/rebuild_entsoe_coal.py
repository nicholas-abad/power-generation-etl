"""Plan, apply and roll back a certified ENTSO-E coal rebuild for one country.

Replaces a country's stored coal rows (the manifest's PSRs) with the payload
built from its certified source archive (scripts/build_entsoe_coal_payload.py),
one month per transaction, with backups and an exact rollback. Runbook:
docs/ENTSOE_COAL_REBUILD.md.

- plan (default, writes nothing persistent): per window, the stored rows'
  count, MWh and server-side hash; blocking findings (stored rows in coal PSRs
  outside the manifest, unit/time key collisions with retained rows, a country
  not configured as rebuilt). The report's plan_sha256 must be passed to apply
  and rollback.
- apply: every pending month must match the approved plan before the first
  write; per window, under the table lock, move the stored rows to the backup,
  insert the payload, and prove before commit that the window equals the
  payload row for row. The tail window (certified end onwards) is applied
  only if the weekly job's resume month (newest stored day in the table plus
  one day, UTC) is not after the certified end's month, checked before the
  run and again under the lock after the tail is deleted. --defer-tail
  completes the certified months without the tail and records
  tail_state='deferred'. Resumable; one manifest and plan per repair id; the
  scope config must be the one the plan was made with.
- rollback: bound to the same approved plan; per window, under the same lock,
  only while nothing has written to the window since apply; restores the
  original rows and ids.
Views are refreshed (UTC, CONCURRENTLY) after apply and rollback; a failure
leaves views_state 'pending' for --refresh-views. Production requires
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
import re
import subprocess

import psycopg2

import entsoe_coal
from refresh_views import DAILY_WORK_MEM, installed_migration_views
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
MW = COLUMNS.index("generation_mw")
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
WHERE = (
    "country_code = %s AND psr_type = ANY(%s) AND timestamp_ms >= %s"
    " AND (%s::bigint IS NULL OR timestamp_ms < %s)"
)
MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


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


# The month the weekly ENTSO-E job resumes from: the newest stored day in the
# whole table plus one day, at the start of its month (src/get_latest_date.py,
# src/incremental_extract.py).
RESUME = (
    "SELECT to_char(date_trunc('month', to_timestamp(max(timestamp_ms) / 1000.0)"
    f" AT TIME ZONE 'UTC' + interval '1 day'), 'YYYY-MM') FROM {TABLE}"
)


def resume_month(cursor):
    cursor.execute(RESUME)
    return cursor.fetchone()[0]


def load_payload(payload_dir):
    """The manifest and its windows; files are hash-checked, lines read lazily."""
    path = payload_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    require(
        tuple(manifest["columns"]) == COLUMNS, "Payload columns differ from the tool"
    )
    for bound in ("start", "end"):
        require(
            re.fullmatch(r"\d{4}-\d{2}-01", manifest[bound]),
            f"Certified {bound} must be the first day of a month",
        )
    months = manifest["months"]
    names = [m["month"] for m in months]
    expected = []
    year, number = map(int, manifest["start"][:7].split("-"))
    end_month = manifest["end"][:7]
    while f"{year:04d}-{number:02d}" < end_month:
        expected.append(f"{year:04d}-{number:02d}")
        year, number = (year + 1, 1) if number == 12 else (year, number + 1)
    require(
        expected and names == expected,
        "Payload months do not cover the certified window",
    )
    windows = []
    for i, month in enumerate(months):
        file = payload_dir / month["file"]
        require(
            hashlib.sha256(file.read_bytes()).hexdigest() == month["sha256"],
            f"Payload file {month['file']} changed",
        )
        end = month_ms(names[i + 1]) if i + 1 < len(months) else month_ms(end_month)
        windows.append(
            {
                "month": month["month"],
                "start": month_ms(month["month"]),
                "end": end,
                "rows": month["rows"],
                "mwh": month["mwh"],
                "payload_sha256": month["sha256"],
                "file": file,
            }
        )
    # From the certified end on, stored rows are deleted and nothing is
    # inserted; the resumed weekly job refills it (see apply's tail guard).
    windows.append(
        {
            "month": f"{end_month}-tail",
            "start": month_ms(end_month),
            "end": None,
            "rows": 0,
            "mwh": "0",
            "payload_sha256": None,
            "file": None,
        }
    )
    return manifest, hashlib.sha256(path.read_bytes()).hexdigest(), windows


def payload_lines(window):
    if window["file"] is None:
        return []
    data = window["file"].read_bytes()
    require(
        hashlib.sha256(data).hexdigest() == window["payload_sha256"],
        f"Payload file for {window['month']} changed",
    )
    lines = gzip.decompress(data).decode().split("\n")
    require(
        lines[0] == "\t".join(COLUMNS) and lines[-1] == "",
        f"Malformed payload for {window['month']}",
    )
    body = lines[1:-1]
    require(len(body) == window["rows"], f"{window['month']}: row count differs")
    return body


def typed(line):
    """A TSV line with MW compared as a float, independent of its text form."""
    fields = line.split("\t")
    fields[MW] = float(fields[MW])
    return tuple(fields)


def scope(manifest, window):
    """Parameters for the window predicate (see WHERE)."""
    return (
        manifest["country"],
        manifest["psrs"],
        window["start"],
        window["end"],
        window["end"],
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
    """Load a window's payload into a temp table, prove it equals the file, and
    return the staged rows' server-side hash."""
    lines = payload_lines(window)
    cursor.execute("DROP TABLE IF EXISTS _rebuild_payload")
    cursor.execute(
        f"CREATE TEMP TABLE _rebuild_payload AS SELECT {COLS} FROM {TABLE} WITH NO DATA"
    )
    if lines:
        cursor.copy_expert(
            f"COPY _rebuild_payload ({COLS}) FROM STDIN WITH (FORMAT text)",
            io.StringIO("\n".join(lines) + "\n"),
        )
    # Read the staged rows back and compare field by field, MW as a float, so a
    # different float text form cannot cause a false mismatch.
    back = io.StringIO()
    cursor.copy_expert(
        f"COPY (SELECT {COLS} FROM _rebuild_payload) TO STDOUT WITH (FORMAT text)",
        back,
    )
    staged = [line for line in back.getvalue().split("\n") if line]
    require(
        sorted(map(typed, staged)) == sorted(map(typed, lines)),
        f"{window['month']}: staged rows differ from the payload file",
    )
    cursor.execute(f"SELECT {HASH} FROM _rebuild_payload")
    return cursor.fetchone()[0]


def window_state(cursor, manifest, window):
    cursor.execute(
        f"SELECT count(*), {MWH}, {HASH} FROM {TABLE} WHERE {WHERE}",
        scope(manifest, window),
    )
    count, mwh, stored = cursor.fetchone()
    return {"rows": count, "mwh": format(D(mwh).normalize(), "f"), "sha256": stored}


def collisions(cursor, psrs):
    """Payload unit/time keys held by rows outside the deletion scope."""
    cursor.execute(
        f"SELECT count(*) FROM _rebuild_payload p JOIN {TABLE} t"
        " ON t.country_code=p.country_code AND t.unit_eic=p.unit_eic"
        " AND t.timestamp_ms=p.timestamp_ms WHERE NOT (t.psr_type = ANY(%s))",
        (psrs,),
    )
    return cursor.fetchone()[0]


def etl_commit(root=Path(__file__).resolve().parents[1]):
    """The ETL checkout's commit, or None if unknown or it has local changes."""
    here = str(root)
    try:
        commit = subprocess.check_output(
            ["git", "-C", here, "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "-C", here, "status", "--porcelain", "--", "src", "config"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return None if dirty else commit


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
            report_windows, clashes = [], []
            for window in windows:
                stage(cursor, window)
                state = window_state(cursor, manifest, window)
                clash = collisions(cursor, psrs)
                if clash:
                    clashes.append({"month": window["month"], "rows": clash})
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
    not_rebuilt = country not in entsoe_coal.REBUILT
    commit = etl_commit()
    report = {
        "repair_id": manifest["repair_id"],
        "target": target,
        "country": country,
        "psrs": psrs,
        "manifest_sha256": manifest_sha,
        "inputs": manifest["inputs"],
        # The scope config this plan was checked against; it must equal the one
        # released with the weekly workflow's pinned commits (runbook step 5).
        "etl": {
            "commit": commit,
            "scope_config_sha256": hashlib.sha256(
                (entsoe_coal.CONFIG / "entsoe-coal-scopes.json").read_bytes()
            ).hexdigest(),
        },
        "blocking": {
            "stored_rows_in_other_coal_psrs": other_psrs,
            "unit_time_collisions": clashes,
            # The weekly path must already write source-period rows with
            # canonical names and reject legacy rows for this country.
            "country_not_configured_as_rebuilt": [country] if not_rebuilt else [],
            # The plan must name the exact released code it was made with.
            "etl_checkout_unknown_or_modified": [] if commit else ["src/ or config/"],
        },
        "windows": report_windows,
    }
    report["plan_sha256"] = digest(report)
    return report


def create_ledger(cursor):
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {HEADER} (
        repair_id text PRIMARY KEY, manifest_sha256 text NOT NULL,
        plan_sha256 text NOT NULL, views_state text NOT NULL DEFAULT 'pending',
        tail_state text NOT NULL DEFAULT 'pending',
        created_at timestamptz NOT NULL DEFAULT now())""")
    # Ledgers created by earlier versions of this tool gain the newer columns.
    cursor.execute(
        f"ALTER TABLE {HEADER} ADD COLUMN IF NOT EXISTS views_state text NOT NULL"
        " DEFAULT 'pending'"
    )
    cursor.execute(
        f"ALTER TABLE {HEADER} ADD COLUMN IF NOT EXISTS tail_state text NOT NULL"
        " DEFAULT 'pending'"
    )
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


def check_approval(manifest, manifest_sha, windows, approved):
    """The approved plan must be the plan of exactly this repair and payload."""
    require(
        approved["plan_sha256"]
        == digest({k: v for k, v in approved.items() if k != "plan_sha256"}),
        "The approved plan's fingerprint does not match its content",
    )
    require(
        approved["repair_id"] == manifest["repair_id"]
        and approved["manifest_sha256"] == manifest_sha,
        "The approved plan is for another repair or manifest",
    )
    require(
        [w["month"] for w in approved["windows"]] == [w["month"] for w in windows],
        "The approved plan covers other windows",
    )
    return {w["month"]: w for w in approved["windows"]}


def bind(connection, manifest, manifest_sha, approved, create):
    """One manifest and plan per repair id, recorded once (by apply)."""
    with connection:
        with connection.cursor() as cursor:
            if create:
                create_ledger(cursor)
            else:
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (HEADER,))
                require(cursor.fetchone()[0], "No rebuild has been applied here")
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
                require(create, f"Repair {manifest['repair_id']} was never applied")
                cursor.execute(
                    f"INSERT INTO {HEADER} (repair_id, manifest_sha256, plan_sha256)"
                    " VALUES (%s, %s, %s)",
                    (manifest["repair_id"], manifest_sha, approved["plan_sha256"]),
                )


def selected(windows, only):
    return [w for w in windows if only is None or w["month"] in only]


def apply(
    connection,
    manifest,
    manifest_sha,
    windows,
    approved,
    only=None,
    lock_timeout="10s",
    statement_timeout="15min",
    results=None,
    defer_tail=False,
):
    results = [] if results is None else results
    planned = check_approval(manifest, manifest_sha, windows, approved)
    blocking = {k: v for k, v in approved["blocking"].items() if v}
    require(not blocking, f"The approved plan has blocking findings: {blocking}")
    require_same_scope_config(approved)
    end_month = manifest["end"][:7]
    tail_window = windows[-1]
    chosen = selected(windows, only)
    if defer_tail:
        require(
            only is None or tail_window["month"] not in only,
            "--defer-tail cannot be combined with a selection that includes the tail",
        )
        chosen = [w for w in chosen if w is not tail_window]
    bind(connection, manifest, manifest_sha, approved, create=True)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT window_start, state FROM {MONTHS} WHERE repair_id=%s",
                (manifest["repair_id"],),
            )
            ledger_states = dict(cursor.fetchall())
            require(
                "rolled_back" not in ledger_states.values(),
                "This repair has rolled-back windows; use a new repair id",
            )
            done = {
                start for start, state in ledger_states.items() if state == "applied"
            }
            pending = [w for w in chosen if w["start"] not in done]
            after = done | {w["start"] for w in pending}
            # Finishing the certified months without the tail would leave legacy
            # rows from the certified end on beside the new weekly rows.
            require(
                defer_tail
                or tail_window["start"] in after
                or not all(w["start"] in after for w in windows[:-1]),
                "This run completes the certified months but leaves the tail"
                f" ({manifest['end']} onwards) pending; include it, or pass"
                " --defer-tail and re-certify through the current month before the"
                " weekly job resumes",
            )
            # The tail is refilled only if the weekly job resumes within it.
            if tail_window in pending:
                resume = resume_month(cursor)
                require(
                    resume is not None and resume <= end_month,
                    f"The weekly job would resume from {resume}, after the tail"
                    f" starts ({manifest['end']}); the tail would never be refilled."
                    " Certify and build through the current month first",
                )
            # Every pending month must still match the approved plan before the
            # first write, so drift cannot leave the country half rebuilt.
            drifted = [
                w["month"]
                for w in pending
                if window_state(cursor, manifest, w)["sha256"]
                != planned[w["month"]]["stored_sha256"]
            ]
    require(not drifted, f"Stored rows changed since the plan in {drifted}; re-plan")
    for window in chosen:
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
                    if state["sha256"] != ledger[2]:
                        require(
                            window["end"] is None,
                            f"{window['month']} changed after apply; not touched",
                        )
                        action = "already applied; refilled by later loads"
                    else:
                        action = "already applied"
                    outcome = {"month": window["month"], "action": action, "written": 0}
                else:
                    outcome = apply_window(cursor, manifest, window, expected)
        # Recorded only once the month's transaction has committed.
        results.append(outcome)
    with connection:
        with connection.cursor() as cursor:
            if defer_tail and tail_window["start"] not in done:
                # Recorded once every selected month has committed.
                cursor.execute(
                    f"UPDATE {HEADER} SET tail_state='deferred' WHERE repair_id=%s"
                    " AND tail_state<>'applied'",
                    (manifest["repair_id"],),
                )
    return results


def tail_state(connection, repair_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT tail_state FROM {HEADER} WHERE repair_id=%s", (repair_id,)
            )
            found = cursor.fetchone()
    return found[0] if found else None


def require_same_scope_config(approved):
    """Apply and rollback run with the scope config the plan was made with."""
    current = hashlib.sha256(
        (entsoe_coal.CONFIG / "entsoe-coal-scopes.json").read_bytes()
    ).hexdigest()
    require(
        approved["etl"]["scope_config_sha256"] == current,
        "config/entsoe-coal-scopes.json differs from the one the plan was made with",
    )


def apply_window(cursor, manifest, window, expected):
    """Replace one window inside the caller's locked transaction."""
    state = window_state(cursor, manifest, window)
    require(
        state["sha256"] == expected["stored_sha256"]
        and state["rows"] == expected["stored_rows"],
        f"{window['month']}: stored rows changed since the plan",
    )
    staged = stage(cursor, window)
    require(
        collisions(cursor, manifest["psrs"]) == 0,
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
    cursor.execute(f"INSERT INTO {TABLE} ({COLS}) SELECT {COLS} FROM _rebuild_payload")
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
        after["sha256"] == staged,
        f"{window['month']}: window hash differs from the staged payload",
    )
    outcome = {
        "month": window["month"],
        "action": "applied",
        "deleted": expected["stored_rows"],
        "inserted": window["rows"],
    }
    if window["end"] is None:
        # Under the lock and after deleting the tail: the weekly job must
        # resume within the tail, or the tail would never be refilled.
        resume = resume_month(cursor)
        require(
            resume is not None and resume <= manifest["end"][:7],
            f"The weekly job would resume from {resume}, after the tail starts"
            f" ({manifest['end']}); the tail would never be refilled",
        )
        outcome["weekly_resume_month"] = resume
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
    cursor.execute(
        f"UPDATE {HEADER} SET views_state='pending'"
        + (", tail_state='applied'" if window["end"] is None else "")
        + " WHERE repair_id=%s",
        (manifest["repair_id"],),
    )
    return outcome


def rollback(
    connection,
    manifest,
    manifest_sha,
    windows,
    approved,
    only=None,
    lock_timeout="10s",
    statement_timeout="15min",
    results=None,
):
    results = [] if results is None else results
    check_approval(manifest, manifest_sha, windows, approved)
    require_same_scope_config(approved)
    bind(connection, manifest, manifest_sha, approved, create=False)
    for window in reversed(selected(windows, only)):
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
                    outcome = {"month": window["month"], "action": "not applied"}
                else:
                    outcome = rollback_window(cursor, manifest, window, ledger)
        # Recorded only once the month's transaction has committed.
        results.append(outcome)
    return results


def rollback_window(cursor, manifest, window, ledger):
    """Restore one window's original rows inside the caller's locked transaction."""
    state = window_state(cursor, manifest, window)
    if state["sha256"] != ledger[2]:
        hint = (
            " (the tail has been refilled by the weekly job; roll back"
            " the certified months with --months)"
            if window["end"] is None
            else ""
        )
        raise ValueError(f"{window['month']} has later writes; rollback refused{hint}")
    cursor.execute(f"DELETE FROM {TABLE} WHERE {WHERE}", scope(manifest, window))
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
    cursor.execute(
        f"UPDATE {HEADER} SET views_state='pending'"
        + (", tail_state='rolled_back'" if window["end"] is None else "")
        + " WHERE repair_id=%s",
        (manifest["repair_id"],),
    )
    return {"month": window["month"], "action": "rolled back"}


def refresh_views(connection, repair_id):
    """Refresh the ENTSO-E views like src/refresh_views.py (UTC, CONCURRENTLY).

    views_state becomes 'refreshed' only after every view refreshed; on failure
    it stays 'pending' and --refresh-views retries."""
    previous = connection.autocommit
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            for view in VIEWS:
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view}")
            # The daily view (once its migration is applied) must follow too, or
            # the daily charts keep the pre-rebuild rows.
            cursor.execute(f"SET work_mem = '{DAILY_WORK_MEM}'")
            for view in installed_migration_views(cursor, ["mv_entsoe_plant_daily"]):
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view}")
            cursor.execute(
                f"UPDATE {HEADER} SET views_state='refreshed' WHERE repair_id=%s",
                (repair_id,),
            )
    finally:
        connection.autocommit = previous


def months_arg(value):
    first, _, last = value.partition("..")
    last = last or first
    if not (MONTH.fullmatch(first) and MONTH.fullmatch(last)) or first > last:
        raise argparse.ArgumentTypeError(
            "expected YYYY-MM or YYYY-MM..YYYY-MM, in order"
        )
    return first, last


def select(windows, wanted):
    """None means every window; a selection that matches nothing is an error."""
    if wanted is None:
        return None
    first, last = wanted
    chosen = {w["month"] for w in windows if first <= w["month"][:7] <= last}
    require(chosen, f"--months {first}..{last} matches no window of this payload")
    return chosen


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
    parser.add_argument(
        "--defer-tail",
        action="store_true",
        help="Complete the certified months without the tail (recorded as deferred)",
    )
    parser.add_argument("--lock-timeout", default="10s")
    parser.add_argument("--statement-timeout", default="15min")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    results, report = [], None
    try:
        manifest, manifest_sha, windows = load_payload(args.payload)
        wanted = select(windows, args.months)
        if args.apply or args.rollback:
            require(
                args.approved_plan and args.expect_plan_sha256,
                "Mutations need --approved-plan and --expect-plan-sha256",
            )
            approved = json.loads(args.approved_plan.read_text())
            require(
                approved["plan_sha256"] == args.expect_plan_sha256,
                "The approved plan does not match --expect-plan-sha256",
            )
            check_approval(manifest, manifest_sha, windows, approved)
        dsn, target = checked_dsn(args.environment, args.allow_production)
        if args.apply or args.rollback:
            require(
                approved["target"] == target, "The approved plan is for another target"
            )
        connection = psycopg2.connect(dsn, connect_timeout=15)
        try:
            if args.apply:
                apply(
                    connection,
                    manifest,
                    manifest_sha,
                    windows,
                    approved,
                    wanted,
                    args.lock_timeout,
                    args.statement_timeout,
                    results,
                    args.defer_tail,
                )
                report = {
                    "applied": results,
                    # Must be 'applied' before the weekly job resumes (runbook).
                    "tail_state": tail_state(connection, manifest["repair_id"]),
                }
                refresh_views(connection, manifest["repair_id"])
            elif args.rollback:
                rollback(
                    connection,
                    manifest,
                    manifest_sha,
                    windows,
                    approved,
                    wanted,
                    args.lock_timeout,
                    args.statement_timeout,
                    results,
                )
                report = {
                    "rolled_back": results,
                    "tail_state": tail_state(connection, manifest["repair_id"]),
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
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        # Windows already committed are recorded in the ledger; report them too.
        partial = {
            "error": message,
            "completed": results,
            "note": "Committed months are in ingestion.entsoe_coal_rebuild_months;"
            " views_state in ingestion.entsoe_coal_rebuilds says whether views"
            " need --refresh-views.",
        }
        try:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(partial, indent=2) + "\n")
        except OSError:
            pass
        print(message)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
