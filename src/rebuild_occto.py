"""Build, plan, apply and roll back a certified OCCTO (Japan) all-fuel rebuild.

Replaces every stored OCCTO row with rows parsed from the certified source
archive (energy_extractors.occto.archive / .parse), one month per transaction,
with a backup and an exact rollback. STAGING ONLY: there is no production
option; every mode checks the pinned staging owner connection before it
connects. Protocol adapted from src/rebuild_entsoe_coal.py at commit 120b096
(branch feat/entsoe-coal-rebuild), minus its ENTSO-E scope and tail logic.

  build-payload  parser A's canonical rows -> monthly payload files + manifest.
                 Refuses unless parser B (docs/validation/occto/independent_parse.py)
                 produced the same digest and reported nothing rejected, no
                 duplicate keys and no unit-month with two fuels.
  plan           per window: stored rows' count, MWh and hash, the payload's;
                 writes nothing. Its plan_sha256 is required by apply/rollback.
  apply          every pending window must still match the plan; then per
                 window, under the table lock: move stored rows to the backup,
                 insert the payload, prove the window equals the payload.
                 Resumable. Then refreshes the three OCCTO views.
  rollback       per window, only while nothing wrote to it since apply;
                 restores the original rows and ids, then refreshes the views.

If the view refresh fails, rerun:
  src/run_in_environment.py --environment staging -- uv run python src/refresh_views.py --source occto

Run every mode through the owner wrapper, with the direct endpoint:
  uv run python src/run_in_environment.py --environment staging --role owner -- \
      uv run python src/rebuild_occto.py <mode> ...
"""

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
import uuid

import psycopg2
from psycopg2.extensions import parse_dsn

from refresh_views import DAILY_WORK_MEM, installed_migration_views

ROOT = Path(__file__).resolve().parents[1]
TABLE = "ingestion.occto_generation_data"
HEADER = "ingestion.occto_rebuilds"
WINDOWS = "ingestion.occto_rebuild_windows"
BACKUP = "ingestion.occto_rebuild_backup"
VIEWS = ("mv_occto_plant_monthly", "mv_occto_unit_monthly", "mv_occto_row_counts")
COLUMNS = (
    "timestamp_ms",
    "plant",
    "unit",
    "plant_code",
    "fuel_code",
    "fuel_type",
    "area_code",
    "area_name",
    "resolution_minutes",
    "generation_mwh",
    "extraction_run_id",
    "created_at_ms",
)
# Parser output order (energy_extractors.occto.parse.ROW_COLUMNS).
CANONICAL = COLUMNS[:9] + ("generation_kwh",)
MWH_INDEX = COLUMNS.index("generation_mwh")
COLS = ", ".join(COLUMNS)
ORDER = 'timestamp_ms, plant COLLATE "C", unit COLLATE "C"'
LINE = (
    "concat_ws(E'\\t', "
    + ", ".join(f"coalesce({c}::text, '\\N')" for c in COLUMNS)
    + ")"
)
# Bounded: one 32-character md5 per row is aggregated, never the rows
# themselves, so a month of all fuels (~0.75M rows) hashes in ~24 MB.
HASH = (
    "encode(sha256(convert_to(coalesce(string_agg(md5({line}), '' ORDER BY {order}), ''),"
    " 'UTF8')), 'hex')"
).format(line=LINE, order=ORDER)
# float8 text is the shortest round-trip form, i.e. the decimal it was loaded from.
MWH = "coalesce(sum(generation_mwh::text::numeric), 0)"
WHERE = "timestamp_ms >= %s AND timestamp_ms < %s"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def month_start_ms(month):
    year, number = map(int, month.split("-"))
    return int(datetime(year, number, 1, tzinfo=timezone.utc).timestamp() * 1000)


def next_month(month):
    year, number = map(int, month.split("-"))
    return f"{year + number // 12:04d}-{number % 12 + 1:02d}"


def month_of(ms):
    t = datetime(1970, 1, 1) + timedelta(milliseconds=ms)
    return f"{t.year:04d}-{t.month:02d}"


# --- target guard ---------------------------------------------------------


def checked_dsn():
    """The pinned direct staging owner connection, checked before connecting."""
    config = json.loads((ROOT / "config/environments.json").read_text())
    staging = config["environments"]["staging"]
    production = config["environments"]["production"]
    dsn = os.environ.get("DIRECT_DATABASE_URL", "")
    require(
        os.environ.get("ETL_ENVIRONMENT") == "staging"
        and os.environ.get("NEON_ENDPOINT_ID") == staging["endpoint_id"]
        and os.environ.get("NEON_BRANCH_ID") == staging["branch_id"],
        "Run through src/run_in_environment.py --environment staging --role owner",
    )
    require(
        production["endpoint_id"] not in dsn and production["branch_id"] not in dsn,
        "Refusing a production connection",
    )
    parts = parse_dsn(dsn)
    require(
        parts.get("host") == f"{staging['endpoint_id']}.{config['proxy_host']}"
        and parts.get("user") == config["roles"]["owner"]
        and parts.get("dbname") == config["database"]
        and parts.get("port") == "5432"
        and parts.get("sslmode") in {"require", "verify-full"}
        and not any(k in parts for k in ("hostaddr", "service"))
        and not any(
            os.environ.get(k) for k in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")
        ),
        "Expected the pinned direct staging owner connection",
    )
    return dsn, f"staging/{staging['branch_id']}/{config['database']}"


# --- payload ------------------------------------------------------------


def build_payload(
    canonical_path, parser_b_report, certificate_path, out_dir, repair_id, load_start
):
    """Split parser A's canonical rows into monthly payload files."""
    require(
        re.fullmatch(r"[a-z0-9][a-z0-9-]{2,62}", repair_id),
        "repair id: lowercase, digits, dashes",
    )
    report_b = json.loads(Path(parser_b_report).read_text())
    certificate = json.loads(Path(certificate_path).read_text())
    require(certificate["ok"], "The archive certificate is not ok")
    require(
        report_b["certificate_digest"] == certificate["digest"],
        "Parser B read another archive",
    )
    require(report_b["load_start"] == load_start, "Parser B used another load start")
    for key in ("rejected", "duplicate_keys", "unit_months_with_several_fuels"):
        require(not report_b[key], f"Parser B reported {key}; resolve before building")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"occto-rebuild:{repair_id}"))
    created_at_ms = int(time.time() * 1000)

    canonical_digest = hashlib.sha256()
    months, handle, current, previous_key = [], None, None, None

    def close():
        if handle:
            handle.close()
            path = out_dir / current["file"]
            current["sha256"] = file_sha256(path)
            current["mwh"] = format(current.pop("_mwh").normalize(), "f")
            months.append(current)

    with gzip.open(canonical_path, "rt", encoding="utf-8", newline="") as source:
        for line in source:
            canonical_digest.update(line.encode("utf-8"))
            fields = line.rstrip("\n").split("\t")
            require(len(fields) == len(CANONICAL), "Malformed canonical line")
            # Payload files are loaded with COPY ... FORMAT text, where a
            # backslash is an escape; refuse rather than load a changed label.
            require("\\" not in line, f"Backslash in a canonical row: {line[:80]!r}")
            key = (int(fields[0]), fields[1], fields[2])
            require(
                previous_key is None or key > previous_key,
                "Canonical rows not strictly sorted",
            )
            previous_key = key
            month = month_of(key[0])
            if current is None or current["month"] != month:
                close()
                current = {
                    "month": month,
                    "file": f"{month}.tsv.gz",
                    "rows": 0,
                    "_mwh": D(0),
                }
                handle = gzip.open(
                    out_dir / current["file"], "wt", encoding="utf-8", newline=""
                )
                handle.write("\t".join(COLUMNS) + "\n")
            mwh = D(fields[9]) / 1000
            row = fields[:9] + [
                format(mwh.normalize(), "f"),
                run_id,
                str(created_at_ms),
            ]
            handle.write("\t".join(row) + "\n")
            current["rows"] += 1
            current["_mwh"] += mwh
    close()
    require(
        canonical_digest.hexdigest() == report_b["digest"],
        "Parser A and parser B disagree",
    )
    require(months, "Empty payload")
    names = [m["month"] for m in months]
    for a, b in zip(names, names[1:]):
        require(
            b == next_month(a), f"Payload has no rows in a month between {a} and {b}"
        )
    end_day = datetime.fromisoformat(certificate["end"]) + timedelta(days=1)
    manifest = {
        "repair_id": repair_id,
        "columns": list(COLUMNS),
        "certificate_digest": certificate["digest"],
        "canonical_sha256": canonical_digest.hexdigest(),
        "load_start": load_start,
        "archive_end": certificate["end"],
        # Exclusive bound: the last key is the archive end day's 24:00 slot.
        "end_ms": int(end_day.replace(tzinfo=timezone.utc).timestamp() * 1000) + 1,
        "extraction_run_id": run_id,
        "created_at_ms": created_at_ms,
        "excluded_days": report_b["excluded_days"],
        "gaps": report_b["gaps"],
        "months": months,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def load_payload(payload_dir):
    """The manifest and its windows. The first window reaches back to the
    earliest stored month, so every stored row lies in exactly one window."""
    payload_dir = Path(payload_dir)
    path = payload_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    require(
        tuple(manifest["columns"]) == COLUMNS, "Payload columns differ from the tool"
    )
    windows = []
    months = manifest["months"]
    for i, month in enumerate(months):
        file = payload_dir / month["file"]
        require(
            file_sha256(file) == month["sha256"],
            f"Payload file {month['file']} changed",
        )
        last = i + 1 == len(months)
        windows.append(
            {
                "month": month["month"],
                "start": month_start_ms(month["month"]),
                "end": manifest["end_ms"]
                if last
                else month_start_ms(months[i + 1]["month"]),
                "rows": month["rows"],
                "mwh": month["mwh"],
                "payload_sha256": month["sha256"],
                "file": file,
            }
        )
    windows[0]["start"] = 0  # anything stored before the payload is replaced too
    return manifest, file_sha256(path), windows


# --- database -----------------------------------------------------------


def preflight(cursor):
    cursor.execute(
        "SELECT array_agg(column_name::text) FROM information_schema.columns"
        " WHERE table_schema='ingestion' AND table_name='occto_generation_data'"
    )
    require(
        sorted(cursor.fetchone()[0] or []) == sorted(("id",) + COLUMNS),
        "occto_generation_data does not have the expected columns",
    )


def stage(cursor, window):
    """Load a window's payload into a temp table, prove it equals the file
    (streamed, MWh compared as float), and return its server-side hash."""
    cursor.execute("DROP TABLE IF EXISTS _occto_payload")
    cursor.execute(
        f"CREATE TEMP TABLE _occto_payload AS SELECT {COLS} FROM {TABLE} WITH NO DATA"
    )
    with gzip.open(window["file"], "rt", encoding="utf-8", newline="") as source:
        require(
            source.readline().rstrip("\n") == "\t".join(COLUMNS),
            f"{window['month']}: bad header",
        )
        cursor.copy_expert(
            f"COPY _occto_payload ({COLS}) FROM STDIN WITH (FORMAT text)", source
        )
    with tempfile.TemporaryFile("w+", encoding="utf-8", newline="") as back:
        cursor.copy_expert(
            f"COPY (SELECT {COLS} FROM _occto_payload ORDER BY {ORDER}) TO STDOUT WITH (FORMAT text)",
            back,
        )
        back.seek(0)
        with gzip.open(window["file"], "rt", encoding="utf-8", newline="") as source:
            source.readline()
            count = 0
            for expected, got in zip(source, back):
                a, b = expected.rstrip("\n").split("\t"), got.rstrip("\n").split("\t")
                a[MWH_INDEX], b[MWH_INDEX] = float(a[MWH_INDEX]), float(b[MWH_INDEX])
                require(
                    a == b,
                    f"{window['month']}: staged row differs from the payload file",
                )
                count += 1
            require(
                source.readline() == "" and back.readline() == "",
                f"{window['month']}: staged rows and payload file differ in length",
            )
    require(count == window["rows"], f"{window['month']}: staged row count differs")
    cursor.execute(f"SELECT {HASH} FROM _occto_payload")
    return cursor.fetchone()[0]


def window_state(cursor, window):
    cursor.execute(
        f"SELECT count(*), {MWH}, {HASH} FROM {TABLE} WHERE {WHERE}",
        (window["start"], window["end"]),
    )
    count, mwh, stored = cursor.fetchone()
    return {"rows": count, "mwh": format(D(mwh).normalize(), "f"), "sha256": stored}


def plan(connection, manifest, manifest_sha, windows, target):
    with connection:
        with connection.cursor() as cursor:
            preflight(cursor)
            cursor.execute(
                f"SELECT count(*) FROM {TABLE} WHERE timestamp_ms >= %s",
                (manifest["end_ms"],),
            )
            retained_after_end = cursor.fetchone()[0]
            report_windows = []
            for window in windows:
                staged = stage(cursor, window)
                state = window_state(cursor, window)
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
                        "staged_sha256": staged,
                    }
                )
            connection.rollback()
    report = {
        "repair_id": manifest["repair_id"],
        "target": target,
        "manifest_sha256": manifest_sha,
        "certificate_digest": manifest["certificate_digest"],
        "canonical_sha256": manifest["canonical_sha256"],
        # Rows from the certified end on are not in any window and stay as they are.
        "retained_rows_after_end": retained_after_end,
        "windows": report_windows,
    }
    report["plan_sha256"] = digest(report)
    return report


def create_ledger(cursor):
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {HEADER} (
        repair_id text PRIMARY KEY, manifest_sha256 text NOT NULL,
        plan_sha256 text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())""")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {WINDOWS} (
        repair_id text NOT NULL REFERENCES {HEADER}(repair_id),
        window_start bigint NOT NULL, window_end bigint NOT NULL, month text NOT NULL,
        payload_sha256 text NOT NULL, stored_sha256 text NOT NULL,
        post_apply_sha256 text NOT NULL, state text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now(), rolled_back_at timestamptz,
        PRIMARY KEY (repair_id, window_start))""")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP} (LIKE {TABLE})")
    cursor.execute(f"ALTER TABLE {BACKUP} ADD COLUMN IF NOT EXISTS repair_id text")
    cursor.execute(f"ALTER TABLE {BACKUP} ADD COLUMN IF NOT EXISTS window_start bigint")
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS occto_rebuild_backup_window ON {BACKUP} (repair_id, window_start)"
    )
    for table in (HEADER, WINDOWS, BACKUP):
        cursor.execute(f"REVOKE ALL ON {table} FROM PUBLIC")
        for role in ("etl_writer", "dashboard_ro"):
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
            if cursor.fetchone():
                cursor.execute(f"REVOKE ALL ON {table} FROM {role}")


def lock(cursor, lock_timeout, statement_timeout):
    cursor.execute("SET LOCAL lock_timeout = %s", (lock_timeout,))
    cursor.execute("SET LOCAL statement_timeout = %s", (statement_timeout,))
    cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")


def check_approval(manifest, manifest_sha, windows, approved, target):
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
    require(approved["target"] == target, "The approved plan is for another target")
    require(
        [(w["month"], w["start"], w["end"]) for w in approved["windows"]]
        == [(w["month"], w["start"], w["end"]) for w in windows],
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
                    "This repair id is bound to another manifest or plan; use a new repair id",
                )
            else:
                require(create, f"Repair {manifest['repair_id']} was never applied")
                cursor.execute(
                    f"INSERT INTO {HEADER} (repair_id, manifest_sha256, plan_sha256) VALUES (%s, %s, %s)",
                    (manifest["repair_id"], manifest_sha, approved["plan_sha256"]),
                )


def ledger_states(cursor, repair_id):
    """window_start -> state for a repair; empty before the first apply."""
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (WINDOWS,))
    if not cursor.fetchone()[0]:
        return {}
    cursor.execute(
        f"SELECT window_start, state FROM {WINDOWS} WHERE repair_id=%s", (repair_id,)
    )
    return dict(cursor.fetchall())


def apply(
    connection,
    manifest,
    manifest_sha,
    windows,
    approved,
    target,
    lock_timeout="10s",
    statement_timeout="15min",
    results=None,
):
    results = [] if results is None else results
    planned = check_approval(manifest, manifest_sha, windows, approved, target)
    with connection:
        with connection.cursor() as cursor:
            states = ledger_states(cursor, manifest["repair_id"])
            require(
                "rolled_back" not in states.values(),
                "This repair has rolled-back windows; use a new repair id",
            )
            pending = [w for w in windows if w["start"] not in states]
            # Every pending window must still match the plan before the first
            # write, so drift cannot leave the table half rebuilt. Checked before
            # the repair id is bound, so a fresh plan can still be applied.
            drifted = [
                w["month"]
                for w in pending
                if window_state(cursor, w)["sha256"]
                != planned[w["month"]]["stored_sha256"]
            ]
    require(
        not drifted,
        f"Stored rows changed since the plan in {drifted}; run plan again and apply"
        " the new plan (nothing was written)",
    )
    bind(connection, manifest, manifest_sha, approved, create=True)
    for window in windows:
        with connection:
            with connection.cursor() as cursor:
                lock(cursor, lock_timeout, statement_timeout)
                cursor.execute(
                    f"SELECT state, payload_sha256, post_apply_sha256 FROM {WINDOWS}"
                    " WHERE repair_id=%s AND window_start=%s",
                    (manifest["repair_id"], window["start"]),
                )
                ledger = cursor.fetchone()
                if ledger:
                    require(
                        ledger[1] == window["payload_sha256"],
                        f"{window['month']} was applied from another payload",
                    )
                    require(
                        window_state(cursor, window)["sha256"] == ledger[2],
                        f"{window['month']} changed after apply; not touched",
                    )
                    outcome = {"month": window["month"], "action": "already applied"}
                else:
                    outcome = apply_window(
                        cursor, manifest, window, planned[window["month"]]
                    )
        results.append(outcome)  # only once the window's transaction committed
    return results


def apply_window(cursor, manifest, window, expected):
    """Replace one window inside the caller's locked transaction."""
    state = window_state(cursor, window)
    require(
        state["sha256"] == expected["stored_sha256"]
        and state["rows"] == expected["stored_rows"],
        f"{window['month']}: stored rows changed since the plan",
    )
    staged = stage(cursor, window)
    require(
        staged == expected["staged_sha256"],
        f"{window['month']}: payload differs from the plan",
    )
    cursor.execute(
        f"WITH moved AS (DELETE FROM {TABLE} WHERE {WHERE} RETURNING id, {COLS})"
        f" INSERT INTO {BACKUP} (id, {COLS}, repair_id, window_start)"
        f" SELECT id, {COLS}, %s, %s FROM moved",
        (window["start"], window["end"], manifest["repair_id"], window["start"]),
    )
    require(
        cursor.rowcount == expected["stored_rows"],
        f"{window['month']}: deleted row count differs from the plan",
    )
    cursor.execute(f"INSERT INTO {TABLE} ({COLS}) SELECT {COLS} FROM _occto_payload")
    require(
        cursor.rowcount == window["rows"],
        f"{window['month']}: inserted row count differs",
    )
    cursor.execute(
        f"SELECT (SELECT count(*) FROM (SELECT {COLS} FROM {TABLE} WHERE {WHERE}"
        f" EXCEPT ALL SELECT {COLS} FROM _occto_payload) a),"
        f" (SELECT count(*) FROM (SELECT {COLS} FROM _occto_payload"
        f" EXCEPT ALL SELECT {COLS} FROM {TABLE} WHERE {WHERE}) b)",
        (window["start"], window["end"]) * 2,
    )
    require(
        cursor.fetchone() == (0, 0),
        f"{window['month']}: window differs from the payload after insert",
    )
    after = window_state(cursor, window)
    require(
        D(after["mwh"]) == D(window["mwh"]),
        f"{window['month']}: window MWh differs from the payload",
    )
    require(
        after["sha256"] == staged,
        f"{window['month']}: window hash differs from the staged payload",
    )
    cursor.execute(
        f"INSERT INTO {WINDOWS} (repair_id, window_start, window_end, month, payload_sha256,"
        " stored_sha256, post_apply_sha256, state) VALUES (%s, %s, %s, %s, %s, %s, %s, 'applied')",
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
    return {
        "month": window["month"],
        "action": "applied",
        "deleted": expected["stored_rows"],
        "inserted": window["rows"],
    }


def rollback(
    connection,
    manifest,
    manifest_sha,
    windows,
    approved,
    target,
    lock_timeout="10s",
    statement_timeout="15min",
    results=None,
):
    results = [] if results is None else results
    check_approval(manifest, manifest_sha, windows, approved, target)
    bind(connection, manifest, manifest_sha, approved, create=False)
    # Like apply: refuse before the first write if any applied window has
    # changed, so a refusal never leaves the table half rolled back.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT window_start, post_apply_sha256 FROM {WINDOWS}"
                " WHERE repair_id=%s AND state='applied'",
                (manifest["repair_id"],),
            )
            applied = dict(cursor.fetchall())
            changed = [
                w["month"]
                for w in windows
                if w["start"] in applied
                and window_state(cursor, w)["sha256"] != applied[w["start"]]
            ]
    require(
        not changed,
        f"{changed} have later writes; rollback refused (nothing was written)",
    )
    for window in reversed(windows):
        with connection:
            with connection.cursor() as cursor:
                lock(cursor, lock_timeout, statement_timeout)
                cursor.execute(
                    f"SELECT state, stored_sha256, post_apply_sha256 FROM {WINDOWS}"
                    " WHERE repair_id=%s AND window_start=%s FOR UPDATE",
                    (manifest["repair_id"], window["start"]),
                )
                ledger = cursor.fetchone()
                if not ledger or ledger[0] != "applied":
                    outcome = {"month": window["month"], "action": "not applied"}
                else:
                    outcome = rollback_window(cursor, manifest, window, ledger)
        results.append(outcome)
    return results


def rollback_window(cursor, manifest, window, ledger):
    """Restore one window's original rows inside the caller's locked transaction."""
    require(
        window_state(cursor, window)["sha256"] == ledger[2],
        f"{window['month']} has later writes; rollback refused",
    )
    cursor.execute(
        f"DELETE FROM {TABLE} WHERE {WHERE}", (window["start"], window["end"])
    )
    cursor.execute(
        f"INSERT INTO {TABLE} (id, {COLS}) SELECT id, {COLS} FROM {BACKUP} WHERE repair_id=%s AND window_start=%s",
        (manifest["repair_id"], window["start"]),
    )
    require(
        window_state(cursor, window)["sha256"] == ledger[1],
        f"{window['month']}: restored rows differ from the original",
    )
    cursor.execute(
        f"UPDATE {WINDOWS} SET state='rolled_back', rolled_back_at=now() WHERE repair_id=%s AND window_start=%s",
        (manifest["repair_id"], window["start"]),
    )
    return {"month": window["month"], "action": "rolled back"}


def refresh_views(connection):
    """Same as src/refresh_views.py --source occto: UTC, CONCURRENTLY."""
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
            for view in installed_migration_views(cursor, ["mv_occto_unit_daily"]):
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view}")
    finally:
        connection.autocommit = previous


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    build = sub.add_parser("build-payload")
    build.add_argument("--canonical", type=Path, required=True)
    build.add_argument("--parser-b-report", type=Path, required=True)
    build.add_argument("--certificate", type=Path, required=True)
    build.add_argument("--out", type=Path, required=True)
    build.add_argument("--repair-id", required=True)
    build.add_argument("--load-start", default="2024-03-25")
    for name in ("plan", "apply", "rollback"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--payload", type=Path, required=True)
        cmd.add_argument("--report", type=Path, required=True)
        if name != "plan":
            cmd.add_argument("--approved-plan", type=Path, required=True)
            cmd.add_argument("--expect-plan-sha256", required=True)
            cmd.add_argument("--lock-timeout", default="10s")
            cmd.add_argument("--statement-timeout", default="15min")
    args = parser.parse_args(argv)
    if args.mode == "build-payload":
        manifest = build_payload(
            args.canonical,
            args.parser_b_report,
            args.certificate,
            args.out,
            args.repair_id,
            args.load_start,
        )
        print(
            json.dumps(
                {k: v for k, v in manifest.items() if k != "months"}
                | {
                    "months": len(manifest["months"]),
                    "rows": sum(m["rows"] for m in manifest["months"]),
                },
                indent=1,
            )
        )
        return 0
    results = []
    try:
        manifest, manifest_sha, windows = load_payload(args.payload)
        approved = None
        if args.mode != "plan":
            approved = json.loads(args.approved_plan.read_text())
            require(
                approved["plan_sha256"] == args.expect_plan_sha256,
                "The approved plan does not match --expect-plan-sha256",
            )
        dsn, target = checked_dsn()
        if approved:
            check_approval(manifest, manifest_sha, windows, approved, target)
        connection = psycopg2.connect(dsn, connect_timeout=15)
        try:
            if args.mode == "plan":
                report = plan(connection, manifest, manifest_sha, windows, target)
            else:
                action = apply if args.mode == "apply" else rollback
                action(
                    connection,
                    manifest,
                    manifest_sha,
                    windows,
                    approved,
                    target,
                    args.lock_timeout,
                    args.statement_timeout,
                    results,
                )
                report = {args.mode: results}
                refresh_views(connection)
                report["views"] = "refreshed"
        finally:
            connection.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=1) + "\n")
        print(
            json.dumps({k: v for k, v in report.items() if k != "windows"}, indent=1)[
                :4000
            ]
        )
        return 0
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection errors can contain credentials; do not echo DSNs.
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        partial = {
            "error": message,
            "completed": results,
            "note": f"Committed windows are in {WINDOWS}. If views were not refreshed, run"
            " src/refresh_views.py --source occto through run_in_environment.py.",
        }
        try:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(partial, indent=1) + "\n")
        except OSError:
            pass
        print(message)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
