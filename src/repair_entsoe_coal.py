"""Plan/apply/roll back the pinned CZ coal repair on the existing table schema.

Default is a read-only plan. Apply and rollback require its exact fingerprint.
Production also requires --allow-production and the pinned owner wrapper.
This command does not refresh views, change mappings, or load other fuels.
"""

import argparse
from collections import Counter
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json, execute_values

from entsoe_coal import ALIASES

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/repairs/entsoe-cz-coal-2023-2024.json"
TABLE = "ingestion.entsoe_generation_data"
LEDGER = "ingestion.entsoe_coal_repairs"
BACKUP = "ingestion.entsoe_coal_repair_backup"
FUELS = {
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B05": "Fossil Hard coal",
}


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def key(row, canonical=False):
    return (
        row["timestamp_ms"],
        row["psr_type"],
        row["canonical_name"] if canonical else row["plant_name"],
    )


def load_release():
    manifest = json.loads(MANIFEST.read_text())
    data = (MANIFEST.parent / manifest["artifact"]).read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["artifact_sha256"]:
        raise ValueError("Repair artifact hash mismatch")
    payload = gzip.decompress(data)
    if hashlib.sha256(payload).hexdigest() != manifest["payload_sha256"]:
        raise ValueError("Repair payload hash mismatch")
    changes = [json.loads(line) for line in payload.splitlines()]
    if Counter(r["action"] for r in changes) != {"interval": 104208, "duplicate": 12}:
        raise ValueError("Unexpected repair population")
    if len({key(r) for r in changes}) != len(changes):
        raise ValueError("Repeated repair key")
    for row in changes:
        if (
            row["psr_type"] not in FUELS
            or not 1704063600000 <= row["timestamp_ms"] < 1735689600000
        ):
            raise ValueError(
                "Repair observation outside CZ coal 2023 boundary / 2024 scope"
            )
        pair = (row["before_minutes"], row["after_minutes"])
        if pair != ((15, 60) if row["action"] == "interval" else (15, 15)):
            raise ValueError("Unexpected interval repair")
        if row["action"] == "duplicate" and row["canonical_name"] == row["plant_name"]:
            raise ValueError("Duplicate repair must retain a different canonical row")
    return manifest, changes


def fetch_rows(cursor, keys):
    rows = {}
    keys = sorted(set(keys))
    # Parameterized bounded batches; no temporary/permanent writes in plan mode.
    for offset in range(0, len(keys), 5000):
        execute_values(
            cursor,
            f"""SELECT to_jsonb(a) FROM (VALUES %s) AS e(ts,psr,name)
            JOIN {TABLE} a ON a.country_code='CZ' AND a.timestamp_ms=e.ts
                AND a.psr_type=e.psr AND a.plant_name=e.name""",
            keys[offset : offset + 5000],
            template="(%s::bigint,%s::text,%s::text)",
            page_size=5000,
        )
        for (row,) in cursor.fetchall():
            if key(row) in rows:
                raise ValueError("Multiple database rows share a repair key")
            rows[key(row)] = row
    return rows


def build_plan(cursor, manifest, changes, target):
    wanted = [key(r) for r in changes] + [
        key(r, True) for r in changes if r["action"] == "duplicate"
    ]
    allowed_keys = set(wanted)
    for row in changes:
        for alias in ALIASES:
            if row["psr_type"] == alias["psr_type"] and row["plant_name"] in {
                alias["source_name"],
                alias["legacy_name"],
            }:
                wanted.extend(
                    (row["timestamp_ms"], row["psr_type"], name)
                    for name in (alias["source_name"], alias["legacy_name"])
                )
    current = fetch_rows(cursor, wanted)
    if set(current) - allowed_keys:
        raise ValueError("Unreviewed alias copy in repair window; review required")
    pending = []
    for change in changes:
        row = current.get(key(change))
        canonical = None
        if change["action"] == "duplicate":
            canonical = current.get(key(change, True))
            if not canonical:
                raise ValueError(
                    "Canonical observation missing; refusing duplicate deletion"
                )
            inspect = [r for r in (row, canonical) if r]
        else:
            if row is None:
                raise ValueError("Audited interval observation missing")
            inspect = [row]
        for actual in inspect:
            if (
                Decimal(str(actual["generation_mw"]))
                != Decimal(change["generation_mw"])
                or actual["fuel_type"] != FUELS[change["psr_type"]]
                or actual["data_type"] == "Actual Consumption"
            ):
                raise ValueError(
                    "Audited coal MW, fuel or metric changed; review required"
                )
            if actual["resolution_minutes"] not in {
                change["before_minutes"],
                change["after_minutes"],
            }:
                raise ValueError("Unexpected coal interval duration")
        if change["action"] == "duplicate":
            if row is not None:
                pending.append(
                    {
                        "action": "duplicate",
                        "original": row,
                        "repaired": None,
                        "canonical": canonical,
                    }
                )
        elif row["resolution_minutes"] == change["before_minutes"]:
            pending.append(
                {
                    "action": "interval",
                    "original": row,
                    "repaired": {**row, "resolution_minutes": change["after_minutes"]},
                    "canonical": None,
                }
            )
    counts = Counter(r["action"] for r in pending)
    delta = sum(
        Decimal(str(r["original"]["generation_mw"]))
        * (
            (r["repaired"]["resolution_minutes"] if r["repaired"] else 0)
            - r["original"]["resolution_minutes"]
        )
        / 60
        for r in pending
    )
    report = {
        "repair_id": manifest["repair_id"],
        "artifact_sha256": manifest["artifact_sha256"],
        "target": target,
        "interval_updates": counts["interval"],
        "duplicate_deletes": counts["duplicate"],
        "already_correct": len(changes) - len(pending),
        "mwh_change": str(delta),
        "current_rows_sha256": digest([current[k] for k in sorted(current)]),
    }
    report["plan_sha256"] = digest(report)
    return report, pending


def ledger_state(cursor, repair_id):
    cursor.execute("SELECT to_regclass(%s)", (LEDGER,))
    if cursor.fetchone()[0] is None:
        return None
    cursor.execute(
        f"SELECT state,artifact_sha256 FROM {LEDGER} WHERE repair_id=%s", (repair_id,)
    )
    return cursor.fetchone()


def create_backup(cursor):
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {LEDGER} (
        repair_id text PRIMARY KEY, artifact_sha256 text NOT NULL, plan_sha256 text NOT NULL,
        state text NOT NULL CHECK(state IN ('applied','rolled_back')),
        recorded_at timestamptz NOT NULL DEFAULT now()
    )""")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {BACKUP} (
        repair_id text NOT NULL REFERENCES {LEDGER}(repair_id), id bigint NOT NULL,
        action text NOT NULL CHECK(action IN ('interval','duplicate')),
        original_row jsonb NOT NULL, repaired_row jsonb, canonical_row jsonb,
        PRIMARY KEY(repair_id,id)
    )""")
    cursor.execute(f"REVOKE ALL ON {LEDGER},{BACKUP} FROM PUBLIC")
    # Neon default privileges can grant DML on new tables; remove those grants.
    for role in ("etl_writer", "dashboard_ro"):
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
        if cursor.fetchone():
            cursor.execute(f"REVOKE ALL ON {LEDGER},{BACKUP} FROM {role}")


def apply_plan(cursor, manifest, pending, report):
    state = ledger_state(cursor, manifest["repair_id"])
    if state and state != ("applied", manifest["artifact_sha256"]):
        raise ValueError("Repair ID already used for another state/artifact")
    if not pending:
        return {"status": "already_correct", "rows_written": 0}
    if state:
        raise ValueError(
            "Previously applied repair has drifted; a new review is required"
        )
    create_backup(cursor)
    cursor.execute(
        f"INSERT INTO {LEDGER}(repair_id,artifact_sha256,plan_sha256,state) VALUES (%s,%s,%s,'applied')",
        (manifest["repair_id"], manifest["artifact_sha256"], report["plan_sha256"]),
    )
    execute_values(
        cursor,
        f"INSERT INTO {BACKUP} VALUES %s",
        [
            (
                manifest["repair_id"],
                r["original"]["id"],
                r["action"],
                Json(r["original"]),
                Json(r["repaired"]) if r["repaired"] is not None else None,
                Json(r["canonical"]) if r["canonical"] is not None else None,
            )
            for r in pending
        ],
        page_size=5000,
    )
    cursor.execute(
        f"""UPDATE {TABLE} a SET resolution_minutes=(b.repaired_row->>'resolution_minutes')::smallint
        FROM {BACKUP} b WHERE b.repair_id=%s AND b.action='interval' AND a.id=b.id
            AND to_jsonb(a)=b.original_row""",
        (manifest["repair_id"],),
    )
    if cursor.rowcount != report["interval_updates"]:
        raise ValueError("Interval update count changed; transaction aborted")
    cursor.execute(
        f"""DELETE FROM {TABLE} a USING {BACKUP} b
        WHERE b.repair_id=%s AND b.action='duplicate' AND a.id=b.id AND to_jsonb(a)=b.original_row""",
        (manifest["repair_id"],),
    )
    if cursor.rowcount != report["duplicate_deletes"]:
        raise ValueError("Duplicate delete count changed; transaction aborted")
    return {"status": "applied", "rows_written": len(pending)}


def rollback(cursor, manifest):
    state = ledger_state(cursor, manifest["repair_id"])
    if not state or state[1] != manifest["artifact_sha256"]:
        raise ValueError("No matching backup ledger")
    cursor.execute(
        f"SELECT action,original_row,repaired_row,canonical_row FROM {BACKUP} WHERE repair_id=%s ORDER BY id",
        (manifest["repair_id"],),
    )
    backups = cursor.fetchall()
    wanted = [key(r[1]) for r in backups] + [key(r[3]) for r in backups if r[3]]
    actual = fetch_rows(cursor, wanted)
    for _, original, repaired, canonical in backups:
        expected = original if state[0] == "rolled_back" else repaired
        if actual.get(key(original)) != expected or (
            canonical and actual.get(key(canonical)) != canonical
        ):
            raise ValueError("Post-repair data changed; rollback requires a new review")
    if state[0] == "rolled_back":
        return {"status": "already_rolled_back", "rows_written": 0}
    cursor.execute(
        f"""UPDATE {TABLE} a SET resolution_minutes=(b.original_row->>'resolution_minutes')::smallint
        FROM {BACKUP} b WHERE b.repair_id=%s AND b.action='interval' AND a.id=b.id""",
        (manifest["repair_id"],),
    )
    restored = cursor.rowcount
    cursor.execute(
        f"""INSERT INTO {TABLE} SELECT (jsonb_populate_record(NULL::{TABLE},b.original_row)).*
        FROM {BACKUP} b WHERE b.repair_id=%s AND b.action='duplicate'""",
        (manifest["repair_id"],),
    )
    restored += cursor.rowcount
    if restored != len(backups):
        raise ValueError("Rollback count mismatch")
    cursor.execute(
        f"UPDATE {LEDGER} SET state='rolled_back' WHERE repair_id=%s",
        (manifest["repair_id"],),
    )
    return {"status": "rolled_back", "rows_written": restored}


def run(connection, manifest, changes, target, mode="plan", expected_plan=None):
    previous = connection.readonly, connection.isolation_level
    try:
        return _run(connection, manifest, changes, target, mode, expected_plan)
    finally:
        connection.readonly, connection.isolation_level = previous


def _run(connection, manifest, changes, target, mode, expected_plan):
    connection.set_session(readonly=mode == "plan", isolation_level="REPEATABLE READ")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='300s'")
            if mode != "plan":
                cursor.execute("SET LOCAL lock_timeout='10s'")
                cursor.execute(f"LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE")
            report, pending = build_plan(cursor, manifest, changes, target)
            if mode == "plan":
                return {"status": "planned", "rows_written": 0, **report}
            if expected_plan != report["plan_sha256"]:
                raise ValueError(
                    "Plan fingerprint changed or missing; no writes allowed"
                )
            if mode == "apply":
                result = apply_plan(cursor, manifest, pending, report)
                _, remaining = build_plan(cursor, manifest, changes, target)
                if remaining:
                    raise ValueError("Post-repair source reconciliation failed")
            elif mode == "rollback":
                result = rollback(cursor, manifest)
            else:
                raise ValueError("Unknown repair mode")
            return {**report, **result}


def checked_dsn(environment, allow_production):
    """Fail before connecting unless the owner wrapper has pinned this target."""
    if environment == "production" and not allow_production:
        raise ValueError(
            "Production access requires explicit --allow-production authorization"
        )
    config = json.loads((ROOT / "config/environments.json").read_text())
    target = config["environments"][environment]
    if (
        os.environ.get("ETL_ENVIRONMENT") != environment
        or os.environ.get("NEON_ENDPOINT_ID") != target["endpoint_id"]
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
    return dsn, f"{environment}/{target['branch_id']}/{config['database']}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment", choices=("staging", "production"), required=True
    )
    parser.add_argument("--allow-production", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    parser.add_argument("--expect-plan-sha256")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    operation = "apply" if args.apply else "rollback" if args.rollback else "plan"
    try:
        if operation != "plan" and not args.expect_plan_sha256:
            raise ValueError(
                "A reviewed --expect-plan-sha256 is required for mutations"
            )
        dsn, target = checked_dsn(args.environment, args.allow_production)
        manifest, changes = load_release()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        if args.report.is_dir():
            raise ValueError("Report path must name a file")
        connection = psycopg2.connect(dsn, connect_timeout=15)
        try:
            report = run(
                connection,
                manifest,
                changes,
                target,
                operation,
                args.expect_plan_sha256,
            )
        finally:
            connection.close()
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        return 0
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection errors can contain credentials; do not echo DSNs.
        print(str(error) if isinstance(error, ValueError) else type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
