"""Apply and restore one audited year's candidates in a NEW local database only.

Seeds all exported years as controls, backs up full affected rows, checks every
repaired source observation, refreshes the real materialized views, then restores
the original rows and proves the complete table and views are unchanged.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from local_postgres import local_settings, verify_local_connection  # noqa: E402
from rehearse_entsoe_coal_repair import same_snapshot  # noqa: E402

COLUMNS = "id,extraction_run_id,created_at_ms,country_code,psr_type,plant_name,fuel_type,data_type,timestamp_ms,generation_mw,resolution_minutes"
RAW = "ingestion.entsoe_generation_data"
VIEWS = ("mv_entsoe_plant_monthly", "mv_entsoe_row_counts")


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class HashSink:
    def __init__(self):
        self.digest = hashlib.sha256()

    def write(self, value):
        self.digest.update(value.encode() if isinstance(value, str) else value)
        return len(value)


def fingerprint(cursor, query):
    sink = HashSink()
    cursor.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV", sink)
    return sink.digest.hexdigest()


def snapshot(cursor):
    result = {
        "all_rows": fingerprint(cursor, f"SELECT {COLUMNS} FROM {RAW} ORDER BY id")
    }
    cursor.execute(
        f"SELECT month::text,plant_name,country_code,fuel_type,generation_mwh::text FROM {VIEWS[0]} ORDER BY 1,2,3,4"
    )
    result["plant_monthly"] = [list(row) for row in cursor.fetchall()]
    result[VIEWS[1]] = fingerprint(cursor, f"SELECT * FROM {VIEWS[1]} ORDER BY 1")
    return result


def normalized(row):
    result = {key: str(value) for key, value in row.items()}
    result["generation_mw"] = Decimal(result["generation_mw"])
    return result


def main(args):
    audit = json.loads((args.audit / "audit.json").read_text())
    require(
        audit["status"] == "source_reconciled"
        and not audit["unresolved_difference_keys"],
        "Resolve source differences before rehearsal",
    )
    require(
        audit["repair_candidates"]["rows"] > 0, "This year has no repairs to rehearse"
    )
    payload = args.audit / audit["repair_candidates"]["path"]
    expected = args.audit / audit["source_observations"]["path"]
    require(
        sha(payload) == audit["repair_candidates"]["sha256"], "Candidate hash mismatch"
    )
    require(
        sha(expected) == audit["source_observations"]["sha256"],
        "Expected-source hash mismatch",
    )
    require(
        sha(ROOT / "config/entsoe-coal-aliases.json")
        == audit["alias_configuration_sha256"],
        "Reviewed aliases changed",
    )
    with gzip.open(payload, "rt") as handle:
        changes = [json.loads(line) for line in handle]
    require(
        len(changes) == audit["repair_candidates"]["rows"], "Candidate count mismatch"
    )
    require(
        len({item["before"]["id"] for item in changes}) == len(changes),
        "Duplicate candidate ID",
    )
    seed = json.loads((args.staging / "manifest.json").read_text())
    audited_year = str(
        datetime.fromisoformat(audit["start"].replace("Z", "+00:00")).year
    )
    require(
        seed["files"][audited_year]["sha256"] == audit["staging"]["sha256"],
        "Rehearsal seed is not the audited annual snapshot",
    )
    for field in ("snapshot_at", "branch_id", "endpoint_id", "transaction_read_only"):
        require(
            seed[field] == audit["staging"][field],
            "Rehearsal seed identity differs from audit",
        )
    for label, entry in seed["files"].items():
        if label.isdigit():
            require(
                sha(args.staging / entry["path"]) == entry["sha256"],
                "Staging seed hash mismatch",
            )
    parts = local_settings(args.dsn, r"coal_hotfix_rehearsal_history_[a-z0-9_]{1,20}")
    admin_parts = {**parts, "dbname": "postgres"}
    admin = psycopg2.connect(**admin_parts, connect_timeout=5)
    admin.autocommit = True
    try:
        verify_local_connection(admin, "postgres")
        with admin.cursor() as cursor:
            # Intentionally no IF EXISTS/DROP: never reuse a database.
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(parts["dbname"]))
            )
    finally:
        admin.close()
    report = {
        "status": "running",
        "database": parts["dbname"],
        "scope": "new local disposable PostgreSQL only",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "candidate_sha256": sha(payload),
        "audit_sha256": sha(args.audit / "audit.json"),
        "script_sha256": sha(Path(__file__)),
        "view_comparison_helper_sha256": sha(
            ROOT / "scripts/rehearse_entsoe_coal_repair.py"
        ),
        "schema_sha256": {
            name: sha(ROOT / "schema" / name)
            for name in (
                "entsoe_generation.sql",
                "materialized_views.sql",
                "row_count_views.sql",
            )
        },
        "seed_manifest_sha256": sha(args.staging / "manifest.json"),
        "seed_files_sha256": {
            label: entry["sha256"]
            for label, entry in seed["files"].items()
            if label.isdigit()
        },
    }
    args.output.mkdir(parents=True, exist_ok=False)
    connection = psycopg2.connect(**parts, connect_timeout=5)
    try:
        with connection:
            verify_local_connection(connection, parts["dbname"])
            with connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                cursor.execute((ROOT / "schema/entsoe_generation.sql").read_text())
                for label, entry in seed["files"].items():
                    if not label.isdigit():
                        continue
                    with gzip.open(args.staging / entry["path"], "rt") as handle:
                        cursor.copy_expert(
                            f"COPY {RAW} ({COLUMNS}) FROM STDIN WITH CSV HEADER", handle
                        )
                    print(f"Seeded {label}: {entry['rows']:,} control rows", flush=True)
                for filename, view in (
                    ("materialized_views.sql", VIEWS[0]),
                    ("row_count_views.sql", VIEWS[1]),
                ):
                    ddl = (ROOT / "schema" / filename).read_text()
                    statements = re.findall(
                        rf"CREATE MATERIALIZED VIEW IF NOT EXISTS {view} AS.*?;|CREATE UNIQUE INDEX IF NOT EXISTS ux_{view}\s+ON {view}.*?;",
                        ddl,
                        re.S,
                    )
                    require(
                        len(statements) == 2, "Expected materialized-view DDL changed"
                    )
                    cursor.execute("\n".join(statements))
                cursor.execute(
                    "CREATE TABLE expected_source (timestamp_ms bigint, psr_type text, plant_name text, generation_mw numeric, resolution_minutes int, unit_eic text)"
                )
                with gzip.open(expected, "rt") as handle:
                    cursor.copy_expert(
                        "COPY expected_source FROM STDIN WITH CSV HEADER", handle
                    )
                cursor.execute(
                    "CREATE UNIQUE INDEX ON expected_source(timestamp_ms,psr_type,plant_name)"
                )
                cursor.execute(f"CREATE TABLE repair_backup (LIKE {RAW} INCLUDING ALL)")
                cursor.execute(
                    "CREATE TABLE planned_changes (id bigint PRIMARY KEY,action text,source_minutes int,retained_id bigint)"
                )
                execute_values(
                    cursor,
                    "INSERT INTO planned_changes VALUES %s",
                    [
                        (
                            int(c["before"]["id"]),
                            c["action"],
                            c["source_minutes"],
                            int(c["retained_id"]) if c.get("retained_id") else None,
                        )
                        for c in changes
                    ],
                )
                cursor.execute(
                    "CREATE TABLE reviewed_aliases (psr_type text,source_name text,legacy_name text)"
                )
                aliases = json.loads(
                    (ROOT / "config/entsoe-coal-aliases.json").read_text()
                )["aliases"]
                execute_values(
                    cursor,
                    "INSERT INTO reviewed_aliases VALUES %s",
                    [
                        (a["psr_type"], a["source_name"], a["legacy_name"])
                        for a in aliases
                    ],
                )
        with connection:
            with connection.cursor() as cursor:
                before = snapshot(cursor)
                report["before"] = before
                report["monthly_view_absolute_tolerance_mwh"] = "0.00001"
                control_sql = f"SELECT {COLUMNS} FROM {RAW} WHERE id NOT IN (SELECT id FROM planned_changes) ORDER BY id"
                control_hash = fingerprint(cursor, control_sql)
                cursor.execute(
                    f"SELECT {COLUMNS} FROM {RAW} WHERE id IN (SELECT id FROM planned_changes UNION SELECT retained_id FROM planned_changes WHERE retained_id IS NOT NULL)"
                )
                current = {
                    str(row[0]): dict(zip(COLUMNS.split(","), row, strict=True))
                    for row in cursor.fetchall()
                }
                for change in changes:
                    original = change["before"]
                    require(
                        normalized(current[original["id"]]) == normalized(original),
                        "Candidate before-image changed",
                    )
                    require(
                        change["action"] in {"duplicate", "interval"},
                        "Unknown candidate action",
                    )
                    if change["action"] == "duplicate":
                        retained = current[change["retained_id"]]
                        for key in (
                            "timestamp_ms",
                            "country_code",
                            "psr_type",
                            "resolution_minutes",
                        ):
                            require(
                                str(retained[key]) == original[key],
                                "Retained observation identity/duration changed",
                            )
                        require(
                            Decimal(str(retained["generation_mw"]))
                            == Decimal(original["generation_mw"]),
                            "Retained observation MW changed",
                        )
                        require(
                            retained["fuel_type"] == original["fuel_type"]
                            and retained["data_type"]
                            in {"Actual Aggregated", "Unknown", original["fuel_type"]},
                            "Retained observation fuel/metric changed",
                        )
                cursor.execute(
                    f"INSERT INTO repair_backup SELECT a.* FROM {RAW} a JOIN planned_changes p USING(id)"
                )
                require(cursor.rowcount == len(changes), "Incomplete full-row backup")
                with gzip.open(args.output / "affected-before.csv.gz", "wt") as handle:
                    cursor.copy_expert(
                        f"COPY (SELECT {COLUMNS} FROM repair_backup ORDER BY id) TO STDOUT WITH CSV HEADER",
                        handle,
                    )
                cursor.execute(
                    f"DELETE FROM {RAW} a USING planned_changes p WHERE a.id=p.id AND p.action='duplicate'"
                )
                report["deleted"] = cursor.rowcount
                cursor.execute(
                    f"UPDATE {RAW} a SET resolution_minutes=p.source_minutes FROM planned_changes p WHERE a.id=p.id AND p.action='interval'"
                )
                report["updated"] = cursor.rowcount
                require(
                    report["deleted"] + report["updated"] == len(changes),
                    "Candidate write count mismatch",
                )
                for view in VIEWS:
                    cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
                require(
                    fingerprint(cursor, control_sql) == control_hash,
                    "Control rows changed",
                )
                lower = int(
                    datetime.fromisoformat(
                        audit["start"].replace("Z", "+00:00")
                    ).timestamp()
                    * 1000
                )
                upper = int(
                    datetime.fromisoformat(
                        audit["end"].replace("Z", "+00:00")
                    ).timestamp()
                    * 1000
                )
                predicate = "country_code='CZ' AND psr_type IN ('B02','B03','B05') AND timestamp_ms>=%s AND timestamp_ms<%s"
                cursor.execute(
                    f"SELECT count(*),sum(generation_mw::numeric*resolution_minutes/60) FROM {RAW} WHERE {predicate}",
                    (lower, upper),
                )
                count, energy = cursor.fetchone()
                require(
                    count == audit["source_rows"]
                    and energy == Decimal(audit["source_mwh"]),
                    "Repaired annual count/energy mismatch",
                )
                cursor.execute(
                    f"""WITH actual AS (
                    SELECT a.timestamp_ms,a.psr_type,coalesce(n.legacy_name,a.plant_name) AS plant_name,
                           a.generation_mw::numeric AS generation_mw,a.resolution_minutes,a.fuel_type,a.data_type
                    FROM {RAW} a LEFT JOIN reviewed_aliases n ON a.psr_type=n.psr_type AND a.plant_name=n.source_name
                    WHERE {predicate.replace("country_code", "a.country_code").replace("psr_type IN", "a.psr_type IN").replace("timestamp_ms", "a.timestamp_ms")}
                ) SELECT count(*) FROM actual a FULL OUTER JOIN expected_source s USING(timestamp_ms,psr_type,plant_name)
                  WHERE a.timestamp_ms IS NULL OR s.timestamp_ms IS NULL
                     OR a.generation_mw IS DISTINCT FROM s.generation_mw
                     OR a.resolution_minutes IS DISTINCT FROM s.resolution_minutes
                     OR a.fuel_type IS DISTINCT FROM CASE s.psr_type WHEN 'B02' THEN 'Fossil Brown coal/Lignite' WHEN 'B03' THEN 'Fossil Coal-derived gas' WHEN 'B05' THEN 'Fossil Hard coal' END
                     OR a.data_type NOT IN ('Actual Aggregated','Unknown',a.fuel_type)""",
                    (lower, upper),
                )
                require(
                    cursor.fetchone()[0] == 0,
                    "Repaired observations differ from source",
                )
                cursor.execute(
                    "SELECT to_char(to_timestamp(timestamp_ms/1000),'YYYY-MM'), sum(generation_mw*resolution_minutes/60) FROM expected_source GROUP BY 1 ORDER BY 1"
                )
                source_monthly = {month: value for month, value in cursor.fetchall()}
                cursor.execute(
                    "SELECT to_char(month,'YYYY-MM'), sum(generation_mwh) FROM mv_entsoe_plant_monthly WHERE country_code='CZ' AND month>=to_timestamp(%s/1000) AND month<to_timestamp(%s/1000) GROUP BY 1 ORDER BY 1",
                    (lower, upper),
                )
                view_monthly = {
                    month: Decimal(str(value)) for month, value in cursor.fetchall()
                }
                require(
                    view_monthly.keys() == source_monthly.keys()
                    and all(
                        abs(view_monthly[month] - value) < Decimal("0.00001")
                        for month, value in source_monthly.items()
                    ),
                    "Monthly materialized view differs from source",
                )
                report.update(
                    after=snapshot(cursor),
                    source_rows=count,
                    source_mwh=str(energy),
                    source_mismatches=0,
                    monthly_view_matches_source=True,
                    monthly_mwh={
                        month: str(value) for month, value in source_monthly.items()
                    },
                    unchanged_control_sha256=control_hash,
                )
        # The repair is committed above. Restore from persisted full-row backup
        # in another transaction, as a real operational rollback would require.
        with connection:
            with connection.cursor() as cursor:
                require(
                    same_snapshot(snapshot(cursor), report["after"]),
                    "Data changed before rollback",
                )
                cursor.execute(
                    f"UPDATE {RAW} a SET resolution_minutes=b.resolution_minutes FROM repair_backup b JOIN planned_changes p USING(id) WHERE a.id=b.id AND p.action='interval'"
                )
                cursor.execute(
                    f"INSERT INTO {RAW} ({COLUMNS}) SELECT {','.join('b.' + c for c in COLUMNS.split(','))} FROM repair_backup b JOIN planned_changes p USING(id) WHERE p.action='duplicate'"
                )
                for view in VIEWS:
                    cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
                after_rollback = snapshot(cursor)
                report["after_rollback"] = after_rollback
                require(
                    same_snapshot(after_rollback, before),
                    "Rollback failed to restore all table/view data",
                )
                report.update(
                    before=before,
                    after_rollback=after_rollback,
                    rollback_exact=True,
                    status="passed",
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )
        print(
            f"PASS: {report['deleted']:,} deletes / {report['updated']:,} interval fixes; exact source match and full rollback",
            flush=True,
        )
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__)
        raise
    finally:
        connection.close()
        (args.output / "rehearsal.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args())
