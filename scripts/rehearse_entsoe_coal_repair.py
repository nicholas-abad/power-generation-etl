"""Full coal repair rehearsal in NEW disposable local PostgreSQL databases.

Accepts only a local socket/127.0.0.1 DSN and coal_hotfix_rehearsal_* database
names. Never loads credential files, contacts Neon, drops databases, or reuses
an existing database. A second database preserves the repaired state for the
full source-loader replay while rollback is checked against the original seed.
"""

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import repair_entsoe_coal as repair  # noqa: E402
from entsoe_coal import ALIASES  # noqa: E402

RAW = repair.TABLE
COAL = "country_code='CZ' AND psr_type IN ('B02','B03','B05')"
COLUMNS = (
    "id,extraction_run_id,created_at_ms,country_code,psr_type,plant_name,"
    "fuel_type,data_type,timestamp_ms,generation_mw,resolution_minutes"
)
YEARS = {2019: (1546300800000, 1577836800000), 2024: (1704067200000, 1735689600000)}
SCOPE = {
    "all": "true",
    "unaffected": "NOT (a.country_code='CZ' AND EXISTS (SELECT 1 FROM rehearsal_keys k WHERE (k.timestamp_ms,k.psr_type,k.plant_name)=(a.timestamp_ms,a.psr_type,a.plant_name)))",
    "2019_coal": f"{COAL} AND timestamp_ms>=1546300800000 AND timestamp_ms<1577836800000",
    "2025_coal": f"{COAL} AND timestamp_ms>=1735689600000 AND timestamp_ms<1767225600000",
    "2024_gas": "country_code='CZ' AND psr_type='B04' AND timestamp_ms>=1704067200000 AND timestamp_ms<1735689600000",
    "poland": "country_code='PL'",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def local_settings(dsn):
    parts = parse_dsn(dsn)
    require(
        set(parts) <= {"host", "port", "dbname", "user", "password"},
        "Only explicit local DSN settings are allowed",
    )
    require(
        parts.get("host") in {"127.0.0.1", "/tmp", "/private/tmp"},
        "Only local PostgreSQL is allowed",
    )
    require(
        re.fullmatch(r"coal_hotfix_rehearsal_[a-z0-9_]{1,28}", parts.get("dbname", "")),
        "Use a new coal_hotfix_rehearsal_* database",
    )
    require(
        parts.get("user") == "postgres" and parts.get("port", "5432").isdigit(),
        "Use the disposable local postgres owner",
    )
    # libpq environment/service settings cannot reroute these connections.
    for name in list(os.environ):
        if name.startswith(("PG", "POSTGRES_")) or name in {
            "DATABASE_URL",
            "DIRECT_DATABASE_URL",
        }:
            del os.environ[name]
    return parts


def connect(parts):
    connection = psycopg2.connect(
        **parts,
        connect_timeout=5,
        options="-c timezone=UTC -c search_path=public,ingestion",
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT inet_server_addr() IS NULL OR inet_server_addr()='127.0.0.1'::inet"
            )
            require(cursor.fetchone()[0], "Connection is not local")
    return connection


def create_database(parts, template=None):
    admin = connect({**parts, "dbname": "postgres"})
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            query = sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(parts["dbname"]), sql.Identifier(template or "template0")
            )
            cursor.execute(query)
    finally:
        admin.close()


def check_manifest(folder, section):
    manifest = json.loads((folder / "manifest.json").read_text())
    for entry in manifest[section].values():
        path = folder / entry["path"]
        require(
            path.resolve().parent == folder.resolve(),
            "Manifest path outside evidence folder",
        )
        require(sha(path) == entry["sha256"], "Evidence file hash mismatch")
    return manifest


def copy_seed(cursor, path, table=RAW):
    with gzip.open(path, "rt", newline="") as source:
        cursor.copy_expert(
            f"COPY {table} ({COLUMNS}) FROM STDIN WITH CSV HEADER", source
        )


def setup(connection, seed, source, changes, output):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute((ROOT / "schema/entsoe_generation.sql").read_text())
            cursor.execute((ROOT / "schema/extraction_metadata.sql").read_text())
            for role in ("etl_writer", "dashboard_ro"):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if not cursor.fetchone():
                    cursor.execute(
                        sql.SQL("CREATE ROLE {}").format(sql.Identifier(role))
                    )
            cursor.execute("GRANT USAGE,CREATE ON SCHEMA ingestion TO etl_writer")
            cursor.execute("GRANT USAGE ON SCHEMA public TO etl_writer,dashboard_ro")
            cursor.execute(
                "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA ingestion TO etl_writer"
            )
            cursor.execute(
                "GRANT USAGE ON ALL SEQUENCES IN SCHEMA ingestion TO etl_writer"
            )
            cursor.execute(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA ingestion GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO etl_writer"
            )
            for label in (
                "current2019coal",
                "current2024fossil",
                "boundary2023",
                "controls2025coal",
                "controls2024pl",
            ):
                copy_seed(cursor, seed / f"{label}.csv.gz")
            cursor.execute(
                f"CREATE TEMP TABLE original2024 (LIKE {RAW}) ON COMMIT DROP"
            )
            copy_seed(cursor, seed / "original2024fossil.csv.gz", "original2024")
            cursor.execute(
                "SELECT count(*) FROM original2024 WHERE country_code<>'CZ' OR psr_type NOT IN ('B02','B03','B04','B05') OR timestamp_ms<1704067200000 OR timestamp_ms>=1735689600000"
            )
            require(cursor.fetchone()[0] == 0, "Original backup outside reviewed scope")
            cursor.execute(f"DELETE FROM {RAW} a USING original2024 b WHERE a.id=b.id")
            cursor.execute(f"INSERT INTO {RAW} SELECT * FROM original2024")
            cursor.execute(
                f"SELECT setval(pg_get_serial_sequence('{RAW}','id'),(SELECT max(id) FROM {RAW}))"
            )
            cursor.execute(
                "CREATE TABLE public.rehearsal_keys (timestamp_ms bigint,psr_type text,plant_name text,PRIMARY KEY(timestamp_ms,psr_type,plant_name))"
            )
            execute_values(
                cursor,
                "INSERT INTO rehearsal_keys VALUES %s",
                [repair.key(row) for row in changes],
                page_size=5000,
            )
            cursor.execute(
                "CREATE TABLE public.expected_coal (timestamp_ms bigint,psr_type text,plant_name text,fuel_type text,generation_mw double precision,resolution_minutes int,PRIMARY KEY(timestamp_ms,psr_type,plant_name))"
            )
            expected_path = output / "expected-coal.csv"
            names = {
                (a["psr_type"], a["source_name"]): a["legacy_name"] for a in ALIASES
            }
            monthly = defaultdict(Decimal)
            with expected_path.open("w", newline="") as expected_file:
                writer = csv.writer(expected_file)
                for year in YEARS:
                    with (source / f"coal{year}.jsonl").open() as records:
                        for line in records:
                            row = json.loads(line)
                            name = names.get(
                                (row["psr_type"], row["plant_name"]), row["plant_name"]
                            )
                            writer.writerow(
                                [
                                    row["timestamp_ms"],
                                    row["psr_type"],
                                    name,
                                    row["fuel_type"],
                                    row["generation_mw"],
                                    row["resolution_minutes"],
                                ]
                            )
                            month = datetime.fromtimestamp(
                                row["timestamp_ms"] / 1000, timezone.utc
                            ).strftime("%Y-%m")
                            monthly[(month, name, row["fuel_type"])] += (
                                Decimal(str(row["generation_mw"]))
                                * row["resolution_minutes"]
                                / 60
                            )
            with expected_path.open() as expected_file:
                cursor.copy_expert(
                    "COPY expected_coal FROM STDIN WITH CSV", expected_file
                )
            cursor.execute(
                "CREATE TABLE public.plant_crosswalk (source_system text,plant_name text,plant_code text,gem_location_id text)"
            )
            cursor.execute(
                "CREATE TABLE public.gem_units (gem_location_id text,tracker text)"
            )
            with gzip.open(
                seed / "frontend_crosswalk.csv.gz", "rt", newline=""
            ) as mappings:
                for row in csv.DictReader(mappings):
                    cursor.execute(
                        "INSERT INTO plant_crosswalk VALUES ('ENTSOE',%s,%s,%s)",
                        tuple(
                            row[key] or None
                            for key in ("plant_name", "plant_code", "gem_location_id")
                        ),
                    )
                    if row["gcpt_included"] == "t":
                        cursor.execute(
                            "INSERT INTO gem_units VALUES (%s,'GCPT')",
                            (row["gem_location_id"],),
                        )
            cursor.execute("GRANT SELECT ON plant_crosswalk,gem_units TO dashboard_ro")
            for filename, view in (
                ("materialized_views.sql", "mv_entsoe_plant_monthly"),
                ("row_count_views.sql", "mv_entsoe_row_counts"),
            ):
                content = (ROOT / "schema" / filename).read_text()
                statements = re.findall(
                    rf"CREATE MATERIALIZED VIEW IF NOT EXISTS {view} AS.*?;|CREATE UNIQUE INDEX IF NOT EXISTS ux_{view}\s+ON {view}.*?;",
                    content,
                    re.S,
                )
                require(
                    len(statements) == 2, "Existing materialized-view DDL not found"
                )
                cursor.execute("\n".join(statements))
                cursor.execute(
                    sql.SQL("GRANT SELECT ON {} TO dashboard_ro").format(
                        sql.Identifier(view)
                    )
                )
            cursor.execute(f"ANALYZE {RAW}")
            cursor.execute("ANALYZE expected_coal; ANALYZE rehearsal_keys")
    return monthly


class HashSink:
    def __init__(self):
        self.digest = hashlib.sha256()

    def write(self, data):
        self.digest.update(data.encode() if isinstance(data, str) else data)
        return len(data)


def fingerprints(cursor):
    result = {}
    for label, predicate in SCOPE.items():
        cursor.execute(f"SELECT count(*) FROM {RAW} a WHERE {predicate}")
        count = cursor.fetchone()[0]
        sink = HashSink()
        cursor.copy_expert(
            f"COPY (SELECT {COLUMNS} FROM {RAW} a WHERE {predicate} ORDER BY id) TO STDOUT WITH CSV",
            sink,
        )
        result[label] = {"rows": count, "sha256": sink.digest.hexdigest()}
    return result


def frontend(cursor):
    cursor.execute("SET LOCAL ROLE dashboard_ro")
    cursor.execute("""SELECT id,count(*)::int AS months,sum(generation) AS generation_mwh,
        sum(extract(epoch FROM (month_start+interval '1 month')-month_start)/3600) AS hours
        FROM (
            SELECT c.gem_location_id AS id,
                date_trunc('month',g.month::timestamptz AT TIME ZONE 'UTC') AS month_start,
                sum(g.generation_mwh) AS generation
            FROM mv_entsoe_plant_monthly g
            JOIN plant_crosswalk c ON c.source_system='ENTSOE' AND coalesce(c.plant_code,c.plant_name)=g.plant_name
            WHERE g.fuel_type IN ('Fossil Hard coal','Fossil Brown coal/Lignite')
                AND g.month>='2024-01-01' AND g.month<'2025-01-01'
                AND g.generation_mwh IS NOT NULL AND c.gem_location_id IS NOT NULL
                AND EXISTS(SELECT 1 FROM gem_units u WHERE u.gem_location_id=c.gem_location_id AND u.tracker='GCPT')
            GROUP BY c.gem_location_id,month_start
        ) monthly GROUP BY id ORDER BY id""")
    rows = [
        dict(
            zip(
                ("gem_location_id", "months", "generation_mwh", "hours"),
                row,
                strict=True,
            )
        )
        for row in cursor.fetchall()
    ]
    for row in rows:
        row["generation_mwh"] = Decimal(str(row["generation_mwh"])).quantize(
            Decimal("0.001")
        )
    cursor.execute("RESET ROLE")
    return rows


def snapshot(connection):
    with connection:
        with connection.cursor() as cursor:
            hashes = fingerprints(cursor)
            cursor.execute(f"""SELECT to_char(to_timestamp(timestamp_ms/1000),'YYYY-MM') AS month,
                plant_name,fuel_type,count(*),sum(generation_mw::numeric*resolution_minutes/60) AS mwh
                FROM {RAW} WHERE {COAL} GROUP BY 1,2,3 ORDER BY 1,2,3""")
            monthly = [list(row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT month::text,row_count FROM mv_entsoe_row_counts ORDER BY month"
            )
            row_counts = [list(row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT month::text,plant_name,country_code,fuel_type,generation_mwh::text FROM mv_entsoe_plant_monthly ORDER BY 1,2,3,4"
            )
            plant_monthly = [list(row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT * FROM plant_crosswalk ORDER BY source_system,plant_name,plant_code,gem_location_id"
            )
            mapping_hash = repair.digest(cursor.fetchall())
            return {
                "fingerprints": hashes,
                "coal_monthly": monthly,
                "row_counts": row_counts,
                "plant_monthly": plant_monthly,
                "crosswalk_sha256": mapping_hash,
                "frontend_2024": frontend(cursor),
            }


def same_snapshot(left, right):
    """Raw rows are exact; parallel float sums may differ below 0.00001 MWh.

    Hashing rounded sums is unsafe at a rounding boundary: e.g. 641.1425000000006
    and 641.142499999999 hash differently despite a 0.0000000000016 MWh delta.
    """
    if {k: v for k, v in left.items() if k != "plant_monthly"} != {
        k: v for k, v in right.items() if k != "plant_monthly"
    }:
        return False
    a, b = left["plant_monthly"], right["plant_monthly"]
    return len(a) == len(b) and all(
        old[:4] == new[:4]
        and abs(Decimal(old[4]) - Decimal(new[4])) < Decimal("0.00001")
        for old, new in zip(a, b, strict=True)
    )


def refresh(parts):
    # Call the real refresh implementation, blocking implicit dotenv discovery.
    with patch("dotenv.load_dotenv", return_value=False):
        import refresh_views
    url = URL.create(
        "postgresql+psycopg2",
        username=parts["user"],
        password=parts.get("password"),
        host=parts["host"],
        port=int(parts.get("port", 5432)),
        database=parts["dbname"],
    )
    engine = create_engine(url)
    try:
        with (
            patch.object(refresh_views, "get_connection_url", return_value=url),
            patch.object(refresh_views, "create_engine", return_value=engine),
        ):
            require(
                refresh_views.refresh_views(refresh_views.SOURCE_VIEWS["entsoe"]),
                "Materialized-view refresh failed",
            )
    finally:
        engine.dispose()


def reconcile(connection, monthly):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""WITH actual AS (
                SELECT * FROM {RAW} WHERE {COAL} AND
                ((timestamp_ms>=1546300800000 AND timestamp_ms<1577836800000) OR
                 (timestamp_ms>=1704067200000 AND timestamp_ms<1735689600000))
            ) SELECT count(*) FROM actual a FULL JOIN expected_coal e USING(timestamp_ms,psr_type,plant_name)
              WHERE a.id IS NULL OR e.timestamp_ms IS NULL OR
              (a.generation_mw,a.resolution_minutes,a.fuel_type) IS DISTINCT FROM
              (e.generation_mw,e.resolution_minutes,e.fuel_type)""")
            require(
                cursor.fetchone()[0] == 0,
                "Raw annual observations do not exactly match source",
            )
            cursor.execute("""SELECT to_char(month,'YYYY-MM'),plant_name,fuel_type,generation_mwh
                FROM mv_entsoe_plant_monthly WHERE country_code='CZ'
                AND fuel_type IN ('Fossil Brown coal/Lignite','Fossil Coal-derived gas','Fossil Hard coal')
                AND (extract(year FROM month)=2019 OR extract(year FROM month)=2024)""")
            actual = {
                (month, name, fuel): Decimal(str(value))
                for month, name, fuel, value in cursor.fetchall()
            }
            require(
                set(actual) == set(monthly),
                "Monthly view has missing or extra source groups",
            )
            maximum = max(abs(value - actual[key]) for key, value in monthly.items())
            require(
                maximum < Decimal("0.00001"),
                "Monthly view differs from source beyond float tolerance",
            )
            cursor.execute(f"""WITH expected AS (
                SELECT date_trunc('month',to_timestamp(timestamp_ms/1000))::date AS month,count(*) AS row_count
                FROM {RAW} GROUP BY 1)
                SELECT count(*) FROM expected e FULL JOIN mv_entsoe_row_counts v USING(month)
                WHERE e.row_count IS DISTINCT FROM v.row_count""")
            require(cursor.fetchone()[0] == 0, "Row-count view differs from raw data")
            cursor.execute(
                f"SELECT sum(generation_mw::numeric*resolution_minutes/60) FROM {RAW} WHERE {COAL} AND timestamp_ms=1704063600000"
            )
            require(
                cursor.fetchone()[0] == Decimal("1255.900"),
                "2023 boundary energy mismatch",
            )
    return {
        "missing_extra_or_mismatched_raw_observations": 0,
        "source_monthly_groups": len(monthly),
        "maximum_monthly_absolute_error_mwh": str(maximum),
        "row_count_mismatches": 0,
    }


def loader_replay(parts, source, connection, monthly):
    with patch("dotenv.load_dotenv", return_value=False):
        from database import PowerGenerationDatabase
    engine = create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=parts["user"],
            password=parts.get("password"),
            host=parts["host"],
            port=int(parts.get("port", 5432)),
            database=parts["dbname"],
        ),
        connect_args={
            "options": "-c timezone=UTC -c search_path=ingestion,public -c role=etl_writer"
        },
    )
    database = PowerGenerationDatabase.__new__(PowerGenerationDatabase)
    database._engine = engine
    report = {"role": "etl_writer", "runs": []}
    baseline = snapshot(connection)
    try:
        prior = None
        for iteration in (1, 2):
            start = time.perf_counter()
            for year in YEARS:
                ok, validation = database.insert_entsoe_jsonl_data(
                    str(source / f"coal{year}.jsonl"), batch_size=100000
                )
                require(
                    ok
                    and validation.invalid_count == 0
                    and validation.duplicate_count == 0,
                    "Full source loader replay failed validation",
                )
                require(
                    validation.valid_count == (224876 if year == 2019 else 530160),
                    "Incomplete loader replay",
                )
            refresh(parts)
            verified = reconcile(connection, monthly)
            after = snapshot(connection)
            for label in ("2025_coal", "2024_gas", "poland"):
                require(
                    after["fingerprints"][label] == baseline["fingerprints"][label],
                    "Loader changed an excluded population",
                )
            require(
                after["fingerprints"]["all"]["rows"]
                == baseline["fingerprints"]["all"]["rows"],
                "Source replay inserted duplicate observations",
            )
            require(
                after["crosswalk_sha256"] == baseline["crosswalk_sha256"],
                "Source replay changed mappings",
            )
            if prior:
                require(
                    same_snapshot(after, prior),
                    "Repeat source replay was not a raw-row no-op",
                )
            report["runs"].append(
                {
                    "iteration": iteration,
                    "seconds": round(time.perf_counter() - start, 3),
                    "verification": verified,
                    "all_rows": after["fingerprints"]["all"],
                    "repeat_raw_rows_identical": prior is not None,
                }
            )
            prior = after
    finally:
        engine.dispose()
    return report


def main(args):
    parts = local_settings(args.dsn)
    loader_parts = {**parts, "dbname": parts["dbname"] + "_loader"}
    seed_manifest = check_manifest(args.seed, "files")
    source_manifest = check_manifest(args.source, "years")
    manifest, changes = repair.load_release()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {
        "status": "running",
        "scope": "isolated old-schema reconstruction; not a production snapshot",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "database": parts["dbname"],
        "extractor_commit": source_manifest["extractor_commit"],
        "etl_code_commit": subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip(),
        "repair_artifact_sha256": manifest["artifact_sha256"],
        "seed_manifest_sha256": sha(args.seed / "manifest.json"),
        "source_manifest_sha256": sha(args.source / "manifest.json"),
        "staging_seed_snapshot_at": seed_manifest["snapshot_at"],
        "production_accessed": False,
        "frontend_query_commit": "26f6b71b825958db483d199a437bb9104a5fe21e",
        "frontend_scope": "ENTSOE portion of period-generation.ts with staged CZ crosswalk and GCPT membership; no other providers, browser, or production checks",
        "timings_seconds": {},
        "view_comparison_absolute_tolerance_mwh": "0.00001",
    }

    def save():
        (args.output / "report.json").write_text(
            json.dumps(report, indent=2, default=str) + "\n"
        )

    def timed(label, function):
        print(f"Starting {label}", flush=True)
        start = time.perf_counter()
        result = function()
        report["timings_seconds"][label] = round(time.perf_counter() - start, 3)
        print(f"Passed {label}: {report['timings_seconds'][label]}s", flush=True)
        save()
        return result

    create_database(parts)
    connection = connect(parts)
    target = "local/" + parts["dbname"]
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT version()")
                report["postgres"] = cursor.fetchone()[0]
        monthly = timed(
            "seed_and_old_schema",
            lambda: setup(connection, args.seed, args.source, changes, args.output),
        )
        before = timed("baseline_snapshot", lambda: snapshot(connection))
        report["before"] = before
        require(
            before["fingerprints"]["all"]["rows"] == 1676016,
            "Reconstructed population differs from audited seed",
        )
        require(
            sum(row[4] for row in before["coal_monthly"] if row[0].startswith("2024"))
            == Decimal("12219119.700"),
            "Reconstructed 2024 baseline disagrees with original audit",
        )
        plan = timed("plan", lambda: repair.run(connection, manifest, changes, target))
        report["plan"] = plan
        require(
            (
                plan["interval_updates"],
                plan["duplicate_deletes"],
                Decimal(plan["mwh_change"]),
            )
            == (104208, 12, Decimal("6564553.500")),
            "Full repair plan differs from release",
        )
        require(
            same_snapshot(snapshot(connection), before),
            "Read-only plan changed the baseline",
        )
        report["apply"] = timed(
            "apply",
            lambda: repair.run(
                connection, manifest, changes, target, "apply", plan["plan_sha256"]
            ),
        )
        timed("refresh_after_apply", lambda: refresh(parts))
        report["source_reconciliation"] = timed(
            "source_reconciliation", lambda: reconcile(connection, monthly)
        )
        after = timed("after_snapshot", lambda: snapshot(connection))
        report["after"] = after
        require(
            after["fingerprints"]["all"]["rows"]
            == before["fingerprints"]["all"]["rows"] - 12,
            "Unexpected raw row count after repair",
        )
        for label in SCOPE.keys() - {"all"}:
            require(
                before["fingerprints"][label] == after["fingerprints"][label],
                f"Repair changed {label}",
            )
        require(
            before["crosswalk_sha256"] == after["crosswalk_sha256"],
            "Repair changed mappings",
        )
        require(
            sum(row["generation_mwh"] for row in before["frontend_2024"])
            == Decimal("2004460.450")
            and sum(row["generation_mwh"] for row in after["frontend_2024"])
            == Decimal("2863828.750"),
            "Mapped frontend projection disagrees with independent audit",
        )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT count(*) FROM {repair.BACKUP}")
                require(cursor.fetchone()[0] == 104220, "Missing backup rows")
                permissions = {}
                for role in ("etl_writer", "dashboard_ro"):
                    for table in (repair.BACKUP, repair.LEDGER):
                        cursor.execute(
                            "SELECT has_table_privilege(%s,%s,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')",
                            (role, table),
                        )
                        permissions[f"{role}:{table}"] = cursor.fetchone()[0]
                require(
                    not any(permissions.values()),
                    "Routine role has repair backup/ledger access",
                )
                report["backup"] = {"rows": 104220, "routine_role_access": permissions}
        corrected_plan = repair.run(connection, manifest, changes, target)
        report["repeat_apply"] = timed(
            "repeat_apply",
            lambda: repair.run(
                connection,
                manifest,
                changes,
                target,
                "apply",
                corrected_plan["plan_sha256"],
            ),
        )
        require(
            report["repeat_apply"]["rows_written"] == 0
            and same_snapshot(snapshot(connection), after),
            "Repeat repair changed data",
        )
        # Clone while no sessions are connected; loader and rollback cannot alter
        # each other's evidence. Both databases remain local and disposable.
        connection.close()
        timed(
            "clone_for_loader", lambda: create_database(loader_parts, parts["dbname"])
        )
        connection = connect(parts)
        report["rollback"] = timed(
            "rollback",
            lambda: repair.run(
                connection,
                manifest,
                changes,
                target,
                "rollback",
                corrected_plan["plan_sha256"],
            ),
        )
        timed("refresh_after_rollback", lambda: refresh(parts))
        restored = timed("rollback_snapshot", lambda: snapshot(connection))
        require(
            same_snapshot(restored, before),
            "Rollback did not exactly restore complete rows, mappings, or views",
        )
        report["rollback_verification"] = {
            "all_rows_and_metadata_restored": True,
            "views_and_mappings_restored": True,
            "all_rows": restored["fingerprints"]["all"],
        }
        restored_plan = repair.run(connection, manifest, changes, target)
        report["repeat_rollback"] = timed(
            "repeat_rollback",
            lambda: repair.run(
                connection,
                manifest,
                changes,
                target,
                "rollback",
                restored_plan["plan_sha256"],
            ),
        )
        require(
            report["repeat_rollback"]["rows_written"] == 0
            and same_snapshot(snapshot(connection), before),
            "Repeat rollback changed data",
        )
        connection.close()
        connection = connect(loader_parts)
        report["loader_replay"] = timed(
            "full_loader_replay_twice",
            lambda: loader_replay(loader_parts, args.source, connection, monthly),
        )
        report["status"] = "passed"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["limitations"] = [
            "Seed is a staging reconstruction: original backup metadata for affected 2024 rows, current staging metadata elsewhere; not a current production snapshot.",
            "Local PostgreSQL 14 uses owner-run view refresh. Neon 17 pg_maintain authorization and production-scale refresh/lock timing are not established by this rehearsal.",
            "2025, gas, and Polish controls are unchanged; their source correctness was not certified.",
            "Frontend check is its pinned ENTSOE query projection with staging mappings, not the complete live frontend total.",
            "No GitHub CI, publication, main merge, production preflight, or production execution occurred.",
        ]
        save()
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = f"{type(error).__name__}: {error}"
        save()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args())
