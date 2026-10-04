"""Prepare or explicitly apply the audited Czech 2024 staging data repair.

Default: read-only plan (temporary comparison tables only). --apply archives
every affected original row in ingestion.entsoe_cz_2024_repair_backup, fixes
15-minute labels on hourly observations, and removes proven duplicate copies.
Never runs outside the pinned staging environment wrapper.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path

import psycopg2

from check_entsoe_staging import (
    COAL,
    JOIN,
    MEASUREMENTS,
    check_benchmark,
    check_source,
    load_expected,
    predicate,
)
from entsoe_identity import prepare_record

LEGACY_COAL_SHA256 = "11a9deef64f321eb1b2b7d3958a82e041312962cb67f2fd15a29fa4690d2096c"
BACKUP = "ingestion.entsoe_cz_2024_repair_backup"


def prepare_repair(cursor, apply=False, expected_counts=(117216, 16)):
    """entsoe_expected must contain the verified, aliased 2024 source rows."""
    if apply:
        # Keep the reviewed snapshot and its mutation in one transaction.
        cursor.execute(
            "LOCK TABLE ingestion.entsoe_generation_data IN SHARE ROW EXCLUSIVE MODE"
        )
    cursor.execute(
        "CREATE TEMP TABLE entsoe_current ON COMMIT DROP AS "
        "SELECT * FROM ingestion.entsoe_generation_data WHERE " + predicate(2024)
    )
    cursor.execute(
        "CREATE UNIQUE INDEX ON entsoe_current(timestamp_ms,country_code,psr_type,plant_name)"
    )
    cursor.execute("ANALYZE entsoe_current")
    cursor.execute(f"""SELECT count(*) FROM entsoe_expected e JOIN entsoe_current a ON {JOIN}
        WHERE a.generation_mw IS DISTINCT FROM e.generation_mw OR a.fuel_type IS DISTINCT FROM e.fuel_type
        OR (a.resolution_minutes IS DISTINCT FROM e.resolution_minutes
            AND NOT (a.resolution_minutes=15 AND e.resolution_minutes=60))""")
    if cursor.fetchone()[0]:
        raise ValueError("Unreviewed MW, fuel or interval changes; no repair allowed")
    cursor.execute(f"""CREATE TEMP TABLE entsoe_repairs ON COMMIT DROP AS
        SELECT a.id, 'interval'::text AS action, e.resolution_minutes AS new_resolution
        FROM entsoe_expected e JOIN entsoe_current a ON {JOIN}
        WHERE a.resolution_minutes=15 AND e.resolution_minutes=60""")
    canonical_join = JOIN.replace("a.", "c.")
    cursor.execute(f"""INSERT INTO entsoe_repairs
        SELECT a.id,'duplicate',NULL::integer FROM entsoe_current a
        JOIN entsoe_expected e ON a.timestamp_ms=e.timestamp_ms AND a.country_code=e.country_code
            AND a.psr_type=e.psr_type AND a.plant_name=e.source_unit_name AND a.plant_name<>e.plant_name
        JOIN entsoe_current c ON {canonical_join}
        WHERE a.generation_mw=c.generation_mw AND a.resolution_minutes=c.resolution_minutes
            AND a.fuel_type=c.fuel_type AND a.unit_eic IS NULL
            AND NOT EXISTS (SELECT 1 FROM entsoe_expected e WHERE {JOIN})""")
    cursor.execute("CREATE UNIQUE INDEX ON entsoe_repairs(id)")
    cursor.execute("SELECT action,count(*) FROM entsoe_repairs GROUP BY action")
    counts = dict(cursor.fetchall())
    intervals, duplicates = counts.get("interval", 0), counts.get("duplicate", 0)
    cursor.execute(f"""SELECT count(*) FROM entsoe_current a LEFT JOIN entsoe_expected e ON {JOIN}
        WHERE e.timestamp_ms IS NULL""")
    if cursor.fetchone()[0] != duplicates:
        raise ValueError("Some existing observations are not proven duplicates")
    if (intervals, duplicates) not in {(0, 0), expected_counts}:
        raise ValueError(f"Repair population changed: {(intervals, duplicates)}")
    buffer = io.StringIO()
    cursor.copy_expert(
        f"COPY (SELECT {MEASUREMENTS} FROM entsoe_current WHERE {COAL} "
        "ORDER BY timestamp_ms,country_code,psr_type,plant_name) TO STDOUT WITH CSV",
        buffer,
    )
    before_hash = hashlib.sha256(buffer.getvalue().encode()).hexdigest()
    cursor.execute(f"""SELECT sum(generation_mw*resolution_minutes/60.0)
        FROM entsoe_current WHERE {COAL}""")
    before_mwh = cursor.fetchone()[0]
    cursor.execute(f"""SELECT sum(a.generation_mw*COALESCE(r.new_resolution,a.resolution_minutes)/60.0)
        FROM entsoe_current a LEFT JOIN entsoe_repairs r USING(id)
        WHERE a.{COAL} AND r.action IS DISTINCT FROM 'duplicate'""")
    after_mwh = cursor.fetchone()[0]
    report = {
        "action": "plan",
        "interval_corrections": intervals,
        "exact_duplicate_copies": duplicates,
        "original_rows_to_archive": intervals + duplicates,
        "coal_before_sha256": before_hash,
        "coal_mwh_before": before_mwh,
        "coal_mwh_after_repair": after_mwh,
        "backup_table": BACKUP,
    }
    if not apply or not (intervals or duplicates):
        if apply:
            report["action"] = "already_corrected"
        return report
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP} (LIKE ingestion.entsoe_generation_data)"
    )
    cursor.execute(f"SELECT count(*) FROM {BACKUP}")
    if cursor.fetchone()[0]:
        raise ValueError(
            "A prior repair backup exists; review it before another repair"
        )
    cursor.execute(f"REVOKE ALL ON {BACKUP} FROM PUBLIC, dashboard_ro")
    cursor.execute(f"GRANT SELECT ON {BACKUP} TO etl_writer")
    cursor.execute(f"""INSERT INTO {BACKUP}
        SELECT a.* FROM ingestion.entsoe_generation_data a JOIN entsoe_repairs r USING(id)""")
    if cursor.rowcount != intervals + duplicates:
        raise ValueError("Backup row count changed")
    cursor.execute("""UPDATE ingestion.entsoe_generation_data a
        SET resolution_minutes=r.new_resolution FROM entsoe_repairs r
        WHERE a.id=r.id AND r.action='interval'""")
    if cursor.rowcount != intervals:
        raise ValueError("Interval repair row count changed")
    cursor.execute("""DELETE FROM ingestion.entsoe_generation_data a
        USING entsoe_repairs r WHERE a.id=r.id AND r.action='duplicate'""")
    if cursor.rowcount != duplicates:
        raise ValueError("Duplicate repair row count changed")
    cursor.execute(f"""SELECT count(*) FROM ingestion.entsoe_generation_data a
        JOIN entsoe_expected e ON {JOIN} WHERE {predicate(2024).replace("country_code=", "a.country_code=").replace("timestamp_ms", "a.timestamp_ms")}
        AND (a.generation_mw IS DISTINCT FROM e.generation_mw
            OR a.resolution_minutes IS DISTINCT FROM e.resolution_minutes)""")
    if cursor.fetchone()[0]:
        raise ValueError("Repaired observations differ from source")
    report["action"] = "applied"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if os.getenv("ETL_ENVIRONMENT") != "staging":
        raise ValueError("This repair requires the pinned staging environment wrapper")
    rows, source = check_source(args.input, args.manifest, 2024)
    check_benchmark(source, 2024)
    with psycopg2.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            load_expected(cursor, [prepare_record(r) for r in rows])
            repair = prepare_repair(cursor, args.apply)
            if (
                repair["interval_corrections"]
                and repair["coal_before_sha256"] != LEGACY_COAL_SHA256
            ):
                # Raising here rolls back both DDL and DML if --apply was set.
                raise ValueError("Existing coal changed since the reviewed repair plan")
    report = {
        "environment": "staging",
        "country": "CZ",
        "year": 2024,
        "source": source,
        "repair": repair,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
