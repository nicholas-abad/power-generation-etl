"""Capture the 2019 coal baseline or reconcile a staging load with its JSONL.

Use through run_in_environment.py. This check requires staging and only
writes a temporary comparison table plus the requested local JSON report.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path

import psycopg2

COLUMNS = (
    "timestamp_ms",
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
    "generation_mwh",
    "resolution_minutes",
)
YEAR = "timestamp_ms >= 1546300800000 AND timestamp_ms < 1577836800000"
MONTHS = "month >= '2019-01-01' AND month < '2020-01-01'"


class Digest:
    def __init__(self):
        self.hash = hashlib.sha256()

    def write(self, data):
        self.hash.update(data if isinstance(data, bytes) else data.encode())


def capture(cursor):
    digest = Digest()
    cursor.copy_expert(
        f"COPY (SELECT {', '.join(COLUMNS)} FROM ingestion.ons_generation_data "
        f"WHERE {YEAR} AND fuel_type='Carvão' ORDER BY timestamp_ms,plant,ons_plant_id) "
        "TO STDOUT WITH CSV",
        digest,
    )
    cursor.execute(
        f"SELECT month::text, sum(generation_mwh) FROM public.mv_ons_plant_monthly "
        f"WHERE {MONTHS} AND fuel_type='Carvão' GROUP BY month ORDER BY month"
    )
    return {"coal_sha256": digest.hash.hexdigest(), "coal_monthly": cursor.fetchall()}


def reconcile(cursor, path):
    columns = ", ".join(COLUMNS)
    cursor.execute(
        f"CREATE TEMP TABLE ons_expected AS SELECT {columns} "
        "FROM ingestion.ons_generation_data WITH NO DATA"
    )
    count = 0
    with path.open() as handle:
        while True:
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            size = 0
            for _ in range(50000):
                line = handle.readline()
                if not line:
                    break
                row = json.loads(line)
                if not 1546300800000 <= row["timestamp_ms"] < 1577836800000:
                    raise ValueError("Input contains an observation outside 2019")
                writer.writerow([row.get(column) for column in COLUMNS])
                size += 1
            if not size:
                break
            buffer.seek(0)
            cursor.copy_expert(
                f"COPY ons_expected ({columns}) FROM STDIN WITH CSV", buffer
            )
            count += size
    if count != 2424001:
        raise ValueError(
            f"Expected the audited 2,424,001 observations; received {count}"
        )
    cursor.execute(
        "SELECT count(*) FROM (SELECT timestamp_ms,ons_plant_id FROM ons_expected GROUP BY 1,2 HAVING count(*)>1) d"
    )
    if cursor.fetchone()[0]:
        raise ValueError("Input contains duplicate timestamp/ONS-ID pairs")
    cursor.execute("ANALYZE ons_expected")
    differences = [
        f"COALESCE(e.{column}, '') IS DISTINCT FROM COALESCE(a.{column}, '')"
        for column in COLUMNS
        if column not in {"timestamp_ms", "generation_mwh", "resolution_minutes"}
    ]
    differences += [
        "a.id IS NULL",
        "e.resolution_minutes IS DISTINCT FROM a.resolution_minutes",
        "abs(e.generation_mwh-a.generation_mwh)>1e-9",
    ]
    cursor.execute(
        "SELECT count(*) FROM ons_expected e LEFT JOIN ingestion.ons_generation_data a "
        "ON e.timestamp_ms=a.timestamp_ms AND e.plant=a.plant "
        "AND e.ons_plant_id=a.ons_plant_id WHERE " + " OR ".join(differences)
    )
    if cursor.fetchone()[0]:
        raise ValueError("Stored observations differ from the extracted 2019 source")
    grain = "month,ons_plant_id,plant,plant_type,fuel_type,state,state_name"
    cursor.execute(
        f"SELECT {grain},sum(generation_mwh),count(*) FROM "
        "(SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0)) AS month,* "
        f"FROM ons_expected) e GROUP BY {grain}"
    )
    expected = {tuple(row[:7]): row[7:] for row in cursor.fetchall()}
    cursor.execute(
        f"SELECT {grain},generation_mwh,observation_count "
        f"FROM public.mv_ons_individual_plant_monthly WHERE {MONTHS}"
    )
    actual = {tuple(row[:7]): row[7:] for row in cursor.fetchall()}
    if actual.keys() != expected.keys():
        raise ValueError("Qualified view has missing or unexpected plant/month groups")
    for key, (mwh, observations) in expected.items():
        actual_mwh, actual_observations = actual[key]
        if observations != actual_observations or not math.isclose(
            mwh, actual_mwh, rel_tol=1e-12, abs_tol=1e-6
        ):
            raise ValueError("Qualified view does not reconcile with source aggregates")
    return {"source_rows_reconciled": count, "qualified_monthly_rows": len(actual)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--expected-jsonl", type=Path)
    args = parser.parse_args()
    if os.environ.get("ETL_ENVIRONMENT") != "staging":
        parser.error("Run through run_in_environment.py --environment staging")
    if bool(args.baseline) != bool(args.expected_jsonl):
        parser.error("--baseline and --expected-jsonl must be provided together")
    with psycopg2.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute("SET LOCAL statement_timeout='300s'")
            report = capture(cursor)
            if args.baseline:
                before = json.loads(args.baseline.read_text())
                if before["coal_sha256"] != report["coal_sha256"]:
                    raise ValueError("Coal observations changed")
                after_months = dict(report["coal_monthly"])
                if set(after_months) != set(dict(before["coal_monthly"])):
                    raise ValueError("Coal month coverage changed")
                for month, mwh in before["coal_monthly"]:
                    if not math.isclose(
                        mwh, after_months[month], rel_tol=0, abs_tol=1e-6
                    ):
                        raise ValueError("Coal monthly generation changed")
                report.update(reconcile(cursor, args.expected_jsonl))
                root = Path(__file__).resolve().parents[1]
                for check in ("dashboard_ro_surface.sql", "no_shadow_tables.sql"):
                    cursor.execute((root / "schema/checks" / check).read_text())
                report["status"] = "passed"
            else:
                report["status"] = "baseline"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(f"ONS 2019 staging {report['status']}: {args.report}")


if __name__ == "__main__":
    main()
