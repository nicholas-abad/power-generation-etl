"""Verify an audited ONS year (or audited year-to-date snapshot) on staging.

Use through run_in_environment.py. This check requires staging and only
writes a temporary comparison table plus the requested local JSON report.
"""

import argparse
import csv
from datetime import UTC, datetime
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
ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "config/ons-benchmarks.json"


def audited_years():
    return tuple(
        sorted(int(year) for year in json.loads(BENCHMARKS.read_text())["years"])
    )


def benchmark(year):
    allowed = audited_years()
    if year not in allowed:
        raise ValueError(f"Only the audited benchmark years {allowed} are allowed")
    return json.loads(BENCHMARKS.read_text())["years"][str(year)]


def last_month(year):
    """Audited cutoff: 12 unless the entry is a year-to-date snapshot."""
    month = benchmark(year).get("last_month", 12)
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid last_month for {year}")
    return month


def period_end(year):
    month = last_month(year)
    return datetime(year + month // 12, month % 12 + 1, 1, tzinfo=UTC)


def year_bounds(year):
    return (
        int(datetime(year, 1, 1, tzinfo=UTC).timestamp() * 1000),
        int(period_end(year).timestamp() * 1000),
    )


def year_predicate(year):
    start, end = year_bounds(year)
    return f"timestamp_ms >= {start} AND timestamp_ms < {end}"


def month_predicate(year):
    return f"month >= '{year}-01-01' AND month < '{period_end(year).date()}'"


def extract_arguments(year):
    """energy-extract ons arguments that reproduce the audited source files."""
    arguments = ["--start-year", str(year), "--end-year", str(year)]
    if last_month(year) < 12:
        arguments += ["--end-month", str(last_month(year))]
    return arguments + ["--all-types", "--individual-plants-only"]


def check_extraction(metadata, year, audited):
    """Refuse incomplete downloads or upstream restatements before loading."""
    if (
        metadata.get("start_year") != year
        or metadata.get("end_year") != year
        or metadata.get("thermal_only") is not False
        or metadata.get("individual_plants_only") is not True
        or metadata.get("failed_downloads_count") != 0
    ):
        raise ValueError(
            "Extraction must be a complete, all-fuel individual-plant year"
        )
    cutoff = last_month(year)
    if metadata.get("end_month") != cutoff or metadata.get("end_month_requested") != (
        cutoff if cutoff < 12 else None
    ):
        raise ValueError(f"Extraction must end at the audited month {cutoff}")
    attempts = metadata.get("url_attempts", [])
    expected_files = {item["label"]: item for item in audited["source_files"]}
    if len(attempts) != len(expected_files) or {
        item.get("label") for item in attempts
    } != set(expected_files):
        raise ValueError("Extraction is missing or repeats an audited source file")
    for item in attempts:
        expected = expected_files[item["label"]]
        if (
            item.get("success") is not True
            or item.get("url") != expected["url"]
            or item.get("sha256") != expected["sha256"]
            or item.get("records") != expected["raw_records"]
        ):
            raise ValueError(
                f"Source {item['label']} differs from the audited benchmark"
            )
    if metadata.get("total_records") != audited["expected_observations"]:
        raise ValueError(
            "Qualified observation count differs from the audited benchmark"
        )
    raw_count = sum(item["raw_records"] for item in expected_files.values())
    if (
        metadata.get("raw_records") != raw_count
        or metadata.get("selected_records") != raw_count
        or metadata.get("excluded_records")
        != raw_count - audited["expected_observations"]
    ):
        raise ValueError("Accepted and excluded source counts do not reconcile")
    return {
        "year": year,
        "status": "source_verified",
        "source_files": audited["source_files"],
        "expected_observations": audited["expected_observations"],
        "extraction_run_id": metadata["extraction_run_id"],
        "extractors_commit": os.getenv("EXTRACTORS_COMMIT"),
        "etl_commit": os.getenv("GITHUB_SHA"),
    }


class Digest:
    def __init__(self):
        self.hash = hashlib.sha256()

    def write(self, data):
        self.hash.update(data if isinstance(data, bytes) else data.encode())


def capture(cursor, year):
    digest = Digest()
    cursor.copy_expert(
        f"COPY (SELECT {', '.join(COLUMNS)} FROM ingestion.ons_generation_data "
        f"WHERE {year_predicate(year)} AND fuel_type='Carvão' "
        "ORDER BY timestamp_ms,plant,ons_plant_id) "
        "TO STDOUT WITH CSV",
        digest,
    )
    cursor.execute(
        f"SELECT month::text, sum(generation_mwh) FROM public.mv_ons_plant_monthly "
        f"WHERE {month_predicate(year)} AND fuel_type='Carvão' GROUP BY month ORDER BY month"
    )
    months = cursor.fetchall()
    if len(months) != last_month(year):
        raise ValueError(f"Expected {last_month(year)} coal baseline months for {year}")
    cursor.execute(
        f"SELECT count(*) FROM ingestion.ons_generation_data "
        f"WHERE {year_predicate(year)} AND fuel_type='Carvão'"
    )
    return {
        "year": year,
        "coal_sha256": digest.hash.hexdigest(),
        "coal_rows": cursor.fetchone()[0],
        "coal_monthly": months,
    }


def reconcile(cursor, path, year, expected_count):
    start, end = year_bounds(year)
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
                if not start <= row["timestamp_ms"] < end:
                    raise ValueError(f"Input contains an observation outside {year}")
                writer.writerow([row.get(column) for column in COLUMNS])
                size += 1
            if not size:
                break
            buffer.seek(0)
            cursor.copy_expert(
                f"COPY ons_expected ({columns}) FROM STDIN WITH CSV", buffer
            )
            count += size
    if count != expected_count:
        raise ValueError(
            f"Expected the audited {expected_count:,} observations; received {count}"
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
        raise ValueError(f"Stored observations differ from the extracted {year} source")
    grain = "month,ons_plant_id,plant,plant_type,fuel_type,state,state_name"
    cursor.execute(
        f"SELECT {grain},sum(generation_mwh),count(*) FROM "
        "(SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0)) AS month,* "
        f"FROM ons_expected) e GROUP BY {grain}"
    )
    expected = {tuple(row[:7]): row[7:] for row in cursor.fetchall()}
    cursor.execute(
        f"SELECT {grain},generation_mwh,observation_count "
        f"FROM public.mv_ons_individual_plant_monthly WHERE {month_predicate(year)}"
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
    if {key[0].month for key in expected} != set(range(1, last_month(year) + 1)):
        raise ValueError("Qualified source does not cover every audited month")
    cursor.execute(
        "SELECT fuel_type,count(DISTINCT ons_plant_id),count(*),sum(generation_mwh) "
        "FROM ons_expected GROUP BY fuel_type ORDER BY fuel_type"
    )
    fuels = [
        dict(zip(("fuel", "ons_ids", "observations", "generation_mwh"), row))
        for row in cursor.fetchall()
    ]
    return {
        "source_rows_reconciled": count,
        "qualified_monthly_rows": len(actual),
        "qualified_months": last_month(year),
        "fuels": fuels,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, choices=audited_years(), default=2019)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--print-extract-args",
        action="store_true",
        help="Print the energy-extract ons arguments for the audited year and exit",
    )
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--expected-jsonl", type=Path)
    parser.add_argument("--preflight-metadata", type=Path)
    args = parser.parse_args()
    if args.print_extract_args:
        print(" ".join(extract_arguments(args.year)))
        return
    if not args.report:
        parser.error("--report is required")
    audited = benchmark(args.year)
    if args.preflight_metadata:
        if args.baseline or args.expected_jsonl:
            parser.error("Metadata preflight cannot be combined with database checks")
        report = check_extraction(
            json.loads(args.preflight_metadata.read_text()), args.year, audited
        )
        write_report(args.report, report)
        return
    if os.environ.get("ETL_ENVIRONMENT") != "staging":
        parser.error("Run through run_in_environment.py --environment staging")
    if bool(args.baseline) != bool(args.expected_jsonl):
        parser.error("--baseline and --expected-jsonl must be provided together")
    with psycopg2.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute("SET LOCAL statement_timeout='300s'")
            report = capture(cursor, args.year)
            if args.baseline:
                before = json.loads(args.baseline.read_text())
                if before.get("year") != args.year:
                    raise ValueError("Coal baseline belongs to a different year")
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
                report.update(
                    reconcile(
                        cursor,
                        args.expected_jsonl,
                        args.year,
                        audited["expected_observations"],
                    )
                )
                for check in ("dashboard_ro_surface.sql", "no_shadow_tables.sql"):
                    cursor.execute((ROOT / "schema/checks" / check).read_text())
                report["status"] = "passed"
            else:
                report["status"] = "baseline"
    write_report(args.report, report)


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(f"ONS {report['year']} staging {report['status']}: {path}")


if __name__ == "__main__":
    main()
