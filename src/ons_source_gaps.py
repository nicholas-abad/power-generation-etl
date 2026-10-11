"""Record the approved source gaps of an audited ONS extraction (migration 021).

Use through run_in_environment.py, after loading the same extraction's JSONL:

    python src/run_in_environment.py --environment staging --role writer -- \
        uv run python src/ons_source_gaps.py --year 2026 \
            --metadata extracted_data/extraction_metadata_<run>.json

The metadata must pass the same preflight as the load (audited source files,
per-fuel audit, identical gap list), and every gap must fall inside an
approved window. All gaps starting inside the audited period are then replaced
in one transaction, so gaps that ONS has since filled disappear.
"""

import argparse
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from check_ons_staging import (
    benchmark,
    check_extraction,
    check_source_gaps,
    year_bounds,
)

COLUMNS = (
    "ons_plant_id",
    "start_ms",
    "end_ms",
    "plant",
    "fuel_type",
    "hours",
    "kind",
    "note",
    "extraction_run_id",
)


def approved_rows(metadata, year):
    check_extraction(metadata, year, benchmark(year))
    return [
        (
            gap["ons_plant_id"],
            gap["start_ms"],
            gap["end_ms"],
            gap["plant"],
            gap["fuel_type"],
            gap["hours"],
            gap["kind"],
            gap["note"],
            metadata["extraction_run_id"],
        )
        for gap in check_source_gaps(year, metadata["source_gaps"])
    ]


def record(cursor, year, rows):
    start, end = year_bounds(year)
    cursor.execute(
        "DELETE FROM ingestion.ons_source_gaps WHERE start_ms >= %s AND start_ms < %s",
        (start, end),
    )
    deleted = cursor.rowcount
    if rows:
        execute_values(
            cursor,
            f"INSERT INTO ingestion.ons_source_gaps ({', '.join(COLUMNS)}) VALUES %s",
            rows,
        )
    cursor.execute(
        "SELECT ons_plant_id, start_ms, end_ms, hours FROM ingestion.ons_source_gaps "
        "WHERE start_ms >= %s AND start_ms < %s ORDER BY 1, 2",
        (start, end),
    )
    if cursor.fetchall() != sorted((r[0], r[1], r[2], r[5]) for r in rows):
        raise ValueError("Recorded gaps differ from the approved extraction")
    return {
        "year": year,
        "replaced": deleted,
        "recorded": len(rows),
        "gap_hours": sum(r[5] for r in rows),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args()
    if not os.environ.get("ETL_ENVIRONMENT"):
        parser.error("Run through run_in_environment.py")
    rows = approved_rows(json.loads(args.metadata.read_text()), args.year)
    with psycopg2.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            summary = record(cursor, args.year, rows)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
