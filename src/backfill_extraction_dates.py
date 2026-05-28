#!/usr/bin/env python3
"""One-time backfill: populate extraction_metadata.start_date / end_date
for historical runs where these columns are NULL.

For each row in extraction_metadata with NULL dates, looks up the min/max
timestamp_ms in the source's raw generation table (matched by extraction_run_id)
and updates the metadata row.

Usage:
    python src/backfill_extraction_dates.py
"""

import os

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

SOURCE_TABLE = {
    "eia": "eia_generation_data",
    "entsoe": "entsoe_generation_data",
    "npp": "npp_generation",
    "ons": "ons_generation_data",
    "oe": "oe_generation_data",
    "oe_facility": "oe_facility_generation_data",
    "occto": "occto_generation_data",
    "chile": "chile_generation_data",
}


def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB"],
        query={"sslmode": os.environ.get("POSTGRES_SSLMODE", "require")},
    )
    return create_engine(url, pool_pre_ping=True)


def main():
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT extraction_run_id::text, source
            FROM extraction_metadata
            WHERE start_date IS NULL OR end_date IS NULL
            ORDER BY extraction_timestamp DESC
        """)).fetchall()

    logger.info(f"{len(rows)} extraction_metadata rows to backfill")

    updated = 0
    skipped = 0
    for run_id, source in rows:
        table = SOURCE_TABLE.get(source)
        if not table:
            logger.warning(f"Unknown source '{source}' — skipping")
            skipped += 1
            continue

        with engine.connect() as conn:
            row = conn.execute(
                text(f"""
                    SELECT
                        TO_CHAR(TO_TIMESTAMP(MIN(timestamp_ms) / 1000), 'YYYY-MM-DD'),
                        TO_CHAR(TO_TIMESTAMP(MAX(timestamp_ms) / 1000), 'YYYY-MM-DD')
                    FROM {table}
                    WHERE extraction_run_id = :run_id
                """),
                {"run_id": run_id},
            ).fetchone()

        if row is None or row[0] is None:
            logger.info(f"  {source} {run_id[:8]} — no rows in {table}, skipping")
            skipped += 1
            continue

        start_date, end_date = row
        with engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE extraction_metadata
                    SET start_date = :start_date, end_date = :end_date
                    WHERE extraction_run_id = :run_id
                """),
                {"start_date": start_date, "end_date": end_date, "run_id": run_id},
            )
            conn.commit()
        logger.info(f"  {source} {run_id[:8]} → {start_date} to {end_date}")
        updated += 1

    engine.dispose()
    logger.success(f"Done: {updated} updated, {skipped} skipped")


if __name__ == "__main__":
    main()
