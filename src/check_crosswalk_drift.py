#!/usr/bin/env python3
"""Detect plants in raw generation tables that are missing from plant_crosswalk.

For each source, finds distinct plant identifiers in the raw generation table
that have no matching row in plant_crosswalk. These plants will appear in the
dashboard with NULL coordinates until the crosswalk is rebuilt.

Exit code:
  0 — query succeeded (drift, if any, is reported in drift_report.json)
  2 — DB connection error or query failure

Usage:
    python src/check_crosswalk_drift.py
"""

import json
import os
import sys

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()


# (source_system, raw_table, raw_key_column, crosswalk_key_column)
# crosswalk_key_column is the plant_crosswalk column to compare against.
SOURCE_CHECKS = [
    ("EIA", "eia_generation_data", "plant_code", "plant_code"),
    ("ENTSOE", "entsoe_generation_data", "plant_name", "plant_name"),
    ("NPP", "npp_generation", "plant", "plant_name"),
    ("ONS", "ons_generation_data", "plant", "plant_name"),
    ("OE", "oe_facility_generation_data", "facility_code", "plant_code"),
    ("OCCTO", "occto_generation_data", "plant", "plant_name"),
    ("CHILE", "chile_generation_data", "plant", "plant_name"),
]


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


def find_missing_plants(engine, source_system, raw_table, raw_key, crosswalk_key):
    sql = text(f"""
        SELECT DISTINCT r.{raw_key} AS plant_id
        FROM {raw_table} r
        WHERE r.{raw_key} IS NOT NULL
          AND r.{raw_key} NOT IN (
              SELECT c.{crosswalk_key}
              FROM plant_crosswalk c
              WHERE c.source_system = :source_system
                AND c.{crosswalk_key} IS NOT NULL
          )
        ORDER BY r.{raw_key}
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"source_system": source_system}).fetchall()
    return [r[0] for r in rows]


def main():
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        sys.exit(2)

    report = {}
    total_missing = 0

    for source, table, raw_key, cw_key in SOURCE_CHECKS:
        try:
            missing = find_missing_plants(engine, source, table, raw_key, cw_key)
        except Exception as e:
            logger.error(f"{source}: query failed — {e}")
            sys.exit(2)

        report[source] = {
            "count": len(missing),
            "raw_table": table,
            "raw_key": raw_key,
            "crosswalk_key": cw_key,
            "sample": missing[:20],
        }
        total_missing += len(missing)
        logger.info(f"{source:8s} missing in crosswalk: {len(missing)}")

    engine.dispose()

    with open("drift_report.json", "w") as f:
        json.dump({"total_missing": total_missing, "by_source": report}, f, indent=2)

    if total_missing > 0:
        logger.warning(f"Drift detected: {total_missing} plants total — see drift_report.json")
    else:
        logger.info("No drift — all plants mapped in crosswalk")


if __name__ == "__main__":
    main()
