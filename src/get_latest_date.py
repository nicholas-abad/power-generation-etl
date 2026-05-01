"""Query the latest data timestamp per source from the Neon database.

Usage:
    python src/get_latest_date.py <source>

Outputs an ISO date string (YYYY-MM-DD) to stdout, or "1970-01-01" if no data exists.
Used by GitHub Actions to compute incremental extraction start dates.
"""

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# Source → (table_name, timestamp_expression)
SOURCE_CONFIG = {
    "eia": ("eia_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "entsoe": ("entsoe_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "npp": ("npp_generation", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "ons": ("ons_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "oe": ("oe_facility_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "occto": ("occto_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
}

FALLBACK_DATE = "1970-01-01"


def get_connection_url() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "power_generation")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "")
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    if sslmode:
        url += f"?sslmode={sslmode}"
    return url


def get_latest_date(source: str) -> str:
    if source not in SOURCE_CONFIG:
        print(f"Unknown source: {source}. Valid: {', '.join(SOURCE_CONFIG.keys())}", file=sys.stderr)
        sys.exit(1)

    table, expr = SOURCE_CONFIG[source]
    engine = create_engine(get_connection_url())

    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT {expr} AS latest FROM {table}"))
            row = result.fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception as e:
        print(f"Warning: could not query {table}: {e}", file=sys.stderr)

    return FALLBACK_DATE


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <source>", file=sys.stderr)
        sys.exit(1)

    date = get_latest_date(sys.argv[1])
    print(date)
