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
    "entsoe": (
        "entsoe_generation_data",
        "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date",
    ),
    "npp": ("npp_generation", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "ons": ("ons_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "oe": (
        "oe_facility_generation_data",
        "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date",
    ),
    "occto": ("occto_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
    "chile": ("chile_generation_data", "MAX(TO_TIMESTAMP(timestamp_ms / 1000))::date"),
}

FALLBACK_DATE = "1970-01-01"


class LatestDateQueryError(RuntimeError):
    """The latest-date query failed (DB unreachable, auth, timeout, ...).

    Distinct from an empty table: callers must NOT treat this as "no data"
    or a transient outage triggers a from-scratch re-extraction window.
    """


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
        print(
            f"Unknown source: {source}. Valid: {', '.join(SOURCE_CONFIG.keys())}",
            file=sys.stderr,
        )
        sys.exit(1)

    table, expr = SOURCE_CONFIG[source]
    engine = create_engine(get_connection_url())

    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT {expr} AS latest FROM {table}"))
            row = result.fetchone()
    except Exception as e:
        raise LatestDateQueryError(f"could not query {table}: {e}") from e

    if row and row[0]:
        return str(row[0])
    # Successful query, genuinely empty table — only here is the epoch
    # fallback correct.
    return FALLBACK_DATE


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <source>", file=sys.stderr)
        sys.exit(1)

    try:
        date = get_latest_date(sys.argv[1])
    except LatestDateQueryError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
    print(date)
