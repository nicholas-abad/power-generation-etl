"""Refresh all materialized views after data loads.

Usage:
    python src/refresh_views.py [--source <source>]

Without --source, refreshes ALL materialized views.
With --source, refreshes only views for that source.
"""

import os
import sys
import time

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text

load_dotenv()

# Source → list of materialized views to refresh.
# `mv_<source>_row_counts` powers the dashboard's /data-quality coverage matrix.
# EIA and OE only have row-count views (their raw tables are small enough that
# the dashboard reads them directly for everything else).
SOURCE_VIEWS = {
    "eia": ["mv_eia_row_counts"],
    "entsoe": ["mv_entsoe_monthly", "mv_entsoe_plant_monthly", "mv_entsoe_row_counts"],
    "ons": ["mv_ons_monthly", "mv_ons_plant_monthly", "mv_ons_row_counts"],
    "npp": ["mv_npp_monthly", "mv_npp_plant_monthly", "mv_npp_row_counts"],
    "oe": ["mv_oe_row_counts"],
    "occto": ["mv_occto_monthly", "mv_occto_plant_monthly", "mv_occto_row_counts"],
    "chile": ["mv_chile_monthly", "mv_chile_plant_monthly", "mv_chile_row_counts"],
}

ALL_VIEWS = [v for views in SOURCE_VIEWS.values() for v in views]


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


def refresh_views(views: list[str]) -> bool:
    engine = create_engine(get_connection_url())
    success = True

    for view in views:
        start = time.time()
        logger.info(f"Refreshing {view}...")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                conn.commit()
            elapsed = time.time() - start
            logger.success(f"Refreshed {view} in {elapsed:.1f}s")
        except Exception as e:
            logger.error(f"Failed to refresh {view}: {e}")
            success = False

    return success


if __name__ == "__main__":
    source = None
    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        if idx + 1 < len(sys.argv):
            source = sys.argv[idx + 1]

    if source:
        if source not in SOURCE_VIEWS:
            logger.error(
                f"Unknown source: {source}. Valid: {', '.join(SOURCE_VIEWS.keys())}"
            )
            sys.exit(1)
        views = SOURCE_VIEWS[source]
        logger.info(f"Refreshing views for {source}: {views}")
    else:
        views = ALL_VIEWS
        logger.info(f"Refreshing all {len(views)} materialized views")

    ok = refresh_views(views)
    sys.exit(0 if ok else 1)
