"""Week-over-week per-source monthly-totals tripwire.

The 2026-09 deep dive found that both real data-loss incidents (Australia's
silently halved history; EIA's 133 TWh window gap) would have been caught by
comparing each source's monthly totals against the previous run's. This check
does exactly that:

  * For every source's monthly view, compute per-month totals (all fuels —
    completeness is fuel-agnostic).
  * Compare MATURE months (older than the two most recent) against the
    snapshot in ingestion.monthly_totals_snapshot. A relative change beyond
    THRESHOLD fails the run, naming the months.
  * Recent months are exempt (they legitimately grow as data arrives).
  * Snapshots are ALWAYS re-captured at the end, so a genuine, verified
    revision alarms exactly once and becomes the new baseline next week.

Exit codes: 0 ok, 1 drift detected, 2 configuration/connection error.
"""

import os
import sys

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text

load_dotenv()

THRESHOLD = 0.15  # |relative change| on a mature month that trips the alarm

# source → (SQL producing (month::date, total_mwh)); all-fuels on purpose.
SOURCE_TOTALS = {
    "eia": "SELECT month::date, SUM(net_generation_mwh) FROM mv_eia_unit_monthly GROUP BY 1",
    "entsoe": "SELECT month::date, SUM(generation_mwh) FROM mv_entsoe_plant_monthly GROUP BY 1",
    "ons": "SELECT month::date, SUM(generation_mwh) FROM mv_ons_plant_monthly GROUP BY 1",
    "npp": "SELECT month::date, SUM(generation_mwh) FROM mv_npp_plant_monthly GROUP BY 1",
    "oe_facility": "SELECT month::date, SUM(generation_mwh) FROM mv_oe_plant_monthly GROUP BY 1",
    "occto": "SELECT month::date, SUM(generation_mwh) FROM mv_occto_plant_monthly GROUP BY 1",
    "chile": "SELECT month::date, SUM(generation_mwh) FROM mv_chile_plant_monthly GROUP BY 1",
    "climatetrace": "SELECT month::date, SUM(generation_mwh) FROM mv_climatetrace_coal_monthly GROUP BY 1",
}


def engine():
    url = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'power_generation')}"
    )
    if os.environ.get("POSTGRES_SSLMODE"):
        url += f"?sslmode={os.environ['POSTGRES_SSLMODE']}"
    return create_engine(url)


def main() -> int:
    e = engine()
    drifts: list[str] = []
    with e.begin() as conn:
        snap = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                text(
                    "SELECT source, month, total_mwh FROM ingestion.monthly_totals_snapshot"
                )
            )
        }
        for source, sql in SOURCE_TOTALS.items():
            rows = sorted(conn.execute(text(sql)), key=lambda r: r[0])
            if not rows:
                logger.warning(f"{source}: no rows in monthly view")
                continue
            mature_cutoff = rows[-1][0] if len(rows) < 3 else rows[-3][0]
            for month, total in rows:
                prev = snap.get((source, month))
                if (
                    prev is not None
                    and month <= mature_cutoff
                    and prev > 0
                    and abs(total - prev) / prev > THRESHOLD
                ):
                    drifts.append(
                        f"{source} {month}: {prev / 1e6:.2f} → {total / 1e6:.2f} TWh "
                        f"({(total - prev) / prev:+.0%})"
                    )
            # re-capture (always): the new totals become next week's baseline
            for month, total in rows:
                conn.execute(
                    text(
                        "INSERT INTO ingestion.monthly_totals_snapshot (source, month, total_mwh) "
                        "VALUES (:s, :m, :t) "
                        "ON CONFLICT (source, month) DO UPDATE SET total_mwh = EXCLUDED.total_mwh, "
                        "captured_at = now()"
                    ),
                    {"s": source, "m": month, "t": float(total or 0)},
                )
            logger.info(f"{source}: {len(rows)} months snapshotted")
    if drifts:
        logger.error(
            "MATURE-MONTH TOTAL DRIFT (>"
            f"{THRESHOLD:.0%}) — verify upstream revision vs data loss:\n  "
            + "\n  ".join(drifts)
            + "\n(now re-baselined: a verified-legitimate change will not re-alarm)"
        )
        return 1
    logger.info("totals drift check OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
