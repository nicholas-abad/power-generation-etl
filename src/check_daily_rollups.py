"""Daily views reconcile with the monthly views the tracker reads.

For every daily view whose migration is applied (024, 025), the daily rows summed
by month must equal the coal-filtered monthly view at the same key: no key-month
on only one side, no difference beyond float noise. Australia's days are on the
network-local date while its monthly view uses UTC months, so its local months
are rebuilt from the raw facility rows instead. Also flags any row that claims more than a
day of coverage without being marked unknown (overlapping intervals).

Read-only. Exit codes: 0 ok, 1 mismatch, 2 configuration/connection error.
Design: docs/DAILY_WEEKLY_DRILLDOWN.md.
"""

import os
import sys
from dataclasses import dataclass

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text

from refresh_views import MIGRATION_VIEWS, migration_view_state, sqlalchemy_scalar

TOLERANCE = 1e-6  # relative, with a 1 MWh floor


@dataclass(frozen=True)
class Rollup:
    keys: tuple[str, ...]
    monthly: str  # SELECT m, <keys>, g


ROLLUPS = {
    "mv_entsoe_plant_daily": Rollup(
        ("plant_name", "country_code", "fuel_type"),
        "SELECT month::date AS m, plant_name, country_code, fuel_type, generation_mwh AS g "
        "FROM mv_entsoe_plant_monthly "
        "WHERE fuel_type IN ('Fossil Hard coal', 'Fossil Brown coal/Lignite')",
    ),
    "mv_ons_plant_daily": Rollup(
        ("plant", "state", "state_name", "fuel_type"),
        "SELECT month::date AS m, plant, state, state_name, fuel_type, generation_mwh AS g "
        "FROM mv_ons_plant_monthly WHERE fuel_type = 'Carvão'",
    ),
    "mv_chile_plant_daily": Rollup(
        ("plant", "region", "comuna", "fuel_type"),
        "SELECT month::date AS m, plant, region, comuna, fuel_type, generation_mwh AS g "
        "FROM mv_chile_plant_monthly WHERE fuel_type = 'Carbón'",
    ),
    "mv_npp_unit_daily": Rollup(
        ("plant", "unit"),
        "SELECT month::date AS m, plant, unit, generation_mwh AS g "
        "FROM mv_npp_unit_monthly WHERE fuel_type = 'THERMAL'",
    ),
    "mv_occto_unit_daily": Rollup(
        ("plant", "unit"),
        "SELECT month::date AS m, plant, unit, generation_mwh AS g "
        "FROM mv_occto_unit_monthly WHERE fuel_type = 'coal'",
    ),
    # Australia: days are network-local dates while mv_oe_plant_monthly uses UTC
    # months, so its local months are rebuilt from the raw rows (as migration 022).
    "mv_oe_plant_daily": Rollup(
        ("facility_code", "fueltech"),
        "SELECT DATE_TRUNC('month', (TO_TIMESTAMP(timestamp_ms / 1000.0) AT TIME ZONE "
        "CASE network_code WHEN 'WEM' THEN 'Australia/Perth' ELSE 'Australia/Brisbane' END))::date AS m, "
        "facility_code, fueltech, SUM(generation_mwh) AS g "
        "FROM ingestion.oe_facility_generation_data "
        "WHERE fueltech IN ('coal_black', 'coal_brown') GROUP BY 1, 2, 3",
    ),
}


def rollup_sql(view: str, r: Rollup) -> str:
    """Key-months where daily and monthly disagree or only one side has rows, with
    both sums. Keys compare NULL-safely with hashable equalities (a FULL JOIN
    cannot use IS NOT DISTINCT FROM): the nullness, then the COALESCEd value, so
    NULL and '' stay different keys, as the views group them."""
    keys = ", ".join(r.keys)
    on = " AND ".join(
        ["d.m = mo.m"]
        + [
            f"(d.{k} IS NULL) = (mo.{k} IS NULL) AND COALESCE(d.{k}::text, '') = COALESCE(mo.{k}::text, '')"
            for k in r.keys
        ]
    )
    shown = ", ".join(f"COALESCE(d.{k}, mo.{k})::text" for k in ("m",) + r.keys)
    group = ", ".join(str(i + 1) for i in range(len(r.keys) + 1))
    return f"""
        WITH d AS (SELECT DATE_TRUNC('month', day)::date AS m, {keys}, SUM(generation_mwh) AS g
                   FROM {view} GROUP BY {group}),
             mo AS ({r.monthly}),
             j AS (SELECT concat_ws(' | ', {shown}) AS k, d.g AS daily, mo.g AS monthly
                   FROM d FULL JOIN mo ON {on})
        SELECT k, daily, monthly FROM j
        WHERE daily IS NULL OR monthly IS NULL
           OR abs(daily - monthly) > {TOLERANCE} * greatest(1, abs(monthly))
        ORDER BY k
    """


def installed(conn, view: str) -> bool:
    return migration_view_state(view, sqlalchemy_scalar(conn)) == "refresh"


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
    load_dotenv()
    problems: list[str] = []
    try:
        with engine().connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET TIME ZONE 'UTC'"))
            conn.execute(text("SET work_mem = '256MB'"))
            for view, r in ROLLUPS.items():
                if not installed(conn, view):
                    logger.info(
                        f"{view}: migration {MIGRATION_VIEWS[view]} not applied, skipped"
                    )
                    continue
                rows = conn.execute(text(rollup_sql(view, r))).all()
                if rows:
                    sample = "; ".join(
                        f"{k}: daily {d} vs monthly {m}" for k, d, m in rows[:5]
                    )
                    problems.append(f"{view}: {len(rows)} disagreements ({sample})")
                overclaimed = conn.execute(
                    text(
                        f"SELECT count(*) FROM {view} "
                        "WHERE covered_minutes > 1440 AND completeness <> 'unknown'"
                    )
                ).scalar()
                if overclaimed:
                    problems.append(
                        f"{view}: {overclaimed} rows over 1440 minutes not marked unknown"
                    )
                if not rows and not overclaimed:
                    logger.info(f"{view}: reconciles with its monthly view")
    except Exception as e:  # noqa: BLE001 - report and exit 2
        logger.error(f"daily rollup check could not run: {e}")
        return 2
    if problems:
        logger.error("DAILY VIEWS DO NOT RECONCILE:\n  " + "\n  ".join(problems))
        return 1
    logger.info("daily rollup check OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
