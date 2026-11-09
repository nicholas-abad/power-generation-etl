"""Chile: every plant-hour (coal table) or series-hour (all-fuel table) of an
extraction window is stored. Read-only.

The weekly run can store a Chile-local day before the Coordinador has published
all of it (2026-10 audit: 623 Saturday plant-hours lost Jun-Aug 2026). The
window's 14-day lookback re-fetches such a day on the next run, so only mature
hours are checked: from local midnight of START_DATE to the end of END_DATE or
of the Chile-local day two days before the run, whichever is earlier. Missing
newer hours in the window are reported as pending, since the next run
re-fetches them. A gap the source never fills fails the two weekly runs whose
lookback still covers it, then leaves the window.

A plant is checked between its first and last stored hour in the whole table,
so a retired plant is not expected to report and a gap at the end of the
mature window is still caught when later hours exist. The second occurrence of
the hour repeated when Chile's clocks go back is allowed: the extractor
localizes ambiguous hours to the first occurrence and drops the duplicate.

--table all_fuels checks ingestion.chile_generation_all_fuels (migration 019)
per series, (plant_key, tipo_tecnologia, subtipo_tecnologia): one plant-hour
there is one row per unit technology, so an hour where the source has published
the coal unit but not yet the gas unit is a missing hour. Each series spans its
own first to last stored hour. That extractor keeps both copies of the repeated
fall-back hour, so the second copy is required there. A series the source
omits in idle hours (2019: TER TARAPACA's diesel unit, 72 stored hours in a
span missing 5,687) fails the check by design; such a unit is decided case by
case.

A manual re-run (start_override) passes --report-only: history before 2022 has
whole plant-days the source never had, which no re-run can fill.

Exit codes: 0 ok (or --report-only), 1 mature hours missing, 2 configuration/connection error.
Usage: uv run python src/check_chile_hours.py --start-date YYYY-MM-DD
           [--end-date YYYY-MM-DD] [--table coal|all_fuels] [--now ISO] [--report-only]
"""

import argparse
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import NamedTuple
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import create_engine, text

from get_latest_date import get_connection_url

CHILE = ZoneInfo("America/Santiago")
HOUR_MS = 3_600_000


class CheckedTable(NamedTuple):
    """A table the check reads (--table) and how it names a series."""

    table_name: str
    series_expression: (
        str  # SQL naming one series: a plant, or a plant's unit technology
    )


TABLES = {
    "coal": CheckedTable("chile_generation_data", "plant"),
    "all_fuels": CheckedTable(
        "chile_generation_all_fuels",
        "plant_key || ' ' || tipo_tecnologia || '/' || subtipo_tecnologia",
    ),
}


def local_midnight_ms(day: date) -> int:
    return int(datetime.combine(day, time(), tzinfo=CHILE).timestamp() * 1000)


def mature_end_ms(now: datetime) -> int:
    """End of the Chile-local day two days before ``now``'s local date."""
    return local_midnight_ms(now.astimezone(CHILE).date() - timedelta(days=1))


def is_fall_back_repeat(hour_ms: int) -> bool:
    """The second occurrence of a local hour repeated when the clocks go back."""
    return datetime.fromtimestamp(hour_ms / 1000, tz=CHILE).fold == 1


def find_gaps(
    spans: dict[str, tuple[int, int]],
    stored: dict[str, set[int]],
    start_ms: int,
    end_ms: int,
    mature_end: int,
    allow_fall_back_repeat: bool = True,
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    """Missing hours per series in [start_ms, end_ms): (mature, pending).

    ``spans``: each series' first and last stored hour in the whole table;
    ``stored``: its stored hours in the window. ``allow_fall_back_repeat``:
    the second copy of the repeated fall-back hour may be absent (coal table).
    """
    mature: dict[str, list[int]] = {}
    pending: dict[str, list[int]] = {}
    for plant, (first, last) in spans.items():
        have = stored.get(plant, set())
        hour = max(first, start_ms)
        hour += -hour % HOUR_MS
        while hour <= last and hour < end_ms:
            if hour not in have and not (
                allow_fall_back_repeat and is_fall_back_repeat(hour)
            ):
                (mature if hour < mature_end else pending).setdefault(plant, []).append(
                    hour
                )
            hour += HOUR_MS
    return mature, pending


def describe(gaps: dict[str, list[int]]) -> str:
    def utc(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M"
        )

    return "; ".join(
        f"{plant}: {len(hours)} h ({utc(hours[0])} .. {utc(hours[-1])} UTC)"
        for plant, hours in sorted(gaps.items())
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        help="last day of the window (default: open)",
    )
    parser.add_argument(
        "--table",
        choices=sorted(TABLES),
        default="coal",
        help="coal: chile_generation_data per plant; all_fuels: chile_generation_all_fuels per series",
    )
    parser.add_argument(
        "--report-only", action="store_true", help="log missing hours, exit 0"
    )
    parser.add_argument(
        "--now", type=datetime.fromisoformat, help="run time (default: now)"
    )
    args = parser.parse_args()
    now = args.now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        parser.error("--now needs a UTC offset")

    start_ms = local_midnight_ms(args.start_date)
    end_ms = (
        local_midnight_ms(args.end_date + timedelta(days=1))
        if args.end_date
        else sys.maxsize
    )
    table, series = TABLES[args.table].table_name, TABLES[args.table].series_expression
    try:
        with create_engine(get_connection_url()).connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            spans = {
                plant: (first, last)
                for plant, first, last in conn.execute(
                    text(
                        f"SELECT {series}, MIN(timestamp_ms), MAX(timestamp_ms) "
                        f"FROM {table} GROUP BY 1"
                    )
                )
            }
            stored: dict[str, set[int]] = {}
            for plant, ts in conn.execute(
                text(
                    f"SELECT DISTINCT {series}, timestamp_ms FROM {table} "
                    "WHERE timestamp_ms >= :s AND timestamp_ms < :e"
                ),
                {"s": start_ms, "e": end_ms},
            ):
                stored.setdefault(plant, set()).add(ts)
    except Exception as e:  # noqa: BLE001 - report and exit 2
        logger.error(f"Chile hour check could not run: {e}")
        return 2

    mature, pending = find_gaps(
        spans,
        stored,
        start_ms,
        end_ms,
        mature_end_ms(now),
        allow_fall_back_repeat=args.table == "coal",
    )
    if pending:
        logger.info(f"pending (re-fetched by the next run): {describe(pending)}")
    if mature:
        message = (
            f"CHILE HOURS MISSING ({table}) since {args.start_date}: {describe(mature)}"
        )
        if args.report_only:
            logger.warning(message)
            return 0
        logger.error(message)
        return 1
    logger.info(
        f"Chile hour check OK ({table}): every hour from {args.start_date} to the mature cutoff is stored"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
