"""Unit tests for the Chile window lookback and the mature-hour check."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from check_chile_hours import HOUR_MS, find_gaps, is_fall_back_repeat, mature_end_ms
from get_latest_date import FALLBACK_DATE, with_lookback


def ms(stamp: str) -> int:
    return int(datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc).timestamp() * 1000)


def hours(first: str, last: str, skip: tuple[str, str] | None = None) -> set[int]:
    """Every hour from ``first`` to ``last`` (UTC), minus ``skip`` (inclusive)."""
    gap = range(ms(skip[0]), ms(skip[1]) + 1) if skip else range(0)
    return {h for h in range(ms(first), ms(last) + 1, HOUR_MS) if h not in gap}


# Guacolda, July 2026: the run on Sunday 26 July (08:42 UTC, 04:42 Chile) stored
# Friday and early Sunday but not the Chile-local Saturday (UTC 25 Jul 04:00 to
# 26 Jul 03:00).
SATURDAY = ("2026-07-25 04:00", "2026-07-26 03:00")
START = ms("2026-07-19 04:00")  # local midnight of the window start, 19 July


class TestWithLookback:
    def test_moves_back(self):
        assert with_lookback("2026-07-26", 14) == "2026-07-12"

    def test_across_new_year(self):
        assert with_lookback("2027-01-05", 14) == "2026-12-22"

    def test_zero_is_unchanged(self):
        assert with_lookback("2026-07-26", 0) == "2026-07-26"

    def test_empty_table_fallback_unchanged(self):
        assert with_lookback(FALLBACK_DATE, 14) == FALLBACK_DATE

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            with_lookback("2026-07-26", -1)


class TestMatureEnd:
    def test_sunday_run_matures_up_to_friday(self):
        # Chile-local Sunday 26 July: Friday 24 is mature, Saturday 25 is not.
        assert mature_end_ms(datetime(2026, 7, 26, 8, 42, tzinfo=timezone.utc)) == ms("2026-07-25 04:00")


class TestFindGaps:
    def test_fresh_saturday_gap_is_pending(self):
        stored = hours("2026-07-19 04:00", "2026-07-26 06:00", skip=SATURDAY)
        mature, pending = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-07-26 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            mature_end_ms(datetime(2026, 7, 26, 8, 42, tzinfo=timezone.utc)),
        )
        assert mature == {}
        assert len(pending["TER GUACOLDA"]) == 24

    def test_same_gap_still_missing_a_week_later_fails(self):
        stored = hours("2026-07-19 04:00", "2026-08-02 06:00", skip=SATURDAY)
        mature, _ = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-08-02 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert len(mature["TER GUACOLDA"]) == 24

    def test_gap_refetched_by_the_lookback_passes(self):
        stored = hours("2026-07-19 04:00", "2026-08-02 06:00")
        mature, pending = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-08-02 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert (mature, pending) == ({}, {})

    def test_last_mature_day_missing_but_later_hours_present_fails(self):
        # Chile-local Friday 31 July missing; Saturday and Sunday stored.
        friday = ("2026-07-31 04:00", "2026-08-01 03:00")
        stored = hours("2026-07-19 04:00", "2026-08-02 06:00", skip=friday)
        mature, _ = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-08-02 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert len(mature["TER GUACOLDA"]) == 24

    def test_retired_plant_is_not_expected(self):
        mature, pending = find_gaps(
            {"TER VENTANAS": (ms("2019-01-01 03:00"), ms("2022-07-06 12:00"))},
            {},
            START,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert (mature, pending) == ({}, {})


class TestFallBackHour:
    # Chile's clocks went back at local midnight on 5 April 2026: local 23:00 on
    # 4 April happened twice, at 02:00 UTC (-03) and 03:00 UTC (-04). The
    # extractor keeps the first; 03:00 UTC is never stored.
    REPEAT = ms("2026-04-05 03:00")

    def test_second_occurrence_detected(self):
        assert is_fall_back_repeat(self.REPEAT)
        assert not is_fall_back_repeat(self.REPEAT - HOUR_MS)
        assert not is_fall_back_repeat(self.REPEAT + HOUR_MS)

    def test_repeat_hour_allowed_but_adjacent_gap_fails(self):
        stored = hours("2026-04-01 03:00", "2026-04-10 03:00") - {self.REPEAT, self.REPEAT + HOUR_MS}
        mature, _ = find_gaps(
            {"TER IEM": (ms("2019-01-01 03:00"), ms("2026-04-10 03:00"))},
            {"TER IEM": stored},
            ms("2026-04-01 03:00"),
            ms("2026-04-08 04:00"),
        )
        assert mature == {"TER IEM": [self.REPEAT + HOUR_MS]}
