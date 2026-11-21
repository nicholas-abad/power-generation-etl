"""Unit tests for the Chile window lookback and the mature-hour check."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from check_chile_hours import HOUR_MS, find_gaps, is_fall_back_repeat, mature_end_ms
import get_latest_date
from get_latest_date import FALLBACK_DATE, with_lookback


def ms(stamp: str) -> int:
    return int(
        datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc).timestamp() * 1000
    )


def hours(first: str, last: str, skip: tuple[str, str] | None = None) -> set[int]:
    """Every hour from ``first`` to ``last`` (UTC), minus ``skip`` (inclusive)."""
    gap = range(ms(skip[0]), ms(skip[1]) + 1) if skip else range(0)
    return {h for h in range(ms(first), ms(last) + 1, HOUR_MS) if h not in gap}


# Guacolda, July 2026: the run on Sunday 26 July (08:42 UTC, 04:42 Chile) stored
# Friday and early Sunday but not the Chile-local Saturday (UTC 25 Jul 04:00 to
# 26 Jul 03:00).
SATURDAY = ("2026-07-25 04:00", "2026-07-26 03:00")
START = ms("2026-07-19 04:00")  # local midnight of the window start, 19 July
OPEN = sys.maxsize  # no end date: the weekly run


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
        assert mature_end_ms(datetime(2026, 7, 26, 8, 42, tzinfo=timezone.utc)) == ms(
            "2026-07-25 04:00"
        )


class TestFindGaps:
    def test_fresh_saturday_gap_is_pending(self):
        stored = hours("2026-07-19 04:00", "2026-07-26 06:00", skip=SATURDAY)
        mature, pending = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-07-26 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            OPEN,
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
            OPEN,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert len(mature["TER GUACOLDA"]) == 24

    def test_gap_refetched_by_the_lookback_passes(self):
        stored = hours("2026-07-19 04:00", "2026-08-02 06:00")
        mature, pending = find_gaps(
            {"TER GUACOLDA": (ms("2019-01-01 03:00"), ms("2026-08-02 06:00"))},
            {"TER GUACOLDA": stored},
            START,
            OPEN,
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
            OPEN,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert len(mature["TER GUACOLDA"]) == 24

    def test_retired_plant_is_not_expected(self):
        mature, pending = find_gaps(
            {"TER VENTANAS": (ms("2019-01-01 03:00"), ms("2022-07-06 12:00"))},
            {},
            START,
            OPEN,
            mature_end_ms(datetime(2026, 8, 2, 8, 41, tzinfo=timezone.utc)),
        )
        assert (mature, pending) == ({}, {})


class TestEndDate:
    # A manual re-run of 7-12 June (Chile-local) on 15 June; the table holds
    # later hours too.
    SPAN = {"TER IEM": (ms("2019-01-01 03:00"), ms("2026-06-15 03:00"))}
    WINDOW = (
        ms("2026-06-07 04:00"),
        ms("2026-06-13 04:00"),
    )  # 7 Jun .. end of 12 Jun local

    def run(self, missing: str) -> dict[str, list[int]]:
        stored = hours("2026-06-07 04:00", "2026-06-15 03:00") - {ms(missing)}
        mature, _ = find_gaps(
            self.SPAN,
            {"TER IEM": stored},
            *self.WINDOW,
            mature_end_ms(datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)),
        )
        return mature

    def test_gap_after_the_end_date_is_ignored(self):
        assert self.run("2026-06-13 04:00") == {}

    def test_gap_inside_the_window_fails(self):
        assert self.run("2026-06-12 12:00") == {"TER IEM": [ms("2026-06-12 12:00")]}


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
        stored = hours("2026-04-01 03:00", "2026-04-10 03:00") - {
            self.REPEAT,
            self.REPEAT + HOUR_MS,
        }
        mature, _ = find_gaps(
            {"TER IEM": (ms("2019-01-01 03:00"), ms("2026-04-10 03:00"))},
            {"TER IEM": stored},
            ms("2026-04-01 03:00"),
            OPEN,
            ms("2026-04-08 04:00"),
        )
        assert mature == {"TER IEM": [self.REPEAT + HOUR_MS]}


class TestAllFuelSeries:
    # The all-fuel table: one series per (plant_key, technology); the extractor
    # keeps both copies of the repeated fall-back hour (5 April 2026, 03:00 UTC).
    COAL, GAS = "386 Térmica/Carbón", "386 Térmica/Gas Natural"
    REPEAT = ms("2026-04-05 03:00")
    DAY = (ms("2026-04-04 03:00"), ms("2026-04-06 04:00"))  # local 4 and 5 April
    SPAN = (ms("2019-01-01 03:00"), ms("2026-04-10 03:00"))

    def check(self, stored: dict[str, set[int]]) -> dict[str, list[int]]:
        mature, _ = find_gaps(
            {self.COAL: self.SPAN, self.GAS: self.SPAN},
            stored,
            *self.DAY,
            ms("2026-04-08 04:00"),
            allow_fall_back_repeat=False,
        )
        return mature

    def test_a_complete_day_with_the_repeated_hour_passes(self):
        full = hours("2026-04-04 03:00", "2026-04-06 03:00")
        assert self.REPEAT in full and len(full) == 49  # 24 + 25 local hours
        assert self.check({self.COAL: full, self.GAS: full}) == {}

    def test_a_series_missing_while_its_sibling_is_present_fails(self):
        full = hours("2026-04-04 03:00", "2026-04-06 03:00")
        late = full - {ms("2026-04-04 12:00")}
        assert self.check({self.COAL: full, self.GAS: late}) == {
            self.GAS: [ms("2026-04-04 12:00")]
        }

    def test_the_repeated_fall_back_hour_missing_fails(self):
        full = hours("2026-04-04 03:00", "2026-04-06 03:00")
        assert self.check({self.COAL: full, self.GAS: full - {self.REPEAT}}) == {
            self.GAS: [self.REPEAT]
        }

    def test_a_sparse_series_fails(self):
        # A unit the source omits in idle hours: decided case by case.
        full = hours("2026-04-04 03:00", "2026-04-06 03:00")
        sparse = {ms("2026-04-04 03:00"), ms("2026-04-06 03:00")}
        assert len(self.check({self.COAL: full, self.GAS: sparse})[self.GAS]) == 47


class TestLatestDateAllFuels:
    def test_maps_to_the_all_fuel_table(self):
        assert (
            get_latest_date.SOURCE_CONFIG["chile_all_fuels"][0]
            == "chile_generation_all_fuels"
        )

    def test_an_empty_table_still_returns_the_fallback(self, monkeypatch):
        # The weekly job's guard fails on an empty table; this contract is
        # why it must (1970 minus the lookback is still 1970).
        class _Result:
            def fetchone(self):
                return (None,)

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, statement):
                assert "FROM chile_generation_all_fuels" in str(statement)
                return _Result()

        class _Engine:
            def connect(self):
                return _Conn()

        monkeypatch.setattr(
            get_latest_date, "get_connection_url", lambda: "postgresql://x"
        )
        monkeypatch.setattr(get_latest_date, "create_engine", lambda url: _Engine())
        latest = get_latest_date.get_latest_date("chile_all_fuels")
        assert with_lookback(latest, 14) == FALLBACK_DATE
