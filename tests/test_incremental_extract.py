#!/usr/bin/env python3
"""Unit tests for the env-var override helpers in incremental_extract."""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from incremental_extract import (
    LONG_WINDOW_MONTHS,
    warn_if_long_window,
    window_end,
    window_start,
)


@pytest.fixture
def loguru_messages():
    """Capture loguru log messages into a list. Loguru bypasses pytest's
    capsys/caplog because it uses its own sinks rather than the stdlib
    logging module."""
    messages: list[str] = []
    sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
    yield messages
    logger.remove(sink_id)


class TestWindowStart:
    def test_uses_override_when_set(self, monkeypatch):
        monkeypatch.setenv("START_OVERRIDE", "2025-01-01")
        assert window_start("entsoe") == date(2025, 1, 1)

    def test_falls_through_to_resume_from_when_unset(self, monkeypatch):
        monkeypatch.delenv("START_OVERRIDE", raising=False)
        with patch(
            "incremental_extract.resume_from", return_value=date(2024, 6, 1)
        ) as mock_resume:
            assert window_start("entsoe") == date(2024, 6, 1)
            mock_resume.assert_called_once_with("entsoe")

    def test_empty_override_falls_through(self, monkeypatch):
        # GitHub Actions exports unset workflow_dispatch inputs as empty strings.
        monkeypatch.setenv("START_OVERRIDE", "")
        with patch(
            "incremental_extract.resume_from", return_value=date(2024, 6, 1)
        ) as mock_resume:
            assert window_start("entsoe") == date(2024, 6, 1)
            mock_resume.assert_called_once()

    def test_malformed_override_raises(self, monkeypatch):
        monkeypatch.setenv("START_OVERRIDE", "not-a-date")
        with pytest.raises(ValueError):
            window_start("entsoe")


class TestWindowEnd:
    def test_uses_override_when_set(self, monkeypatch):
        monkeypatch.setenv("END_OVERRIDE", "2025-03-31")
        assert window_end(date(2026, 5, 3)) == date(2025, 3, 31)

    def test_defaults_to_today_when_unset(self, monkeypatch):
        monkeypatch.delenv("END_OVERRIDE", raising=False)
        assert window_end(date(2026, 5, 3)) == date(2026, 5, 3)

    def test_empty_override_defaults_to_today(self, monkeypatch):
        monkeypatch.setenv("END_OVERRIDE", "")
        assert window_end(date(2026, 5, 3)) == date(2026, 5, 3)

    def test_malformed_override_raises(self, monkeypatch):
        monkeypatch.setenv("END_OVERRIDE", "garbage")
        with pytest.raises(ValueError):
            window_end(date(2026, 5, 3))


class TestWarnIfLongWindow:
    def test_short_window_does_not_warn(self, loguru_messages):
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2025, 3, 31))
        assert not any("may exceed" in m for m in loguru_messages)

    def test_entsoe_at_threshold_does_not_warn(self, loguru_messages):
        # 3 months exactly — the ENTSO-E ceiling since one-day requests
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2025, 3, 31))
        assert not any("may exceed" in m for m in loguru_messages)

    def test_entsoe_just_over_threshold_warns(self, loguru_messages):
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2025, 4, 30))
        joined = "\n".join(loguru_messages)
        assert "may exceed" in joined
        assert "4 months" in joined
        assert "entsoe" in joined

    def test_occto_keeps_twelve_month_ceiling(self, loguru_messages):
        warn_if_long_window("occto", date(2025, 1, 1), date(2025, 12, 31))
        assert not any("may exceed" in m for m in loguru_messages)
        warn_if_long_window("occto", date(2025, 1, 1), date(2026, 1, 31))
        assert any("13 months" in m for m in loguru_messages)

    def test_thresholds(self):
        # ENTSO-E: ~95 min per month of one-day requests; OCCTO ~6 min
        assert LONG_WINDOW_MONTHS == {"entsoe": 3, "occto": 12}


class TestEntsoeEmptyOutputGuard:
    """A fully-past ENTSOE month that produces no JSONL must raise, mirroring
    the OCCTO guard — otherwise the job goes green, latest_date never advances,
    and the same month re-extracts forever loading nothing."""

    def _run_entsoe(self, monkeypatch, tmp_path, start, end):
        import incremental_extract as ie

        monkeypatch.setenv("START_OVERRIDE", start)
        monkeypatch.setenv("END_OVERRIDE", end)
        monkeypatch.setattr(ie, "EXTRACTED_DATA", tmp_path)
        # subprocess is a no-op: simulates extraction that writes nothing
        monkeypatch.setattr(ie, "run_extract", lambda *a, **k: 0)
        return ie.extract_entsoe()

    def test_past_month_with_no_output_raises(self, monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match="produced no entsoe_"):
            self._run_entsoe(monkeypatch, tmp_path, "2025-01-01", "2025-01-01")

    def test_current_month_with_no_output_only_warns(
        self, monkeypatch, tmp_path, loguru_messages
    ):
        today = date.today()
        first = today.replace(day=1).isoformat()
        # current month, empty output → warn, no raise, returns 1 iteration
        n = self._run_entsoe(monkeypatch, tmp_path, first, first)
        assert n == 1
        assert any("publication lag" in m for m in loguru_messages)


class TestOcctoWindow:
    """OCCTO stores 24:00 as the next day's 00:00, so latest_date + 1 skipped a
    day per run (every Sunday since 2026-06). Each run now refetches a lookback
    and ends at the last fully published JST day."""

    def test_sunday_run_refetches_the_skipped_sunday(self):
        from incremental_extract import occto_window

        # run on Sunday 2026-09-27; the stored max slot is Sat 24:00 =
        # 2026-09-27 00:00, so resume_from says Monday 2026-09-28
        start, end = occto_window(date(2026, 9, 28), date(2026, 9, 27), False, date(2026, 9, 27))
        assert end == date(2026, 9, 25)
        assert start == date(2026, 9, 12)  # 14 days ending at the published day
        assert start <= date(2026, 9, 20) <= end  # the previously lost Sunday

    def test_far_behind_resume_wins(self):
        from incremental_extract import occto_window

        start, _ = occto_window(date(2026, 1, 1), date(2026, 9, 27), False, date(2026, 9, 27))
        assert start == date(2026, 1, 1)

    def test_start_override_is_honoured_but_end_still_clamped(self):
        from incremental_extract import occto_window

        start, end = occto_window(date(2026, 9, 26), date(2026, 12, 31), True, date(2026, 9, 27))
        assert (start, end) == (date(2026, 9, 26), date(2026, 9, 25))

    def test_extractor_is_called_with_the_json_backend(self, monkeypatch, tmp_path):
        import incremental_extract as ie

        calls = []
        monkeypatch.setenv("START_OVERRIDE", "2025-01-01")
        monkeypatch.setenv("END_OVERRIDE", "2025-01-03")
        monkeypatch.setattr(ie, "EXTRACTED_DATA", tmp_path)

        def fake_extract(cmd, **kwargs):
            calls.append(cmd)
            (tmp_path / "occto_generation_x_etl.jsonl").write_text("")
            return 0

        monkeypatch.setattr(ie, "run_extract", fake_extract)
        monkeypatch.setattr(ie, "load_and_remove", lambda source, f: f.unlink())
        assert ie.extract_occto() == 1
        cmd = calls[0]
        assert cmd[cmd.index("--backend") + 1] == "json"
        assert cmd[cmd.index("--start-date") + 1] == "2025-01-01"
        assert cmd[cmd.index("--end-date") + 1] == "2025-01-03"

    def test_override_before_publication_is_clamped(self):
        from incremental_extract import occto_window

        start, _ = occto_window(date(2024, 1, 1), date(2024, 4, 30), True, date(2026, 9, 27))
        assert start == date(2024, 3, 25)
