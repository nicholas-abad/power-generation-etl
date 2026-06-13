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
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2025, 6, 30))
        assert not any("may exceed" in m for m in loguru_messages)

    def test_at_threshold_does_not_warn(self, loguru_messages):
        # 12 months exactly — should NOT warn (LONG_WINDOW_MONTHS is the
        # max allowed without warning).
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2025, 12, 31))
        assert not any("may exceed" in m for m in loguru_messages)

    def test_just_over_threshold_warns(self, loguru_messages):
        warn_if_long_window("entsoe", date(2025, 1, 1), date(2026, 1, 31))
        joined = "\n".join(loguru_messages)
        assert "may exceed" in joined
        assert "13 months" in joined
        assert "entsoe" in joined

    def test_threshold_constant_is_12(self):
        # Sanity check the documented soft ceiling.
        assert LONG_WINDOW_MONTHS == 12


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
        monkeypatch.setattr(ie, "run", lambda *a, **k: None)
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
