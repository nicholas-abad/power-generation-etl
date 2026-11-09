#!/usr/bin/env python3
"""Unit tests for the env-var override helpers in incremental_extract."""

import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from incremental_extract import (
    LONG_WINDOW_MONTHS,
    entsoe_chunks,
    keep_coal_plants,
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
    def test_occto_keeps_twelve_month_ceiling(self, loguru_messages):
        warn_if_long_window("occto", date(2025, 1, 1), date(2025, 12, 31))
        assert not any("may exceed" in m for m in loguru_messages)
        warn_if_long_window("occto", date(2025, 1, 1), date(2026, 1, 31))
        assert any("13 months" in m for m in loguru_messages)

    def test_thresholds(self):
        # OCCTO ~6 min per month; ENTSO-E uses a time budget instead
        assert LONG_WINDOW_MONTHS == {"occto": 12}


class TestEntsoeEmptyOutputGuard:
    """A long-published ENTSOE chunk that produces no JSONL must raise —
    otherwise the job goes green, latest_date never advances, and the same
    days re-extract forever loading nothing."""

    def _run_entsoe(self, monkeypatch, tmp_path, start, end):
        import incremental_extract as ie

        monkeypatch.setenv("START_OVERRIDE", start)
        monkeypatch.setenv("END_OVERRIDE", end)
        monkeypatch.delenv("JOB_STARTED_AT", raising=False)
        monkeypatch.setattr(ie, "EXTRACTED_DATA", tmp_path)
        # subprocess is a no-op: simulates extraction that writes nothing
        monkeypatch.setattr(ie, "run_extract", lambda *a, **k: 0)
        return ie.extract_entsoe()

    def test_past_chunk_with_no_output_raises(self, monkeypatch, tmp_path):
        with pytest.raises(RuntimeError, match="produced no entsoe_"):
            self._run_entsoe(monkeypatch, tmp_path, "2025-01-01", "2025-01-01")

    def test_recent_chunk_with_no_output_only_warns(
        self, monkeypatch, tmp_path, loguru_messages
    ):
        today = date.today().isoformat()
        n = self._run_entsoe(monkeypatch, tmp_path, today, today)
        assert n == 1
        assert any("publication lag" in m for m in loguru_messages)


class TestEntsoeChunks:
    def _labels(self, chunks):
        return [(c.label, c.is_lookback) for c in chunks]

    def test_new_days_come_before_the_lookback(self):
        chunks = entsoe_chunks(date(2026, 11, 6), date(2026, 11, 9), 7, 3)
        assert self._labels(chunks) == [
            ("2026-11-06..2026-11-08", False),
            ("2026-11-09..2026-11-09", False),
            ("2026-10-30..2026-11-01", True),
            ("2026-11-02..2026-11-04", True),
            ("2026-11-05..2026-11-05", True),
        ]

    def test_a_month_boundary_does_not_split_a_chunk(self):
        chunks = entsoe_chunks(date(2026, 10, 30), date(2026, 11, 1), 0, 3)
        assert self._labels(chunks) == [("2026-10-30..2026-11-01", False)]

    def test_up_to_date_run_only_refetches_the_lookback(self):
        chunks = entsoe_chunks(date(2026, 11, 10), date(2026, 11, 9), 3, 3)
        assert self._labels(chunks) == [("2026-11-07..2026-11-09", True)]

    def test_empty_table_starts_at_2019(self):
        chunks = entsoe_chunks(date(2018, 12, 1), date(2019, 1, 2), 7, 3)
        assert self._labels(chunks) == [("2019-01-01..2019-01-02", False)]


class _FakeEntsoe:
    """Extractor + loader + clock: each chunk takes ``minutes_per_day`` per day
    and writes one JSONL; loads are recorded in order."""

    def __init__(self, ie, tmp_path, minutes_per_day, exit_codes=None):
        self.ie, self.tmp_path = ie, tmp_path
        self.minutes_per_day = minutes_per_day
        self.exit_codes = exit_codes or {}
        self.clock = 0.0
        self.events: list[str] = []

    def now(self):
        return self.clock

    def run_extract(self, cmd, env):
        label = f"{env['ENTSOE_START_DATE']}..{env['ENTSOE_END_DATE']}"
        days = (
            date.fromisoformat(env["ENTSOE_END_DATE"])
            - date.fromisoformat(env["ENTSOE_START_DATE"])
        ).days + 1
        self.events.append(f"extract {label}")
        self.clock += days * self.minutes_per_day * 60
        (self.tmp_path / f"entsoe_{label}_etl.jsonl").write_text("{}\n")
        return self.exit_codes.get(label, 0)

    def load(self, source, f):
        self.events.append(f"load {f.name}")
        f.unlink()

    def install(self, monkeypatch, start, end, latest=None):
        ie = self.ie
        monkeypatch.setattr(ie, "EXTRACTED_DATA", self.tmp_path)
        monkeypatch.setattr(ie, "run_extract", self.run_extract)
        monkeypatch.setattr(ie, "load_and_remove", self.load)
        monkeypatch.setattr(ie, "_now", self.now)
        monkeypatch.delenv("JOB_STARTED_AT", raising=False)
        monkeypatch.setenv("END_OVERRIDE", end)
        if start:
            monkeypatch.setenv("START_OVERRIDE", start)
        else:
            monkeypatch.delenv("START_OVERRIDE", raising=False)
            monkeypatch.setattr(ie, "get_latest_date", lambda source: latest)


@pytest.fixture
def ie():
    import incremental_extract

    return incremental_extract


class TestEntsoeLoop:
    def test_each_chunk_is_loaded_before_the_next_is_extracted(
        self, ie, monkeypatch, tmp_path
    ):
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=1)
        fake.install(monkeypatch, "2026-10-01", "2026-10-06")
        assert ie.extract_entsoe() == 2
        assert fake.events == [
            "extract 2026-10-01..2026-10-03",
            "load entsoe_2026-10-01..2026-10-03_etl.jsonl",
            "extract 2026-10-04..2026-10-06",
            "load entsoe_2026-10-04..2026-10-06_etl.jsonl",
        ]

    def test_a_slow_run_stops_before_the_budget_and_keeps_what_it_loaded(
        self, ie, monkeypatch, tmp_path
    ):
        # 50 min per day: 150 min per chunk, so two chunks fit in 320 min.
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=50)
        fake.install(monkeypatch, "2026-10-01", "2026-10-12")
        with pytest.raises(RuntimeError, match="BEHIND: first unfinished day 2026-10-07"):
            ie.extract_entsoe()
        assert [e for e in fake.events if e.startswith("load")] == [
            "load entsoe_2026-10-01..2026-10-03_etl.jsonl",
            "load entsoe_2026-10-04..2026-10-06_etl.jsonl",
        ]

    def test_every_run_moves_forward_even_if_only_one_chunk_fits(
        self, ie, monkeypatch, tmp_path
    ):
        # 100 min per day: one 3-day chunk fits. Lookback first would redo the
        # same stored days every week; new days first moves the latest day.
        latest = "2026-10-29"
        for _ in range(2):
            fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=100)
            fake.install(monkeypatch, None, "2026-11-09", latest=latest)
            with pytest.raises(RuntimeError, match="BEHIND"):
                ie.extract_entsoe()
            loaded = [e for e in fake.events if e.startswith("load")]
            assert len(loaded) == 1 and "2026-10-30" <= loaded[0][12:22]
            latest = loaded[0][24:34]
        assert latest == "2026-11-04"

    def test_a_first_chunk_over_budget_by_estimate_does_not_start(
        self, ie, monkeypatch, tmp_path
    ):
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=1)
        fake.install(monkeypatch, "2026-10-01", "2026-10-03")
        monkeypatch.setenv("JOB_STARTED_AT", str(-300 * 60))  # 300 min ago
        with pytest.raises(RuntimeError, match="first unfinished day 2026-10-01"):
            ie.extract_entsoe()
        assert fake.events == []

    def test_a_repair_reports_its_own_first_unfinished_day(
        self, ie, monkeypatch, tmp_path
    ):
        # The table maximum (29 Oct) is later than anything the repair did:
        # the follow-up must start from the repair's own first unfinished day.
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=50)
        fake.install(monkeypatch, "2026-10-01", "2026-11-09")
        monkeypatch.setattr(ie, "get_latest_date", lambda source: "2026-10-29")
        with pytest.raises(RuntimeError, match="start_override must pass 2026-10-07"):
            ie.extract_entsoe()

    def test_lookback_skipped_for_time_only_warns(
        self, ie, monkeypatch, tmp_path, loguru_messages
    ):
        # 3 new days at 90 min/day = 270 min; no lookback chunk fits after it.
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=90)
        fake.install(monkeypatch, None, "2026-11-09", latest="2026-11-06")
        assert ie.extract_entsoe() == 1
        assert any("lookback chunks skipped" in m for m in loguru_messages)

    def test_partial_chunks_are_all_reported(self, ie, monkeypatch, tmp_path):
        fake = _FakeEntsoe(
            ie,
            tmp_path,
            minutes_per_day=1,
            exit_codes={"2026-10-01..2026-10-03": 2, "2026-10-04..2026-10-06": 2},
        )
        fake.install(monkeypatch, "2026-10-01", "2026-10-09")
        with pytest.raises(RuntimeError) as err:
            ie.extract_entsoe()
        assert "2026-10-01..2026-10-03" in str(err.value)
        assert "2026-10-04..2026-10-06" in str(err.value)
        assert len([e for e in fake.events if e.startswith("load")]) == 3

    def test_the_workflow_is_told_once_rows_are_stored(
        self, ie, monkeypatch, tmp_path
    ):
        out = tmp_path / "github_output"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out))
        fake = _FakeEntsoe(ie, tmp_path, minutes_per_day=1)
        fake.install(monkeypatch, "2026-10-01", "2026-10-03")
        ie.extract_entsoe()
        assert out.read_text() == "loaded=true\n"


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
        assert start == date(2025, 12, 31)  # the latest stored date itself

    def test_far_behind_run_starts_at_the_first_unfetched_day(self, monkeypatch):
        import incremental_extract as ie

        # stored through Sat 2026-09-26 24:00 = 2026-09-27 00:00; the job then
        # did not run for four weeks
        monkeypatch.delenv("START_OVERRIDE", raising=False)
        monkeypatch.setattr(ie, "get_latest_date", lambda source: "2026-09-27")
        start, end = ie.occto_window(ie.window_start("occto"), date(2026, 10, 25), False, date(2026, 10, 25))
        assert (start, end) == (date(2026, 9, 27), date(2026, 10, 23))

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
        monkeypatch.setattr(ie, "occto_coal_plants", set)
        assert ie.extract_occto() == 1
        cmd = calls[0]
        assert cmd[cmd.index("--backend") + 1] == "json"
        assert cmd[cmd.index("--start-date") + 1] == "2025-01-01"
        assert cmd[cmd.index("--end-date") + 1] == "2025-01-03"

    def test_override_before_publication_is_clamped(self):
        from incremental_extract import occto_window

        start, _ = occto_window(date(2024, 1, 1), date(2024, 4, 30), True, date(2026, 9, 27))
        assert start == date(2024, 3, 25)


def test_occto_files_keep_only_plants_with_coal(tmp_path):
    rows = [
        {"plant": "碧南火力発電所", "unit": "1号機", "fuel_type": "coal"},
        # coal stored earlier, coal unit idle in this window
        {"plant": "鹿島火力発電所", "unit": "7号機", "fuel_type": "LNG"},
        {"plant": "柏崎刈羽原子力発電所", "unit": "6号機", "fuel_type": "nuclear"},
        {"plant": "富津火力発電所", "unit": "1号機", "fuel_type": "LNG"},
    ]
    f = tmp_path / "occto_x_etl.jsonl"
    f.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    known = {"鹿島火力発電所"}
    assert keep_coal_plants(f, known) == 2
    kept = [json.loads(line)["plant"] for line in f.read_text().splitlines()]
    assert kept == ["碧南火力発電所", "鹿島火力発電所"]
    assert "碧南火力発電所" in known  # learned from the file
