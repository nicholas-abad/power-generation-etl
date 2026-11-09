#!/usr/bin/env python3
"""Incremental extraction for ENTSOE (3-day chunks) and OCCTO (months).

Usage:
    python src/incremental_extract.py {entsoe|occto}

Reads latest_date from Neon, computes the chunks needed (clamped to a
per-source MIN_START_DATE), then for each chunk: invokes the extractor
binary directly, loads the produced JSONL into Neon, and removes it, so a
run that stops early keeps every chunk it finished.

ENTSO-E example: latest stored day 2026-11-05, run on 2026-11-09 → the 2
newest stored days (may be incomplete) and the new days first (11-04..11-06,
11-07..11-09), then the 7-day lookback (10-28..10-30, 10-31..11-02, 11-03),
each loaded before the next starts, until the time budget runs out.

Design: see /Users/nicholasabad/.claude/plans/effervescent-hatching-horizon.md
"""

import json
import os
import subprocess
import sys
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from loguru import logger
from sqlalchemy import create_engine, text

from get_latest_date import get_connection_url, get_latest_date

REPO_ROOT = Path(__file__).resolve().parent.parent
EXTRACTORS_DIR = REPO_ROOT / "extractors"
EXTRACTED_DATA = REPO_ROOT / "extracted_data"
EXTRACT_BIN = EXTRACTORS_DIR / ".venv" / "bin" / "energy-extract"
LOAD_SCRIPT = REPO_ROOT / "src" / "database_management.py"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    min_start_date: date


SOURCES = {
    "entsoe": SourceConfig("entsoe", date(2019, 1, 1)),
    # OCCTO publishes nationwide unit data from 2024-03-25 (earlier days are
    # Kyushu-only or empty; the extractor refuses them as an empty window).
    "occto": SourceConfig("occto", date(2024, 3, 25)),
}


def gh_group(label: str) -> None:
    print(f"::group::{label}", flush=True)


def gh_endgroup() -> None:
    print("::endgroup::", flush=True)


def add_months(d: date, n: int) -> date:
    """Add n months to d, clamping day to the last day of the new month."""
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, min(d.day, monthrange(y, m)[1]))


def resume_from(source: str) -> date:
    """First date NOT yet in DB, clamped to MIN_START_DATE."""
    cfg = SOURCES[source]
    latest = date.fromisoformat(get_latest_date(source))
    if latest < cfg.min_start_date:
        latest = cfg.min_start_date - timedelta(days=1)
    return latest + timedelta(days=1)


def window_start(source: str) -> date:
    """Honor START_OVERRIDE env var if set (for historical re-runs from the
    GitHub Actions UI); otherwise fall through to incremental resume."""
    override = os.environ.get("START_OVERRIDE")
    return date.fromisoformat(override) if override else resume_from(source)


def window_end(today: date) -> date:
    """Honor END_OVERRIDE env var if set; otherwise default to today."""
    override = os.environ.get("END_OVERRIDE")
    return date.fromisoformat(override) if override else today


# 350-min job timeout. OCCTO: ~6 min per month -> ~12 months. ENTSO-E is
# chunked by days and stopped by ENTSOE_TIME_BUDGET_MINUTES instead.
LONG_WINDOW_MONTHS = {"occto": 12}


def warn_if_long_window(source: str, start: date, end: date) -> None:
    """Warn (don't fail) when an override window is large enough to risk
    hitting the 350-minute job timeout. The user can still proceed — this
    is a heads-up, not a guardrail."""
    months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    if months > LONG_WINDOW_MONTHS[source]:
        logger.warning(
            f"{source}: extracting {months} months ({start} → {end}) "
            f"may exceed the 350-min job timeout — consider splitting into "
            f"smaller windows."
        )


# OCCTO labels each half-hour by its END, so a day's last slot (24:00) is stored
# as the next day's 00:00 and the latest stored "date" is a day nobody has
# fetched yet. Resuming from latest + 1 therefore skipped one day per run
# (every Sunday since 2026-06). Every run now refetches a lookback window and
# stops at the last fully published day; the loader upserts (DO UPDATE), so
# overlap is harmless and also picks up late revisions and failed days.
OCCTO_LOOKBACK_DAYS = 14
OCCTO_PUBLICATION_LAG_DAYS = 2  # TSOs publish ~15:00 JST the next day
JST = timezone(timedelta(hours=9))


def occto_window(
    resume: date, end: date, override: bool, now_jst: date
) -> tuple[date, date]:
    """(start, end) for OCCTO: end at the last fully published day; without a
    START_OVERRIDE, start no later than OCCTO_LOOKBACK_DAYS before that.

    OCCTO stores 24:00 as the next day's 00:00, so the latest stored date is
    the day after the last complete day and resume_from's +1 skips a day:
    step back one day so a run that is far behind still starts at the first
    unfetched day."""
    published = now_jst - timedelta(days=OCCTO_PUBLICATION_LAG_DAYS)
    end = min(end, published)
    if not override:
        resume = min(
            resume - timedelta(days=1),
            published - timedelta(days=OCCTO_LOOKBACK_DAYS - 1),
        )
    return max(resume, SOURCES["occto"].min_start_date), end


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """subprocess.run with check=True; force unbuffered Python in children."""
    logger.info(f"$ {' '.join(cmd)}")
    env = kwargs.pop("env", None) or os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    return subprocess.run(cmd, check=True, env=env, **kwargs)


def run_extract(cmd: list[str], **kwargs) -> int:
    """Like run(), but tolerates the extractor's exit 2 (= PARTIAL: data files
    written, some periods failed — see extractors cli/runner.py). Returns the
    exit code so the caller can load the good rows, keep iterating, and fail
    the whole job at the end. Any other non-zero still raises."""
    logger.info(f"$ {' '.join(cmd)}")
    env = kwargs.pop("env", None) or os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    proc = subprocess.run(cmd, check=False, env=env, **kwargs)
    if proc.returncode not in (0, 2):
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    if proc.returncode == 2:
        logger.error(
            "extractor reported PARTIAL extraction (exit 2) — "
            "loading what succeeded; job will fail at the end"
        )
    return proc.returncode


def load_and_remove(source: str, jsonl: Path) -> None:
    """Load JSONL into Neon then delete. Skips empty files (current
    month may legitimately have 0 records, and the load step has a
    latent bug when given empty input)."""
    if jsonl.stat().st_size == 0:
        logger.warning(f"{jsonl.name} is empty — skipping load")
        jsonl.unlink()
        return
    run(
        [sys.executable, str(LOAD_SCRIPT), "load-data", source, str(jsonl)],
        cwd=REPO_ROOT,
    )
    jsonl.unlink()


# ENTSO-E data item 16.1.A allows one day per request, so a run makes one
# request per area per day (101 areas). Measured ~3.3 min per day per 50 areas
# in Oct 2026 and 6-7 in Nov 2026 (slower API): a whole month no longer fits
# the 350-min job, and the old month-at-a-time loop loaded nothing when it was
# cancelled (weekly runs of 1 and 8 Nov 2026). Chunks of a few days are loaded
# one by one, new days before the lookback, so any run that loads one chunk
# moves the latest stored day forward.
ENTSOE_CHUNK_DAYS = 3
# Days already stored, re-fetched to pick up late TSO corrections (the loader
# upserts, so overlap is harmless). They run after the new days, and only if
# the budget allows.
ENTSOE_LOOKBACK_DAYS = 7
# Job limit 350 min, minus a margin for the last load and the job's teardown.
ENTSOE_TIME_BUDGET_MINUTES = 320
# Per-day estimate before the first chunk has been timed (Nov 2026: ~14).
ENTSOE_INITIAL_MINUTES_PER_DAY = 15
# The newest stored days may still be incomplete (TSOs publish late), and a
# chunk that ended longer ago than this must produce rows. Without an
# override these days are re-fetched with the new days, not in the lookback,
# so a slow run cannot drop them (PR #146 review).
ENTSOE_PUBLICATION_LAG_DAYS = 2


class DateChunk(NamedTuple):
    """Days to extract in one extractor call, both ends inclusive."""

    start_date: date
    end_date: date
    is_lookback: bool

    @property
    def days(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def label(self) -> str:
        return f"{self.start_date}..{self.end_date}"


def _split_days(
    start: date, end: date, chunk_days: int, is_lookback: bool
) -> list[DateChunk]:
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append(DateChunk(current, chunk_end, is_lookback))
        current = chunk_end + timedelta(days=1)
    return chunks


def entsoe_chunks(
    new_start: date, end: date, lookback_days: int, chunk_days: int
) -> list[DateChunk]:
    """Chunks for one run: days new_start..end oldest first (they must fit the
    budget), then the ``lookback_days`` before new_start, oldest first (only if
    time is left). Nothing before MIN_START_DATE."""
    floor = SOURCES["entsoe"].min_start_date
    new_start = max(new_start, floor)
    lookback_start = max(new_start - timedelta(days=lookback_days), floor)
    lookback_end = min(new_start - timedelta(days=1), end)
    return _split_days(new_start, end, chunk_days, False) + _split_days(
        lookback_start, lookback_end, chunk_days, True
    )


def _now() -> float:
    return time.time()


def job_started_at() -> float:
    """Job start (epoch seconds) from JOB_STARTED_AT, set by the workflow's
    first step so setup time counts against the budget; now when unset."""
    value = os.environ.get("JOB_STARTED_AT")
    if value:
        return float(value)
    logger.warning("JOB_STARTED_AT unset: the ENTSO-E budget counts from now")
    return _now()


def mark_entsoe_loaded() -> None:
    """Tell the workflow that rows were stored, so the views refresh even if
    this job ends BEHIND or PARTIAL."""
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("loaded=true\n")


def extract_entsoe() -> int:
    today = date.today()
    override = bool(os.environ.get("START_OVERRIDE"))
    new_start = window_start("entsoe")
    if not override:
        new_start -= timedelta(days=ENTSOE_PUBLICATION_LAG_DAYS)
    chunks = entsoe_chunks(
        new_start,
        window_end(today),
        lookback_days=0 if override else ENTSOE_LOOKBACK_DAYS,
        chunk_days=ENTSOE_CHUNK_DAYS,
    )
    if not chunks:
        logger.info("ENTSOE up to date — 0 chunks")
        return 0

    started = job_started_at()
    # Slowest observed minutes per day so far, so the next chunk is sized
    # against the worst case of this run.
    observed_minutes_per_day: list[float] = []
    partial: list[str] = []
    first_unfinished: date | None = None
    lookback_skipped: list[str] = []
    last_new_day: date | None = None
    n = 0
    try:
        for chunk in chunks:
            elapsed = (_now() - started) / 60
            minutes_per_day = max(
                observed_minutes_per_day, default=ENTSOE_INITIAL_MINUTES_PER_DAY
            )
            if elapsed + chunk.days * minutes_per_day > ENTSOE_TIME_BUDGET_MINUTES:
                if chunk.is_lookback:
                    lookback_skipped.append(chunk.label)
                    continue
                first_unfinished = chunk.start_date
                break

            gh_group(f"Extract ENTSOE {chunk.label}")
            chunk_started = _now()
            rc = run_extract(
                [
                    str(EXTRACT_BIN),
                    "entsoe",
                    "--output",
                    str(EXTRACTED_DATA),
                    "--yes",
                    "--log-level",
                    "INFO",
                ],
                env={
                    **os.environ,
                    "ENTSOE_START_DATE": chunk.start_date.isoformat(),
                    "ENTSOE_END_DATE": chunk.end_date.isoformat(),
                },
            )
            if rc == 2:
                partial.append(chunk.label)
            entsoe_files = sorted(EXTRACTED_DATA.glob("entsoe_*_etl.jsonl"))
            for f in entsoe_files:
                load_and_remove("entsoe", f)
            if entsoe_files:
                mark_entsoe_loaded()
            elif rc == 0 and chunk.end_date < today - timedelta(
                days=ENTSOE_PUBLICATION_LAG_DAYS
            ):
                # Zero rows from all areas for days long published means the
                # extractor's output changed, not the source. Loaded chunks stay.
                raise RuntimeError(
                    f"ENTSOE extraction for {chunk.label} succeeded but produced no "
                    f"entsoe_*_etl.jsonl in {EXTRACTED_DATA} — output naming may "
                    f"have changed"
                )
            elif rc == 0:
                logger.warning(
                    f"No ENTSOE output for {chunk.label} (recent days — likely "
                    f"publication lag)"
                )
            gh_endgroup()
            n += 1
            if not chunk.is_lookback:
                last_new_day = chunk.end_date
            observed_minutes_per_day.append((_now() - chunk_started) / 60 / chunk.days)
    except Exception:
        if partial:
            logger.error(f"ENTSOE PARTIAL chunks before the error: {partial}")
        raise

    logger.info(
        f"ENTSOE STATUS: {n} chunks done; new days done through "
        f"{last_new_day or 'none'}; first unfinished day "
        f"{first_unfinished or 'none'}; lookback chunks skipped {len(lookback_skipped)}"
    )
    if lookback_skipped:
        logger.warning(
            f"ENTSOE: lookback skipped for time: {lookback_skipped}. Late "
            f"corrections in those days are not re-fetched automatically; "
            f"backfill them with start_override/end_override if needed"
        )
    problems = []
    if first_unfinished:
        problems.append(
            f"ENTSOE BEHIND: first unfinished day {first_unfinished}. Chunks before "
            f"it are stored; the next run continues from the latest stored day "
            f"(a repair with start_override must pass {first_unfinished} as "
            f"start_override)."
        )
    if partial:
        problems.append(
            f"ENTSOE PARTIAL chunks (some country requests failed after "
            f"retries): {partial} — good rows were loaded; see "
            f"failures_<start>_<end>.json in the job artifacts and backfill "
            f"those days with start_override/end_override."
        )
    if problems:
        raise RuntimeError(" | ".join(problems))
    return n


# Scope decision (2026-10-13 meeting): other fuels only at plants that also
# burn coal; plants with no coal are deferred. The json backend returns every
# fuel at every plant, so each file is cut to the plants with a coal unit,
# known from this file or from coal already stored (a coal unit idle for the
# whole window must not drop its plant's other units). It looks backwards
# only: a plant new to coal whose coal first runs in a later month of the same
# run keeps no other-fuel rows from the earlier months; the next weekly
# lookback refills recent ones, a long START_OVERRIDE backfill does not.
OCCTO_COAL = "coal"


def occto_coal_plants() -> set[str]:
    """OCCTO plants with a coal unit already in the database."""
    engine = create_engine(get_connection_url())
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT plant FROM occto_generation_data WHERE fuel_type = :coal"
            ),
            {"coal": OCCTO_COAL},
        )
        return {row[0] for row in rows}


def keep_coal_plants(jsonl: Path, coal_plants: set[str]) -> int:
    """Rewrite ``jsonl`` with only the rows of coal plants (updating
    ``coal_plants`` with the file's own); returns the rows dropped."""
    rows = [
        json.loads(line)
        for line in jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    coal_plants.update(r["plant"] for r in rows if r.get("fuel_type") == OCCTO_COAL)
    kept = [r for r in rows if r.get("plant") in coal_plants]
    jsonl.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in kept),
        encoding="utf-8",
    )
    return len(rows) - len(kept)


def extract_occto() -> int:
    today = date.today()
    resume, end_date = occto_window(
        window_start("occto"),
        window_end(today),
        override=bool(os.environ.get("START_OVERRIDE")),
        now_jst=datetime.now(JST).date(),
    )
    if resume > end_date:
        logger.info("OCCTO up to date — 0 iterations")
        return 0
    warn_if_long_window("occto", resume, end_date)
    coal_plants = occto_coal_plants()

    n = 0
    partial_chunks: list[str] = []
    current = resume.replace(day=1)
    end_month = end_date.replace(day=1)
    while current <= end_month:
        next_first = add_months(current, 1)
        iter_start = max(current, resume)
        iter_end = min(next_first - timedelta(days=1), end_date)
        gh_group(f"Extract OCCTO {iter_start} → {iter_end}")
        rc = run_extract(
            [
                str(EXTRACT_BIN),
                "occto",
                "--start-date",
                iter_start.isoformat(),
                "--end-date",
                iter_end.isoformat(),
                "--backend",
                "json",
                "--output",
                str(EXTRACTED_DATA),
            ],
        )
        # Glob instead of guessing the exact filename (like the ENTSOE branch)
        # — a silent naming-convention change in the extractor must not mean
        # "extraction green, nothing loaded, forever".
        if rc == 2:
            partial_chunks.append(f"{iter_start}→{iter_end}")
        occto_files = sorted(EXTRACTED_DATA.glob("occto_*_etl.jsonl"))
        if occto_files:
            for f in occto_files:
                dropped = keep_coal_plants(f, coal_plants)
                logger.info(
                    f"{f.name}: {dropped:,} rows of plants without coal left out"
                )
                load_and_remove("occto", f)
        elif iter_end < today - timedelta(days=2):
            # A fully-past chunk should always produce a file; only very
            # recent windows can legitimately be empty (publication lag).
            raise RuntimeError(
                f"OCCTO extraction for {iter_start} → {iter_end} succeeded "
                f"but produced no occto_*_etl.jsonl in {EXTRACTED_DATA} — "
                f"output naming may have changed"
            )
        else:
            logger.warning(
                f"No OCCTO output for {iter_start} → {iter_end} "
                f"(recent window — likely publication lag)"
            )
        gh_endgroup()
        n += 1
        current = next_first
    if partial_chunks:
        raise RuntimeError(
            f"OCCTO PARTIAL chunks (truncated/failed after retries): "
            f"{partial_chunks} — good rows were loaded; the failed chunks "
            f"need a start_override backfill."
        )
    return n


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCES:
        print(f"Usage: {sys.argv[0]} {{entsoe|occto}}", file=sys.stderr)
        sys.exit(1)
    if not EXTRACT_BIN.exists():
        print(
            f"Extractor binary not found at {EXTRACT_BIN}. "
            f"Run 'cd extractors && uv sync --extra <source>' first.",
            file=sys.stderr,
        )
        sys.exit(2)
    EXTRACTED_DATA.mkdir(exist_ok=True)
    n = {"entsoe": extract_entsoe, "occto": extract_occto}[sys.argv[1]]()
    logger.success(f"Completed {sys.argv[1]}: {n} iterations")


if __name__ == "__main__":
    main()
