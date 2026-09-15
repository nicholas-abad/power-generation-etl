#!/usr/bin/env python3
"""Month-by-month incremental extraction for ENTSOE and OCCTO.

Usage:
    python src/incremental_extract.py {entsoe|occto}

Reads latest_date from Neon, computes the chunks needed (clamped to a
per-source MIN_START_DATE), then for each chunk: invokes the extractor
binary directly, loads the produced JSONL into Neon, and removes it.
Fail-fast on any subprocess error.

Design: see /Users/nicholasabad/.claude/plans/effervescent-hatching-horizon.md
"""

import os
import subprocess
import sys
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from get_latest_date import get_latest_date

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
    "occto": SourceConfig("occto", date(2024, 1, 1)),
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


# 350-min job timeout / ~5min per ENTSOE month / ~6min per OCCTO month
# both work out to ~12-month soft ceiling before risking a timeout.
LONG_WINDOW_MONTHS = 12


def warn_if_long_window(source: str, start: date, end: date) -> None:
    """Warn (don't fail) when an override window is large enough to risk
    hitting the 350-minute job timeout. The user can still proceed — this
    is a heads-up, not a guardrail."""
    months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    if months > LONG_WINDOW_MONTHS:
        logger.warning(
            f"{source}: extracting {months} months ({start} → {end}) "
            f"may exceed the 350-min job timeout — consider splitting into "
            f"smaller windows."
        )


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


def extract_entsoe() -> int:
    today = date.today()
    start = window_start("entsoe").replace(day=1)
    end = window_end(today).replace(day=1)
    if start > end:
        logger.info("ENTSOE up to date — 0 iterations")
        return 0
    warn_if_long_window("entsoe", start, end)

    n = 0
    partial_months: list[str] = []
    current = start
    while current <= end:
        gh_group(f"Extract ENTSOE {current.year}-{current.month:02d}")
        env = {
            **os.environ,
            "START_YEAR": str(current.year),
            "END_YEAR": str(current.year),
            "MONTHS": str(current.month),
        }
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
            env=env,
        )
        if rc == 2:
            partial_months.append(f"{current.year}-{current.month:02d}")
        entsoe_files = sorted(EXTRACTED_DATA.glob("entsoe_*_etl.jsonl"))
        if entsoe_files:
            for f in entsoe_files:
                load_and_remove("entsoe", f)
        elif current < today.replace(day=1):
            # Same guard as the OCCTO branch: a fully-past month must produce
            # output. An empty glob there means extraction went green but
            # loaded nothing — latest_date never advances and the same month
            # re-extracts forever, silently. Only the current month may be
            # legitimately empty (publication lag / month just started).
            raise RuntimeError(
                f"ENTSOE extraction for {current.year}-{current.month:02d} "
                f"succeeded but produced no entsoe_*_etl.jsonl in "
                f"{EXTRACTED_DATA} — output naming may have changed"
            )
        else:
            logger.warning(
                f"No ENTSOE output for {current.year}-{current.month:02d} "
                f"(current month — likely publication lag)"
            )
        gh_endgroup()
        n += 1
        current = add_months(current, 1)
    if partial_months:
        raise RuntimeError(
            f"ENTSOE PARTIAL months (some country/PSR combos failed after "
            f"retries): {partial_months} — good rows were loaded; the failed "
            f"combos need a start_override backfill. See failures.json in the "
            f"job artifacts."
        )
    return n


def extract_occto() -> int:
    today = date.today()
    resume = window_start("occto")
    end_date = window_end(today)
    if resume > end_date:
        logger.info("OCCTO up to date — 0 iterations")
        return 0
    warn_if_long_window("occto", resume, end_date)

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
                "--headless",
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
