"""Benchmark gates reject changed sources and partial historical years."""

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import check_ons_staging  # noqa: E402
from check_ons_staging import benchmark, check_extraction, year_bounds  # noqa: E402


def metadata_for(year):
    audited = benchmark(year)
    raw_count = sum(item["raw_records"] for item in audited["source_files"])
    return {
        "start_year": year,
        "end_year": year,
        "thermal_only": False,
        "individual_plants_only": True,
        "failed_downloads_count": 0,
        "extraction_run_id": "benchmark-test",
        "raw_records": raw_count,
        "selected_records": raw_count,
        "total_records": audited["expected_observations"],
        "excluded_records": raw_count - audited["expected_observations"],
        "url_attempts": [
            {
                "label": item["label"],
                "url": item["url"],
                "sha256": item["sha256"],
                "records": item["raw_records"],
                "success": True,
            }
            for item in audited["source_files"]
        ],
    }


@pytest.mark.parametrize(
    "year,days",
    [
        (2019, 365),
        (2020, 366),
        (2021, 365),
        (2022, 365),
        (2023, 365),
        (2024, 366),
        (2025, 365),
    ],
)
def test_historical_year_boundaries_include_leap_day(year, days):
    start, end = year_bounds(year)
    assert (end - start) // 86400000 == days
    assert start == int(datetime(year, 1, 1, tzinfo=UTC).timestamp() * 1000)


@pytest.mark.parametrize("year", [2019, 2020, 2021, 2022, 2023, 2024, 2025])
def test_complete_audited_extraction_is_accepted(year):
    report = check_extraction(metadata_for(year), year, benchmark(year))
    assert report["status"] == "source_verified"


def test_year_selection_requires_a_committed_audit(monkeypatch, tmp_path):
    manifest = tmp_path / "benchmarks.json"
    manifest.write_text('{"years": {"2019": {}}}')
    monkeypatch.setattr(check_ons_staging, "BENCHMARKS", manifest)
    assert check_ons_staging.audited_years() == (2019,)
    with pytest.raises(ValueError, match="Only the audited"):
        year_bounds(2020)


@pytest.mark.parametrize(
    "change",
    [
        {"thermal_only": True},
        {"individual_plants_only": False},
        {"start_year": 2019},
        {"end_year": 2025},
        {"failed_downloads_count": 1},
        {"total_records": 2424001},
        {"raw_records": 0},
        {"excluded_records": 0},
    ],
)
def test_wrong_scope_or_incomplete_counts_fail(change):
    metadata = metadata_for(2024)
    metadata.update(change)
    with pytest.raises(ValueError):
        check_extraction(metadata, 2024, benchmark(2024))


@pytest.mark.parametrize(
    "mutation", ["missing", "duplicate", "hash", "url", "records", "failed"]
)
def test_monthly_download_manifest_must_match_exactly(mutation):
    metadata = metadata_for(2024)
    attempts = metadata["url_attempts"]
    if mutation == "missing":
        attempts.pop()
    elif mutation == "duplicate":
        attempts[-1] = deepcopy(attempts[0])
    elif mutation == "hash":
        attempts[0]["sha256"] = "0" * 64
    elif mutation == "url":
        attempts[0]["url"] = "https://example.com/unrelated.parquet"
    elif mutation == "records":
        attempts[0]["records"] -= 1
    else:
        attempts[0]["success"] = False
    with pytest.raises(ValueError):
        check_extraction(metadata, 2024, benchmark(2024))
