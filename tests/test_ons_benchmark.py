"""Benchmark gates reject changed sources and partial historical years."""

from copy import deepcopy
import json
from datetime import UTC, datetime
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import check_ons_staging  # noqa: E402
from check_ons_staging import (  # noqa: E402
    benchmark,
    capture,
    check_extraction,
    check_fuel_accounting,
    extract_arguments,
    year_bounds,
)


def metadata_for(year):
    audited = benchmark(year)
    raw_count = sum(item["raw_records"] for item in audited["source_files"])
    return {
        "start_year": year,
        "end_year": year,
        "thermal_only": False,
        "individual_plants_only": True,
        "failed_downloads_count": 0,
        "end_month": audited.get("last_month", 12),
        "end_month_requested": audited.get("last_month"),
        "extraction_run_id": "benchmark-test",
        "raw_records": raw_count,
        "selected_records": raw_count,
        "total_records": audited["expected_observations"],
        "excluded_records": raw_count - audited["expected_observations"],
        "coverage_counts": {
            "accepted": {
                "fuel_type": {
                    fuel: item["accepted_rows"]
                    for fuel, item in audited["fuels"].items()
                    if item["accepted_rows"]
                }
            }
        },
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


@pytest.mark.parametrize(
    "change",
    [{"end_month": 11}, {"end_month": None}, {"end_month_requested": 12}],
)
def test_complete_year_requires_full_cutoff(change):
    metadata = metadata_for(2024)
    metadata.update(change)
    with pytest.raises(ValueError, match="audited month"):
        check_extraction(metadata, 2024, benchmark(2024))


def test_extract_arguments_reproduce_complete_benchmarks():
    selection = ["--all-types", "--individual-plants-only"]
    assert extract_arguments(2019) == ["--start-year", "2019", "--end-year", "2019", *selection]
    assert extract_arguments(2024) == ["--start-year", "2024", "--end-year", "2024", *selection]


def fuel_entry(raw, accepted, rejected=None, eligible=None):
    return {
        "raw_rows": raw,
        "raw_mwh": float(raw),
        "rows_by_modality": {},
        "eligible_rows": accepted if eligible is None else eligible,
        "accepted_rows": accepted,
        "accepted_mwh": float(accepted),
        "rejected_eligible": rejected or {},
    }


@pytest.fixture
def year_to_date(monkeypatch, tmp_path):
    """A 2026 Jan-Aug entry shaped like the committed monthly audits."""
    files = [
        {
            "label": f"2026-{m:02d}",
            "url": f"https://example.invalid/GERACAO_USINA-2_2026_{m:02d}.parquet",
            "sha256": f"{m:064x}",
            "raw_records": 10,
        }
        for m in range(1, 9)
    ]
    manifest = tmp_path / "benchmarks.json"
    manifest.write_text(
        json.dumps(
            {
                "years": {
                    "2026": {
                        "last_month": 8,
                        "expected_observations": 40,
                        "source_files": files,
                        "fuels": {"Carvão": fuel_entry(raw=80, accepted=40)},
                    }
                }
            }
        )
    )
    monkeypatch.setattr(check_ons_staging, "BENCHMARKS", manifest)
    return 2026


def test_year_to_date_bounds_stop_at_the_audited_month(year_to_date):
    start, end = year_bounds(year_to_date)
    assert start == int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
    assert end == int(datetime(2026, 9, 1, tzinfo=UTC).timestamp() * 1000)
    assert "month < '2026-09-01'" in check_ons_staging.month_predicate(year_to_date)
    assert extract_arguments(year_to_date)[4:6] == ["--end-month", "8"]


def test_year_to_date_preflight(year_to_date):
    metadata = metadata_for(year_to_date)
    assert check_extraction(metadata, year_to_date, benchmark(year_to_date))["status"] == "source_verified"
    for change in ({"end_month": 9}, {"end_month_requested": None}):
        bad = {**metadata, **change}
        with pytest.raises(ValueError, match="audited month 8"):
            check_extraction(bad, year_to_date, benchmark(year_to_date))
    extra = deepcopy(metadata)
    extra["url_attempts"].append({**extra["url_attempts"][-1], "label": "2026-09"})
    with pytest.raises(ValueError, match="missing or repeats"):
        check_extraction(extra, year_to_date, benchmark(year_to_date))


class FakeCursor:
    def __init__(self, months):
        self.months = months
        self.queries = []

    def copy_expert(self, query, digest):
        self.queries.append(query)

    def execute(self, query):
        self.queries.append(query)

    def fetchall(self):
        return self.months

    def fetchone(self):
        return (0,)


@pytest.mark.parametrize("count, ok", [(8, True), (7, False), (12, False)])
def test_year_to_date_coal_baseline_month_count(year_to_date, count, ok):
    cursor = FakeCursor([(f"2026-{m:02d}-01", 1.0) for m in range(1, count + 1)])
    if ok:
        assert len(capture(cursor, year_to_date)["coal_monthly"]) == 8
        assert all("1788220800000" in q for q in cursor.queries if "timestamp_ms <" in q)
    else:
        with pytest.raises(ValueError, match="Expected 8 coal baseline months"):
            capture(cursor, year_to_date)


@pytest.mark.parametrize("year", [2019, 2020, 2021, 2022, 2023, 2024, 2025])
def test_committed_fuel_audits_pass_the_gate(year):
    fuels = check_fuel_accounting(year)
    assert fuels["Carvão"]["accepted_rows"] > 0


def test_unapproved_2026_null_generation_blocks_the_gate():
    with pytest.raises(ValueError, match="loses eligible rows in 2026"):
        check_fuel_accounting(2026)


def gate_manifest(monkeypatch, tmp_path, fuels, policy=None, expected=None):
    manifest = tmp_path / "gate.json"
    manifest.write_text(
        json.dumps(
            {
                "fuel_policy": policy or {},
                "years": {
                    "2030": {
                        "expected_observations": expected
                        if expected is not None
                        else sum(f["accepted_rows"] for f in fuels.values()),
                        "fuels": fuels,
                    }
                },
            }
        )
    )
    monkeypatch.setattr(check_ons_staging, "BENCHMARKS", manifest)


def test_gate_requires_an_independent_audit(monkeypatch, tmp_path):
    gate_manifest(monkeypatch, tmp_path, {})
    with pytest.raises(ValueError, match="No independent per-fuel audit"):
        check_fuel_accounting(2030)


def test_fuel_without_accepted_rows_needs_approval(monkeypatch, tmp_path):
    fuels = {"Carvão": fuel_entry(10, 10), "Nova": fuel_entry(5, 0, eligible=0)}
    gate_manifest(monkeypatch, tmp_path, fuels)
    with pytest.raises(ValueError, match="Nova has no accepted rows"):
        check_fuel_accounting(2030)
    gate_manifest(
        monkeypatch, tmp_path, fuels, {"without_individual_plants": {"Nova": "approved"}}
    )
    assert check_fuel_accounting(2030)["Nova"]["accepted_rows"] == 0


@pytest.mark.parametrize(
    "approved, ok",
    [
        (None, False),
        ({"counts": {"unresolved_identity": 2}}, True),
        ({"counts": {"unresolved_identity": 3}}, False),
        ({"counts": {"invalid_generation": 2}}, False),
    ],
)
def test_partly_lost_fuel_needs_exact_approval(monkeypatch, tmp_path, approved, ok):
    """An eligible fuel losing only some rows still fails without approval."""
    fuels = {"Gás": fuel_entry(12, 10, {"unresolved_identity": 2}, eligible=12)}
    policy = {"eligible_rejections": {"2030": {"Gás": approved}}} if approved else {}
    gate_manifest(monkeypatch, tmp_path, fuels, policy)
    if ok:
        check_fuel_accounting(2030)
    else:
        with pytest.raises(ValueError, match="loses eligible rows"):
            check_fuel_accounting(2030)


def test_fuel_table_must_add_up(monkeypatch, tmp_path):
    gate_manifest(monkeypatch, tmp_path, {"Carvão": fuel_entry(10, 10)}, expected=11)
    with pytest.raises(ValueError, match="does not add up"):
        check_fuel_accounting(2030)


@pytest.mark.parametrize(
    "fuel_counts",
    [
        {"Carvão": 1},  # a fuel the extractor dropped
        {"Carvão": 1, "Gás": 1, "Nova": 1},  # a fuel the audit never saw
    ],
)
def test_extractor_fuel_counts_must_match_the_audit(fuel_counts):
    metadata = metadata_for(2024)
    accepted = metadata["coverage_counts"]["accepted"]["fuel_type"]
    metadata["coverage_counts"]["accepted"]["fuel_type"] = {
        fuel: accepted.get(fuel, count) for fuel, count in fuel_counts.items()
    }
    with pytest.raises(ValueError, match="per fuel differ"):
        check_extraction(metadata, 2024, benchmark(2024))
