"""Reject scope drift, altered XML, missing days and edited observation files."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from check_entsoe_staging import check_benchmark, check_source, digest, PSRS, ROOT  # noqa: E402
from tests.test_entsoe_units import record  # noqa: E402


@pytest.fixture
def source(tmp_path):
    rows, requests = [], []
    start = datetime(2019, 2, 1, tzinfo=UTC)
    for day in range(28):
        first = start + timedelta(days=day)
        row = record(timestamp_ms=int(first.timestamp() * 1000))
        rows.append(row)
        raw = tmp_path / f"{day}.xml"
        raw.write_text(f"<test-source>{day}</test-source>")
        requests.append(
            {
                "label": str(day),
                "country": "CZ",
                "start": first.isoformat(),
                "end": (first + timedelta(days=1)).isoformat(),
                "psr_type": None,
                "status": "success",
                "path": raw.name,
                "sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
                "content_sha256": digest([row]),
                "observations": 1,
            }
        )
    manifest = {
        "identified_units": True,
        "timezone": "UTC",
        "country_codes": ["CZ"],
        "months": [2],
        "start_year": 2019,
        "end_year": 2019,
        "psr_types": sorted(PSRS),
        "extraction_run_id": rows[0]["extraction_run_id"],
        "source_requests": requests,
        "total_records": 28,
        "unit_count": 1,
        "content_sha256": digest(rows),
        "generation_mwh": 2800.0,
        "fuel_counts": {"Fossil Brown coal/Lignite": 28},
    }
    return tmp_path, rows, manifest


def write(source):
    root, rows, manifest = source
    path, metadata = root / "input.jsonl", root / "manifest.json"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    metadata.write_text(json.dumps(manifest))
    return path, metadata


def test_source_hashes_and_scope_pass(source):
    rows, report = check_source(*write(source), 2019, 2)
    assert len(rows) == report["observations"] == 28
    assert report["days"] == 28


def test_fixed_benchmark_rejects_rewritten_manifest():
    benchmark = json.loads((ROOT / "config/entsoe-benchmarks.json").read_text())
    report = benchmark["benchmarks"]["2019"].copy()
    check_benchmark(report, 2019)
    report["content_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="Audited CZ 2019 benchmark changed"):
        check_benchmark(report, 2019)


@pytest.mark.parametrize(
    "change",
    [
        "wrong_country",
        "wrong_year",
        "wrong_month",
        "missing_day",
        "changed_xml",
        "changed_observation",
        "duplicate_key",
        "missing_request",
        "wrong_run",
        "missing_psr",
        "repeated_request",
    ],
)
def test_drift_is_rejected(source, change):
    root, rows, manifest = source
    if change == "wrong_country":
        manifest["country_codes"] = ["FR"]
    elif change == "wrong_year":
        manifest["start_year"] = 2024
    elif change == "wrong_month":
        manifest["months"] = [3]
    elif change == "missing_day":
        rows.pop()
    elif change == "changed_xml":
        (root / "0.xml").write_text("changed")
    elif change == "changed_observation":
        rows[0]["generation_mw"] = 123
    elif change == "duplicate_key":
        rows.append(deepcopy(rows[0]))
    elif change == "missing_request":
        manifest["source_requests"].pop()
    elif change == "wrong_run":
        manifest["extraction_run_id"] = "other"
    elif change == "missing_psr":
        manifest["psr_types"].pop()
    elif change == "repeated_request":
        manifest["source_requests"].append(deepcopy(manifest["source_requests"][0]))
    with pytest.raises(ValueError):
        check_source(*write(source), 2019, 2)
