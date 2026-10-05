"""Evidence matching must not turn identity/metric conflicts into repair candidates."""

import csv
from decimal import Decimal
import gzip
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "history_audit", ROOT / "scripts/audit_entsoe_coal_history.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
UNIT = "27W-GU-ECHVG1--C"
FUEL = "Fossil Brown coal/Lignite"
TIME = 1735689600000


def stored(ident, name, metric="Actual Aggregated", fuel=FUEL, power="0"):
    return dict(
        id=str(ident),
        extraction_run_id="11111111-2222-3333-4444-555555555555",
        created_at_ms="1700000000000",
        country_code="CZ",
        psr_type="B02",
        plant_name=name,
        fuel_type=fuel,
        data_type=metric,
        timestamp_ms=str(TIME),
        generation_mw=power,
        resolution_minutes="60",
    )


def run_audit(
    tmp_path,
    monkeypatch,
    rows,
    eic=UNIT,
    source_name="ECHV_G1____",
    end="2026-01-01T00:00Z",
    power="0",
):
    staging = tmp_path / "staging"
    staging.mkdir()
    path = staging / "2025.csv.gz"
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest = {
        "snapshot_at": "fixture",
        "branch_id": "fixture",
        "endpoint_id": "fixture",
        "transaction_read_only": "on",
        "files": {
            "2025": {"path": path.name, "sha256": MODULE.sha(path), "rows": len(rows)}
        },
    }
    (staging / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(
        MODULE,
        "source_records",
        lambda *args: (
            {(TIME, eic): ("B02", source_name, Decimal(power), 60, None)},
            {
                "start": "2025-01-01T00:00Z",
                "end": end,
                "direct_block_mwh": power,
                "source_rows": 1,
                "source_days": 365,
            },
        ),
    )
    output = tmp_path / "result"
    MODULE.audit(tmp_path / "source", staging, "2025", output, tmp_path / "extractor")
    return json.loads((output / "audit.json").read_text())


@pytest.mark.parametrize("conflict", ["consumption", "fuel"])
def test_zero_alias_conflicts_never_delete_generation(tmp_path, monkeypatch, conflict):
    canonical = stored(
        1,
        "ECHV_G1___",
        metric="Actual Consumption"
        if conflict == "consumption"
        else "Actual Aggregated",
        fuel="Fossil Gas" if conflict == "fuel" else FUEL,
    )
    report = run_audit(tmp_path, monkeypatch, [canonical, stored(2, "ECHV_G1____")])
    assert report["status"] == "unresolved_differences"
    assert report["comparison"]["metric_or_fuel_conflict_keys"] == 1
    assert report["repair_candidates"]["rows"] == 0


@pytest.mark.parametrize("source_name", ["ECHV_G1___", "ECHV_G1____"])
def test_both_alias_spellings_require_reviewed_eic(tmp_path, monkeypatch, source_name):
    with pytest.raises(ValueError, match="reviewed EIC"):
        run_audit(
            tmp_path,
            monkeypatch,
            [stored(1, "ECHV_G1___"), stored(2, "ECHV_G1____")],
            eic="27W-GU-ECHVG2--8",
            source_name=source_name,
        )


def test_proven_duplicate_with_legacy_metric_label_is_reversible_candidate(
    tmp_path, monkeypatch
):
    report = run_audit(
        tmp_path,
        monkeypatch,
        [
            stored(1, "ECHV_G1___", metric=FUEL, power="100"),
            stored(2, "ECHV_G1____", power="100"),
        ],
        power="100",
    )
    assert report["status"] == "source_reconciled"
    assert report["comparison"]["duplicate_rows"] == 1
    assert Decimal(report["repair_candidates"]["delta_mwh"]) == -100
    with gzip.open(tmp_path / "result/repair-candidates.jsonl.gz", "rt") as handle:
        change = json.loads(next(handle))
    assert change["before"]["id"] == "2"
    assert change["retained_id"] == "1"


def test_partial_year_cannot_be_presented_as_full_annual_comparison(
    tmp_path, monkeypatch
):
    with pytest.raises(ValueError, match="explicit comparison cutoff"):
        run_audit(
            tmp_path, monkeypatch, [stored(1, "ECHV_G1___")], end="2025-02-01T00:00Z"
        )
