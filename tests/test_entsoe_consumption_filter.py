#!/usr/bin/env python3
"""ENTSOE loads must drop 'Actual Consumption' rows at the door.

The natural key (timestamp_ms, country_code, psr_type, plant_name) omits
data_type, so a consumption row (value ~0) that arrives before the matching
generation row permanently displaces it: in-batch dedup keeps first, and
ON CONFLICT DO NOTHING keeps existing. Observed in production: the IE,
GB_NIR and PT histories were consumption-dominated zeros.
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import PowerGenerationDatabase


def _record(plant: str, data_type: str, ts: int = 1746057600000) -> dict:
    return {
        "extraction_run_id": "11111111-2222-3333-4444-555555555555",
        "created_at_ms": 1746057600000,
        "timestamp_ms": ts,
        "country_code": "IE",
        "psr_type": "B04",
        "plant_name": plant,
        "fuel_type": "Fossil Gas",
        "data_type": data_type,
        "generation_mw": 0.0 if data_type == "Actual Consumption" else 123.0,
        "resolution_minutes": 30,
    }


def _db_with_captured_batches(monkeypatch):
    db = PowerGenerationDatabase(
        host="localhost", port=5432, database="x", username="x", password="x"
    )
    captured = []

    def fake_batch(batch, expected_columns, validator, batch_num, *a, **k):
        valid, report = validator.validate_file(batch, "entsoe", "test")
        captured.extend(valid)
        return (
            len(valid),
            report.valid_count,
            report.invalid_count,
            report.duplicate_count,
        )

    monkeypatch.setattr(db, "_insert_entsoe_batch", fake_batch)
    monkeypatch.setattr(db, "_get_date_range_for_run", lambda *a, **k: (None, None))
    monkeypatch.setattr(db, "insert_extraction_metadata", lambda *a, **k: True)
    return db, captured


def test_consumption_rows_never_reach_the_insert(monkeypatch):
    db, captured = _db_with_captured_batches(monkeypatch)
    records = [
        _record("Plant A", "Actual Consumption"),  # would displace Plant A's
        _record("Plant A", "Actual Aggregated"),  # generation without the filter
        _record("Plant B", "Actual Aggregated"),
        _record("Plant B", "Actual Consumption"),
    ]
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        path = f.name

    ok, report = db.insert_entsoe_jsonl_data(path)
    assert ok is True
    assert len(captured) == 2, "exactly the two Aggregated rows must be inserted"
    assert {r["data_type"] for r in captured} == {"Actual Aggregated"}
    assert all(r["generation_mw"] == 123.0 for r in captured), (
        "the surviving rows must be the real generation values, not the zeros"
    )


def test_consumption_only_file_is_a_clean_noop(monkeypatch):
    db, captured = _db_with_captured_batches(monkeypatch)
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for i in range(5):
            f.write(json.dumps(_record(f"P{i}", "Actual Consumption")) + "\n")
        path = f.name

    ok, report = db.insert_entsoe_jsonl_data(path)
    assert ok is True, "nothing-to-load is success, not failure"
    assert captured == []


def test_legacy_data_types_pass_through(monkeypatch):
    db, captured = _db_with_captured_batches(monkeypatch)
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write(json.dumps(_record("Old Plant", "Unknown")) + "\n")
        path = f.name

    ok, _ = db.insert_entsoe_jsonl_data(path)
    assert ok is True
    assert len(captured) == 1 and captured[0]["data_type"] == "Unknown"


def test_legacy_consumption_suffix_is_dropped(monkeypatch):
    """Legacy format: data_type leaked into plant_name as a suffix while the
    data_type FIELD is 'Unknown'. The field-level filter misses it, but
    stripping '_Actual Consumption' would collapse it onto the generation
    sibling's key and displace it. It must be dropped at the suffix step."""
    db, captured = _db_with_captured_batches(monkeypatch)
    records = [
        # legacy rows: field says 'Unknown', metric is in the plant_name suffix
        {**_record("Plant A_Actual Consumption", "Unknown"), "generation_mw": 0.0},
        {**_record("Plant A_Actual Aggregated", "Unknown"), "generation_mw": 123.0},
    ]
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        path = f.name

    ok, _ = db.insert_entsoe_jsonl_data(path)
    assert ok is True
    assert len(captured) == 1, "only the aggregated row survives"
    assert captured[0]["plant_name"] == "Plant A", "suffix stripped to bare name"
    assert captured[0]["generation_mw"] == 123.0, "real generation kept, not the zero"
