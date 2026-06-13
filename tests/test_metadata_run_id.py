#!/usr/bin/env python3
"""ONS/Chile extraction metadata must be keyed to the run-id the inserted
rows actually carry (from the *_etl.jsonl file), not a fresh local uuid4.

Bug: both methods generated a local uuid4 and used it for both
_get_date_range_for_run and insert_extraction_metadata, while the rows kept
the file's id — so the date-range lookup matched 0 rows (NULL start/end) and
the metadata row pointed at no data. ENTSOE/NPP/EIA/OE already key to the
file's id; this pins ONS/Chile to the same behavior.
"""

import json
import sys
import tempfile
import uuid
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import PowerGenerationDatabase

FILE_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _ons_record(i: int) -> dict:
    return {
        "extraction_run_id": FILE_RUN_ID,
        "created_at_ms": 1746057600000,
        "timestamp_ms": 1746057600000 + i * 3600000,
        "plant": f"Plant {i}",
        "ons_plant_id": f"P{i}",
        "fuel_type": "Carvão",
        "generation_mwh": 10.0 + i,
        "resolution_minutes": 60,
    }


def _chile_record(i: int) -> dict:
    return {
        "extraction_run_id": FILE_RUN_ID,
        "created_at_ms": 1746057600000,
        "timestamp_ms": 1746057600000 + i * 3600000,
        "plant": f"Central {i}",
        "chile_plant_id": f"C{i}",
        "fuel_type": "Carbón",
        "generation_mwh": 5.0 + i,
        "resolution_minutes": 60,
    }


def _run(monkeypatch, insert_fn, recs):
    db = PowerGenerationDatabase(
        host="localhost", port=5432, database="x", username="x", password="x"
    )
    captured = {}
    # report inserts without a DB
    monkeypatch.setattr(db, "_execute_with_retry", lambda fn: len(recs))

    def fake_date_range(table, run_id):
        captured["date_run_id"] = run_id
        return (None, None)

    def fake_meta(**kw):
        captured["meta_run_id"] = kw["extraction_run_id"]
        return True

    monkeypatch.setattr(db, "_get_date_range_for_run", fake_date_range)
    monkeypatch.setattr(db, "insert_extraction_metadata", fake_meta)

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
        path = f.name
    insert_fn(db)(path)
    return captured


def test_ons_metadata_uses_file_run_id(monkeypatch):
    cap = _run(
        monkeypatch,
        lambda db: db.insert_ons_jsonl_data,
        [_ons_record(i) for i in range(3)],
    )
    assert cap["meta_run_id"] == FILE_RUN_ID
    assert cap["date_run_id"] == FILE_RUN_ID
    assert uuid.UUID(cap["meta_run_id"])  # well-formed


def test_chile_metadata_uses_file_run_id(monkeypatch):
    cap = _run(
        monkeypatch,
        lambda db: db.insert_chile_jsonl_data,
        [_chile_record(i) for i in range(3)],
    )
    assert cap["meta_run_id"] == FILE_RUN_ID
    assert cap["date_run_id"] == FILE_RUN_ID


def test_get_all_record_counts_includes_chile():
    # static check: the table list must cover chile (was omitted)
    import inspect

    src = inspect.getsource(PowerGenerationDatabase.get_all_record_counts)
    assert "chile_generation_data" in src
