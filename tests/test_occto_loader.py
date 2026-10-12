"""OCCTO loader: the upsert only updates columns the file carries."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from database import PowerGenerationDatabase

BASE = {
    "timestamp_ms": 1736812800000,
    "plant": "碧南火力発電所",
    "unit": "４号機",
    "fuel_type": "coal",
    "resolution_minutes": 30,
    "generation_mwh": 350.0,
}
LABELS = {"fuel_code": "03", "plant_code": "1", "area_code": "04", "area_name": "中部"}


def update_columns(monkeypatch, tmp_path, record):
    db = PowerGenerationDatabase(host="localhost", port=5432, database="x", username="x", password="x")
    captured = {}

    def fake_upsert(df, table, **kwargs):
        captured.update(kwargs, columns=list(df.columns))
        return len(df)

    monkeypatch.setattr(db, "_upsert_via_staging", fake_upsert)
    monkeypatch.setattr(db, "_execute_with_retry", lambda fn: fn())
    path = tmp_path / "occto.jsonl"
    path.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    db.insert_occto_jsonl_data(str(path))
    return captured


def test_full_records_update_codes_and_labels(monkeypatch, tmp_path):
    cap = update_columns(monkeypatch, tmp_path, {**BASE, **LABELS})
    assert set(LABELS) <= set(cap["update_columns"])


def test_reload_without_labels_never_nulls_them(monkeypatch, tmp_path):
    cap = update_columns(monkeypatch, tmp_path, BASE)
    assert not set(LABELS) & set(cap["update_columns"])
    assert set(cap["update_columns"]) <= set(cap["columns"])
    assert "generation_mwh" in cap["update_columns"]
