#!/usr/bin/env python3
"""Climate TRACE loader: validation schema, revision-aware upsert, metadata.

Climate TRACE is the first source that REVISES history between releases
(every weekly package re-states 2021→now), so its loader upserts with
ON CONFLICT DO UPDATE instead of the house-default DO NOTHING — with an
IS DISTINCT FROM guard so unchanged rows are not rewritten. These tests pin:
the validator schema, the generated upsert SQL, the update-column set
(bookkeeping fields excluded, or the no-op guard would never fire), the
file-run-id metadata convention, and zero-written == success semantics.
"""

import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import PowerGenerationDatabase
from validator import DataValidator

FILE_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _record(i: int = 0, **overrides) -> dict:
    base = {
        "extraction_run_id": FILE_RUN_ID,
        "created_at_ms": 1746057600000,
        "climatetrace_id": f"254519{i:02d}",
        "plant_name": f"Plant {i}",
        "country_code": "CHN",
        "fuel_type": "coal",
        "latitude": 40.0 + i,
        "longitude": 111.0 + i,
        "timestamp_ms": 1609459200000 + i * 2678400000,
        "generation_mwh": 100000.0 + i,
        "capacity_mw": 1000.0,
        "capacity_factor": 0.5,
        "activity_confidence": "high",
        "emissions_tonnes": 95000.0,
        "emissions_factor": 0.95,
        "gas": "co2e_100yr",
        "ct_version": "v5_8_0",
    }
    base.update(overrides)
    return base


# --- validator schema --------------------------------------------------------


def test_valid_record_passes():
    valid, report = DataValidator().validate_file([_record()], "climatetrace", "x")
    assert report.valid_count == 1 and report.invalid_count == 0


def test_negative_generation_rejected():
    valid, report = DataValidator().validate_file(
        [_record(generation_mwh=-5.0)], "climatetrace", "x"
    )
    assert report.invalid_count == 1


def test_missing_natural_key_field_rejected():
    rec = _record()
    del rec["climatetrace_id"]
    valid, report = DataValidator().validate_file([rec], "climatetrace", "x")
    assert report.invalid_count == 1


def test_duplicate_key_is_plant_month():
    # Same (climatetrace_id, timestamp_ms) → duplicate, even if values differ.
    recs = [_record(), _record(generation_mwh=42.0)]
    valid, report = DataValidator().validate_file(recs, "climatetrace", "x")
    assert report.valid_count == 1 and report.duplicate_count == 1


def test_nullable_optionals_accepted():
    rec = _record(
        latitude=None,
        longitude=None,
        capacity_mw=None,
        capacity_factor=None,
        activity_confidence=None,
        emissions_tonnes=None,
        emissions_factor=None,
    )
    valid, report = DataValidator().validate_file([rec], "climatetrace", "x")
    assert report.valid_count == 1


# --- upsert SQL construction --------------------------------------------------


class _FakeCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 3

    def execute(self, sql):
        self.executed.append(sql)

    def copy_from(self, *a, **kw):
        pass


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _db():
    return PowerGenerationDatabase(
        host="localhost", port=5432, database="x", username="x", password="x"
    )


def _fake_engine(db, conn):
    class _Eng:
        def raw_connection(self):
            return conn

    # `engine` is a lazy property over _engine — inject there.
    db._engine = _Eng()


def test_upsert_with_update_columns_builds_do_update(monkeypatch):
    db = _db()
    conn = _FakeConn()
    _fake_engine(db, conn)
    df = pd.DataFrame(
        [{"climatetrace_id": "1", "timestamp_ms": 1, "generation_mwh": 2.0}]
    )
    n = db._upsert_via_staging(
        df,
        "climatetrace_generation_data",
        conflict_columns=["climatetrace_id", "timestamp_ms"],
        update_columns=["generation_mwh", "ct_version"],
    )
    assert n == 3
    insert_sql = conn.cursor_obj.executed[-1]
    assert "INSERT INTO climatetrace_generation_data AS t" in insert_sql
    assert "ON CONFLICT (climatetrace_id, timestamp_ms) DO UPDATE SET" in insert_sql
    assert "generation_mwh = EXCLUDED.generation_mwh" in insert_sql
    assert "ct_version = EXCLUDED.ct_version" in insert_sql
    # The no-op guard: unchanged rows must not be rewritten.
    assert (
        "WHERE (t.generation_mwh, t.ct_version) IS DISTINCT FROM "
        "(EXCLUDED.generation_mwh, EXCLUDED.ct_version)" in insert_sql
    )


def test_upsert_without_update_columns_stays_do_nothing():
    db = _db()
    conn = _FakeConn()
    _fake_engine(db, conn)
    df = pd.DataFrame([{"climatetrace_id": "1", "timestamp_ms": 1}])
    db._upsert_via_staging(
        df,
        "climatetrace_generation_data",
        conflict_columns=["climatetrace_id", "timestamp_ms"],
    )
    insert_sql = conn.cursor_obj.executed[-1]
    assert "DO NOTHING" in insert_sql
    assert "AS t" not in insert_sql
    assert "DO UPDATE" not in insert_sql


# --- loader behavior -----------------------------------------------------------


def _run_loader(monkeypatch, recs, upsert_return=None):
    db = _db()
    captured = {"upsert_kwargs": None, "df": None}

    def fake_upsert(df, table, **kwargs):
        captured["df"] = df
        captured["table"] = table
        captured["upsert_kwargs"] = kwargs
        return len(df) if upsert_return is None else upsert_return

    monkeypatch.setattr(db, "_upsert_via_staging", fake_upsert)
    monkeypatch.setattr(db, "_execute_with_retry", lambda fn: fn())

    def fake_date_range(table, run_id):
        captured["date_run_id"] = run_id
        return (None, None)

    def fake_meta(**kw):
        captured["meta"] = kw
        return True

    monkeypatch.setattr(db, "_get_date_range_for_run", fake_date_range)
    monkeypatch.setattr(db, "insert_extraction_metadata", fake_meta)

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
        path = f.name
    success, report = db.insert_climatetrace_jsonl_data(path)
    return success, report, captured


def test_loader_uses_update_columns_without_bookkeeping_fields(monkeypatch):
    success, report, cap = _run_loader(monkeypatch, [_record(i) for i in range(3)])
    assert success
    kwargs = cap["upsert_kwargs"]
    assert kwargs["conflict_columns"] == ["climatetrace_id", "timestamp_ms"]
    update_cols = kwargs["update_columns"]
    assert "generation_mwh" in update_cols and "ct_version" in update_cols
    # Bookkeeping fields must NOT be updated on conflict — they are fresh
    # every run and would defeat the IS DISTINCT FROM no-op guard.
    assert "extraction_run_id" not in update_cols
    assert "created_at_ms" not in update_cols


def test_loader_drops_unknown_columns(monkeypatch):
    recs = [_record(0, unexpected_field="x")]
    success, report, cap = _run_loader(monkeypatch, recs)
    assert success
    assert "unexpected_field" not in cap["df"].columns


def test_loader_metadata_keyed_to_file_run_id(monkeypatch):
    success, report, cap = _run_loader(monkeypatch, [_record(i) for i in range(2)])
    assert cap["date_run_id"] == FILE_RUN_ID
    assert cap["meta"]["extraction_run_id"] == FILE_RUN_ID
    assert cap["meta"]["source"] == "climatetrace"


def test_zero_written_is_success_not_failure(monkeypatch):
    # Re-loading an unchanged package writes 0 rows (no-op guard) — that is
    # the weekly steady state, not a failure; and no metadata row is written.
    success, report, cap = _run_loader(
        monkeypatch, [_record(i) for i in range(2)], upsert_return=0
    )
    assert success is True
    assert report.valid_count == 2
    assert "meta" not in cap


def test_all_invalid_fails_loudly(monkeypatch):
    recs = [_record(0, generation_mwh=-1.0), _record(1, generation_mwh=-2.0)]
    success, report, cap = _run_loader(monkeypatch, recs)
    assert success is False


def test_get_all_record_counts_includes_climatetrace():
    import inspect

    src = inspect.getsource(PowerGenerationDatabase.get_all_record_counts)
    assert "climatetrace_generation_data" in src
