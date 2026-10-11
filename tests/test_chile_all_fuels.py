"""Chile all-fuel staging pilot (migration 019): validator, loader, views.

The database tests need a disposable LOCAL PostgreSQL (COAL_TEST_PG_DSN, the
same guarded fixture convention as test_entsoe_coal_hotfix.py); never Neon.
"""

import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from database import PowerGenerationDatabase
from local_postgres import local_settings, verify_local_connection
import refresh_views
from validator import CHILE_SCHEMA, DataValidator

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "11111111-2222-3333-4444-555555555555"
TS = 1717214400000  # 2024-06-01 00:00 local (UTC-4)


def rec(**kw):
    base = {
        "extraction_run_id": RUN_ID,
        "created_at_ms": 1760000000000,
        "plant_key": "386",
        "chile_plant_id": "386",
        "plant": "TER MEJILLONES",
        "region": "Antofagasta",
        "comuna": "Mejillones",
        "tipo_tecnologia": "Térmica",
        "subtipo_tecnologia": "Carbón",
        "fuel_type": "Carbón",
        "timestamp_ms": TS,
        "generation_mwh": 100.0,
        "resolution_minutes": 60,
        "unit_count": 2,
    }
    base.update(kw)
    return base


GAS = dict(subtipo_tecnologia="Gas Natural", fuel_type="Gas Natural", generation_mwh=50.0, unit_count=1)
BESS = dict(plant_key="name:BESS ALFALFAL VR1", chile_plant_id=None, plant="BESS ALFALFAL VR1",
            tipo_tecnologia="BESS", subtipo_tecnologia="Retiro", fuel_type="BESS", generation_mwh=0.0)
SOLAR = dict(plant_key="394", chile_plant_id="394", plant="PFV X", tipo_tecnologia="Solar",
             subtipo_tecnologia="", fuel_type="Solar", generation_mwh=0.0)


def validate(records):
    return DataValidator().validate_file(records, "chile_all_fuels")


# --- validator (no database) ----------------------------------------------


def test_two_fuels_in_one_plant_hour_both_survive():
    valid, report = validate([rec(), rec(**GAS)])
    assert len(valid) == 2 and report.duplicate_count == 0
    # The coal schema's fuel-blind key is what would have dropped the gas row.
    assert "subtipo_tecnologia" not in CHILE_SCHEMA["duplicate_key"]


def test_true_duplicate_is_reported():
    valid, report = validate([rec(), rec(generation_mwh=99.0)])
    assert len(valid) == 1 and report.duplicate_count == 1


def test_empty_subtype_and_null_id_are_valid():
    valid, report = validate([rec(**SOLAR), rec(**BESS)])
    assert report.invalid_count == 0 and len(valid) == 2


@pytest.mark.parametrize("bad", [{"fuel_type": ""}, {"plant_key": ""}, {"generation_mwh": -1.0}])
def test_invalid_records_rejected(bad):
    _, report = validate([rec(**bad)])
    assert report.invalid_count == 1


# --- view registration and migration text ----------------------------------


def test_all_fuel_view_is_not_in_default_refresh():
    staging = {v for views in refresh_views.STAGING_VIEWS.values() for v in views}
    assert "mv_chile_all_fuels_plant_monthly" in staging
    assert not staging & set(refresh_views.ALL_VIEWS)
    assert "mv_chile_plant_monthly" in refresh_views.ALL_VIEWS


def test_migration_keeps_view_off_dashboard_and_records_ledger():
    sql = (ROOT / "schema/migrations/019_chile_all_fuels.sql").read_text()
    assert "REVOKE ALL ON public.mv_chile_all_fuels_plant_monthly FROM PUBLIC, dashboard_ro" in sql
    assert "('019'," in sql
    assert "chile_generation_data" not in sql.split("\\ir")[1], "coal table must not be touched"
    rollback = (ROOT / "schema/migrations/rollback/019_chile_all_fuels.sql").read_text()
    assert "chile_generation_data" not in rollback.replace("ingestion.chile_generation_data (coal)", "")


# --- loader against local PostgreSQL ---------------------------------------


@pytest.fixture
def pg():
    dsn = os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set COAL_TEST_PG_DSN to a disposable local database")
    with patch.dict(os.environ):
        parts = local_settings(dsn, r"(?:coal_hotfix_tests_[a-z0-9_]+|power_generation_test)")
        conn = psycopg2.connect(**parts, connect_timeout=5)
        engine = None
        try:
            with conn:
                verify_local_connection(conn, parts["dbname"])
                with conn.cursor() as cur:
                    cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
                    cur.execute("DROP MATERIALIZED VIEW IF EXISTS public.mv_chile_all_fuels_plant_monthly")
                    cur.execute((ROOT / "schema/chile_generation.sql").read_text())
                    cur.execute((ROOT / "schema/chile_generation_all_fuels.sql").read_text())
            engine = create_engine(
                URL.create(
                    "postgresql+psycopg2",
                    username=parts["user"],
                    password=parts.get("password"),
                    host=parts["host"],
                    port=int(parts.get("port", 5432)),
                    database=parts["dbname"],
                ),
                connect_args={"options": "-c search_path=ingestion,public"},
            )
            db = PowerGenerationDatabase.__new__(PowerGenerationDatabase)
            db._engine = engine
            yield conn, db
        finally:
            if engine is not None:
                engine.dispose()
            conn.close()


def write(tmp_path, records, name="2024-06-01.jsonl"):
    p = tmp_path / name
    p.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records))
    return str(p)


def rows(conn):
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT plant_key, subtipo_tecnologia, plant, fuel_type, generation_mwh "
            "FROM ingestion.chile_generation_all_fuels ORDER BY 1, 2"
        )
        return cur.fetchall()


def test_load_mixed_plant_and_replay_is_idempotent(pg, tmp_path):
    conn, db = pg
    path = write(tmp_path, [rec(), rec(**GAS), rec(**BESS), rec(**SOLAR)])
    ok, report = db.insert_chile_all_fuels_jsonl_data(path)
    assert ok and report.valid_count == 4
    first = rows(conn)
    assert [(r[0], r[1]) for r in first] == [
        ("386", "Carbón"), ("386", "Gas Natural"), ("394", ""), ("name:BESS ALFALFAL VR1", "Retiro")
    ]
    ok, _ = db.insert_chile_all_fuels_jsonl_data(path)
    assert ok and rows(conn) == first


def test_rename_and_recategorisation_update_in_place(pg, tmp_path):
    conn, db = pg
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**GAS)]))
    renamed = [rec(plant="TER MEJILLONES (CTM)"), rec(**{**GAS, "fuel_type": "Gas", "plant": "TER MEJILLONES (CTM)"})]
    ok, _ = db.insert_chile_all_fuels_jsonl_data(write(tmp_path, renamed, "replay.jsonl"))
    assert ok
    after = rows(conn)
    assert len(after) == 2
    assert {r[2] for r in after} == {"TER MEJILLONES (CTM)"}
    assert [r[3] for r in after] == ["Carbón", "Gas"]


def test_any_invalid_record_writes_nothing(pg, tmp_path):
    conn, db = pg
    ok, report = db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**{**GAS, "generation_mwh": -5.0})]))
    assert not ok and report.invalid_count == 1
    assert rows(conn) == []
    ok, report = db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec()], "dup.jsonl"))
    assert not ok and report.duplicate_count == 1
    assert rows(conn) == []


def test_coal_table_untouched_and_view_refreshes(pg, tmp_path):
    conn, db = pg
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.chile_generation_data (extraction_run_id, created_at_ms, plant, "
            "chile_plant_id, fuel_type, timestamp_ms, generation_mwh) VALUES (%s, 1, 'TER IEM', '488', 'Carbón', %s, 300)",
            (RUN_ID, TS),
        )
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**GAS), rec(timestamp_ms=TS + 3_600_000)]))
    with conn, conn.cursor() as cur:
        cur.execute("SELECT plant, generation_mwh FROM ingestion.chile_generation_data")
        assert cur.fetchall() == [("TER IEM", 300.0)]
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly")
        cur.execute(
            "SELECT subtipo_tecnologia, generation_mwh, hours FROM public.mv_chile_all_fuels_plant_monthly ORDER BY 1"
        )
        assert cur.fetchall() == [("Carbón", 200.0, 2), ("Gas Natural", 50.0, 1)]


# --- check_chile_all_fuels -------------------------------------------------

import check_chile_all_fuels as checks  # noqa: E402
from datetime import date  # noqa: E402


def test_day_bounds_follow_santiago_dst():
    lo, hi = checks.day_bounds_ms(date(2024, 6, 1))
    assert lo == TS and hi - lo == 24 * 3_600_000
    lo, hi = checks.day_bounds_ms(date(2024, 9, 8))  # spring forward: 23-hour day
    assert hi - lo == 23 * 3_600_000
    lo, hi = checks.day_bounds_ms(date(2024, 4, 6))  # fall back at the end of the 6th: 25 hours
    assert hi - lo == 25 * 3_600_000


def test_fingerprint_reconcile_and_coal_report(pg, tmp_path):
    conn, db = pg
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.chile_generation_data (extraction_run_id, created_at_ms, plant, "
            "chile_plant_id, fuel_type, timestamp_ms, generation_mwh) VALUES "
            "(%s, 1, 'TER MEJILLONES', '386', 'Carbón', %s, 150), (%s, 1, 'TER IEM', '488', 'Carbón', %s, 300)",
            (RUN_ID, TS, RUN_ID, TS),
        )
    with db._engine.connect() as c:
        before = checks.fingerprint(c)

    day = [rec(), rec(**GAS), rec(plant_key="378", chile_plant_id="378", plant="TER TARAPACA", generation_mwh=70.0),
           rec(plant_key="488", chile_plant_id="488", plant="TER IEM", generation_mwh=300.0)]
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, day))
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        json.dumps({"part_key": "386_2024", "status": "ok", "by_local_day": {"2024-06-01": [4, 520.0]}}) + "\n"
        + json.dumps({"part_key": "387_2023", "status": "failed", "error": "502"}) + "\n"
    )
    with conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly")

    with db._engine.connect() as c:
        assert checks.fingerprint(c)["md5"] == before["md5"], "all-fuel load must not touch the coal table"
        assert checks.reconcile(c, str(manifest), date(2024, 6, 1), date(2024, 6, 3), [386]) == []
        problems = checks.reconcile(c, str(manifest), date(2023, 12, 31), date(2024, 6, 1), [386, 387])
        assert problems == [
            "386_2023: not in manifest (never extracted?)",
            "387_2023: manifest status failed: 502",
            "387_2024: not in manifest (never extracted?)",
        ]
        counts = checks.coal_report(c, date(2024, 6, 1), date(2024, 6, 1), str(tmp_path / "coal.csv"))
    assert counts == {"revised": 1, "new_coal_plant": 1, "unchanged": 1}
    with db._engine.connect() as c:  # a coal unit with zero output is still listed
        db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(plant_key="117", chile_plant_id="117",
                                                                  plant="TER BOCAMINA", generation_mwh=0.0)], "z.jsonl"))
        assert checks.coal_report(c, date(2024, 6, 1), date(2024, 6, 1), str(tmp_path / "c2.csv"))["new_coal_plant"] == 2
    report = (tmp_path / "coal.csv").read_text()
    assert "386,TER MEJILLONES,150.0,100.0,50.0,Gas Natural,revised" in report


def test_compare_tolerates_float_sum_order(tmp_path):
    fp = {"rows": 2, "md5": "x", "generation_mwh": 0.1 + 0.2, "months": {"2024-06": {"rows": 2, "generation_mwh": 0.3}}}
    a, b, c = tmp_path / "a.json", tmp_path / "b.json", tmp_path / "c.json"
    a.write_text(json.dumps(fp))
    b.write_text(json.dumps({**fp, "generation_mwh": 0.3}))
    c.write_text(json.dumps({**fp, "md5": "y"}))
    assert checks.main(["compare", str(a), str(b)]) == 0
    assert checks.main(["compare", str(a), str(c)]) == 1


def test_reconcile_uses_local_day_counts_for_dst_hour(pg, tmp_path):
    conn, db = pg
    # The repeated fall-back hour (2024-04-06 23:00 -04 = 03:00Z on the 7th)
    # is in the 2024-04-07 part file but belongs to local day 2024-04-06.
    repeated = 1712458800000
    first_hour_7th = 1712462400000
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(timestamp_ms=repeated), rec(timestamp_ms=first_hour_7th)]))
    lines = [{"part_key": "386_2024", "status": "ok",
              "by_local_day": {"2024-04-06": [1, 100.0], "2024-04-07": [1, 100.0]}}]
    manifest = tmp_path / "m.jsonl"
    manifest.write_text("".join(json.dumps(x) + "\n" for x in lines))
    with conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly")
    with db._engine.connect() as c:
        assert checks.reconcile(c, str(manifest), date(2024, 4, 6), date(2024, 4, 7), [386]) == []


def test_reconcile_plant_year_manifest(pg, tmp_path):
    conn, db = pg
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**GAS)]))
    lines = [
        {"part_key": "386_2024", "status": "ok", "by_local_day": {"2024-06-01": [2, 150.0]}},
        {"part_key": "378_2024", "status": "failed", "error": "502"},
    ]
    manifest = tmp_path / "m.jsonl"
    manifest.write_text("".join(json.dumps(x) + "\n" for x in lines))
    with conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly")
    with db._engine.connect() as c:
        problems = checks.reconcile(c, str(manifest), date(2024, 5, 31), date(2024, 6, 2), [386, 378])
    assert problems == ["378_2024: manifest status failed: 502"]


def test_reconcile_flags_partial_coverage_and_stale_view(pg, tmp_path):
    conn, db = pg
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**GAS)]))
    manifest = tmp_path / "m.jsonl"
    manifest.write_text(json.dumps({"part_key": "386_2024", "status": "ok", "start": "2024-01-01",
                                    "end": "2024-06-01", "by_local_day": {"2024-06-01": [2, 150.0]}}) + "\n")
    with conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly")
        # a rename after the refresh: monthly totals unchanged, labels stale
        cur.execute("UPDATE ingestion.chile_generation_all_fuels SET plant = 'TER MEJILLONES (CTM)'")
    with db._engine.connect() as c:
        assert checks.reconcile(c, str(manifest), date(2024, 6, 1), date(2024, 6, 1), [386]) == [
            "view 2024-06 386 Térmica/Carbón: view and table differ (refresh the view?)",
            "view 2024-06 386 Térmica/Gas Natural: view and table differ (refresh the view?)",
        ]
        problems = checks.reconcile(c, str(manifest), date(2024, 6, 1), date(2024, 6, 30), [386])
    assert problems[0] == "386_2024: covers 2024-01-01..2024-06-01, needed 2024-06-01..2024-06-30"


def test_coal_report_statuses_at_plant_level(pg, tmp_path):
    conn, db = pg
    may, june = TS - 31 * 24 * 3_600_000, TS
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.chile_generation_data (extraction_run_id, created_at_ms, plant, chile_plant_id, "
            "fuel_type, timestamp_ms, generation_mwh) VALUES (%s, 1, 'TER IEM', '488', 'Carbón', %s, 300), "
            "(%s, 1, 'TER MEJILLONES', '386', 'Carbón', %s, 0)",
            (RUN_ID, may, RUN_ID, june),
        )
    # IEM: coal in May in both tables, in June only in all-fuels -> missing_from_coal_table.
    # Mejillones: June in the coal table at 0, all-fuels has only a zero-output gas row -> present, unchanged.
    db.insert_chile_all_fuels_jsonl_data(write(tmp_path, [
        rec(plant_key="488", chile_plant_id="488", plant="TER IEM", timestamp_ms=may, generation_mwh=300.0),
        rec(plant_key="488", chile_plant_id="488", plant="TER IEM", generation_mwh=280.0),
        rec(**{**GAS, "generation_mwh": 0.0}),
    ]))
    with db._engine.connect() as c:
        counts = checks.coal_report(c, date(2024, 5, 1), date(2024, 6, 30), str(tmp_path / "r.csv"))
    assert counts == {"unchanged": 2, "missing_from_coal_table": 1}


def test_cli_argument_errors_exit_2(monkeypatch):
    monkeypatch.setattr(checks, "engine", lambda: create_engine("sqlite://"))
    assert checks.main(["reconcile", "m.jsonl", "2024-01-01"]) == 2
