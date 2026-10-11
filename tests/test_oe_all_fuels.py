"""OpenElectricity all-fuel staging pilot (migration 022): validator, loader, views, checks.

The database tests need a disposable LOCAL PostgreSQL (COAL_TEST_PG_DSN, the
same guarded fixture convention as test_entsoe_coal_hotfix.py); never Neon.
"""

import json
import os
from datetime import date
from pathlib import Path
import sys
from unittest.mock import patch

import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import check_oe_all_fuels as checks  # noqa: E402
from database import PowerGenerationDatabase  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402
import refresh_views  # noqa: E402
from validator import DataValidator  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "11111111-2222-3333-4444-555555555555"
OCT1_NEM = 1759240800000  # 2025-10-01 00:00 +10:00 (= 2025-09-30 14:00 UTC)
OCT1_WEM = 1759248000000  # 2025-10-01 00:00 +08:00
DAY = 86_400_000


def rec(**kw):
    base = {
        "extraction_run_id": RUN_ID, "created_at_ms": 1760000000000, "network_code": "NEM",
        "network_region": "NSW1", "facility_code": "ERARING", "facility_name": "Eraring",
        "unit_code": "ER01", "fueltech": "coal_black", "fueltech_group": "coal", "unit_status": "operating",
        "direction": "injection", "basis": "reported", "timestamp_ms": OCT1_NEM, "energy_mwh": 10000.0,
        "capacity_registered_mw": 720.0, "latitude": -33.06, "longitude": 151.55, "resolution_minutes": 1440,
    }
    base.update(kw)
    return base


CHG = dict(facility_code="BULGANA", unit_code="BULGBL1", fueltech="battery_charging",
           fueltech_group="battery", direction="withdrawal", energy_mwh=50.0)
DIS = dict(facility_code="BULGANA", unit_code="BULGBG1", fueltech="battery_discharging",
           fueltech_group="battery", direction="injection", energy_mwh=40.0)
NET = dict(facility_code="LGAPWF1", unit_code="LGAPBS1", fueltech="battery", fueltech_group="battery",
           direction="net", energy_mwh=-12.5)
AUX = dict(unit_code="ER02", direction="auxiliary", energy_mwh=3.2)
DERIVED = dict(facility_code="LIMBESS", unit_code="LIMBESS1", fueltech="battery_charging",
               fueltech_group="battery", direction="withdrawal", basis="derived", energy_mwh=20.0)


def validate(records):
    return DataValidator().validate_file(records, "oe_all_fuels")


# --- validator (no database) ----------------------------------------------------


def test_directions_and_signs():
    valid, report = validate([rec(), rec(**CHG), rec(**DIS), rec(**NET), rec(**AUX), rec(**DERIVED)])
    assert report.invalid_count == 0 and len(valid) == 6


@pytest.mark.parametrize("bad", [
    {"energy_mwh": -1.0},                         # negative injection
    {"direction": "charging"},                    # unknown direction
    {"basis": "guessed"},
    {"unit_code": ""},
])
def test_invalid_records_rejected(bad):
    _, report = validate([rec(**bad)])
    assert report.invalid_count == 1


def test_same_unit_day_two_directions_both_survive_but_true_duplicates_do_not():
    valid, report = validate([rec(), rec(**{"direction": "auxiliary", "energy_mwh": 1.0})])
    assert len(valid) == 2 and report.duplicate_count == 0
    _, report = validate([rec(), rec(energy_mwh=1.0)])
    assert report.duplicate_count == 1


def test_view_registered_only_as_staging():
    staging = {v for vs in refresh_views.STAGING_VIEWS.values() for v in vs}
    assert "mv_oe_all_fuels_unit_monthly" in staging
    assert not staging & set(refresh_views.ALL_VIEWS)
    assert "mv_oe_plant_monthly" in refresh_views.ALL_VIEWS


def test_migration_keeps_view_off_dashboard_and_records_ledger():
    sql = (ROOT / "schema/migrations/022_oe_all_fuels.sql").read_text()
    assert "REVOKE ALL ON public.mv_oe_all_fuels_unit_monthly FROM PUBLIC, dashboard_ro" in sql
    assert "('022'," in sql
    assert "oe_facility_generation_data" not in sql.split("\\ir")[1]
    assert "oe_unit_generation_all_fuels" in (ROOT / "schema/checks/no_shadow_tables.sql").read_text()


# --- loader, view and checks against local PostgreSQL -------------------------------


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
                    cur.execute("DROP MATERIALIZED VIEW IF EXISTS public.mv_oe_all_fuels_unit_monthly")
                    cur.execute((ROOT / "schema/oe_facility_generation.sql").read_text())
                    cur.execute((ROOT / "schema/oe_unit_generation_all_fuels.sql").read_text())
            engine = create_engine(
                URL.create("postgresql+psycopg2", username=parts["user"], password=parts.get("password"),
                           host=parts["host"], port=int(parts.get("port", 5432)), database=parts["dbname"]),
                connect_args={"options": "-c search_path=ingestion,public"},
            )
            db = PowerGenerationDatabase.__new__(PowerGenerationDatabase)
            db._engine = engine
            yield conn, db
        finally:
            if engine is not None:
                engine.dispose()
            conn.close()


def write(tmp_path, records, name="NEM_2025-10__2025-10-01_2025-10-31.jsonl"):
    p = tmp_path / name
    p.write_text("".join(json.dumps(r) + "\n" for r in records))
    return str(p)


def rows(conn):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT unit_code, direction, basis, energy_mwh FROM ingestion.oe_unit_generation_all_fuels ORDER BY 1, 2")
        return cur.fetchall()


def refresh(conn):
    with conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("REFRESH MATERIALIZED VIEW public.mv_oe_all_fuels_unit_monthly")


def test_load_all_directions_and_replay_is_idempotent(pg, tmp_path):
    conn, db = pg
    path = write(tmp_path, [rec(), rec(**CHG), rec(**DIS), rec(**NET), rec(**AUX), rec(**DERIVED)])
    ok, report = db.insert_oe_all_fuels_jsonl_data(path)
    assert ok and report.valid_count == 6
    first = rows(conn)
    assert ("LGAPBS1", "net", "reported", -12.5) in first and ("LIMBESS1", "withdrawal", "derived", 20.0) in first
    assert db.insert_oe_all_fuels_jsonl_data(path)[0] and rows(conn) == first


def test_any_invalid_record_writes_nothing(pg, tmp_path):
    conn, db = pg
    ok, report = db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**{**DIS, "energy_mwh": -4.0})]))
    assert not ok and report.invalid_count == 1 and rows(conn) == []


def test_view_buckets_network_local_months(pg, tmp_path):
    conn, db = pg
    # 1 October local midnight (30 September in UTC) for both networks
    assert db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [
        rec(), rec(timestamp_ms=OCT1_NEM - DAY, energy_mwh=7000.0),  # 30 September local
    ], "NEM_2025-09__2025-09-30_2025-10-31.jsonl"))[0]
    assert db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [
        rec(network_code="WEM", unit_code="COLLIE_G1", timestamp_ms=OCT1_WEM, energy_mwh=5000.0),
    ], "WEM_2025-10__2025-10-01_2025-10-31.jsonl"))[0]
    refresh(conn)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT network_code, to_char(month,'YYYY-MM'), SUM(energy_mwh) FROM public.mv_oe_all_fuels_unit_monthly GROUP BY 1, 2 ORDER BY 1, 2")
        assert cur.fetchall() == [("NEM", "2025-09", 7000.0), ("NEM", "2025-10", 10000.0), ("WEM", "2025-10", 5000.0)]


def test_fingerprint_reconcile_and_fuel_report(pg, tmp_path):
    conn, db = pg
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.oe_facility_generation_data (extraction_run_id, created_at_ms, network_code, "
            "network_region, facility_code, facility_name, fueltech, fueltech_group, timestamp_ms, generation_mwh, "
            "resolution_minutes) VALUES (%s, 1, 'NEM', 'NSW1', 'ERARING', 'Eraring', 'coal_black', 'coal', %s, 10000, 1440)",
            (RUN_ID, OCT1_NEM),
        )
    with db._engine.connect() as c:
        before = checks.fingerprint(c)
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**CHG)]))
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps({"part_key": "NEM_2025-10", "status": "ok", "start": "2025-10-01",
                                    "end": "2025-10-31", "by_local_day": {"2025-10-01": [2, 10050.0]}}) + "\n")
    refresh(conn)
    with db._engine.connect() as c:
        assert checks.fingerprint(c)["md5"] == before["md5"]
        assert checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [str(manifest)]) == []
        problems = checks.reconcile(c, date(2025, 9, 1), date(2025, 10, 31), [str(manifest)])
        assert problems == ["NEM_2025-09: not in manifest (never extracted?)"]
        n = checks.fuel_report(c, date(2025, 10, 1), date(2025, 10, 31), str(tmp_path / "f.csv"))
    report = (tmp_path / "f.csv").read_text()
    assert n == 2 and "2025-10,NEM,coal,injection,10000.0,1,10000.0" in report


def test_reconcile_flags_a_stale_view(pg, tmp_path):
    conn, db = pg
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec()]))
    refresh(conn)
    with conn, conn.cursor() as cur:
        cur.execute("UPDATE ingestion.oe_unit_generation_all_fuels SET facility_name = 'Eraring (renamed)'")
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps({"part_key": "NEM_2025-10", "status": "ok", "start": "2025-10-01",
                                    "end": "2025-10-31", "by_local_day": {"2025-10-01": [1, 10000.0]}}) + "\n")
    with db._engine.connect() as c:
        assert checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [str(manifest)]) == [
            "view 2025-10-01 NEM ER01 injection: view and table differ (refresh the view?)"
        ]


# --- review fixes: replace on reload, finite energy, reconcile preconditions -------------


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_non_finite_energy_rejected(value):
    _, report = validate([rec(energy_mwh=value), rec(**{**NET, "energy_mwh": value})])
    assert report.invalid_count == 2


def test_reloading_a_month_replaces_rows_whose_direction_changed(pg, tmp_path):
    conn, db = pg
    # first extraction: the battery day only had its net aggregate
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(**{**NET, "unit_code": "BAT1"})], "NEM_2025-10__2025-10-01_2025-10-31.jsonl"))
    # re-extraction: components now reported for the same unit-day
    v2 = [rec(), rec(**{**DIS, "unit_code": "BAT1G1"}), rec(**{**CHG, "unit_code": "BAT1L1"})]
    ok, _ = db.insert_oe_all_fuels_jsonl_data(write(tmp_path, v2))
    assert ok
    got = rows(conn)
    assert ("BAT1", "net", "reported", -12.5) not in got
    assert sorted((u, d) for u, d, _, _ in got) == [("BAT1G1", "injection"), ("BAT1L1", "withdrawal"), ("ER01", "injection")]


def test_reload_never_touches_other_networks_or_days(pg, tmp_path):
    conn, db = pg
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(network_code="WEM", unit_code="W1", timestamp_ms=OCT1_WEM)], "WEM_2025-10__2025-10-01_2025-10-31.jsonl"))
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(timestamp_ms=OCT1_NEM + DAY, unit_code="ER09")], "NEM_2025-10__2025-10-02_2025-10-02.jsonl"))
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec()], "NEM_2025-10__2025-10-01_2025-10-01.jsonl"))  # NEM, 1 October only
    assert {u for u, *_ in rows(conn)} == {"W1", "ER09", "ER01"}


def test_reconcile_preconditions(pg, tmp_path):
    conn, db = pg
    manifest = tmp_path / "m.jsonl"
    manifest.write_text(json.dumps({"part_key": "NEM_2025-10", "status": "ok", "by_local_day": {}}) + "\n")
    with db._engine.connect() as c:
        with pytest.raises(ValueError, match="at least one manifest"):
            checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [])
        with pytest.raises(ValueError, match="after end"):
            checks.reconcile(c, date(2025, 10, 31), date(2025, 10, 1), [str(manifest)])
        with pytest.raises(ValueError, match="two manifests for NEM"):
            checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [str(manifest), str(manifest)])
        problems = checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [str(manifest)])
    assert problems[0] == "NEM_2025-10: manifest line has no start/end"
    assert checks.main(["reconcile", "2025-10-01", "2025-10-31"]) == 2


def test_reconcile_flags_stale_region_labels(pg, tmp_path):
    conn, db = pg
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec()]))
    refresh(conn)
    with conn, conn.cursor() as cur:
        cur.execute("UPDATE ingestion.oe_unit_generation_all_fuels SET network_region = 'QLD1'")
    manifest = tmp_path / "m.jsonl"
    manifest.write_text(json.dumps({"part_key": "NEM_2025-10", "status": "ok", "start": "2025-10-01",
                                    "end": "2025-10-31", "by_local_day": {"2025-10-01": [1, 10000.0]}}) + "\n")
    with db._engine.connect() as c:
        assert checks.reconcile(c, date(2025, 10, 1), date(2025, 10, 31), [str(manifest)]) == [
            "view 2025-10-01 NEM ER01 injection: view and table differ (refresh the view?)"
        ]


def test_a_file_mixing_networks_is_refused(pg, tmp_path):
    conn, db = pg
    ok, _ = db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(network_code="WEM", unit_code="W1")]))
    assert not ok and rows(conn) == []


def test_a_reload_replaces_the_declared_days_even_when_one_is_gone(pg, tmp_path):
    conn, db = pg
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(), rec(timestamp_ms=OCT1_NEM + 30 * DAY, unit_code="ER31")]))
    # re-extraction of October no longer has the 31st
    assert db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec()]))[0]
    assert {u for u, *_ in rows(conn)} == {"ER01"}


@pytest.mark.parametrize("name, records", [
    ("NEM_2025-10.jsonl", [rec()]),  # no declared range
    ("NEM_2025-10__2025-10-02_2025-10-31.jsonl", [rec()]),  # a record before the range
    ("WEM_2025-10__2025-10-01_2025-10-31.jsonl", [rec()]),  # a NEM record in a WEM part
    ("NEM_2025-10__2025-10-01_2025-10-31.jsonl", []),  # empty: would replace nothing
])
def test_a_part_that_does_not_match_its_name_is_refused(pg, tmp_path, name, records):
    conn, db = pg
    db.insert_oe_all_fuels_jsonl_data(write(tmp_path, [rec(unit_code="KEEP")], "NEM_2025-10__2025-10-01_2025-10-01.jsonl"))
    ok, _ = db.insert_oe_all_fuels_jsonl_data(write(tmp_path, records, name))
    assert not ok and {u for u, *_ in rows(conn)} == {"KEEP"}
