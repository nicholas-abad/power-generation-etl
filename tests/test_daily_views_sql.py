"""Daily/weekly views (migrations 024, 025) on an isolated local PostgreSQL.

Runs the real migrations over a few raw rows per source and checks each daily
row's day, energy, coverage and completeness, the weekly flags, and the
reconciliation check. Set ONS_TEST_PG_DSN explicitly (local servers only); CI
supplies its disposable service.
"""

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import check_daily_rollups  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402

TABLES = [
    "extraction_metadata", "entsoe_generation", "ons_generation", "occto_generation",
    "npp_generation", "chile_generation", "oe_facility_generation", "eia_generation",
    "eia_generator_info", "gcpt_coal_metadata", "climatetrace_generation",
]  # fmt: skip
RUN = str(uuid.uuid4())


def ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def hours(day: datetime, n: int = 24):
    return [int((day + timedelta(hours=h)).timestamp() * 1000) for h in range(n)]


@pytest.fixture
def db(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN is required for local PostgreSQL integration")
    assert shutil.which("psql"), "psql is required to apply the actual migrations"
    with patch.dict(os.environ):
        parts = local_settings(dsn, r"power_generation_test")
        name = f"daily_test_{uuid.uuid4().hex}"
        admin = psycopg2.connect(**parts, connect_timeout=5)
        admin.autocommit = True
        verify_local_connection(admin, parts["dbname"])
        with admin.cursor() as cur:
            for role in ["etl_writer", "dashboard_ro"]:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if not cur.fetchone():
                    cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            cur.execute(
                sql.SQL("ALTER DATABASE {} SET search_path TO public, ingestion").format(
                    sql.Identifier(name)
                )
            )
        test_parts = {**parts, "dbname": name}
        conn = psycopg2.connect(**test_parts, connect_timeout=5)
        conn.autocommit = True
        verify_local_connection(conn, name)
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
            for table in TABLES:
                cur.execute((ROOT / f"schema/{table}.sql").read_text())
            cur.execute("CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY, notes text)")
            # the views' owner reads the raw tables, as in production (migration 009)
            cur.execute("GRANT USAGE ON SCHEMA ingestion TO etl_writer")
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ingestion TO etl_writer")
            load_fixture(cur)
            cur.execute((ROOT / "schema/materialized_views.sql").read_text())
        for migration in ["024_daily_weekly_generation.sql", "025_occto_daily.sql"]:
            subprocess.run(
                ["psql", make_dsn(**test_parts), "-X", "-q", "-v", "ON_ERROR_STOP=1",
                 "-f", str(ROOT / "schema/migrations" / migration)],
                check=True, capture_output=True, text=True,
            )  # fmt: skip
        monkeypatch.setattr(
            check_daily_rollups,
            "engine",
            lambda: create_engine("postgresql+psycopg2://", connect_args=test_parts),
        )
        try:
            yield conn
        finally:
            conn.close()
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            admin.close()


def load_fixture(cur):
    def entsoe(ts, mw, fuel="Fossil Hard coal", res=60, plant="Plant E"):
        cur.execute(
            "INSERT INTO ingestion.entsoe_generation_data (extraction_run_id, created_at_ms, country_code,"
            " psr_type, plant_name, fuel_type, data_type, timestamp_ms, generation_mw, resolution_minutes)"
            " VALUES (%s, 1, 'DE', %s, %s, %s, 'A73', %s, %s, %s)",
            (RUN, "B05" if fuel == "Fossil Hard coal" else "B04", plant, fuel, ts, mw, res),
        )

    for ts in hours(datetime(2026, 1, 5, tzinfo=UTC)):
        entsoe(ts, 10)  # complete
    for ts in hours(datetime(2026, 1, 6, tzinfo=UTC), 12):
        entsoe(ts, 10)  # partial
    for ts in hours(datetime(2026, 1, 7, tzinfo=UTC), 23):
        entsoe(ts, 10)
    for q in range(4):  # the last hour in 15-minute slots: mixed lengths
        entsoe(ms(2026, 1, 7, 23) + q * 900_000, 8, res=15)
    entsoe(ms(2026, 1, 5, 0), 99, fuel="Fossil Gas")  # not coal

    def ons(ts, mwh, plant="Plant O", pid="ID1", state_name="Santa Catarina"):
        cur.execute(
            "INSERT INTO ingestion.ons_generation_data (extraction_run_id, created_at_ms, plant,"
            " ons_plant_id, fuel_type, state, state_name, timestamp_ms, generation_mwh, resolution_minutes)"
            " VALUES (%s, 1, %s, %s, 'Carvão', 'SC', %s, %s, %s, 60)",
            (RUN, plant, pid, state_name, ts, mwh),
        )

    for d in range(6):  # 2025-12-29 .. 2026-01-03, the second day all zeros
        for ts in hours(datetime(2025, 12, 29, tzinfo=UTC) + timedelta(days=d)):
            ons(ts, 0 if d == 1 else 5)
    for ts in hours(datetime(2026, 1, 4, tzinfo=UTC), 23):
        ons(ts, 5)  # hour 23 missing ...
    ons(ms(2026, 1, 4, 0), 5, pid="ID2")  # ... and a second ID doubles hour 0
    ons(ms(2026, 2, 2, 0), 10, plant="Plant N", pid="N1", state_name=None)
    ons(ms(2026, 2, 2, 0), 20, plant="Plant N", pid="N2", state_name="")

    def occto(ts, mwh, plant="Plant J", unit="1号機", fuel="coal"):
        cur.execute(
            "INSERT INTO ingestion.occto_generation_data (extraction_run_id, created_at_ms, plant, unit,"
            " fuel_type, area_name, timestamp_ms, generation_mwh, resolution_minutes)"
            " VALUES (%s, 1, %s, %s, %s, 'Tokyo', %s, %s, 30)",
            (RUN, plant, unit, fuel, ts, mwh),
        )

    for s in range(1, 49):  # a JST day by period-end label: 00:30 .. 24:00
        occto(ms(2026, 3, 2) + s * 1_800_000, 1)
    occto(ms(2026, 3, 2, 0), 10, plant="Plant K", unit="U")  # coal ...
    occto(ms(2026, 3, 2, 0, 30), 20, plant="Plant K", unit="U", fuel="oil")  # ... MAX is oil

    def npp(ts, mwh, fuel):
        cur.execute(
            "INSERT INTO ingestion.npp_generation (extraction_run_id, created_at_ms, plant, unit,"
            " plant_and_unit, fuel_type, timestamp_ms, generation_mwh, resolution_minutes)"
            " VALUES (%s, 1, 'PLANT I', 'Unit 1', 'PLANT I Unit 1', %s, %s, %s, 1440)",
            (RUN, fuel, ts, mwh),
        )

    npp(ms(2026, 1, 10), 100, "THERMAL")
    npp(ms(2026, 1, 11), 50, None)  # unlabelled day of a THERMAL unit-month: kept

    def chile(ts, mwh):
        cur.execute(
            "INSERT INTO ingestion.chile_generation_data (extraction_run_id, created_at_ms, plant,"
            " fuel_type, region, comuna, timestamp_ms, generation_mwh, resolution_minutes)"
            " VALUES (%s, 1, 'TER X', 'Carbón', 'Antofagasta', 'Mejillones', %s, %s, 60)",
            (RUN, ts, mwh),
        )

    for ts in hours(datetime(2026, 1, 5, tzinfo=UTC)):
        chile(ts, 3)

    def oe(ts, mwh, network, facility, fueltech="coal_black"):
        cur.execute(
            "INSERT INTO ingestion.oe_facility_generation_data (extraction_run_id, created_at_ms,"
            " network_code, facility_code, facility_name, fueltech, timestamp_ms, generation_mwh,"
            " resolution_minutes) VALUES (%s, 1, %s, %s, %s, %s, %s, %s, 1440)",
            (RUN, network, facility, facility, fueltech, ts, mwh),
        )

    oe(ms(2026, 1, 14, 14), 100, "NEM", "NEMF")  # 00:00 AEST on 15 January
    oe(ms(2026, 1, 14, 16), 50, "WEM", "WEMF", "coal_brown")  # 00:00 AWST on 15 January
    oe(ms(2026, 1, 31, 14), 70, "NEM", "NEMF")  # 1 February locally, January in UTC


def rows(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def test_daily_rows_carry_the_right_day_energy_and_completeness(db):
    assert rows(db, "SELECT day::text, generation_mwh, covered_minutes, completeness"
                    " FROM mv_entsoe_plant_daily ORDER BY day") == [
        ("2026-01-05", 240.0, 1440, "complete"),
        ("2026-01-06", 120.0, 720, "partial"),
        ("2026-01-07", 238.0, 1440, "unknown"),  # mixed 60/15-minute slots
    ]  # fmt: skip
    ons = dict(rows(db, "SELECT day::text, completeness FROM mv_ons_plant_daily WHERE plant = 'Plant O'"))
    assert ons["2025-12-30"] == "complete"  # reported zeros, all hours present
    assert ons["2026-01-04"] == "unknown"  # a doubled hour must not hide the missing one
    assert rows(db, "SELECT generation_mwh FROM mv_ons_plant_daily WHERE day = '2025-12-30'") == [(0.0,)]
    assert rows(db, "SELECT day::text, observation_count, completeness FROM mv_occto_unit_daily"
                    " ORDER BY day") == [("2026-03-02", 47, "partial"), ("2026-03-03", 1, "partial")]  # fmt: skip
    assert rows(db, "SELECT day::text, generation_mwh FROM mv_npp_unit_daily ORDER BY day") == [
        ("2026-01-10", 100.0),
        ("2026-01-11", 50.0),
    ]
    assert rows(db, "SELECT DISTINCT completeness FROM mv_chile_plant_daily") == [("unknown",)]
    assert rows(db, "SELECT facility_code, day::text FROM mv_oe_plant_daily ORDER BY 2, 1") == [
        ("NEMF", "2026-01-15"),
        ("WEMF", "2026-01-15"),
        ("NEMF", "2026-02-01"),
    ]


def test_weekly_views_and_the_year_boundary(db):
    assert rows(db, "SELECT week_start::text, days_reported, days_complete, is_complete"
                    " FROM ons_plant_weekly WHERE plant = 'Plant O'") == [("2025-12-29", 7, 6, False)]  # fmt: skip


def test_the_rollup_check_passes_and_catches_a_lost_row(db):
    assert check_daily_rollups.main() == 0  # incl. NULL vs '' state_name kept apart
    with db.cursor() as cur:
        cur.execute("DELETE FROM ingestion.ons_generation_data WHERE plant = 'Plant O' AND timestamp_ms = %s",
                    (ms(2026, 1, 2, 5),))  # fmt: skip
        cur.execute("REFRESH MATERIALIZED VIEW mv_ons_plant_daily")  # daily only: now disagrees
    assert check_daily_rollups.main() == 1
