"""Fuel groups (config/fuel-groups.json) and Chile's fuel-group view (migration 026).

The database test runs the real migrations 019 and 026 on an isolated LOCAL
PostgreSQL (ONS_TEST_PG_DSN; CI supplies its disposable service) and refreshes
the view through refresh_views, as the weekly job does.
"""

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
import fuel_groups  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402
import refresh_views  # noqa: E402

RUN = "11111111-2222-3333-4444-555555555555"
MARCH = 1709251200000  # 2024-03-01T00:00Z
HOUR = 3_600_000


def document(**sources):
    return {"groups": {"coal": "Coal", "gas": "Gas"}, "sources": sources}


# --- the list ---------------------------------------------------------------


def test_the_committed_list_holds_the_decided_groups():
    groups = {(s, label): g for s, label, g, _ in fuel_groups.rows()}
    assert groups[("CHILE", "PetCoke")] == "other_fossil"
    assert groups[("CHILE", "Diésel")] == "oil"
    assert groups[("OE", "distillate")] == "oil"
    assert groups[("OE", "pumps")] is None  # pumping is consumption
    assert groups[("CHILE", "BESS")] is None  # batteries out for now


def test_every_chile_label_seen_on_staging_is_decided():
    # Distinct fuel_type of ingestion.chile_generation_all_fuels on staging,
    # 2026-11-09 (the 19 plants that have had a coal unit).
    known = {label for s, label, _, _ in fuel_groups.rows() if s == "CHILE"}
    assert {"Carbón", "Gas Natural", "PetCoke", "Diésel"} <= known


@pytest.mark.parametrize(
    "source, message",
    [
        ({"grouped": {"Carbón": "lignite"}}, "unknown group"),
        (
            {"grouped": {"Carbón": "coal"}, "excluded": {"Carbón": "x"}},
            "both grouped and excluded",
        ),
        ({"excluded": {"BESS": ""}}, "needs a reason"),
    ],
)
def test_an_invalid_list_fails_loudly(source, message):
    with pytest.raises(ValueError, match=message):
        fuel_groups.parse(document(CHILE=source))


def test_the_surface_check_expects_the_view_once_026_is_recorded():
    check = (ROOT / "schema/checks/dashboard_ro_surface.sql").read_text()
    assert "version = '026'" in check and "public.mv_chile_fuel_group_monthly" in check


# --- the view on a local PostgreSQL -----------------------------------------


@pytest.fixture
def db(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN is required for local PostgreSQL integration")
    assert shutil.which("psql"), "psql is required to apply the actual migrations"
    with patch.dict(os.environ):
        parts = local_settings(dsn, r"power_generation_test")
        name = f"fuel_groups_test_{uuid.uuid4().hex}"
        admin = psycopg2.connect(**parts, connect_timeout=5)
        admin.autocommit = True
        verify_local_connection(admin, parts["dbname"])
        with admin.cursor() as cur:
            for role in ["etl_writer", "dashboard_ro"]:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if not cur.fetchone():
                    cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        test_parts = {**parts, "dbname": name}
        conn = psycopg2.connect(**test_parts, connect_timeout=5)
        conn.autocommit = True
        verify_local_connection(conn, name)
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA ingestion")
            cur.execute(
                "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY, notes text)"
            )

        def migrate(version):
            subprocess.run(
                ["psql", make_dsn(**test_parts), "-X", "-q", "-v", "ON_ERROR_STOP=1",
                 "-f", str(next((ROOT / "schema/migrations").glob(f"{version}_*.sql")))],
                check=True, capture_output=True, text=True,
            )  # fmt: skip

        monkeypatch.setattr(
            refresh_views,
            "create_engine",
            lambda url: create_engine(
                "postgresql+psycopg2://", connect_args=test_parts
            ),
        )
        monkeypatch.setattr(refresh_views, "get_connection_url", lambda: "")
        try:
            yield conn, migrate
        finally:
            conn.close()
            with admin.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )
            admin.close()


def insert(conn, plant_key, plant, subtype, fuel_type, ts, mwh, tipo="Térmica"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.chile_generation_all_fuels (extraction_run_id, created_at_ms,"
            " plant_key, chile_plant_id, plant, tipo_tecnologia, subtipo_tecnologia, fuel_type,"
            " timestamp_ms, generation_mwh) VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s)",
            (RUN, plant_key, plant_key, plant, tipo, subtype, fuel_type, ts, mwh),
        )


def view(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_char(month AT TIME ZONE 'UTC', 'YYYY-MM'), plant, fuel_group,"
            " generation_mwh, hours FROM public.mv_chile_fuel_group_monthly ORDER BY 2, 3"
        )
        return cur.fetchall()


def test_026_needs_019(db):
    conn, migrate = db
    with pytest.raises(subprocess.CalledProcessError) as error:
        migrate("026")
    assert "migration 019" in error.value.stderr


def test_each_plant_month_is_split_into_its_fuel_groups(db):
    conn, migrate = db
    migrate("019")
    migrate("026")
    # Mejillones: two coal units and a gas unit in one hour, coal again the next.
    insert(conn, "386", "TER MEJILLONES", "Carbón", "Carbón", MARCH, 100.0)
    insert(conn, "386", "TER MEJILLONES", "Gas Natural", "Gas Natural", MARCH, 50.0)
    insert(conn, "386", "TER MEJILLONES", "Carbón", "Carbón", MARCH + HOUR, 90.0)
    insert(conn, "182", "TER PETROPOWER", "PetCoke", "PetCoke", MARCH, 40.0)
    insert(conn, "999", "BESS X", "Retiro", "BESS", MARCH, 7.0, tipo="BESS")
    assert view(conn) == []  # created empty
    assert refresh_views.refresh_views(["mv_chile_fuel_group_monthly"])
    assert view(conn) == [
        ("2024-03", "TER MEJILLONES", "coal", 190.0, 2),
        ("2024-03", "TER MEJILLONES", "gas", 50.0, 1),
        ("2024-03", "TER PETROPOWER", "other_fossil", 40.0, 1),
    ]  # batteries left out
    with conn.cursor() as cur:
        cur.execute(
            "SELECT has_table_privilege('dashboard_ro', 'public.mv_chile_fuel_group_monthly', 'SELECT')"
        )
        assert cur.fetchone() == (True,)
        cur.execute("SELECT count(*) FROM ingestion.fuel_groups")
        assert cur.fetchone() == (len(fuel_groups.rows()),)


def test_an_unknown_label_fails_the_refresh_and_keeps_the_view(db):
    conn, migrate = db
    migrate("019")
    migrate("026")
    insert(conn, "386", "TER MEJILLONES", "Carbón", "Carbón", MARCH, 100.0)
    assert refresh_views.refresh_views(["mv_chile_fuel_group_monthly"])
    before = view(conn)
    insert(conn, "386", "TER MEJILLONES", "Biomasa", "Biomasa", MARCH, 5.0)
    assert not refresh_views.refresh_views(["mv_chile_fuel_group_monthly"])
    assert view(conn) == before


def test_the_view_is_skipped_until_026_and_the_rollback_returns_to_it(db):
    conn, migrate = db
    migrate("019")
    assert refresh_views.refresh_views(["mv_chile_fuel_group_monthly"])  # skipped
    migrate("026")
    subprocess.run(
        ["psql", conn.dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1",
         "-f", str(ROOT / "schema/migrations/rollback/026_chile_fuel_groups.sql")],
        check=True, capture_output=True, text=True,
    )  # fmt: skip
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.mv_chile_fuel_group_monthly'), to_regclass('ingestion.fuel_groups')"
        )
        assert cur.fetchone() == (None, None)
    assert refresh_views.refresh_views(["mv_chile_fuel_group_monthly"])  # skipped again
