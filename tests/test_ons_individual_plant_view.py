"""Exercise migration 017 and the real ONS loader on an isolated local DB.

Set ONS_TEST_PG_DSN explicitly. This test never uses the project's .env
connection and refuses non-local servers. CI supplies its disposable service.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import parse_dsn, make_dsn
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from database import PowerGenerationDatabase  # noqa: E402


@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN is required for local PostgreSQL integration")
    params = parse_dsn(dsn)
    host = params.get("host", "")
    assert host in {"localhost", "127.0.0.1", "::1"} or host.startswith("/"), (
        "The migration test requires a local disposable PostgreSQL server"
    )
    assert shutil.which("psql"), "psql is required to test the actual migration"
    name = f"ons_test_{uuid.uuid4().hex}"
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
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
    params["dbname"] = name
    test_dsn = make_dsn(**params)
    conn = psycopg2.connect(test_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute((ROOT / "schema/ons_generation.sql").read_text())
        cur.execute((ROOT / "schema/extraction_metadata.sql").read_text())
        cur.execute(
            "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY, notes text)"
        )
        cur.execute("GRANT USAGE ON SCHEMA public, ingestion TO etl_writer")
        cur.execute("GRANT SELECT ON ingestion.ons_generation_data TO etl_writer")
    monkeypatch.setenv("POSTGRES_SSLMODE", "disable")
    db = PowerGenerationDatabase(
        host=host,
        port=int(params.get("port", 5432)),
        database=name,
        username=params.get("user", "postgres"),
        password=params.get("password", "unused"),
    )
    try:
        yield conn, db, test_dsn
    finally:
        db.engine.dispose()
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
        admin.close()


def record(plant_id, **overrides):
    return {
        "extraction_run_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "created_at_ms": 1790985600000,
        "timestamp_ms": 1548979200000,  # February 1, 2019, source-clock encoding
        "plant": f"Plant {plant_id}",
        "ons_plant_id": plant_id,
        "plant_type": "TÉRMICA",
        "fuel_type": "Carvão",
        "subsystem_id": "S",
        "subsystem": "SUL",
        "state": "RS",
        "state_name": "RIO GRANDE DO SUL",
        "operation_mode": "TIPO I",
        "ceg": "UTE.RS.000001",
        "generation_mwh": 12.5,
        "resolution_minutes": 60,
        **overrides,
    }


def migrate(dsn):
    result = subprocess.run(
        [
            "psql",
            "-X",
            dsn,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(ROOT / "schema/migrations/017_ons_individual_plant_view.sql"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_real_loader_migration_and_replay(database, tmp_path):
    conn, db, dsn = database
    rows = [
        record("COAL"),
        record("HYDRO", plant_type="HIDROELÉTRICA", fuel_type="Hidráulica"),
        record("SOLAR", plant_type="FOTOVOLTAICA", fuel_type="Fotovoltaica"),
        record("WIND", plant_type="EOLIELÉTRICA", fuel_type="Eólica"),
        record("NUCLEAR", plant_type="NUCLEAR", fuel_type="Nuclear", ceg=None),
        record("BIO", fuel_type="Biomassa", operation_mode=" tipo ii-b "),
        record("GAS", fuel_type="Gás", generation_mwh=0),
        record("UNKNOWN_FUEL", fuel_type="New source fuel", operation_mode="TIPO II-A"),
    ]
    source_file = tmp_path / "ons_etl.jsonl"
    source_file.write_text("".join(json.dumps(row) + "\n" for row in rows))
    ok, report = db.insert_ons_jsonl_data(str(source_file), chunk_lines=3)
    assert ok and report.valid_count == 8 and report.invalid_count == 0
    migrate(dsn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ons_plant_id, fuel_type, observation_count FROM public.mv_ons_individual_plant_monthly"
        )
        assert set(cur.fetchall()) == {
            (r["ons_plant_id"], r["fuel_type"], 1) for r in rows
        }
        cur.execute(
            "SELECT start_date::text, end_date::text FROM ingestion.extraction_metadata"
        )
        assert cur.fetchall() == [("2019-02-01", "2019-02-01")]
        cur.execute(
            "SELECT has_table_privilege('dashboard_ro', 'public.mv_ons_individual_plant_monthly', 'SELECT')"
        )
        assert cur.fetchone() == (False,)
        cur.execute(
            "SELECT has_table_privilege('etl_writer', 'public.mv_ons_individual_plant_monthly', 'SELECT')"
        )
        assert cur.fetchone() == (True,)
    ok, _ = db.insert_ons_jsonl_data(str(source_file), chunk_lines=3)
    assert ok
    migrate(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM ingestion.ons_generation_data")
        assert cur.fetchone() == (8,)
        cur.execute(
            "SELECT count(*) FROM ingestion.schema_migrations WHERE version='017'"
        )
        assert cur.fetchone() == (1,)
        cur.execute(
            "SELECT generation_mwh FROM ingestion.ons_generation_data WHERE ons_plant_id='COAL'"
        )
        assert cur.fetchone() == (12.5,)


def test_qualification_and_utc_month_boundary(database):
    conn, _, dsn = database
    rows = [
        record("KEEP", ceg=None, generation_mwh=0),
        record("KEEP", plant="Ineligible collision", operation_mode="TIPO III"),
        record("DUP", plant="First name"),
        record("DUP", plant="Second name"),
        record("III", operation_mode="TIPO III"),
        record("SMALL", operation_mode="Pequenas Usinas (Tipo III)"),
        record("IIC", operation_mode="TIPO II-C"),
        record("SET", operation_mode="Conjunto de Usinas"),
        record("MMGD", operation_mode="Pequenas Usinas (MMGD)"),
        record("MODE", operation_mode="New modality"),
        record(None),
        record(" \t- \t"),
        record("NAME", plant="\t\n"),
        record("FUEL", fuel_type=None),
        record("TYPE", plant_type="-"),
        record("CEG", ceg="first;second"),
        record("INF", generation_mwh=float("inf")),
        record("NAN", generation_mwh=float("nan")),
        record("RESOLUTION", resolution_minutes=30),
    ]
    columns = list(rows[0])
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO ingestion.ons_generation_data ("
            + ",".join(columns)
            + ") VALUES %s",
            [[r[c] for c in columns] for r in rows],
        )
    migrate(dsn)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'America/Sao_Paulo'")
        cur.execute(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_ons_individual_plant_monthly"
        )
        cur.execute(
            "SELECT (month AT TIME ZONE 'UTC')::text, ons_plant_id, observation_count, generation_mwh FROM public.mv_ons_individual_plant_monthly"
        )
        assert cur.fetchall() == [("2019-02-01 00:00:00", "KEEP", 1, 0.0)]
