"""A disposable local PostgreSQL database per test, for real-loader tests.

Needs ONS_TEST_PG_DSN (or COAL_TEST_PG_DSN) pointing at a local server whose
user can create databases. Refuses non-local servers; never uses .env.
"""

import os
from pathlib import Path
import shutil
import sys
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn, make_dsn
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from database import PowerGenerationDatabase  # noqa: E402

@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN") or os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN or COAL_TEST_PG_DSN is required for local PostgreSQL integration")
    params = parse_dsn(dsn)
    host = params.get("host", "")
    assert host in {"localhost", "127.0.0.1", "::1"} or host.startswith("/"), (
        "The migration test requires a local disposable PostgreSQL server"
    )
    assert shutil.which("psql"), "psql is required to test the actual migration"
    name = f"entsoe_test_{uuid.uuid4().hex}"
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
