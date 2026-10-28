"""Migration 023: one spelling per EIA generator ID, on stored rows and new loads.

Set ONS_TEST_PG_DSN to a local disposable PostgreSQL server; each test uses a
fresh database and never reads the project's .env.
"""

import json
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from database import PowerGenerationDatabase  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402

JUL_2025 = 1751328000000
AUG_2025 = 1754006400000
SEP_2025 = 1756684800000


@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN is required for local PostgreSQL integration")
    assert shutil.which("psql"), "psql is required to test the actual migration"
    with patch.dict(os.environ):
        parts = local_settings(dsn, r"power_generation_test")
        name = f"eia_key_{uuid.uuid4().hex}"
        admin = psycopg2.connect(**parts, connect_timeout=5)
        admin.autocommit = True
        verify_local_connection(admin, parts["dbname"])
        with admin.cursor() as cur:
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
            cur.execute((ROOT / "schema/eia_generation.sql").read_text())
            cur.execute((ROOT / "schema/extraction_metadata.sql").read_text())
            cur.execute(
                "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY, notes text)"
            )
        monkeypatch.setenv("POSTGRES_SSLMODE", "disable")
        db = PowerGenerationDatabase(
            host=parts["host"],
            port=int(parts.get("port", 5432)),
            database=name,
            username=parts["user"],
            password=parts.get("password", "unused"),
        )
        try:
            yield conn, db, make_dsn(**test_parts)
        finally:
            db.engine.dispose()
            conn.close()
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
            admin.close()


def record(plant, generator, ts, mwh=100.0, **kw):
    return {
        "extraction_run_id": str(uuid.uuid4()),
        "created_at_ms": 1790985600000,
        "timestamp_ms": ts,
        "utility_id": "1",
        "plant_code": plant,
        "generator_id": generator,
        "state": "WY",
        "prime_mover": "ST",
        "net_generation_mwh": mwh,
        "eia_plant_unit_id": f"{plant}|{generator}",
        **kw,
    }


def insert(conn, records):
    cols = list(records[0])
    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                f"INSERT INTO ingestion.eia_generation_data ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))})",
                [r[c] for c in cols],
            )


def run(dsn, path):
    return subprocess.run(
        ["psql", "-X", dsn, "-v", "ON_ERROR_STOP=1", "-f", str(ROOT / path)],
        capture_output=True,
        text=True,
    )


def migrate(dsn):
    return run(dsn, "schema/migrations/023_eia_generator_key.sql")


def stored(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT plant_code, timestamp_ms, generator_id, generator_id_reported, "
            "eia_plant_unit_id, net_generation_mwh FROM ingestion.eia_generation_data "
            "ORDER BY plant_code, timestamp_ms, generator_id"
        )
        return cur.fetchall()


def test_key_rule(database):
    conn, _, dsn = database
    assert migrate(dsn).returncode == 0
    cases = {
        ("56609", "01"): "1",  # Dry Fork's Form 860 spelling
        ("56609", "1"): "1",
        ("55479", "0001"): "1",
        ("1", "01A"): "1A",
        ("1", "TG 3"): "TG3",
        ("1", "st2"): "ST2",  # letters kept: ST2 (coal) is not GT2 (oil)
        ("1", "GT2"): "GT2",
        ("1", "0"): "0",
        ("1", "00"): "0",
        ("6166", "MB1"): "1",  # Rockport only
        ("6166", "MB2"): "2",
        ("7", "MB1"): "MB1",
        ("56032", "0001"): "0001",  # Form 860 lists 0001 and 001 as two turbines
        ("56032", "001"): "001",
    }
    with conn.cursor() as cur:
        for (plant, gid), want in cases.items():
            cur.execute("SELECT eia_generator_key(%s, %s)", (plant, gid))
            assert cur.fetchone()[0] == want, (plant, gid)
        cur.execute("SELECT eia_generator_key('1', NULL)")
        assert cur.fetchone()[0] is None


def test_migration_rekeys_history_and_keeps_eia_spelling(database):
    conn, _, dsn = database
    insert(
        conn,
        [
            record("56609", "1", JUL_2025),
            record("6166", "MB1", AUG_2025, 580.0),
            record("6166", "1", SEP_2025, 427.0),
            record("50392", "TG 3", JUL_2025),
            record("56032", "0001", JUL_2025),
        ],
    )
    result = migrate(dsn)
    assert result.returncode == 0, result.stderr
    assert stored(conn) == [
        ("50392", JUL_2025, "TG3", "TG 3", "50392|TG3", 100.0),
        ("56032", JUL_2025, "0001", "0001", "56032|0001", 100.0),
        ("56609", JUL_2025, "1", "1", "56609|1", 100.0),
        ("6166", AUG_2025, "1", "MB1", "6166|1", 580.0),
        ("6166", SEP_2025, "1", "1", "6166|1", 427.0),
    ]
    # Re-running changes nothing.
    assert migrate(dsn).returncode == 0
    assert len(stored(conn)) == 5


def test_two_spellings_in_one_month_refuse_the_migration(database):
    conn, _, dsn = database
    insert(conn, [record("6166", "MB1", AUG_2025), record("6166", "1", AUG_2025)])
    result = migrate(dsn)
    assert result.returncode != 0 and "uq_eia_natural_key" in result.stderr
    with conn.cursor() as cur:
        cur.execute("SELECT to_regproc('public.eia_generator_key')")
        assert cur.fetchone()[0] is None  # whole migration rolled back
        cur.execute("SELECT generator_id FROM ingestion.eia_generation_data")
        assert {r[0] for r in cur.fetchall()} == {"MB1", "1"}


def test_loads_after_the_migration_store_the_key_and_revise_in_place(
    database, tmp_path
):
    conn, db, dsn = database
    assert migrate(dsn).returncode == 0
    first = tmp_path / "first.jsonl"
    first.write_text(json.dumps(record("6166", "MB1", AUG_2025, 500.0)) + "\n")
    assert db.insert_eia_jsonl_data(str(first))[0]
    # EIA revises the month and spells the unit "1" this time: same row.
    second = tmp_path / "second.jsonl"
    second.write_text(json.dumps(record("6166", "1", AUG_2025, 580.0)) + "\n")
    assert db.insert_eia_jsonl_data(str(second))[0]
    assert stored(conn) == [("6166", AUG_2025, "1", "MB1", "6166|1", 580.0)]


def test_two_spellings_in_one_load_keep_the_later_row_and_the_rest_of_the_file(
    database, tmp_path
):
    conn, db, dsn = database
    assert migrate(dsn).returncode == 0
    batch = tmp_path / "batch.jsonl"
    batch.write_text(
        "\n".join(
            json.dumps(r)
            for r in [
                record("6166", "MB1", AUG_2025, 500.0),
                record("50392", "TG 3", AUG_2025, 90.0),
                record("6166", "1", AUG_2025, 580.0),
            ]
        )
        + "\n"
    )
    assert db.insert_eia_jsonl_data(str(batch))[0]
    assert stored(conn) == [
        ("50392", AUG_2025, "TG3", "TG 3", "50392|TG3", 90.0),
        ("6166", AUG_2025, "1", "1", "6166|1", 580.0),
    ]


def test_rollback_restores_eia_spelling(database):
    conn, _, dsn = database
    insert(conn, [record("6166", "MB1", AUG_2025), record("50392", "TG 3", JUL_2025)])
    assert migrate(dsn).returncode == 0
    result = run(dsn, "schema/migrations/rollback/023_eia_generator_key.sql")
    assert result.returncode == 0, result.stderr
    with conn.cursor() as cur:
        cur.execute(
            "SELECT plant_code, generator_id, eia_plant_unit_id FROM ingestion.eia_generation_data ORDER BY 1"
        )
        assert cur.fetchall() == [
            ("50392", "TG 3", "50392|TG 3"),
            ("6166", "MB1", "6166|MB1"),
        ]
        cur.execute(
            "SELECT to_regproc('public.eia_generator_key'), "
            "(SELECT count(*) FROM ingestion.schema_migrations WHERE version = '023')"
        )
        assert cur.fetchone() == (None, 0)
