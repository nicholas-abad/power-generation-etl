"""Remove rename duplicates, key ONS rows on the plant ID, and roll back.

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

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import repair_ons_rename_duplicates as repair  # noqa: E402
from database import PowerGenerationDatabase  # noqa: E402

HOUR = 3_600_000
MAY_1 = 1777593600000  # 2026-05-01T00:00 in the loader's encoding
JAN_1 = 1767225600000


@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv("ONS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("ONS_TEST_PG_DSN is required for local PostgreSQL integration")
    params = parse_dsn(dsn)
    host = params.get("host", "")
    assert host in {"localhost", "127.0.0.1", "::1"} or host.startswith("/")
    assert shutil.which("psql"), "psql is required to test the actual migration"
    name = f"ons_rename_{uuid.uuid4().hex}"
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
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
        # The state before migration 020: only the name-based natural key.
        cur.execute("DROP INDEX ingestion.uq_ons_plant_hour")
        cur.execute(
            "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY, notes text)"
        )
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


def record(plant_id, plant, timestamp_ms, mwh=10.0, **overrides):
    return {
        "extraction_run_id": str(uuid.uuid4()),
        "created_at_ms": 1790985600000,
        "timestamp_ms": timestamp_ms,
        "plant": plant,
        "ons_plant_id": plant_id,
        "plant_type": "TÉRMICA",
        "fuel_type": "Carvão",
        "subsystem_id": "S",
        "subsystem": "SUL",
        "state": "RS",
        "state_name": "RIO GRANDE DO SUL",
        "operation_mode": "TIPO I",
        "ceg": "UTE.RS.000001",
        "generation_mwh": mwh,
        "resolution_minutes": 60,
        **overrides,
    }


def load(db, tmp_path, rows):
    path = tmp_path / f"{uuid.uuid4().hex}_etl.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    ok, _ = db.insert_ons_jsonl_data(str(path))
    assert ok


def migrate(dsn):
    return subprocess.run(
        ["psql", "-X", dsn, "-v", "ON_ERROR_STOP=1", "-f",
         str(ROOT / "schema/migrations/020_ons_plant_hour_key.sql")],
        capture_output=True,
        text=True,
    )


def transaction(conn, action):
    """Run action(cursor) in one committed transaction, as the command does."""
    conn.autocommit = False
    try:
        with conn, conn.cursor() as cur:
            return action(cur)
    finally:
        conn.autocommit = True


def rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ons_plant_id, plant, timestamp_ms, generation_mwh "
            "FROM ingestion.ons_generation_data ORDER BY 1, 3, 2"
        )
        return cur.fetchall()


@pytest.fixture
def renamed(database, tmp_path, monkeypatch):
    """Production's state: old and new names for the same plant-hours."""
    conn, db, dsn = database
    # The pre-020 loader: name-based conflict target.
    monkeypatch.setattr(
        "database.ONS_PLANT_HOUR", "timestamp_ms, plant, COALESCE(ons_plant_id, '')"
    )
    load(db, tmp_path, [
        record("RSUTEC", "Candiota III", MAY_1, 200.0),
        record("RSUTEC", "Candiota III", MAY_1 + HOUR, 201.0),
        record("BAUTCA", "Erb Candeias", JAN_1, 5.0),
        record(None, "PQU PA TERM", MAY_1, 1.0, operation_mode="Pequenas Usinas (Tipo III)"),
    ])
    load(db, tmp_path, [
        record("RSUTEC", "CANDIOTA III", MAY_1, 200.0),
        record("RSUTEC", "CANDIOTA III", MAY_1 + HOUR, 201.5),
        record("BAUTCA", "ERB Candeias", JAN_1, 5.1),
        record("SCUTLA", "U.JLACERDA A", MAY_1, 90.0),
    ])
    monkeypatch.undo()
    monkeypatch.setenv("POSTGRES_SSLMODE", "disable")
    source = {
        (MAY_1, "RSUTEC"): "CANDIOTA III",
        (MAY_1 + HOUR, "RSUTEC"): "CANDIOTA III",
        (JAN_1, "BAUTCA"): "ERB Candeias",
        (MAY_1, "SCUTLA"): "U.JLACERDA A",
    }
    monkeypatch.setattr(
        repair, "source_names", lambda _: (source, {"2026-01", "2026-05", "2026-06"})
    )
    return conn, db, dsn, source


def test_plan_apply_migrate_load_and_rollback(renamed, tmp_path):
    conn, db, dsn, _ = renamed
    assert len(rows(conn)) == 8
    assert "duplicate ONS plant-hours" in migrate(dsn).stderr  # refuses first

    with conn.cursor() as cur:
        planned = repair.plan(cur, None)
    assert (planned["duplicate_plant_hours"], planned["rows_to_delete"]) == (3, 3)
    assert {r["plant"] for r in planned["deletions"]} == {"Candiota III", "Erb Candeias"}
    assert planned["summary"]["2026-05 Carvão"] == {"rows": 2, "mwh": 401.0}

    with pytest.raises(ValueError, match="fingerprint"):
        transaction(conn, lambda cur: repair.apply(cur, planned, "0" * 64))
    applied = transaction(conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"]))
    assert applied["deleted"] == 3
    after = rows(conn)
    assert ("RSUTEC", "CANDIOTA III", MAY_1, 200.0) in after
    assert not any(r[1] in {"Candiota III", "Erb Candeias"} for r in after)
    assert (None, "PQU PA TERM", MAY_1, 1.0) in after  # ID-less rows untouched

    result = migrate(dsn)
    assert result.returncode == 0, result.stderr
    assert migrate(dsn).returncode == 0  # re-runnable

    # A later rename now updates the row in place instead of adding one.
    load(db, tmp_path, [record("RSUTEC", "UTE CANDIOTA III", MAY_1, 199.0)])
    assert [r for r in rows(conn) if r[0] == "RSUTEC" and r[2] == MAY_1] == [
        ("RSUTEC", "UTE CANDIOTA III", MAY_1, 199.0)
    ]
    load(db, tmp_path, [record(None, "PQU PA TERM", MAY_1, 1.5, operation_mode="x")])
    assert [r for r in rows(conn) if r[1] == "PQU PA TERM"] == [(None, "PQU PA TERM", MAY_1, 1.5)]

    with pytest.raises(ValueError, match="Drop migration 020"):
        transaction(conn, lambda cur: repair.rollback(cur, planned["fingerprint"]))
    with conn.cursor() as cur:
        cur.execute("DROP INDEX ingestion.uq_ons_plant_hour")
    restored = transaction(conn, lambda cur: repair.rollback(cur, planned["fingerprint"]))
    assert restored["restored"] == 3
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM ingestion.ons_rename_duplicate_backup")
        assert cur.fetchone() == (0,)
    restored = rows(conn)
    assert ("RSUTEC", "Candiota III", MAY_1, 200.0) in restored
    assert len(restored) == 8


def test_apply_refuses_rows_changed_after_the_plan(renamed):
    conn, _, _, _ = renamed
    with conn.cursor() as cur:
        planned = repair.plan(cur, None)
        cur.execute(
            "UPDATE ingestion.ons_generation_data SET generation_mwh = 1 WHERE plant = 'Candiota III'"
        )
    with pytest.raises(ValueError, match="Rows changed since the plan"):
        transaction(conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"]))
    assert len(rows(conn)) == 8  # nothing deleted


@pytest.mark.parametrize(
    "change, message",
    [
        ({(MAY_1, "RSUTEC"): "SOMETHING ELSE"}, "0 copies match"),
        ({(JAN_1, "BAUTCA"): None}, "0 copies match"),
    ],
)
def test_plan_refuses_ambiguous_or_unaudited_duplicates(renamed, monkeypatch, change, message):
    conn, _, _, source = renamed
    monkeypatch.setattr(
        repair, "source_names", lambda _: ({**source, **change}, {"2026-01", "2026-05", "2026-06"})
    )
    with conn.cursor() as cur:
        with pytest.raises(ValueError, match=message):
            repair.plan(cur, None)
    monkeypatch.setattr(repair, "source_names", lambda _: (source, {"2026-05"}))
    with conn.cursor() as cur:
        with pytest.raises(ValueError, match="outside the audit"):
            repair.plan(cur, None)
