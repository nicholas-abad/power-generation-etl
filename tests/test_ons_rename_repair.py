"""Remove republication leftovers, key ONS rows on the plant ID, and roll back.

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

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn
from psycopg2.extras import execute_values
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import repair_ons_rename_duplicates as repair  # noqa: E402
from database import PowerGenerationDatabase  # noqa: E402

HOUR = 3_600_000
MAY_1 = 1777593600000  # 2026-05-01T00:00 in the loader's encoding
JAN_1 = 1767225600000
MONTHS = {f"2026-{m:02d}" for m in range(1, 9)}


def test_source_index_matches_the_loader_encoding():
    names, keys, missing = {}, set(), set()
    repair.index_source(
        pd.DataFrame(
            {
                "din_instante": pd.to_datetime(
                    ["2026-05-01 00:00", "2026-05-01 00:00", "2026-05-01 01:00"]
                ),
                "id_ons": ["RSUTEC", None, "GOUCDR"],
                "nom_usina": ["CANDIOTA III", "PQU PA TERM", "UTE CODORA"],
                "val_geracao": [200.0, 1.0, None],
            }
        ),
        names,
        keys,
        missing,
    )
    # Null-valued hours still carry the plant's current name.
    assert names == {
        (MAY_1, "RSUTEC"): "CANDIOTA III",
        (MAY_1 + HOUR, "GOUCDR"): "UTE CODORA",
    }
    assert keys == {(MAY_1, "RSUTEC"), (MAY_1, "name:PQU PA TERM")}
    assert missing == {(MAY_1 + HOUR, "GOUCDR")}
    with pytest.raises(ValueError, match="twice"):
        repair.index_source(
            pd.DataFrame(
                {
                    "din_instante": pd.to_datetime(["2026-05-01 00:00"]),
                    "id_ons": ["RSUTEC"],
                    "nom_usina": ["Candiota III"],
                    "val_geracao": [1.0],
                }
            ),
            names,
            keys,
            missing,
        )


def test_production_needs_the_flag_and_the_pinned_owner(monkeypatch):
    config = json.loads((ROOT / "config/environments.json").read_text())
    target = config["environments"]["staging"]
    with pytest.raises(ValueError, match="allow-production"):
        repair.pinned_dsn("production", False)
    with pytest.raises(ValueError, match="run_in_environment"):
        repair.pinned_dsn(None, True)
    monkeypatch.setenv("NEON_ENDPOINT_ID", target["endpoint_id"])
    monkeypatch.setenv("NEON_BRANCH_ID", target["branch_id"])
    host = f"{target['endpoint_id']}.{config['proxy_host']}"
    for user, ok in (
        (config["roles"]["owner"], True),
        (config["roles"]["writer"], False),
    ):
        monkeypatch.setenv(
            "DATABASE_URL",
            f"postgresql://{user}:x@{host}:5432/{config['database']}?sslmode=require",
        )
        if ok:
            assert repair.pinned_dsn("staging", False)
        else:
            with pytest.raises(ValueError, match="owner"):
                repair.pinned_dsn("staging", False)
    # Staging credentials presented as production are refused before connecting.
    with pytest.raises(ValueError, match="pinned"):
        repair.pinned_dsn("production", True)


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


def insert(conn, records):
    columns = list(records[0])
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO ingestion.ons_generation_data ({', '.join(columns)}) VALUES %s",
            [[r[c] for c in columns] for r in records],
        )


def load(db, tmp_path, records):
    path = tmp_path / f"{uuid.uuid4().hex}_etl.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return db.insert_ons_jsonl_data(str(path))[0]


def migrate(dsn):
    return subprocess.run(
        [
            "psql",
            "-X",
            dsn,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(ROOT / "schema/migrations/020_ons_plant_hour_key.sql"),
        ],
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
def renamed(database, monkeypatch):
    """Production's state after republished files: renames, withdrawals, nulls."""
    conn, db, dsn = database
    aggregate = {"operation_mode": "Pequenas Usinas (Tipo III)"}
    insert(
        conn,
        [
            record("RSUTEC", "Candiota III", MAY_1, 200.0),
            record("RSUTEC", "Candiota III", MAY_1 + HOUR, 201.0),
            record("BAUTCA", "Erb Candeias", JAN_1, 5.0),
            record(None, "PQU PA TERM", MAY_1, 1.0, **aggregate),
            # The same aggregate under its withdrawn identity (ID and longer name).
            record("PQU_PA_T", "PQU PA TERM - CARGA GLOBAL", MAY_1, 1.0, **aggregate),
            record("RSUTEC", "CANDIOTA III", MAY_1, 200.0),
            record("RSUTEC", "CANDIOTA III", MAY_1 + HOUR, 201.5),
            record("BAUTCA", "ERB Candeias", JAN_1, 5.1),
            record("SCUTLA", "U.JLACERDA A", MAY_1, 90.0),
            # A zero from an earlier file version; ONS now publishes this hour as null.
            record("GOUCDR", "Codora", MAY_1, 0.0, fuel_type="Biomassa"),
        ],
    )
    source = {
        (MAY_1, "RSUTEC"): "CANDIOTA III",
        (MAY_1 + HOUR, "RSUTEC"): "CANDIOTA III",
        (JAN_1, "BAUTCA"): "ERB Candeias",
        (MAY_1, "SCUTLA"): "U.JLACERDA A",
    }
    keys = {repair.plant_hour(t, i, n) for (t, i), n in source.items()}
    keys.add(repair.plant_hour(MAY_1, None, "PQU PA TERM"))
    missing = {repair.plant_hour(MAY_1, "GOUCDR", "UTE CODORA")}
    monkeypatch.setattr(
        repair, "source_names", lambda _: (source, keys, missing, MONTHS)
    )
    return conn, db, dsn, source, keys, missing


def test_plan_apply_migrate_load_and_rollback(renamed, tmp_path):
    conn, db, dsn, _, _, _ = renamed
    assert len(rows(conn)) == 10
    assert not load(db, tmp_path, [record("RSUTEC", "X", MAY_1)])  # needs 020 first
    assert "duplicate ONS plant-hours" in migrate(dsn).stderr  # refuses first

    with conn.cursor() as cur:
        planned = repair.plan(cur, None)
    assert (planned["duplicate_plant_hours"], planned["duplicate_rows"]) == (3, 3)
    assert (planned["withdrawn_rows"], planned["now_missing_rows"]) == (1, 1)
    assert planned["rows_to_delete"] == 5
    assert {r["plant"] for r in planned["deletions"]} == {
        "Candiota III",
        "Erb Candeias",
        "PQU PA TERM - CARGA GLOBAL",
        "Codora",
    }
    assert planned["summary"]["duplicate 2026-05 Carvão"] == {"rows": 2, "mwh": 401.0}

    with pytest.raises(ValueError, match="fingerprint"):
        transaction(conn, lambda cur: repair.apply(cur, planned, "0" * 64))
    applied = transaction(
        conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"])
    )
    assert applied["deleted"] == 5
    after = rows(conn)
    assert ("RSUTEC", "CANDIOTA III", MAY_1, 200.0) in after
    assert (None, "PQU PA TERM", MAY_1, 1.0) in after
    assert len(after) == 5

    result = migrate(dsn)
    assert result.returncode == 0, result.stderr
    assert migrate(dsn).returncode == 0  # re-runnable

    # A later rename now updates the row in place instead of adding one.
    assert load(db, tmp_path, [record("RSUTEC", "UTE CANDIOTA III", MAY_1, 199.0)])
    assert [r for r in rows(conn) if r[0] == "RSUTEC" and r[2] == MAY_1] == [
        ("RSUTEC", "UTE CANDIOTA III", MAY_1, 199.0)
    ]
    # Two source rows for one plant-hour are refused, not silently collapsed.
    clash = MAY_1 + 2 * HOUR
    assert not load(
        db, tmp_path, [record("RSUTEC", "A", clash), record("RSUTEC", "B", clash)]
    )
    assert not [r for r in rows(conn) if r[2] == clash]

    with pytest.raises(ValueError, match="Drop migration 020"):
        transaction(conn, lambda cur: repair.rollback(cur, planned["fingerprint"]))
    with conn.cursor() as cur:
        cur.execute("DROP INDEX ingestion.uq_ons_plant_hour")
        # Undo the rename loaded after the repair, as the documented order does.
        cur.execute(
            "UPDATE ingestion.ons_generation_data SET plant = 'CANDIOTA III', "
            "generation_mwh = 200 WHERE plant = 'UTE CANDIOTA III'"
        )
    restored = transaction(
        conn, lambda cur: repair.rollback(cur, planned["fingerprint"])
    )
    assert restored["restored"] == 5
    assert len(rows(conn)) == 10
    with conn.cursor() as cur:
        cur.execute("SELECT state FROM ingestion.ons_republication_repairs")
        assert cur.fetchall() == [("rolled_back",)]
        cur.execute(
            "SELECT count(*), count(rolled_back_at) "
            "FROM ingestion.ons_rename_duplicate_backup"
        )
        assert cur.fetchone() == (5, 5)  # the archive is kept
    with pytest.raises(ValueError, match="not an applied repair"):
        transaction(conn, lambda cur: repair.rollback(cur, planned["fingerprint"]))


def test_rollback_refuses_rows_that_would_collide(renamed):
    conn, _, _, _, _, _ = renamed
    with conn.cursor() as cur:
        planned = repair.plan(cur, None)
    transaction(conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"]))
    insert(conn, [record("GOUCDR", "Codora", MAY_1, 0.0, fuel_type="Biomassa")])
    with pytest.raises(ValueError, match="collide"):
        transaction(conn, lambda cur: repair.rollback(cur, planned["fingerprint"]))


def test_archive_is_owner_only(renamed):
    conn, _, _, _, _, _ = renamed
    with conn.cursor() as cur:
        for role in ("etl_writer", "dashboard_ro"):
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
        cur.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ingestion GRANT ALL ON TABLES TO etl_writer"
        )
        planned = repair.plan(cur, None)
    transaction(conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"]))
    with conn.cursor() as cur:
        for role in ("etl_writer", "dashboard_ro"):
            cur.execute(
                "SELECT has_table_privilege(%s, "
                "'ingestion.ons_rename_duplicate_backup', 'SELECT')",
                (role,),
            )
            assert cur.fetchone() == (False,)
        cur.execute(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_name = 'ons_rename_duplicate_backup' AND column_name = 'id'"
        )
        assert cur.fetchone() == (None,)


def kept_row(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(repair.COLUMNS)} FROM ingestion.ons_generation_data "
            "WHERE plant = 'CANDIOTA III' AND timestamp_ms = %s",
            (MAY_1,),
        )
        return repair.as_record(cur.fetchone())


@pytest.mark.parametrize(
    "tamper, message",
    [
        ("changed_row", "Rows changed"),
        ("manifest", "audited manifest"),
        ("outside", "outside the audited months"),
        ("all_copies", "lost every copy"),
    ],
)
def test_apply_refuses_unsafe_plans(renamed, tamper, message):
    conn, _, _, _, _, _ = renamed
    with conn.cursor() as cur:
        planned = repair.plan(cur, None)
    if tamper == "changed_row":
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion.ons_generation_data SET generation_mwh = 1 "
                "WHERE plant = 'Candiota III'"
            )
    elif tamper == "manifest":
        planned["manifest_sha256"] = "0" * 64
    else:
        if tamper == "outside":
            planned["deletions"][0]["timestamp_ms"] = 1735689600000  # 2025-01
        else:
            kept = kept_row(conn)
            planned["deletions"].append(kept)
            planned["reasons"][str(kept["id"])] = "duplicate"
        planned["fingerprint"] = repair.fingerprint_of(
            planned["manifest_sha256"], planned["deletions"]
        )
    with pytest.raises(ValueError, match=message):
        transaction(
            conn, lambda cur: repair.apply(cur, planned, planned["fingerprint"])
        )
    assert len(rows(conn)) == 10  # nothing deleted


def test_plan_refuses_ambiguous_unaudited_or_mass_stale(renamed, monkeypatch):
    conn, _, _, source, keys, missing = renamed

    def plan_with(names, known, months):
        monkeypatch.setattr(
            repair, "source_names", lambda _: (names, known, missing, months)
        )
        with conn.cursor() as cur:
            return repair.plan(cur, None)

    with pytest.raises(ValueError, match="0 copies match"):
        plan_with({**source, (MAY_1, "RSUTEC"): "SOMETHING ELSE"}, keys, MONTHS)
    with pytest.raises(ValueError, match="outside the audit"):
        plan_with(source, keys, {"2026-05"})
    # Source keys that do not line up with stored rows (e.g. a shifted timestamp).
    with pytest.raises(ValueError, match="do not line up"):
        plan_with(source, {(t + 1, k) for t, k in keys}, MONTHS)
