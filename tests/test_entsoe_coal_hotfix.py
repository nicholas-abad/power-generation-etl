"""Real PostgreSQL regressions on the pre-expansion schema; never use Neon."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import threading

import pandas as pd
import psycopg2
from psycopg2.extensions import parse_dsn
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from database import PowerGenerationDatabase
from entsoe_coal import check_coal_records
import repair_entsoe_coal as repair

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "11111111-2222-3333-4444-555555555555"
TS = 1704067200000


@pytest.fixture
def pg():
    dsn = os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set COAL_TEST_PG_DSN to a disposable local database")
    parts = parse_dsn(dsn)
    host = parts.get("host", "")
    assert host in {"localhost", "127.0.0.1", "/private/tmp", "/tmp"}
    assert (
        parts.get("dbname", "").startswith("coal_hotfix_")
        or parts.get("dbname") == "power_generation_test"
    )
    conn = psycopg2.connect(dsn)
    with conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
            cur.execute((ROOT / "schema/entsoe_generation.sql").read_text())
            cur.execute("SET search_path=ingestion,public")
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname='etl_writer'")
            if not cur.fetchone():
                cur.execute("CREATE ROLE etl_writer")
            cur.execute(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA ingestion GRANT ALL ON TABLES TO etl_writer"
            )
    engine = create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=parts.get("user"),
            password=parts.get("password"),
            host=host,
            port=int(parts.get("port", 5432)),
            database=parts["dbname"],
        ),
        connect_args={"options": "-c search_path=ingestion,public"},
    )
    db = PowerGenerationDatabase.__new__(PowerGenerationDatabase)
    db._engine = engine
    yield conn, db
    engine.dispose()
    conn.close()


def record(name="ECHV_G1___", minutes=60, mw=100, **kw):
    return {
        "timestamp_ms": TS,
        "country_code": "CZ",
        "psr_type": "B02",
        "plant_name": name,
        "fuel_type": "Fossil Brown coal/Lignite",
        "data_type": "Actual Aggregated",
        "generation_mw": mw,
        "resolution_minutes": minutes,
        "extraction_run_id": RUN_ID,
        "created_at_ms": TS,
        **kw,
    }


def insert(conn, *rows):
    with conn:
        with conn.cursor() as cur:
            for row in rows:
                keys = list(row)
                cur.execute(
                    f"INSERT INTO ingestion.entsoe_generation_data ({','.join(keys)}) VALUES ({','.join(['%s'] * len(keys))})",
                    list(row.values()),
                )


def upsert(db, *rows, verified=False):
    df = pd.DataFrame(rows)
    df["_source_resolution_verified"] = verified
    return db._upsert_via_staging(
        df,
        "entsoe_generation_data",
        ["timestamp_ms", "country_code", "psr_type", "plant_name"],
        update_columns=[
            "generation_mw",
            "resolution_minutes",
            "extraction_run_id",
            "created_at_ms",
        ],
    )


def rows(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_jsonb(a) FROM ingestion.entsoe_generation_data a ORDER BY id"
            )
            return [r[0] for r in cur.fetchall()]


def test_alias_replay_cannot_undo_duration_or_insert_duplicate(pg):
    conn, db = pg
    insert(conn, record())
    before = rows(conn)
    with pytest.raises(ValueError, match="interval mismatch"):
        upsert(db, record("ECHV_G1____", minutes=15))
    assert rows(conn) == before
    assert upsert(db, record("ECHV_G1____")) == 0
    assert rows(conn) == before


def test_source_revision_preserves_existing_raw_name(pg):
    conn, db = pg
    insert(conn, record("ECHV_G1____"))
    assert upsert(db, record(mw=200), verified=True) == 1
    actual = rows(conn)
    assert len(actual) == 1 and actual[0]["plant_name"] == "ECHV_G1____"
    assert actual[0]["generation_mw"] == 200


def test_existing_overlap_fails_without_deleting_either_copy(pg):
    conn, db = pg
    insert(conn, record(), record("ECHV_G1____"))
    before = rows(conn)
    with pytest.raises(ValueError, match="overlap"):
        upsert(db, record(), verified=True)
    assert rows(conn) == before


def test_concurrent_alias_writers_produce_one_observation(pg):
    conn, db = pg
    start = threading.Barrier(2)

    def write(name):
        start.wait(timeout=5)
        return upsert(db, record(name), verified=True)

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(write, ["ECHV_G1___", "ECHV_G1____"]))
    assert sorted(results) == [0, 1]
    assert len(rows(conn)) == 1


def test_conflicting_aliases_in_one_batch_fail(pg):
    conn, db = pg
    with pytest.raises(ValueError, match="Conflicting"):
        upsert(db, record(), record("ECHV_G1____", mw=200), verified=True)
    assert rows(conn) == []


def test_other_country_and_fuel_are_unaffected(pg):
    conn, db = pg
    insert(conn, record(country_code="PL"), record(psr_type="B04"))
    assert (
        upsert(
            db,
            record(country_code="PL", minutes=15),
            record(psr_type="B04", minutes=15),
        )
        == 2
    )
    assert {r["resolution_minutes"] for r in rows(conn)} == {15}


def test_bad_identity_and_raw_input_conflicts_fail():
    with pytest.raises(ValueError, match="EIC"):
        check_coal_records([record(unit_eic="27W-GU-ECHVG2--8")])
    with pytest.raises(ValueError, match="Conflicting"):
        check_coal_records([record(), record(mw=99)])


def test_real_loader_preserves_literal_source_name_and_provenance(
    pg, tmp_path, monkeypatch
):
    conn, db = pg
    monkeypatch.setattr(db, "_get_date_range_for_run", lambda *a: (None, None))
    monkeypatch.setattr(db, "insert_extraction_metadata", lambda **k: True)
    insert(conn, record(minutes=15))
    path = tmp_path / "input.jsonl"
    source = record(
        "ECHV_G1____", resolution_source="xml_period", unit_eic="27W-GU-ECHVG1--C"
    )
    path.write_text(json.dumps(source) + "\n")
    assert db.insert_entsoe_jsonl_data(str(path))[0]
    assert rows(conn)[0]["resolution_minutes"] == 60
    literal = record(
        "Plant_Actual Consumption",
        resolution_source="xml_period",
        unit_eic="27W-NEW-UNIT---X",
    )
    path.write_text(json.dumps(literal) + "\n")
    assert db.insert_entsoe_jsonl_data(str(path))[0]
    assert any(r["plant_name"] == "Plant_Actual Consumption" for r in rows(conn))


def fixture_release(conn):
    insert(
        conn,
        record(minutes=15),
        record("EDET_G2___", minutes=15),
        record("EDET_G2____", minutes=15),
        record("Unrelated", minutes=30, country_code="PL"),
        record("Gas", minutes=15, psr_type="B04"),
        record("Next year", timestamp_ms=1735689600000),
    )
    manifest = {
        "repair_id": "fixture-coal-repair",
        "artifact_sha256": "fixture-pinned-input",
    }
    changes = [
        {
            "action": action,
            "timestamp_ms": TS,
            "psr_type": "B02",
            "plant_name": name,
            "generation_mw": "100",
            "before_minutes": 15,
            "after_minutes": after,
            "canonical_name": canonical,
        }
        for action, name, after, canonical in [
            ("interval", "ECHV_G1___", 60, "ECHV_G1___"),
            ("duplicate", "EDET_G2____", 15, "EDET_G2___"),
        ]
    ]
    return manifest, changes


def test_plan_apply_repeat_backup_permissions_and_rollback(pg):
    conn, _ = pg
    manifest, changes = fixture_release(conn)
    before = rows(conn)
    plan = repair.run(conn, manifest, changes, "local")
    assert plan["rows_written"] == 0 and Decimal(plan["mwh_change"]) == 50
    assert rows(conn) == before
    result = repair.run(conn, manifest, changes, "local", "apply", plan["plan_sha256"])
    assert result["rows_written"] == 2
    after = rows(conn)
    assert [
        r for r in after if r["plant_name"] in {"Unrelated", "Gas", "Next year"}
    ] == [r for r in before if r["plant_name"] in {"Unrelated", "Gas", "Next year"}]
    plan = repair.run(conn, manifest, changes, "local")
    assert (
        repair.run(conn, manifest, changes, "local", "apply", plan["plan_sha256"])[
            "rows_written"
        ]
        == 0
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ingestion.entsoe_coal_repair_backup")
            assert cur.fetchone()[0] == 2
            cur.execute(
                "SELECT has_table_privilege('etl_writer','ingestion.entsoe_coal_repair_backup','INSERT,UPDATE,DELETE,TRUNCATE')"
            )
            assert cur.fetchone()[0] is False
    assert (
        repair.run(conn, manifest, changes, "local", "rollback", plan["plan_sha256"])[
            "rows_written"
        ]
        == 2
    )
    assert rows(conn) == before
    plan = repair.run(conn, manifest, changes, "local")
    assert (
        repair.run(conn, manifest, changes, "local", "rollback", plan["plan_sha256"])[
            "rows_written"
        ]
        == 0
    )


def test_changed_plan_and_changed_source_abort_without_writes(pg):
    conn, _ = pg
    manifest, changes = fixture_release(conn)
    plan = repair.run(conn, manifest, changes, "local")
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion.entsoe_generation_data SET created_at_ms=created_at_ms+1 WHERE plant_name='ECHV_G1___'"
            )
    before = rows(conn)
    with pytest.raises(ValueError, match="fingerprint"):
        repair.run(conn, manifest, changes, "local", "apply", plan["plan_sha256"])
    assert rows(conn) == before
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion.entsoe_generation_data SET generation_mw=101 WHERE plant_name='ECHV_G1___'"
            )
    with pytest.raises(ValueError, match="MW"):
        repair.run(conn, manifest, changes, "local")


def test_later_revision_blocks_rollback(pg):
    conn, _ = pg
    manifest, changes = fixture_release(conn)
    plan = repair.run(conn, manifest, changes, "local")
    repair.run(conn, manifest, changes, "local", "apply", plan["plan_sha256"])
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion.entsoe_generation_data SET created_at_ms=created_at_ms+1 WHERE plant_name='EDET_G2___'"
            )
    before = rows(conn)
    plan = repair.run(conn, manifest, changes, "local")
    with pytest.raises(ValueError, match="Post-repair"):
        repair.run(conn, manifest, changes, "local", "rollback", plan["plan_sha256"])
    assert rows(conn) == before


def test_pinned_artifact_has_only_audited_coal_changes():
    manifest, changes = repair.load_release()
    assert len(changes) == 104220
    assert manifest["interval_mwh_increase"] == "6564553.500"
    assert sum(r["timestamp_ms"] == 1704063600000 for r in changes) == 24


def test_production_refused_before_connecting(monkeypatch, tmp_path):
    monkeypatch.setattr(
        repair.psycopg2,
        "connect",
        lambda *a, **k: pytest.fail("Connected to production"),
    )
    assert (
        repair.main(
            ["--environment", "production", "--report", str(tmp_path / "report.json")]
        )
        == 1
    )


def test_failure_after_updates_rolls_back_data_and_backup(pg):
    conn, _ = pg
    manifest, changes = fixture_release(conn)
    before = rows(conn)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE FUNCTION ingestion.reject_delete() RETURNS trigger LANGUAGE plpgsql AS
                $$ BEGIN RAISE EXCEPTION 'simulated delete failure'; END $$""")
            cur.execute("""CREATE TRIGGER reject_delete BEFORE DELETE ON ingestion.entsoe_generation_data
                FOR EACH ROW EXECUTE FUNCTION ingestion.reject_delete()""")
    plan = repair.run(conn, manifest, changes, "local")
    with pytest.raises(psycopg2.Error, match="simulated delete failure"):
        repair.run(conn, manifest, changes, "local", "apply", plan["plan_sha256"])
    assert rows(conn) == before
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('ingestion.entsoe_coal_repair_backup')")
            assert cur.fetchone()[0] is None


def test_modified_artifact_is_rejected(monkeypatch, tmp_path):
    manifest = json.loads(repair.MANIFEST.read_text())
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    (tmp_path / manifest["artifact"]).write_bytes(b"changed payload")
    monkeypatch.setattr(repair, "MANIFEST", path)
    with pytest.raises(ValueError, match="artifact hash mismatch"):
        repair.load_release()


@pytest.mark.parametrize(
    "change", [{"plant_name": "Unreviewed name"}, {"psr_type": "B05"}]
)
def test_known_eic_name_or_fuel_restatement_requires_review(change):
    with pytest.raises(ValueError, match="changed name or fuel"):
        check_coal_records(
            [
                record(
                    unit_eic="27W-GU-ECHVG1--C",
                    resolution_source="xml_period",
                    **change,
                )
            ]
        )


def test_repair_rejects_extra_alias_copy(pg):
    conn, _ = pg
    manifest, changes = fixture_release(conn)
    insert(conn, record("ECHV_G1____", minutes=15))
    before = rows(conn)
    with pytest.raises(ValueError, match="Unreviewed alias copy"):
        repair.run(conn, manifest, changes, "local")
    assert rows(conn) == before
