"""The certified coal rebuild on a disposable local PostgreSQL (never Neon)."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import rebuild_entsoe_coal as rebuild  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402

RUN = "11111111-2222-3333-4444-555555555555"
MARCH, APRIL, MAY = 1709251200000, 1711929600000, 1714521600000
HOUR = 3600000


@pytest.fixture
def pg():
    dsn = os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set COAL_TEST_PG_DSN to a disposable local database")
    with patch.dict(os.environ):
        parts = local_settings(
            dsn, r"(?:coal_hotfix_tests_[a-z0-9_]+|power_generation_test)"
        )
        conn = psycopg2.connect(**parts, connect_timeout=5)
        try:
            with conn:
                verify_local_connection(conn, parts["dbname"])
                with conn.cursor() as cur:
                    cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
                    cur.execute((ROOT / "schema/entsoe_generation.sql").read_text())
                    cur.execute(
                        "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY)"
                    )
                    cur.execute(
                        "INSERT INTO ingestion.schema_migrations VALUES ('018')"
                    )
                    cur.execute("SET search_path=ingestion,public")
            yield conn
        finally:
            conn.close()


def stored(
    ts, name="Belchatow 2", mw=100.0, minutes=15, country="PL", psr="B02", eic=None
):
    return dict(
        timestamp_ms=ts,
        country_code=country,
        psr_type=psr,
        plant_name=name,
        fuel_type="Fossil Brown coal/Lignite",
        data_type="Unknown",
        generation_mw=mw,
        resolution_minutes=minutes,
        unit_eic=eic,
        production_unit_eic=None,
        source_unit_name=None,
        extraction_run_id=RUN,
        created_at_ms=1,
    )


def insert(conn, *rows):
    with conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"INSERT INTO ingestion.entsoe_generation_data ({','.join(row)})"
                f" VALUES ({','.join(['%s'] * len(row))})",
                list(row.values()),
            )


def table(conn):
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, to_jsonb(t) - 'id' FROM ingestion.entsoe_generation_data t"
            " ORDER BY timestamp_ms, country_code, psr_type, plant_name"
        )
        return cur.fetchall()


def line(ts, mw="100", name="Belchatow 2", eic="19W000000000095U", psr="B02"):
    return "\t".join(
        map(
            str,
            [
                ts,
                "PL",
                psr,
                name,
                "Fossil Brown coal/Lignite",
                "Actual Aggregated",
                mw,
                60,
                eic,
                "\\N",
                name,
                "99999999-8888-7777-6666-555555555555",
                2,
            ],
        )
    )


def payload(
    tmp_path, months, end="2024-05-01", psrs=("B02",), repair_id="test-rebuild"
):
    root = tmp_path / "payload"
    root.mkdir(exist_ok=True)
    entries = []
    for month, lines in months.items():
        lines = sorted(lines, key=lambda x: (int(x.split("\t")[0]), x.split("\t")[3]))
        data = (
            "\t".join(rebuild.COLUMNS) + "\n" + "".join(x + "\n" for x in lines)
        ).encode()
        path = root / f"{month}.tsv.gz"
        path.write_bytes(gzip.compress(data, mtime=0))
        mwh = sum(int(x.split("\t")[6]) * 60 / 60 for x in lines)
        entries.append(
            {
                "month": month,
                "file": path.name,
                "rows": len(lines),
                "mwh": str(int(mwh)),
                "excluded": [],
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    manifest = {
        "repair_id": repair_id,
        "country": "PL",
        "psrs": list(psrs),
        "start": "2024-03-01",
        "end": end,
        "columns": list(rebuild.COLUMNS),
        "inputs": {"test": True},
        "months": entries,
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


def prepared(tmp_path, conn, months, **kw):
    manifest, sha, windows = rebuild.load_payload(payload(tmp_path, months, **kw))
    report = rebuild.plan(conn, manifest, sha, windows, "local")
    return manifest, sha, windows, report


MONTHS = {
    "2024-03": [line(MARCH), line(MARCH + HOUR, "200")],
    "2024-04": [line(APRIL, "50")],
}


def test_apply_replaces_window_exactly_and_repeats_as_a_noop(pg, tmp_path):
    insert(
        pg,
        stored(MARCH),
        stored(MARCH + HOUR, mw=200.0),
        stored(APRIL, mw=50.0),
        stored(MAY + HOUR, mw=7.0),  # after the certified end: the tail window
        stored(MARCH, country="CZ"),
        stored(MARCH, psr="B04", name="Gas"),
    )
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert [w["stored_rows"] for w in report["windows"]] == [2, 1, 1]
    assert report["blocking"] == {
        "stored_rows_in_other_coal_psrs": {},
        "unit_time_collisions": [],
    }
    results = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["action"] for r in results] == ["applied"] * 3
    rows = [r for _, r in table(pg)]
    pl_coal = [r for r in rows if r["country_code"] == "PL" and r["psr_type"] == "B02"]
    assert [
        (r["generation_mw"], r["resolution_minutes"], r["unit_eic"]) for r in pl_coal
    ] == [
        (100, 60, "19W000000000095U"),
        (200, 60, "19W000000000095U"),
        (50, 60, "19W000000000095U"),
    ]
    assert {(r["country_code"], r["psr_type"]) for r in rows} == {
        ("PL", "B02"),
        ("CZ", "B02"),
        ("PL", "B04"),
    }
    before = table(pg)
    again = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["written"] for r in again] == [0, 0, 0] and table(pg) == before


def test_changed_stored_rows_after_plan_abort_without_writes(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    insert(pg, stored(APRIL, name="Late arrival"))
    before = table(pg)
    with pytest.raises(ValueError, match="changed since the plan"):
        rebuild.apply(pg, manifest, sha, windows, report)
    assert [r for r in table(pg) if r[1]["country_code"] == "PL"] == [
        r for r in before if r[1]["country_code"] == "PL"
    ]


def test_tampered_payload_file_is_rejected(tmp_path):
    root = payload(tmp_path, MONTHS)
    path = root / "2024-03.tsv.gz"
    path.write_bytes(
        gzip.compress(
            gzip.decompress(path.read_bytes()).replace(b"\t200\t", b"\t201\t")
        )
    )
    with pytest.raises(ValueError, match="changed"):
        rebuild.load_payload(root)


def test_payload_must_cover_every_month_of_the_window(tmp_path):
    with pytest.raises(ValueError, match="do not cover"):
        rebuild.load_payload(payload(tmp_path, {"2024-03": [line(MARCH)]}))


def test_rollback_restores_original_rows_and_ids(pg, tmp_path):
    insert(
        pg,
        stored(MARCH),
        stored(APRIL, name="Legacy Name", mw=12.5),
        stored(MAY + HOUR),
    )
    original = table(pg)
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    assert table(pg) != original
    results = rebuild.rollback(pg, manifest, windows)
    assert [r["action"] for r in results] == ["rolled back"] * 3
    assert table(pg) == original
    with pytest.raises(ValueError, match="rolled back; use a new repair id"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_rollback_after_a_later_write_is_refused(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    with pg, pg.cursor() as cur:
        cur.execute(
            "UPDATE ingestion.entsoe_generation_data SET generation_mw=1"
            " WHERE timestamp_ms=%s AND country_code='PL'",
            (APRIL,),
        )
    before = table(pg)
    with pytest.raises(ValueError, match="later writes; rollback refused"):
        rebuild.rollback(pg, manifest, windows)
    assert table(pg) == before


def test_resume_is_bound_to_one_payload(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report, only={"2024-03"})
    rest = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["action"] for r in rest] == ["already applied", "applied", "applied"]
    changed = {
        "2024-03": [line(MARCH, "101"), line(MARCH + HOUR, "200")],
        "2024-04": MONTHS["2024-04"],
    }
    other = tmp_path / "other"
    other.mkdir()
    m2, sha2, w2, r2 = prepared(other, pg, changed)
    with pytest.raises(ValueError, match="bound to another manifest or plan"):
        rebuild.apply(pg, m2, sha2, w2, r2)


def test_unit_time_collision_with_a_retained_row_blocks(pg, tmp_path):
    insert(pg, stored(MARCH, psr="B04", name="Gas", eic="19W000000000095U"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["unit_time_collisions"] == [
        {"month": "2024-03", "rows": 1}
    ]
    with pytest.raises(ValueError, match="blocking findings"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_stored_coal_in_an_uncertified_psr_blocks(pg, tmp_path):
    insert(pg, stored(MARCH, psr="B05", name="Hard coal unit"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["stored_rows_in_other_coal_psrs"] == {"B05": 1}
    with pytest.raises(ValueError, match="blocking findings"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_missing_migration_018_fails_preflight(pg, tmp_path):
    with pg, pg.cursor() as cur:
        cur.execute("DELETE FROM ingestion.schema_migrations")
    manifest, sha, windows = rebuild.load_payload(payload(tmp_path, MONTHS))
    with pytest.raises(ValueError, match="018"):
        rebuild.plan(pg, manifest, sha, windows, "local")


def test_production_is_refused_before_connecting():
    with pytest.raises(ValueError, match="--allow-production"):
        rebuild.checked_dsn("production", False)
