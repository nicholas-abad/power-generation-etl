"""Remove a unit stored under two names, refuse on any drift, and roll back.

Needs ONS_TEST_PG_DSN or COAL_TEST_PG_DSN (a local disposable PostgreSQL
server, see tests/local_db.py); each test uses a fresh database.
"""

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import repair_entsoe_source_duplicates as repair  # noqa: E402
from tests.local_db import database  # noqa: F401,E402

HOUR = 3_600_000
START = 1735686000000  # 2024-12-31T23:00Z
KEEP, REMOVE = "EPC2_B21__", "EPC2_B21___"
EIC = "27W-GU-EPC2B21-L"
TABLE = repair.TABLE


def record(plant_name, hour, mw=100.0, **changes):
    return {
        "extraction_run_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "created_at_ms": 1790985600000,
        "timestamp_ms": START + hour * HOUR,
        "country_code": "CZ",
        "plant_name": plant_name,
        "psr_type": "B04",
        "fuel_type": "Fossil Gas",
        # The two copies came from different extractor versions.
        "data_type": "Fossil Gas" if plant_name == KEEP else "Actual Aggregated",
        "generation_mw": mw,
        "resolution_minutes": 60,
        **changes,
    }


def insert(connection, rows):
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(
                f"INSERT INTO {TABLE} ({','.join(row)}) "
                f"VALUES ({','.join(['%s'] * len(row))})",
                tuple(row.values()),
            )


def stored(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT to_jsonb(a) FROM {TABLE} a ORDER BY id")
        return [row for (row,) in cursor.fetchall()]


def names(connection):
    return sorted(
        (r["country_code"], r["plant_name"], (r["timestamp_ms"] - START) // HOUR)
        for r in stored(connection)
    )


def scalar(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


@pytest.fixture
def duplicated(database, tmp_path):  # noqa: F811
    """Three hours stored under both names, inside a three-hour manifest range."""
    connection, _, _ = database
    with connection.cursor() as cursor:
        cursor.execute((ROOT / "schema/entsoe_generation.sql").read_text())
        # As on Neon: new tables are readable by the ETL and dashboard roles.
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ingestion "
            "GRANT SELECT ON TABLES TO etl_writer, dashboard_ro"
        )
    insert(
        connection,
        [record(KEEP, h, 100.0 + h) for h in range(4)]
        + [record(REMOVE, h, 100.0 + h) for h in range(3)]
        + [
            # Hour 3 is the range's (exclusive) end: the same duplicate, not listed.
            record(REMOVE, 3, 103.0),
            # The removed name in another country and another unit in range.
            record(REMOVE, 0, country_code="SK"),
            record("EPC2_B22___", 0),
        ],
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "repair_id": "test-v1",
                "cases": [
                    {
                        "id": "pocerady",
                        "country_code": "CZ",
                        "psr_type": "B04",
                        "keep": KEEP,
                        "remove": REMOVE,
                        "start_utc": "2024-12-31T23:00:00Z",
                        "end_utc": "2025-01-01T02:00:00Z",
                        "source": {"unit_eic": EIC, "xml_sha256": {"a.xml": "0" * 64}},
                    }
                ],
            }
        )
    )

    def run(command, sha=None):
        return repair.run(connection, command, "test", sha, manifest)

    return connection, run


def change(connection, plant_name, hour, statement):
    with connection.cursor() as cursor:
        cursor.execute(
            f"{statement} WHERE plant_name = %s AND timestamp_ms = %s "
            "AND country_code = 'CZ'",
            (plant_name, START + hour * HOUR),
        )
        assert cursor.rowcount == 1


def test_the_reviewed_manifest_names_exactly_the_two_cases():
    manifest, _ = repair.load_manifest()
    assert [
        (c["country_code"], c["psr_type"], c["keep"], c["remove"])
        for c in manifest["cases"]
    ] == [
        ("CZ", "B04", "EPC2_B21__", "EPC2_B21___"),
        ("NL", "B05", "NLROTTETH_", "NLROTTETH__1"),
    ]
    for case in manifest["cases"]:
        # Half-open UTC range: 2024-12-31T23:00Z up to 2025-03-31T22:00Z.
        assert (case["start_ms"], case["end_ms"]) == (1735686000000, 1743458400000)
        assert all(len(v) == 64 for v in case["source"]["xml_sha256"].values())


def test_removes_the_duplicate_and_nothing_outside_the_range(duplicated):
    connection, run = duplicated
    before = stored(connection)
    plan, pairs = run("plan")
    assert plan["rows_to_delete"] == 3
    assert plan["cases"]["pocerady"]["mwh_counted_twice"] == 303.0
    assert plan["cases"]["pocerady"]["by_month"] == {
        "2024-12": {"rows": 1, "mwh": 100.0},
        "2025-01": {"rows": 2, "mwh": 203.0},
    }
    assert [(p["delete"]["plant_name"], p["keep"]["plant_name"]) for p in pairs] == [
        (REMOVE, KEEP)
    ] * 3
    # The plan writes nothing.
    assert stored(connection) == before
    assert scalar(connection, f"SELECT to_regclass('{repair.LEDGER}')") is None

    result, _ = run("apply", plan["plan_sha256"])
    assert result["deleted"] == 3
    assert names(connection) == [
        ("CZ", "EPC2_B21__", 0),
        ("CZ", "EPC2_B21__", 1),
        ("CZ", "EPC2_B21__", 2),
        ("CZ", "EPC2_B21__", 3),
        ("CZ", "EPC2_B21___", 3),
        ("CZ", "EPC2_B22___", 0),
        ("SK", "EPC2_B21___", 0),
    ]
    # Every surviving row is byte-for-byte what it was.
    assert [r for r in before if r in stored(connection)] == stored(connection)
    assert scalar(connection, f"SELECT count(*) FROM {repair.BACKUP}") == 3
    for table in (repair.LEDGER, repair.BACKUP):
        for role in ("etl_writer", "dashboard_ro"):
            assert not scalar(
                connection, f"SELECT has_table_privilege('{role}','{table}','SELECT')"
            )
    with pytest.raises(ValueError, match="Nothing to remove"):
        run("apply", run("plan")[0]["plan_sha256"])


@pytest.mark.parametrize(
    "statement, message",
    [
        (f"UPDATE {TABLE} SET generation_mw = 101.5", "generation_mw differs"),
        (f"UPDATE {TABLE} SET resolution_minutes = 15", "resolution_minutes differs"),
        (f"UPDATE {TABLE} SET fuel_type = 'Fossil Oil'", "fuel_type differs"),
        (f"UPDATE {TABLE} SET data_type = 'Actual Consumption'", "consumption"),
        (f"UPDATE {TABLE} SET unit_eic = '27W-GU-EPC2B22-H'", "not the reviewed unit"),
        (f"DELETE FROM {TABLE}", "no row under the kept name"),
    ],
)
def test_plan_refuses_a_row_without_an_identical_twin(duplicated, statement, message):
    connection, run = duplicated
    change(connection, KEEP, 1, statement)
    with pytest.raises(ValueError, match=message):
        run("plan")


def test_the_reviewed_unit_eic_on_either_copy_is_accepted(duplicated):
    connection, run = duplicated
    change(connection, KEEP, 1, f"UPDATE {TABLE} SET unit_eic = '{EIC}'")
    assert run("plan")[0]["rows_to_delete"] == 3


def test_apply_refuses_another_fingerprint(duplicated):
    connection, run = duplicated
    before = stored(connection)
    run("plan")
    with pytest.raises(ValueError, match="re-plan"):
        run("apply", "0" * 64)
    assert stored(connection) == before
    assert scalar(connection, f"SELECT to_regclass('{repair.LEDGER}')") is None


@pytest.mark.parametrize(
    "plant_name, statement",
    [
        # Columns the twin comparison ignores: the plan binds the full images.
        (KEEP, f"UPDATE {TABLE} SET created_at_ms = created_at_ms + 1"),
        (REMOVE, f"UPDATE {TABLE} SET data_type = 'Unknown'"),
        (KEEP, f"DELETE FROM {TABLE}"),
        (REMOVE, f"DELETE FROM {TABLE}"),
    ],
)
def test_apply_refuses_rows_changed_since_the_plan(duplicated, plant_name, statement):
    connection, run = duplicated
    plan, _ = run("plan")
    change(connection, plant_name, 1, statement)
    before = stored(connection)
    with pytest.raises(ValueError, match="re-plan|no row under the kept name"):
        run("apply", plan["plan_sha256"])
    assert stored(connection) == before
    assert scalar(connection, f"SELECT to_regclass('{repair.LEDGER}')") is None


@pytest.mark.parametrize(
    "statement, message",
    [
        (f"UPDATE {TABLE} SET generation_mw = 1", "kept twin changed"),
        (f"UPDATE {TABLE} SET created_at_ms = created_at_ms + 1", "kept twin changed"),
        (f"DELETE FROM {TABLE}", "kept twin changed or disappeared"),
    ],
)
def test_rollback_refuses_once_a_kept_twin_changed(duplicated, statement, message):
    connection, run = duplicated
    sha = run("plan")[0]["plan_sha256"]
    run("apply", sha)
    change(connection, KEEP, 1, statement)
    before = stored(connection)
    with pytest.raises(ValueError, match=message):
        run("rollback", sha)
    assert stored(connection) == before
    assert scalar(connection, f"SELECT state FROM {repair.LEDGER}") == "applied"


def test_rollback_refuses_a_collision_and_an_unknown_fingerprint(duplicated):
    connection, run = duplicated
    with pytest.raises(ValueError, match="No applied repair"):
        run("rollback", "0" * 64)
    sha = run("plan")[0]["plan_sha256"]
    run("apply", sha)
    # A reload has stored the removed name again for one deleted interval.
    insert(connection, [record(REMOVE, 1, 101.0)])
    before = stored(connection)
    with pytest.raises(ValueError, match="collide"):
        run("rollback", sha)
    assert stored(connection) == before
    assert scalar(connection, f"SELECT state FROM {repair.LEDGER}") == "applied"


def test_apply_rollback_apply_restores_exactly_the_deleted_rows(duplicated):
    connection, run = duplicated
    original = stored(connection)
    plan, pairs = run("plan")
    sha = plan["plan_sha256"]
    run("apply", sha)
    repaired = stored(connection)
    assert [r for r in original if r not in repaired] == [p["delete"] for p in pairs]

    assert run("rollback", sha)[0]["restored"] == 3
    # Ids, run ids and every other column are back; nothing else moved.
    assert stored(connection) == original
    with pytest.raises(ValueError, match="No applied repair"):
        run("rollback", sha)

    # The restored state plans to the same fingerprint and applies again.
    assert run("plan")[0]["plan_sha256"] == sha
    run("apply", sha)
    assert stored(connection) == repaired
    with pytest.raises(ValueError, match="re-plan"):
        run("apply", sha)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT state, rows, (SELECT count(*) FROM {repair.BACKUP} b "
            f"WHERE b.apply_id = l.apply_id) FROM {repair.LEDGER} l ORDER BY apply_id"
        )
        assert cursor.fetchall() == [("rolled_back", 3, 3), ("applied", 3, 3)]
    assert run("rollback", sha)[0]["restored"] == 3
    assert stored(connection) == original


def test_mutations_need_a_fingerprint_and_production_needs_the_flag(
    capsys, tmp_path, monkeypatch
):
    monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
    plan_file = tmp_path / "plan.json"
    assert repair.main(["apply", "--environment", "staging"]) == 1
    assert "--expect-plan-sha256 is required" in capsys.readouterr().out
    assert repair.main(["rollback", "--environment", "staging"]) == 1
    assert "--expect-plan-sha256 is required" in capsys.readouterr().out
    arguments = ["plan", "--environment", "production", "--plan-file", str(plan_file)]
    assert repair.main(arguments) == 1
    assert "--allow-production" in capsys.readouterr().out
    # Outside the pinned owner wrapper nothing connects, even with the flag.
    assert repair.main([*arguments, "--allow-production"]) == 1
    assert "pinned environment owner wrapper" in capsys.readouterr().out
    assert not plan_file.exists()
