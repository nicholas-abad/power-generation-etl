"""Real migration/loader checks in the same disposable local PG as ONS CI."""

import json
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import event

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from entsoe_identity import prepare_record  # noqa: E402
from validator import DataValidator  # noqa: E402
from check_entsoe_staging import load_expected, preflight  # noqa: E402
from tests.test_ons_individual_plant_view import database  # noqa: F401,E402


def record(**changes):
    return {
        "extraction_run_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "created_at_ms": 1790985600000,
        "timestamp_ms": 1551398400000,
        "country_code": "CZ",
        "plant_name": "ECHV_G1____",
        "psr_type": "B02",
        "fuel_type": "Fossil Brown coal/Lignite",
        "generation_mw": 100.0,
        "data_type": "Actual Aggregated",
        "resolution_minutes": 60,
        "unit_eic": "27W-GU-ECHVG1--C",
        "production_unit_eic": "27W-PU-ECHV----Y",
        **changes,
    }


@pytest.fixture
def entsoe_db(database):
    connection, db, dsn = database
    with connection.cursor() as cursor:
        cursor.execute((ROOT / "schema/entsoe_generation.sql").read_text())
        # Start from the pre-018 schema and an existing legacy dashboard view.
        cursor.execute(
            "ALTER TABLE ingestion.entsoe_generation_data DROP COLUMN unit_eic CASCADE, DROP COLUMN production_unit_eic CASCADE, DROP COLUMN source_unit_name"
        )
        cursor.execute("""CREATE MATERIALIZED VIEW public.mv_entsoe_plant_monthly AS
            SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0)) AS month,
            plant_name,country_code,fuel_type,sum(generation_mw*resolution_minutes/60.0) AS generation_mwh
            FROM ingestion.entsoe_generation_data GROUP BY 1,2,3,4""")
        cursor.execute("GRANT SELECT ON ingestion.entsoe_generation_data TO etl_writer")
        cursor.execute("GRANT SELECT ON public.mv_entsoe_plant_monthly TO dashboard_ro")
    return connection, db, dsn


def migrate(dsn):
    result = subprocess.run(
        [
            "psql",
            "-X",
            dsn,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(ROOT / "schema/migrations/018_entsoe_unit_identifiers.sql"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def load(db, tmp_path, rows, **kwargs):
    path = tmp_path / "entsoe.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return db.insert_entsoe_jsonl_data(str(path), **kwargs)


def test_migration_alias_backfill_replay_and_legacy_compatibility(entsoe_db, tmp_path):
    connection, db, dsn = entsoe_db
    original = record(plant_name="ECHV_G1___", data_type="Fossil Brown coal/Lignite")
    original.pop("unit_eic")
    original.pop("production_unit_eic")
    with connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO ingestion.entsoe_generation_data ({','.join(original)}) VALUES ({','.join(['%s'] * len(original))})",
            tuple(original.values()),
        )
    migrate(dsn)
    migrate(dsn)  # migration is repeatable
    rows = [
        record(),
        record(
            plant_name="Hydro",
            unit_eic="27W-GU-EDALG1--2",
            production_unit_eic="27W-PU-EDAL----O",
            psr_type="B10",
            fuel_type="Hydro Pumped Storage",
            resolution_minutes=15,
        ),
    ]
    ok, report = load(db, tmp_path, rows, batch_size=1)
    assert ok and report.valid_count == 2 and report.written_count == 2
    ok, repeated = load(db, tmp_path, rows, batch_size=1)
    assert ok and repeated.written_count == 0 and repeated.duplicate_count == 0
    with connection.cursor() as cursor:
        cursor.execute(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_entsoe_unit_monthly"
        )
        cursor.execute(
            "SELECT plant_name,source_unit_name,generation_mwh,observation_count,observed_minutes FROM public.mv_entsoe_unit_monthly ORDER BY plant_name"
        )
        assert cursor.fetchall() == [
            ("ECHV_G1___", "ECHV_G1____", 100.0, 1, 60),
            ("Hydro", "Hydro", 25.0, 1, 15),
        ]
        cursor.execute(
            "SELECT has_table_privilege('dashboard_ro','public.mv_entsoe_unit_monthly','SELECT'), has_table_privilege('dashboard_ro','public.mv_entsoe_plant_monthly','SELECT')"
        )
        assert cursor.fetchone() == (False, True)
    # A legacy replay retains exact XML intervals, even if its DataFrame-wide
    # inference labels an hourly observation as 15 minutes (CZ 2024 drift).
    original["data_type"] = "Unknown"
    original["resolution_minutes"] = 15
    ok, legacy = load(db, tmp_path, [original])
    assert ok and legacy.written_count == 0
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT unit_eic,data_type,generation_mw,resolution_minutes FROM ingestion.entsoe_generation_data WHERE plant_name='ECHV_G1___'"
        )
        assert cursor.fetchone() == ("27W-GU-ECHVG1--C", "Actual Aggregated", 100.0, 60)


def test_conflicting_unit_identity_is_rejected(entsoe_db, tmp_path):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    assert load(db, tmp_path, [record()])[0]
    bad = record(plant_name="ECHV_G1___", unit_eic="27W-GU-ECHVG2--8")
    # Use an unrelated valid EIC so the DB identity guard, rather than alias
    # vocabulary validation, must catch the collision with the existing row.
    bad["unit_eic"] = "27W-GU-EDALG1--2"
    ok, _ = load(db, tmp_path, [bad])
    assert not ok
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*),min(unit_eic) FROM ingestion.entsoe_generation_data"
        )
        assert cursor.fetchone() == (1, "27W-GU-ECHVG1--C")


def test_second_name_cannot_duplicate_a_unit_time(entsoe_db, tmp_path):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    row = record(plant_name="Hydro", unit_eic="27W-GU-EDALG1--2")
    assert load(db, tmp_path, [row])[0]
    assert not load(db, tmp_path, [{**row, "plant_name": "Other Hydro"}])[0]
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM ingestion.entsoe_generation_data")
        assert cursor.fetchone() == (1,)


def test_metadata_date_range_filters_run_before_min_max(entsoe_db, tmp_path):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    rows = [record(), record(timestamp_ms=1554076800000)]
    assert load(db, tmp_path, rows)[0]
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement, parameters))

    event.listen(db.engine, "before_cursor_execute", capture)
    try:
        assert db._get_date_range_for_run(
            "entsoe_generation_data", rows[0]["extraction_run_id"]
        ) == ("2019-03-01", "2019-04-01")
    finally:
        event.remove(db.engine, "before_cursor_execute", capture)
    statement, params = statements[-1]
    with connection.cursor() as cursor:
        cursor.execute("EXPLAIN (FORMAT JSON) " + statement, params)
        plan = cursor.fetchone()[0]
        # The planner must aggregate the filtered CTE, rather than use its
        # pathological MIN/MAX timestamp-index shortcut over the full table.
        assert '"Node Type": "CTE Scan"' in json.dumps(plan)
    assert db._get_date_range_for_run(
        "entsoe_generation_data", "00000000-0000-0000-0000-000000000000"
    ) == (None, None)


@pytest.mark.parametrize("change", ["missing_gas", "changed_eic", "second_name"])
def test_preflight_rejects_missing_rows_and_identity_collisions(
    entsoe_db, tmp_path, change
):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    existing = record(
        plant_name="Gas",
        unit_eic="27W-GU-EDALG1--2",
        psr_type="B04",
        fuel_type="Fossil Gas",
    )
    assert load(db, tmp_path, [existing])[0]
    incoming = existing.copy()
    if change == "missing_gas":
        incoming["timestamp_ms"] += 3600000
    elif change == "changed_eic":
        incoming["unit_eic"] = "27W-GU-EDALG2--X"
    else:
        incoming["plant_name"] = "Renamed Gas"
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            load_expected(cursor, [prepare_record(incoming)])
            with pytest.raises(ValueError, match="Source comparison failed"):
                preflight(cursor, 2019, 3)
    finally:
        connection.rollback()
        connection.autocommit = True


def test_alias_is_explicit_and_keeps_source_text():
    row = prepare_record(record())
    assert row["plant_name"] == "ECHV_G1___"
    assert row["source_unit_name"] == "ECHV_G1____"
    assert prepare_record(row) == row
    with pytest.raises(ValueError, match="unreviewed"):
        prepare_record(record(plant_name="Changed name"))
    unknown = prepare_record(record(country_code="FR", plant_name="Example____"))
    assert unknown["plant_name"] == "Example____"


@pytest.mark.parametrize(
    "field,value",
    [
        ("unit_eic", None),
        ("unit_eic", "bad"),
        ("production_unit_eic", "bad"),
        ("generation_mw", float("nan")),
        ("generation_mw", float("inf")),
        ("generation_mw", "bad"),
        ("resolution_minutes", 5),
        ("data_type", "Actual Consumption"),
    ],
)
def test_invalid_identified_records_fail_validation(field, value):
    result = DataValidator().validate_entsoe_record(record(**{field: value}))
    assert not result.valid
