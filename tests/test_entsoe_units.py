"""Real migration/loader checks in the same disposable local PG as ONS CI."""

import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest
from sqlalchemy import event

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from entsoe_identity import prepare_record  # noqa: E402
from validator import DataValidator  # noqa: E402
from check_entsoe_staging import load_expected, preflight  # noqa: E402
from tests.local_db import database  # noqa: F401,E402


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


def load_as_extracted(db, tmp_path, rows, **kwargs):
    """Write rows the way the extractor does: one DataFrame, so every row
    carries every column and missing values become null."""
    path = tmp_path / "entsoe.jsonl"
    pd.DataFrame(rows).to_json(path, orient="records", lines=True)
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


def test_one_file_with_identified_coal_and_legacy_gas_loads(entsoe_db, tmp_path):
    # The weekly extractor emits allow-listed coal as source-period rows (with
    # EICs) and every other fuel through the legacy path, in the same file.
    connection, db, dsn = entsoe_db
    migrate(dsn)
    coal = record(resolution_source="xml_period")
    gas = record(
        plant_name="Gas plant_Fossil Gas", psr_type="B04", resolution_minutes=15
    )
    for key in ("unit_eic", "production_unit_eic"):
        gas.pop(key)
    ok, report = load_as_extracted(
        db, tmp_path, [coal, gas, dict(coal, timestamp_ms=1551402000000)], batch_size=1
    )
    assert ok and report.valid_count == 3 and report.written_count == 3
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT psr_type, plant_name, unit_eic, source_unit_name, resolution_minutes "
            "FROM ingestion.entsoe_generation_data ORDER BY psr_type, timestamp_ms"
        )
        assert cursor.fetchall() == [
            ("B02", "ECHV_G1___", "27W-GU-ECHVG1--C", "ECHV_G1____", 60),
            ("B02", "ECHV_G1___", "27W-GU-ECHVG1--C", "ECHV_G1____", 60),
            ("B04", "Gas plant", None, None, 15),
        ]


def test_before_migration_018_the_weekly_file_loads_as_on_main(entsoe_db, tmp_path):
    # Production runs this code before 018 is applied, with the pinned extractor
    # emitting EICs for allow-listed coal: everything loads by the name key.
    connection, db, dsn = entsoe_db
    coal = record(resolution_source="xml_period")
    gas = record(
        plant_name="Gas plant_Fossil Gas", psr_type="B04", resolution_minutes=15
    )
    for key in ("unit_eic", "production_unit_eic"):
        gas.pop(key)
    rows = [coal, gas]
    ok, report = load_as_extracted(db, tmp_path, rows, batch_size=1)
    assert ok and report.written_count == 2
    ok, report = load(db, tmp_path, [dict(gas, generation_mw=90.0)], batch_size=1)
    assert ok and report.written_count == 1
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT psr_type, plant_name, generation_mw, resolution_minutes "
            "FROM ingestion.entsoe_generation_data ORDER BY psr_type"
        )
        assert cursor.fetchall() == [
            ("B02", "ECHV_G1___", 100.0, 60),
            ("B04", "Gas plant", 90.0, 15),
        ]
    unnamed = record(
        plant_name=None, unit_eic="27W-GU-UNNAMED-X", resolution_source="xml_period"
    )
    ok, _ = load(db, tmp_path, [unnamed], batch_size=1)
    assert not ok


def test_load_fails_when_many_rows_are_invalid(entsoe_db, tmp_path):
    # 2026-10-25: 94% of the weekly file was rejected and the load still
    # counted as a successful extraction.
    connection, db, dsn = entsoe_db
    gas = record(plant_name="Gas plant", psr_type="B04")
    for key in ("unit_eic", "production_unit_eic"):
        gas.pop(key)
    bad = dict(gas, plant_name="Other gas plant", generation_mw=-1.0)
    ok, report = load(db, tmp_path, [gas, bad])
    assert not ok and report.invalid_count == 1 and report.written_count == 1


def test_refresh_skips_the_unit_view_until_migration_018(entsoe_db, monkeypatch):
    import refresh_views

    connection, db, dsn = entsoe_db
    monkeypatch.setattr(refresh_views, "create_engine", lambda url: db.engine)
    monkeypatch.setattr(refresh_views, "get_connection_url", lambda: "")
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE UNIQUE INDEX ON public.mv_entsoe_plant_monthly "
            "(month, plant_name, country_code, fuel_type)"
        )
    assert refresh_views.refresh_views(["mv_entsoe_unit_monthly"])
    migrate(dsn)
    assert refresh_views.refresh_views(["mv_entsoe_unit_monthly"])
    assert not refresh_views.refresh_views(["mv_entsoe_missing_monthly"])


def test_identified_batches_lock_before_the_identity_check(monkeypatch):
    # Two identified writers must not both pass the identity check before
    # either upserts: the table lock is taken first.
    import pandas as pd
    from database import PowerGenerationDatabase

    executed = []

    class Cursor:
        rowcount = 0

        def execute(self, sql, *args):
            executed.append(sql)

        def copy_from(self, *a, **k):
            pass

        def fetchone(self):
            return (False,)

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    db = PowerGenerationDatabase.__new__(PowerGenerationDatabase)
    db._engine = type("E", (), {"raw_connection": lambda self: Conn()})()
    monkeypatch.setattr("database.prepare_coal_upsert", lambda cursor: None)
    frame = pd.DataFrame([record(psr_type="B04", plant_name="Gas")])
    frame["_source_resolution_verified"] = False
    db._upsert_via_staging(
        frame,
        "entsoe_generation_data",
        ["timestamp_ms", "country_code", "psr_type", "plant_name"],
        update_columns=["generation_mw", "unit_eic"],
        identity_columns=["unit_eic", "production_unit_eic"],
    )
    lock = next(i for i, sql in enumerate(executed) if sql.startswith("LOCK TABLE"))
    check = next(
        i for i, sql in enumerate(executed) if "IS DISTINCT FROM s.unit_eic" in sql
    )
    assert lock < check


def test_unnamed_series_are_named_by_eic_or_left_out(entsoe_db, tmp_path):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    # A unit already stored under a name (not in the identity table).
    stored = record(
        plant_name="Ledvice 4",
        unit_eic="27W-GU-ELE4----5",
        source_unit_name="Ledvice 4",
        timestamp_ms=1551394800000,
    )
    assert load(db, tmp_path, [stored])[0]
    unnamed = [
        record(plant_name=None, unit_eic="27W-GU-ELE4----5"),  # stored name
        record(plant_name=None),  # reviewed identity table: ECHV_G1___
        record(plant_name=None, unit_eic="27W-GU-NEVER---1"),  # never named
    ]
    ok, report = load(db, tmp_path, unnamed)
    assert ok and report.written_count == 2
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT unit_eic, plant_name, source_unit_name FROM ingestion.entsoe_generation_data"
            " WHERE timestamp_ms=%s ORDER BY unit_eic",
            (1551398400000,),
        )
        assert cursor.fetchall() == [
            ("27W-GU-ECHVG1--C", "ECHV_G1___", None),
            ("27W-GU-ELE4----5", "Ledvice 4", None),
        ]


def test_unnamed_units_never_merge_and_replays_keep_their_time_name(
    entsoe_db, tmp_path
):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    earlier, later = 1551394800000, 1551398400000
    # Two units that once shared a label, at different times, plus a unit
    # renamed over time (old name at `earlier`, new name at `later`).
    history = [
        record(
            plant_name="Shared",
            unit_eic="27W-GU-SHAREA--1",
            source_unit_name="Shared",
            timestamp_ms=earlier - 3600000,
        ),
        record(
            plant_name="Shared",
            unit_eic="27W-GU-SHAREB--2",
            source_unit_name="Shared",
            timestamp_ms=earlier,
        ),
        record(
            plant_name="Old name",
            unit_eic="27W-GU-RENAME--3",
            source_unit_name="Old name",
            timestamp_ms=earlier,
        ),
        record(
            plant_name="New name",
            unit_eic="27W-GU-RENAME--3",
            source_unit_name="New name",
            timestamp_ms=later,
        ),
    ]
    assert load(db, tmp_path, history)[0]
    # An unnamed replay at `earlier` keeps the name the unit had then: it
    # updates that row (no second row, no unit/time key violation).
    ok, _ = load(
        db,
        tmp_path,
        [record(plant_name=None, unit_eic="27W-GU-RENAME--3", timestamp_ms=earlier)],
    )
    assert ok
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT plant_name FROM ingestion.entsoe_generation_data"
            " WHERE unit_eic='27W-GU-RENAME--3' ORDER BY timestamp_ms"
        )
        assert [r[0] for r in cursor.fetchall()] == ["Old name", "New name"]
    # Both "Shared" units arriving unnamed at one new time would merge: refused.
    ok, _ = load(
        db,
        tmp_path,
        [
            record(
                plant_name=None,
                unit_eic="27W-GU-SHAREA--1",
                timestamp_ms=later + 3600000,
            ),
            record(
                plant_name=None,
                unit_eic="27W-GU-SHAREB--2",
                timestamp_ms=later + 3600000,
            ),
        ],
    )
    assert not ok
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM ingestion.entsoe_generation_data")
        assert cursor.fetchone()[0] == 4
