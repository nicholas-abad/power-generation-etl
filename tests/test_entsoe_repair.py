"""The proposed repair is scoped, backed up, repeatable and fails on MW drift."""

import sys

import pytest

from tests.test_entsoe_units import database, entsoe_db, migrate, record  # noqa: F401
from entsoe_identity import prepare_record
from check_entsoe_staging import load_expected
from repair_entsoe_cz_2024_staging import BACKUP, main, prepare_repair


def seed(connection):
    source = record(timestamp_ms=1704067200000)
    expected = prepare_record(source)
    original = {
        k: v
        for k, v in expected.items()
        if k not in {"unit_eic", "production_unit_eic", "source_unit_name"}
    }
    original["resolution_minutes"] = 15
    with connection.cursor() as cursor:
        for row in [original, {**original, "plant_name": source["plant_name"]}]:
            cursor.execute(
                f"INSERT INTO ingestion.entsoe_generation_data ({','.join(row)}) VALUES ({','.join(['%s'] * len(row))})",
                tuple(row.values()),
            )
    return expected


def test_plan_apply_backup_and_repeat(entsoe_db):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    expected = seed(connection)
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            load_expected(cursor, [expected])
            plan = prepare_repair(cursor, expected_counts=(1, 1))
            assert plan["action"] == "plan"
            assert plan["original_rows_to_archive"] == 2
            assert plan["coal_mwh_before"] == 50
            assert plan["coal_mwh_after_repair"] == 100
            cursor.execute(
                "SELECT count(*),min(resolution_minutes) FROM ingestion.entsoe_generation_data"
            )
            assert cursor.fetchone() == (2, 15)
        connection.rollback()
        with connection.cursor() as cursor:
            load_expected(cursor, [expected])
            assert prepare_repair(cursor, True, (1, 1))["action"] == "applied"
            cursor.execute(
                "SELECT plant_name,generation_mw,resolution_minutes FROM ingestion.entsoe_generation_data"
            )
            assert cursor.fetchall() == [("ECHV_G1___", 100, 60)]
            cursor.execute(f"SELECT count(*),min(resolution_minutes) FROM {BACKUP}")
            assert cursor.fetchone() == (2, 15)
            cursor.execute(
                f"SELECT has_table_privilege('dashboard_ro','{BACKUP}','SELECT')"
            )
            assert cursor.fetchone() == (False,)
        connection.commit()
        with connection.cursor() as cursor:
            load_expected(cursor, [expected])
            repeated = prepare_repair(cursor, True, (1, 1))
            assert repeated["action"] == "already_corrected"
            assert repeated["original_rows_to_archive"] == 0
        connection.rollback()
    finally:
        connection.rollback()
        connection.autocommit = True


@pytest.mark.parametrize("change", ["mw", "population"])
def test_unreviewed_changes_leave_originals_untouched(entsoe_db, change):
    connection, db, dsn = entsoe_db
    migrate(dsn)
    expected = seed(connection)
    if change == "mw":
        expected["generation_mw"] += 1
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            load_expected(cursor, [expected])
            with pytest.raises(ValueError):
                prepare_repair(
                    cursor, True, (2, 1) if change == "population" else (1, 1)
                )
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*),min(resolution_minutes),max(generation_mw) FROM ingestion.entsoe_generation_data"
            )
            assert cursor.fetchone() == (2, 15, 100)
            cursor.execute(f"SELECT to_regclass('{BACKUP}')")
            assert cursor.fetchone() == (None,)
    finally:
        connection.rollback()
        connection.autocommit = True


def test_production_is_rejected_before_reading_source_or_connecting(monkeypatch):
    monkeypatch.setenv("ETL_ENVIRONMENT", "production")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "repair",
            "--input",
            "missing",
            "--manifest",
            "missing",
            "--report",
            "missing",
            "--apply",
        ],
    )
    with pytest.raises(ValueError, match="staging environment"):
        main()
