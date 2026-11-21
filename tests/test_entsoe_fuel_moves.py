"""Move stored rows of a converted unit to its real fuel, refuse on any drift, and roll back.

Needs ONS_TEST_PG_DSN or COAL_TEST_PG_DSN (a local disposable PostgreSQL
server, see tests/local_db.py); each test uses a fresh database.
"""

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import entsoe_fuel_exceptions as fx  # noqa: E402
import repair_entsoe_fuel_moves as repair  # noqa: E402
from tests.local_db import database  # noqa: F401,E402

HOUR = 3_600_000
MARCH = 1709251200000  # 2024-03-01T00:00Z
UNIT = "45W000000000036L"  # Studstrup 3
TABLE = repair.TABLE
RECORD = {
    "id": "DK_CA-studstrup-3-biomass", "area": "DK_CA", "unit_eic": UNIT, "unit_name": "Studstrupvaerket 3",
    "reviewed_names": ["Studstrupvaerket 3"], "from_month": "2024-03", "to_month": "2024-03",
    "source_psr": "B05", "treatment": "count_as_other_fuel", "counted_psr": "B01",
    "reason": "test", "decided_by": "test", "decided_on": "2026-11-21", "ref": "test",
}  # fmt: skip
RECORDS = fx.parse({"exceptions": [RECORD]})


def row(hour, psr="B05", eic=UNIT, name="Studstrupvaerket 3", mw=100.0, timestamp=None):
    return {
        "extraction_run_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "created_at_ms": 1790985600000,
        "timestamp_ms": timestamp if timestamp is not None else MARCH + hour * HOUR,
        "country_code": "DK_CA",
        "plant_name": name,
        "psr_type": psr,
        "fuel_type": {"B05": "Fossil Hard coal", "B01": "Biomass"}[psr],
        "data_type": "Actual Aggregated",
        "generation_mw": mw,
        "resolution_minutes": 60,
        "unit_eic": eic,
    }


def insert(connection, rows):
    with connection.cursor() as cursor:
        for r in rows:
            cursor.execute(
                f"INSERT INTO {TABLE} ({','.join(r)}) VALUES ({','.join(['%s'] * len(r))})", tuple(r.values())
            )


def labels(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT plant_name, psr_type, fuel_type, (timestamp_ms - {MARCH}) / {HOUR} FROM {TABLE} ORDER BY 1, 4, 2")
        return cursor.fetchall()


@pytest.fixture
def stored(database):  # noqa: F811
    """Three March hours of Studstrup 3 as coal, one in April (outside the record), another unit."""
    connection, _, _ = database
    with connection.cursor() as cursor:
        cursor.execute((ROOT / "schema/entsoe_generation.sql").read_text())
    insert(
        connection,
        [row(h, mw=100.0 + h) for h in range(3)]
        + [row(0, timestamp=1711929600000)]  # 2024-04-01: outside the record's window
        + [row(0, eic="45W000000000035N", name="Fynsvaerket 7")],
    )

    def run(command, sha=None):
        return repair.run(connection, command, "test", sha, RECORDS)

    return connection, run


def test_plan_lists_exactly_the_records_rows_with_their_mwh(stored):
    _, run = stored
    plan, moves = run("plan")
    assert plan["rows_to_move"] == 3
    assert plan["records"]["DK_CA-studstrup-3-biomass"]["mwh"] == 303.0
    assert {m["row"]["timestamp_ms"] for m in moves} == {MARCH, MARCH + HOUR, MARCH + 2 * HOUR}


def test_apply_moves_the_rows_and_rollback_puts_them_back(stored):
    connection, run = stored
    before = labels(connection)
    plan, _ = run("plan")
    report, _ = run("apply", plan["plan_sha256"])
    assert report["moved"] == 3
    after = labels(connection)
    assert [r for r in after if r[0] == "Studstrupvaerket 3"] == [
        ("Studstrupvaerket 3", "B01", "Biomass", 0),
        ("Studstrupvaerket 3", "B01", "Biomass", 1),
        ("Studstrupvaerket 3", "B01", "Biomass", 2),
        ("Studstrupvaerket 3", "B05", "Fossil Hard coal", 744),  # April stays: outside the window
    ]
    assert ("Fynsvaerket 7", "B05", "Fossil Hard coal", 0) in after
    run("rollback", plan["plan_sha256"])
    assert labels(connection) == before


def test_apply_refuses_a_plan_whose_rows_changed(stored):
    connection, run = stored
    plan, _ = run("plan")
    insert(connection, [row(5)])
    with pytest.raises(ValueError, match="changed since the reviewed plan"):
        run("apply", plan["plan_sha256"])


def test_a_stored_row_where_a_moved_row_would_land_refuses_the_plan(stored):
    connection, run = stored
    # ENTSO-E also reported the unit as biomass at hour 1, under another name.
    insert(connection, [row(1, psr="B01", eic=None, name="Studstrupvaerket 3")])
    with pytest.raises(ValueError, match="already sit where moved rows would land"):
        run("plan")


def test_rollback_refuses_after_a_moved_row_changed(stored):
    connection, run = stored
    plan, _ = run("plan")
    run("apply", plan["plan_sha256"])
    with connection.cursor() as cursor:
        cursor.execute(f"UPDATE {TABLE} SET generation_mw = 1 WHERE psr_type = 'B01' AND timestamp_ms = {MARCH}")
    with pytest.raises(ValueError, match="rollback requires a new review"):
        run("rollback", plan["plan_sha256"])


def test_nothing_to_move_is_refused(stored):
    _, run = stored
    plan, _ = run("plan")
    run("apply", plan["plan_sha256"])
    again, _ = run("plan")
    with pytest.raises(ValueError, match="Nothing to move"):
        run("apply", again["plan_sha256"])
