"""An ENTSO-E area that stops returning coal fails the weekly check
(schema/checks/entsoe_coal_areas.sql). Runs the real SQL with psql against a
disposable local database (see tests/local_db.py)."""

import subprocess
import time

from tests.local_db import ROOT, database  # noqa: F401

CHECK = ROOT / "schema/checks/entsoe_coal_areas.sql"
DAY_MS = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _run_check(conn, dsn, rows) -> subprocess.CompletedProcess:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE ingestion.entsoe_generation_data "
            "(country_code text, psr_type text, timestamp_ms bigint)"
        )
        cur.executemany(
            "INSERT INTO ingestion.entsoe_generation_data VALUES (%s, %s, %s)", rows
        )
    return subprocess.run(
        ["psql", dsn, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(CHECK)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_an_area_whose_coal_stopped_twelve_days_ago_fails_the_check(database):  # noqa: F811
    conn, _, dsn = database
    now = _now_ms()
    result = _run_check(
        conn,
        dsn,
        [
            ("CZ", "B02", now - 2 * DAY_MS),
            ("PL", "B05", now - 12 * DAY_MS),
            ("PL", "B04", now - 1 * DAY_MS),  # gas still arrives; not coal
        ],
    )

    assert result.returncode != 0
    assert "PL — last coal row" in result.stderr
    assert "CZ —" not in result.stderr


def test_areas_with_recent_coal_pass(database):  # noqa: F811
    conn, _, dsn = database
    now = _now_ms()
    result = _run_check(
        conn,
        dsn,
        [("CZ", "B02", now - 2 * DAY_MS), ("PL", "B05", now - 6 * DAY_MS)],
    )

    assert result.returncode == 0, result.stderr
    assert "all 2 areas" in result.stderr


def test_an_area_silent_for_longer_than_the_window_is_no_longer_reported(database):  # noqa: F811
    conn, _, dsn = database
    now = _now_ms()
    result = _run_check(
        conn,
        dsn,
        [("CZ", "B02", now - 2 * DAY_MS), ("IT", "B05", now - 60 * DAY_MS)],
    )

    assert result.returncode == 0, result.stderr


def test_no_coal_rows_at_all_fails(database):  # noqa: F811
    conn, _, dsn = database
    result = _run_check(conn, dsn, [("PL", "B04", _now_ms())])

    assert result.returncode != 0
    assert "no ENTSO-E coal rows at all" in result.stderr
