"""Structure of .github/workflows/weekly-extraction.yml for the Chile jobs.

The workflow cannot be run in CI; these checks read the YAML and catch wiring
mistakes that only show up on a Sunday (a job missing from the failure issue,
a refresh that never runs after a partial load).
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from local_postgres import local_settings, verify_local_connection  # noqa: E402

WORKFLOW = (
    Path(__file__).resolve().parents[1] / ".github/workflows/weekly-extraction.yml"
)
JOBS = yaml.safe_load(WORKFLOW.read_text())["jobs"]
ALL_FUELS = JOBS["extract-chile-all-fuels"]


def test_all_fuels_runs_after_the_coal_job_unless_cancelled():
    assert ALL_FUELS["needs"] == ["extract-chile"]
    assert ALL_FUELS["if"].startswith("${{ !cancelled() && (")
    assert "always()" not in ALL_FUELS["if"]
    assert "'Chile all fuels (Coordinador)'" in ALL_FUELS["if"]


def test_both_chile_jobs_share_one_concurrency_group():
    # One CL_API_KEY: two jobs at once would exceed its 60 requests/hour.
    groups = {
        JOBS[j]["concurrency"]["group"]
        for j in ("extract-chile", "extract-chile-all-fuels")
    }
    assert groups == {"coordinador-api"}


def test_every_step_after_the_guard_needs_migration_019():
    steps = ALL_FUELS["steps"]
    guard = next(i for i, s in enumerate(steps) if s.get("id") == "guard")
    for step in steps[guard + 1 :]:
        assert "steps.guard.outputs.applied == 'true'" in step.get("if", ""), step[
            "name"
        ]


def test_the_pilot_view_is_refreshed_before_reconcile():
    names = [s["name"] for s in ALL_FUELS["steps"]]
    refresh = next(
        i
        for i, n in enumerate(names)
        if n.startswith("Refresh the all-fuel pilot view")
    )
    reconcile = next(i for i, n in enumerate(names) if n.startswith("Reconcile"))
    assert refresh < reconcile


def test_refresh_views_runs_after_a_partial_all_fuel_load():
    refresh = JOBS["refresh-views"]
    assert "extract-chile-all-fuels" in refresh["needs"]
    assert "needs.extract-chile-all-fuels.outputs.loaded == 'true'" in refresh["if"]
    assert ALL_FUELS["outputs"]["loaded"] == "${{ steps.load.outputs.loaded }}"


def test_every_job_the_notifier_needs_is_named_in_its_issue():
    notify = JOBS["notify-failure"]
    script = notify["steps"][0]["with"]["script"]
    named = set(re.findall(r"'([a-z-]+)': '\$\{\{ needs\.", script))
    assert set(notify["needs"]) == named


# --- the guard step against a disposable LOCAL PostgreSQL ------------------


def _guard(env_db: dict, start_override: str, tmp_path) -> tuple[int, str]:
    """Run the guard step's script as the runner would; (exit code, outputs)."""
    script = next(s for s in ALL_FUELS["steps"] if s.get("id") == "guard")["run"]
    out = tmp_path / "github_output"
    out.write_text("")
    env = {
        **os.environ,
        **env_db,
        "PGSSLMODE": "disable",
        "START_OVERRIDE": start_override,
        "GITHUB_OUTPUT": str(out),
    }
    rc = subprocess.run(["bash", "-c", script], env=env, capture_output=True).returncode
    return rc, out.read_text()


@pytest.fixture
def guard_db():
    dsn = os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set COAL_TEST_PG_DSN to a disposable local database")
    if not shutil.which("psql"):
        pytest.skip("psql not installed")
    with patch.dict(os.environ):
        parts = local_settings(
            dsn, r"(?:coal_hotfix_tests_[a-z0-9_]+|power_generation_test)"
        )
        conn = psycopg2.connect(**parts, connect_timeout=5)
        conn.autocommit = True
        verify_local_connection(conn, parts["dbname"])
        env_db = {
            "POSTGRES_HOST": parts["host"],
            "POSTGRES_PORT": str(parts.get("port", 5432)),
            "POSTGRES_USER": parts["user"],
            "POSTGRES_DB": parts["dbname"],
            "PGPASSWORD": parts.get("password", ""),
        }
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
            cur.execute("CREATE SCHEMA ingestion")
        try:
            yield conn.cursor(), env_db
        finally:
            conn.close()


def test_guard_skips_without_the_ledger_or_019_and_fails_on_an_empty_table(
    guard_db, tmp_path
):
    cur, env_db = guard_db
    assert _guard(env_db, "", tmp_path) == (0, "applied=false\n")  # no ledger table
    cur.execute(
        "CREATE TABLE ingestion.schema_migrations (version TEXT PRIMARY KEY, notes TEXT)"
    )
    assert _guard(env_db, "", tmp_path) == (0, "applied=false\n")  # ledger without 019
    cur.execute("INSERT INTO ingestion.schema_migrations VALUES ('019', '')")
    cur.execute(
        "CREATE TABLE ingestion.chile_generation_all_fuels (timestamp_ms BIGINT)"
    )
    assert _guard(env_db, "", tmp_path) == (
        1,
        "applied=true\n",
    )  # empty: run the backfill
    assert _guard(env_db, "2026-01-01", tmp_path) == (
        0,
        "applied=true\n",
    )  # the backfill itself
    cur.execute("INSERT INTO ingestion.chile_generation_all_fuels VALUES (1)")
    assert _guard(env_db, "", tmp_path) == (0, "applied=true\n")  # a weekly run


def test_a_guard_query_error_fails_instead_of_skipping(guard_db, tmp_path):
    _, env_db = guard_db
    rc, out = _guard({**env_db, "POSTGRES_DB": "no_such_database"}, "", tmp_path)
    assert rc != 0 and "applied=false" not in out
