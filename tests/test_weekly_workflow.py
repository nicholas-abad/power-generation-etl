"""Structure of .github/workflows/weekly-extraction.yml for the Chile jobs.

The workflow cannot be run in CI; these checks read the YAML and catch wiring
mistakes that only show up on a Sunday (a job missing from the failure issue,
a refresh that never runs after a partial load).
"""

import re
from pathlib import Path

import yaml

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
