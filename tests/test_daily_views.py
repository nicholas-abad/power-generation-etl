#!/usr/bin/env python3
"""Daily/weekly generation views (migrations 024, 025): refresh registration and
the migration-aware refresh rule (no database)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import refresh_views  # noqa: E402

DAILY = {
    "mv_entsoe_plant_daily": ("entsoe", "024"),
    "mv_ons_plant_daily": ("ons", "024"),
    "mv_chile_plant_daily": ("chile", "024"),
    "mv_npp_unit_daily": ("npp", "024"),
    "mv_oe_plant_daily": ("oe", "024"),
    "mv_occto_unit_daily": ("occto", "025"),
}


def test_each_daily_view_is_refreshed_with_its_source_and_gated_by_its_migration():
    for view, (source, version) in DAILY.items():
        assert view in refresh_views.SOURCE_VIEWS[source]
        assert refresh_views.MIGRATION_VIEWS[view] == version
        # refreshed after the source's monthly views
        views = refresh_views.SOURCE_VIEWS[source]
        assert views.index(view) == len(views) - 1


@pytest.mark.parametrize(
    "recorded, exists, action",
    [(False, False, "skip"), (True, True, "refresh"), (True, False, "fail"), (False, True, "refresh")],
)
def test_migration_view_action(recorded, exists, action):
    assert refresh_views.migration_view_action(recorded, exists) == action


class FakeCursor:
    """Answers to_regclass and ledger lookups from two sets."""

    def __init__(self, relations, versions):
        self.relations, self.versions, self.answer = relations, versions, None

    def execute(self, sql, params):
        (value,) = params
        if "to_regclass" in sql:
            self.answer = value in self.relations
        else:
            self.answer = value in self.versions

    def fetchone(self):
        return (self.answer,)


def test_rebuild_tools_refresh_installed_daily_views_only():
    ledger = refresh_views.LEDGER
    cursor = FakeCursor({ledger, "public.mv_entsoe_plant_daily"}, {"024"})
    assert refresh_views.installed_migration_views(
        cursor, ["mv_entsoe_plant_daily", "mv_occto_unit_daily"]
    ) == ["mv_entsoe_plant_daily"]
    # recorded but missing: a dropped view must not pass silently
    cursor = FakeCursor({ledger}, {"024"})
    with pytest.raises(RuntimeError, match="024 is recorded"):
        refresh_views.installed_migration_views(cursor, ["mv_entsoe_plant_daily"])
    # no ledger table at all (fresh database): skip
    assert refresh_views.installed_migration_views(FakeCursor(set(), set()), ["mv_npp_unit_daily"]) == []


def test_every_daily_view_has_a_rollup_check():
    import check_daily_rollups as c

    assert set(c.ROLLUPS) == {v for v in refresh_views.MIGRATION_VIEWS if v.endswith("_daily")}
    assert not c.ROLLUPS["mv_oe_plant_daily"].by_month  # local days vs UTC months
    sql = c.rollup_sql("mv_npp_unit_daily", c.ROLLUPS["mv_npp_unit_daily"])
    assert "d.m = mo.m" in sql and "IS NOT DISTINCT FROM" not in sql  # hashable FULL JOIN
