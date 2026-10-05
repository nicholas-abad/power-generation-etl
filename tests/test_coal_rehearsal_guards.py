"""The rehearsal/export tools must reject other targets before connecting."""

from pathlib import Path
from copy import deepcopy
import runpy

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "dsn",
    [
        "host=example.neon.tech dbname=coal_hotfix_rehearsal_test user=postgres",
        "host=/private/tmp dbname=postgres user=postgres",
        "host=/private/tmp dbname=neondb user=postgres",
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=etl_writer",
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=postgres hostaddr=192.0.2.1",
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=postgres service=production",
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=postgres options='-c search_path=public'",
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=postgres port=invalid",
    ],
)
def test_rehearsal_refuses_other_targets_before_connecting(dsn, monkeypatch):
    module = runpy.run_path(str(ROOT / "scripts/rehearse_entsoe_coal_repair.py"))

    def forbidden(*args, **kwargs):
        pytest.fail("Rejected target reached the database connector")

    monkeypatch.setattr(psycopg2, "connect", forbidden)
    with pytest.raises(ValueError):
        module["local_settings"](dsn)


def test_rehearsal_clears_inherited_connection_overrides(monkeypatch):
    module = runpy.run_path(str(ROOT / "scripts/rehearse_entsoe_coal_repair.py"))
    monkeypatch.setattr(module["os"], "environ", {})
    for key in (
        "PGHOSTADDR",
        "PGSERVICE",
        "PGSERVICEFILE",
        "PGOPTIONS",
        "DATABASE_URL",
        "DIRECT_DATABASE_URL",
        "POSTGRES_HOST",
    ):
        monkeypatch.setenv(key, "must-not-be-used")
    parts = module["local_settings"](
        "host=/private/tmp dbname=coal_hotfix_rehearsal_test user=postgres"
    )
    assert parts["host"] == "/private/tmp"
    assert not any(
        key.startswith(("PG", "POSTGRES_"))
        or key in {"DATABASE_URL", "DIRECT_DATABASE_URL"}
        for key in module["os"].environ
    )


def test_export_rejects_production_before_connecting_or_creating_files(
    monkeypatch, tmp_path
):
    module = runpy.run_path(str(ROOT / "scripts/export_entsoe_coal_rehearsal.py"))
    monkeypatch.setenv("ETL_ENVIRONMENT", "production")
    monkeypatch.setenv(
        "DATABASE_URL", "host=example.neon.tech dbname=neondb user=neondb_owner"
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Production export reached the database connector")

    monkeypatch.setattr(psycopg2, "connect", forbidden)
    destination = tmp_path / "export"
    with pytest.raises(ValueError, match="pinned staging"):
        module["export"](destination)
    assert not destination.exists()


def test_view_float_tolerance_never_hides_raw_drift_or_material_view_changes():
    module = runpy.run_path(str(ROOT / "scripts/rehearse_entsoe_coal_repair.py"))
    original = {
        "fingerprints": {"all": {"rows": 1, "sha256": "exact-original"}},
        "plant_monthly": [
            ["2024-01-01", "EPVR.B2", "CZ", "Fossil Gas", "641.1425000000006"]
        ],
    }
    repeated = deepcopy(original)
    repeated["plant_monthly"][0][4] = "641.142499999999"
    assert module["same_snapshot"](original, repeated)
    repeated["fingerprints"]["all"]["sha256"] = "changed-raw-record"
    assert not module["same_snapshot"](original, repeated)
    repeated = deepcopy(original)
    repeated["plant_monthly"][0][4] = "641.143"
    assert not module["same_snapshot"](original, repeated)
    repeated = deepcopy(original)
    repeated["plant_monthly"][0][1] = "another plant"
    assert not module["same_snapshot"](original, repeated)
