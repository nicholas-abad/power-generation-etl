"""Prove environment selection fails closed before running any DB command."""

import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import unquote, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import run_in_environment  # noqa: E402
from run_in_environment import build_environment  # noqa: E402

CONFIG = json.loads((ROOT / "config/environments.json").read_text())


def credentials():
    endpoint = CONFIG["environments"]["staging"]["endpoint_id"]
    return {
        "POSTGRES_HOST": f"{endpoint}.{CONFIG['proxy_host']}",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": CONFIG["database"],
        "POSTGRES_USER": "etl_writer",
        "POSTGRES_PASSWORD": "test/@:#$' secret",
        "POSTGRES_SSLMODE": "require",
    }


def test_all_connection_forms_target_staging_and_replace_inherited_production():
    values = credentials()
    inherited = {
        "POSTGRES_PASSWORD": "production-secret",
        "PGHOSTADDR": "203.0.113.1",
        "PGSERVICE": "production",
        "PGOPTIONS": "-c search_path=wrong",
        "DATABASE_URL": "production-url",
        "PATH": "/bin",
    }
    child = build_environment("staging", "writer", values, inherited, CONFIG)
    assert "PGHOSTADDR" not in child and "PGSERVICE" not in child
    assert child["PGHOST"] == child["POSTGRES_HOST"] == values["POSTGRES_HOST"]
    assert (
        child["PGPASSWORD"] == child["POSTGRES_PASSWORD"] == values["POSTGRES_PASSWORD"]
    )
    parsed = urlsplit(child["DATABASE_URL"])
    assert parsed.hostname == values["POSTGRES_HOST"]
    assert unquote(parsed.password) == values["POSTGRES_PASSWORD"]
    assert child["PATH"] == "/bin"
    assert child["ETL_ENVIRONMENT"] == "staging"


@pytest.mark.parametrize(
    "key,value",
    [
        (
            "POSTGRES_HOST",
            "ep-empty-star-ag0bt9wd-pooler.c-2.eu-central-1.aws.neon.tech",
        ),
        ("POSTGRES_USER", "neondb_owner"),
        ("POSTGRES_DB", "postgres"),
        ("POSTGRES_PORT", "5433"),
        ("POSTGRES_SSLMODE", "disable"),
        ("POSTGRES_PASSWORD", ""),
    ],
)
def test_wrong_target_or_incomplete_credentials_are_rejected(key, value):
    values = {**credentials(), key: value}
    with pytest.raises(ValueError):
        build_environment("staging", "writer", values, {}, CONFIG)


def test_missing_staging_password_never_falls_back_to_production():
    values = credentials()
    del values["POSTGRES_PASSWORD"]
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        build_environment("staging", "writer", values, credentials(), CONFIG)


def test_probe_ignores_inherited_libpq_routing_and_restores_it_on_failure(monkeypatch):
    monkeypatch.setenv("PGHOSTADDR", "203.0.113.1")
    monkeypatch.setenv("PGSERVICE", "production")
    child = build_environment("staging", "writer", credentials(), os.environ, CONFIG)

    def connect(url, **kwargs):
        assert "PGHOSTADDR" not in os.environ
        assert "PGSERVICE" not in os.environ
        assert url == child["DATABASE_URL"]
        raise run_in_environment.psycopg2.OperationalError("probe failed")

    monkeypatch.setattr(run_in_environment.psycopg2, "connect", connect)
    with pytest.raises(run_in_environment.psycopg2.OperationalError):
        run_in_environment.check_connection(child)
    assert os.environ["PGHOSTADDR"] == "203.0.113.1"
    assert os.environ["PGSERVICE"] == "production"


@pytest.mark.parametrize("failure", ["production_host", "missing_password"])
def test_cli_rejects_wrong_or_missing_credentials_before_starting_command(
    tmp_path, failure
):
    values = credentials()
    if failure == "production_host":
        values["POSTGRES_HOST"] = (
            "ep-empty-star-ag0bt9wd.c-2.eu-central-1.aws.neon.tech"
        )
    else:
        del values["POSTGRES_PASSWORD"]
    inherited = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("POSTGRES_")
    }
    sentinel = tmp_path / "command-ran"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "src/run_in_environment.py"),
            "--environment",
            "staging",
            "--from-process",
            "--",
            sys.executable,
            "-c",
            "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()",
            str(sentinel),
        ],
        env={**inherited, **values},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Environment check failed" in result.stderr
    assert credentials()["POSTGRES_PASSWORD"] not in result.stderr
    assert not sentinel.exists()
