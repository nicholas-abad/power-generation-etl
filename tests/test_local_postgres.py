"""Local-only routing must be enforced before destructive test/rehearsal SQL."""

import os
from pathlib import Path
import runpy

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "suffix",
    [
        " hostaddr=192.0.2.99",
        " service=production",
        " options='-c role=owner'",
    ],
)
def test_destructive_fixture_rejects_connection_overrides(monkeypatch, suffix):
    module = runpy.run_path(str(ROOT / "tests/test_entsoe_coal_hotfix.py"))
    monkeypatch.setenv(
        "COAL_TEST_PG_DSN",
        "host=localhost dbname=coal_hotfix_tests_guard user=postgres" + suffix,
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Rejected DSN reached the database connector")

    monkeypatch.setattr(psycopg2, "connect", forbidden)
    with pytest.raises(ValueError):
        next(module["pg"].__wrapped__())


def test_destructive_fixture_clears_inherited_routing(monkeypatch):
    module = runpy.run_path(str(ROOT / "tests/test_entsoe_coal_hotfix.py"))
    monkeypatch.setenv(
        "COAL_TEST_PG_DSN",
        "host=localhost dbname=coal_hotfix_tests_guard user=postgres",
    )
    for key in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"):
        monkeypatch.setenv(key, "must-not-be-used")

    class StopBeforeConnection(Exception):
        pass

    def intercepted(*args, **kwargs):
        assert not any(
            os.environ.get(k) for k in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")
        )
        assert os.environ["PYTHON_DOTENV_DISABLED"] == "1"
        raise StopBeforeConnection()

    monkeypatch.setattr(psycopg2, "connect", intercepted)
    with pytest.raises(StopBeforeConnection):
        next(module["pg"].__wrapped__())


def test_server_identity_is_checked_before_destructive_sql(monkeypatch):
    module = runpy.run_path(str(ROOT / "tests/test_entsoe_coal_hotfix.py"))
    monkeypatch.setenv(
        "COAL_TEST_PG_DSN",
        "host=127.0.0.1 dbname=coal_hotfix_tests_guard user=postgres",
    )
    statements = []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return self

        def execute(self, sql):
            statements.append(sql)

        def fetchone(self):
            return ("coal_hotfix_tests_guard", "192.0.2.99")

        def close(self):
            pass

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: FakeConnection())
    with pytest.raises(ValueError, match="expected local"):
        next(module["pg"].__wrapped__())
    assert statements == ["SELECT current_database(), inet_server_addr()::text"]
