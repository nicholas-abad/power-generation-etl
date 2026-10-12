"""The certified coal rebuild on a disposable local PostgreSQL (never Neon)."""

from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from unittest.mock import patch

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import entsoe_coal  # noqa: E402
import rebuild_entsoe_coal as rebuild  # noqa: E402

REAL_ETL_COMMIT = rebuild.etl_commit  # the fixture below stubs it
from local_postgres import local_settings, verify_local_connection  # noqa: E402

RUN = "11111111-2222-3333-4444-555555555555"
MARCH, APRIL, MAY = 1709251200000, 1711929600000, 1714521600000
HOUR = 3600000


@pytest.fixture(autouse=True)
def pl_rebuilt(monkeypatch):
    monkeypatch.setattr(entsoe_coal, "REBUILT", frozenset({"PL"}))
    # Tests run from a working tree with local changes; pin a commit.
    monkeypatch.setattr(rebuild, "etl_commit", lambda: "test-commit")


@pytest.fixture
def pg():
    dsn = os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set COAL_TEST_PG_DSN to a disposable local database")
    with patch.dict(os.environ):
        parts = local_settings(
            dsn, r"(?:coal_hotfix_tests_[a-z0-9_]+|power_generation_test)"
        )
        conn = psycopg2.connect(**parts, connect_timeout=5)
        try:
            with conn:
                verify_local_connection(conn, parts["dbname"])
                with conn.cursor() as cur:
                    cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
                    cur.execute((ROOT / "schema/entsoe_generation.sql").read_text())
                    cur.execute(
                        "CREATE TABLE ingestion.schema_migrations (version text PRIMARY KEY)"
                    )
                    cur.execute(
                        "INSERT INTO ingestion.schema_migrations VALUES ('018')"
                    )
                    cur.execute("SET search_path=ingestion,public")
            yield conn
        finally:
            conn.close()


def stored(
    ts, name="Belchatow 2", mw=100.0, minutes=15, country="PL", psr="B02", eic=None
):
    return dict(
        timestamp_ms=ts,
        country_code=country,
        psr_type=psr,
        plant_name=name,
        fuel_type="Fossil Brown coal/Lignite",
        data_type="Unknown",
        generation_mw=mw,
        resolution_minutes=minutes,
        unit_eic=eic,
        production_unit_eic=None,
        source_unit_name=None,
        extraction_run_id=RUN,
        created_at_ms=1,
    )


def insert(conn, *rows):
    with conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"INSERT INTO ingestion.entsoe_generation_data ({','.join(row)})"
                f" VALUES ({','.join(['%s'] * len(row))})",
                list(row.values()),
            )


def table(conn):
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, to_jsonb(t) - 'id' FROM ingestion.entsoe_generation_data t"
            " ORDER BY timestamp_ms, country_code, psr_type, plant_name"
        )
        return cur.fetchall()


def line(ts, mw="100", name="Belchatow 2", eic="19W000000000095U", psr="B02"):
    return "\t".join(
        map(
            str,
            [
                ts,
                "PL",
                psr,
                name,
                "Fossil Brown coal/Lignite",
                "Actual Aggregated",
                mw,
                60,
                eic,
                "\\N",
                name,
                "99999999-8888-7777-6666-555555555555",
                2,
            ],
        )
    )


def payload(
    tmp_path, months, end="2024-05-01", psrs=("B02",), repair_id="test-rebuild"
):
    root = tmp_path / "payload"
    root.mkdir(exist_ok=True)
    entries = []
    for month, lines in months.items():
        lines = sorted(lines, key=lambda x: (int(x.split("\t")[0]), x.split("\t")[3]))
        data = (
            "\t".join(rebuild.COLUMNS) + "\n" + "".join(x + "\n" for x in lines)
        ).encode()
        path = root / f"{month}.tsv.gz"
        path.write_bytes(gzip.compress(data, mtime=0))
        mwh = sum((Decimal(x.split("\t")[6]) for x in lines), Decimal(0))
        entries.append(
            {
                "month": month,
                "file": path.name,
                "rows": len(lines),
                "mwh": format(mwh.normalize(), "f"),
                "excluded": [],
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    manifest = {
        "repair_id": repair_id,
        "country": "PL",
        "psrs": list(psrs),
        "start": "2024-03-01",
        "end": end,
        "columns": list(rebuild.COLUMNS),
        "inputs": {"test": True},
        "months": entries,
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


def prepared(tmp_path, conn, months, **kw):
    manifest, sha, windows = rebuild.load_payload(payload(tmp_path, months, **kw))
    report = rebuild.plan(conn, manifest, sha, windows, "local")
    return manifest, sha, windows, report


MONTHS = {
    "2024-03": [line(MARCH), line(MARCH + HOUR, "200")],
    "2024-04": [line(APRIL, "50")],
}


def test_apply_replaces_window_exactly_and_repeats_as_a_noop(pg, tmp_path):
    insert(
        pg,
        stored(MARCH),
        stored(MARCH + HOUR, mw=200.0),
        stored(APRIL, mw=50.0),
        stored(MAY + HOUR, mw=7.0),  # after the certified end: the tail window
        stored(MARCH, country="CZ"),
        stored(MARCH, psr="B04", name="Gas"),
    )
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert [w["stored_rows"] for w in report["windows"]] == [2, 1, 1]
    assert not any(report["blocking"].values())
    results = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["action"] for r in results] == ["applied"] * 3
    rows = [r for _, r in table(pg)]
    pl_coal = [r for r in rows if r["country_code"] == "PL" and r["psr_type"] == "B02"]
    assert [
        (r["generation_mw"], r["resolution_minutes"], r["unit_eic"]) for r in pl_coal
    ] == [
        (100, 60, "19W000000000095U"),
        (200, 60, "19W000000000095U"),
        (50, 60, "19W000000000095U"),
    ]
    assert {(r["country_code"], r["psr_type"]) for r in rows} == {
        ("PL", "B02"),
        ("CZ", "B02"),
        ("PL", "B04"),
    }
    before = table(pg)
    again = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["written"] for r in again] == [0, 0, 0] and table(pg) == before


def test_changed_stored_rows_after_plan_abort_without_writes(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    insert(pg, stored(APRIL, name="Late arrival"))
    before = table(pg)
    with pytest.raises(ValueError, match="changed since the plan"):
        rebuild.apply(pg, manifest, sha, windows, report)
    assert [r for r in table(pg) if r[1]["country_code"] == "PL"] == [
        r for r in before if r[1]["country_code"] == "PL"
    ]


def test_tampered_payload_file_is_rejected(tmp_path):
    root = payload(tmp_path, MONTHS)
    path = root / "2024-03.tsv.gz"
    path.write_bytes(
        gzip.compress(
            gzip.decompress(path.read_bytes()).replace(b"\t200\t", b"\t201\t")
        )
    )
    with pytest.raises(ValueError, match="changed"):
        rebuild.load_payload(root)


def test_payload_must_cover_every_month_of_the_window(tmp_path):
    with pytest.raises(ValueError, match="do not cover"):
        rebuild.load_payload(payload(tmp_path, {"2024-03": [line(MARCH)]}))


def test_rollback_restores_original_rows_and_ids(pg, tmp_path):
    insert(
        pg,
        stored(MARCH),
        stored(APRIL, name="Legacy Name", mw=12.5),
        stored(MAY + HOUR),
    )
    original = table(pg)
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    assert table(pg) != original
    results = rebuild.rollback(pg, manifest, sha, windows, report)
    assert [r["action"] for r in results] == ["rolled back"] * 3
    assert table(pg) == original
    with pytest.raises(ValueError, match="rolled-back windows; use a new repair id"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_rollback_after_a_later_write_is_refused(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    with pg, pg.cursor() as cur:
        cur.execute(
            "UPDATE ingestion.entsoe_generation_data SET generation_mw=1"
            " WHERE timestamp_ms=%s AND country_code='PL'",
            (APRIL,),
        )
    before = table(pg)
    with pytest.raises(ValueError, match="later writes; rollback refused"):
        rebuild.rollback(pg, manifest, sha, windows, report)
    assert table(pg) == before


def test_resume_is_bound_to_one_payload(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report, only={"2024-03"})
    rest = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["action"] for r in rest] == ["already applied", "applied", "applied"]
    changed = {
        "2024-03": [line(MARCH, "101"), line(MARCH + HOUR, "200")],
        "2024-04": MONTHS["2024-04"],
    }
    other = tmp_path / "other"
    other.mkdir()
    m2, sha2, w2, r2 = prepared(other, pg, changed)
    with pytest.raises(ValueError, match="bound to another manifest or plan"):
        rebuild.apply(pg, m2, sha2, w2, r2)


def test_unit_time_collision_with_a_retained_row_blocks(pg, tmp_path):
    insert(pg, stored(MARCH, psr="B04", name="Gas", eic="19W000000000095U"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["unit_time_collisions"] == [
        {"month": "2024-03", "rows": 1}
    ]
    with pytest.raises(ValueError, match="blocking findings"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_stored_coal_in_an_uncertified_psr_blocks(pg, tmp_path):
    insert(pg, stored(MARCH, psr="B05", name="Hard coal unit"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["stored_rows_in_other_coal_psrs"] == {"B05": 1}
    with pytest.raises(ValueError, match="blocking findings"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_missing_migration_018_fails_preflight(pg, tmp_path):
    with pg, pg.cursor() as cur:
        cur.execute("DELETE FROM ingestion.schema_migrations")
    manifest, sha, windows = rebuild.load_payload(payload(tmp_path, MONTHS))
    with pytest.raises(ValueError, match="018"):
        rebuild.plan(pg, manifest, sha, windows, "local")


def test_production_is_refused_before_connecting():
    with pytest.raises(ValueError, match="--allow-production"):
        rebuild.checked_dsn("production", False)


def test_tail_is_refused_when_the_weekly_job_would_resume_after_it(pg, tmp_path):
    # Another country's data reaches the last hour of May, so the weekly job
    # resumes in June and would never refill PL's May tail.
    end_of_may = 1717196400000  # 2024-05-31T23:00Z
    insert(pg, stored(MARCH), stored(MAY + HOUR), stored(end_of_may, country="CZ"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    before = table(pg)
    with pytest.raises(ValueError, match="would resume from 2024-06"):
        rebuild.apply(pg, manifest, sha, windows, report)
    assert table(pg) == before
    # The certified months alone may be applied, but only as a recorded deferral.
    with pytest.raises(ValueError, match="leaves the tail"):
        rebuild.apply(pg, manifest, sha, windows, report, only={"2024-03", "2024-04"})
    rebuild.apply(
        pg, manifest, sha, windows, report, only={"2024-03", "2024-04"}, defer_tail=True
    )
    with pg, pg.cursor() as cur:
        cur.execute("SELECT tail_state FROM ingestion.entsoe_coal_rebuilds")
        assert cur.fetchone()[0] == "deferred"


def test_applying_the_tail_records_the_weekly_resume_month(pg, tmp_path):
    insert(pg, stored(MARCH), stored(MAY + HOUR))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    results = rebuild.apply(pg, manifest, sha, windows, report)
    assert results[-1]["weekly_resume_month"] == "2024-04"
    with pg, pg.cursor() as cur:
        cur.execute("SELECT tail_state FROM ingestion.entsoe_coal_rebuilds")
        assert cur.fetchone()[0] == "applied"


def test_refilled_tail_does_not_block_resume_and_rollback_says_how(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    insert(
        pg, stored(MAY + HOUR, name="Belchatow 2", mw=9.0, minutes=60)
    )  # weekly refill
    again = rebuild.apply(pg, manifest, sha, windows, report)
    assert again[-1]["action"] == "already applied; refilled by later loads"
    with pytest.raises(
        ValueError, match="roll back the certified months with --months"
    ):
        rebuild.rollback(pg, manifest, sha, windows, report)
    done = rebuild.rollback(
        pg, manifest, sha, windows, report, only={"2024-03", "2024-04"}
    )
    assert [r["action"] for r in done] == ["rolled back", "rolled back"]


def test_rollback_needs_the_approved_plan_of_this_repair(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    other = tmp_path / "other"
    other.mkdir()
    _, _, _, foreign = prepared(other, pg, MONTHS, repair_id="another-rebuild")
    before = table(pg)
    with pytest.raises(ValueError, match="another repair or manifest"):
        rebuild.rollback(pg, manifest, sha, windows, foreign)
    tampered = dict(report, windows=report["windows"][:-1])
    with pytest.raises(ValueError, match="fingerprint does not match"):
        rebuild.rollback(pg, manifest, sha, windows, tampered)
    assert table(pg) == before


def test_country_not_configured_as_rebuilt_blocks(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(entsoe_coal, "REBUILT", frozenset())
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["country_not_configured_as_rebuilt"] == ["PL"]
    with pytest.raises(ValueError, match="blocking findings"):
        rebuild.apply(pg, manifest, sha, windows, report)


@pytest.mark.parametrize("value", ["2024-13", "typo", "2024-04..2024-03"])
def test_malformed_month_selection_is_rejected(value):
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        rebuild.months_arg(value)


def test_month_selection_matching_nothing_is_an_error(tmp_path):
    _, _, windows = rebuild.load_payload(payload(tmp_path, MONTHS))
    with pytest.raises(ValueError, match="matches no window"):
        rebuild.select(windows, ("2025-01", "2025-02"))
    assert rebuild.select(windows, None) is None
    assert rebuild.select(windows, ("2024-04", "2024-05")) == {
        "2024-04",
        "2024-05-tail",
    }


def test_certified_start_must_be_a_month_boundary(tmp_path):
    root = payload(tmp_path, MONTHS)
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["start"] = "2024-03-15"
    (root / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="start must be the first day of a month"):
        rebuild.load_payload(root)


def test_float_text_form_does_not_cause_a_false_mismatch(pg, tmp_path):
    # PostgreSQL prints 0.00001 as 1e-05; staging compares MW as a float.
    months = {"2024-03": [line(MARCH, "0.00001")], "2024-04": [line(APRIL, "50")]}
    root = payload(tmp_path, months)
    manifest = json.loads((root / "manifest.json").read_text())
    (root / "manifest.json").write_text(json.dumps(manifest))
    insert(pg, stored(MARCH))
    manifest, sha, windows = rebuild.load_payload(root)
    report = rebuild.plan(pg, manifest, sha, windows, "local")
    applied = rebuild.apply(pg, manifest, sha, windows, report)
    assert [r["action"] for r in applied] == ["applied"] * 3


def test_views_state_returns_to_pending_after_changes(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report, only={"2024-03"})

    def views_state():
        with pg, pg.cursor() as cur:
            cur.execute("SELECT views_state FROM ingestion.entsoe_coal_rebuilds")
            return cur.fetchone()[0]

    assert views_state() == "pending"
    with pg, pg.cursor() as cur:
        cur.execute("UPDATE ingestion.entsoe_coal_rebuilds SET views_state='refreshed'")
    rebuild.apply(pg, manifest, sha, windows, report, only={"2024-04"}, defer_tail=True)
    assert views_state() == "pending"


def entsoe_views(conn):
    """Create only the three ENTSO-E views (and their unique indexes) from the
    repo's schema files."""
    statements = []
    pattern = re.compile(r"CREATE (MATERIALIZED VIEW|UNIQUE INDEX)\b.*", re.S | re.I)
    for name in (
        "materialized_views.sql",
        "row_count_views.sql",
        "entsoe_unit_monthly.sql",
    ):
        for statement in (ROOT / "schema" / name).read_text().split(";"):
            body = "\n".join(
                line
                for line in statement.splitlines()
                if not line.strip().startswith("--")
            )
            match = pattern.search(body)
            if match and any(view in match.group(0) for view in rebuild.VIEWS):
                statements.append(match.group(0))
    with conn, conn.cursor() as cur:
        cur.execute("SET search_path=public,ingestion")
        for statement in statements:
            cur.execute(statement)
    return len(statements)


def test_views_refresh_concurrently_and_mark_refreshed(pg, tmp_path):
    assert entsoe_views(pg) == 6
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    rebuild.refresh_views(pg, manifest["repair_id"])
    with pg, pg.cursor() as cur:
        cur.execute("SELECT views_state FROM ingestion.entsoe_coal_rebuilds")
        assert cur.fetchone()[0] == "refreshed"
        cur.execute("SELECT count(*) FROM public.mv_entsoe_unit_monthly")
        assert cur.fetchone()[0] > 0


def test_cli_refusal_writes_a_partial_report(tmp_path):
    root = payload(tmp_path, MONTHS)
    report = tmp_path / "report.json"
    code = rebuild.main(
        [
            "--environment",
            "staging",
            "--payload",
            str(root),
            "--months",
            "2030-01",
            "--report",
            str(report),
        ]
    )
    written = json.loads(report.read_text())
    assert code == 1 and written["completed"] == []
    assert "matches no window" in written["error"]


def header(pg, column):
    with pg, pg.cursor() as cur:
        cur.execute(f"SELECT {column} FROM ingestion.entsoe_coal_rebuilds")
        return cur.fetchone()[0]


def test_defer_tail_alone_leaves_the_tail_untouched(pg, tmp_path):
    insert(pg, stored(MARCH), stored(MAY + HOUR, name="Tail row"))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    results = rebuild.apply(pg, manifest, sha, windows, report, defer_tail=True)
    assert [r["month"] for r in results] == ["2024-03", "2024-04"]
    assert header(pg, "tail_state") == "deferred"
    assert any(r["plant_name"] == "Tail row" for _, r in table(pg))
    with pytest.raises(ValueError, match="cannot be combined"):
        rebuild.apply(
            pg, manifest, sha, windows, report, only={"2024-05-tail"}, defer_tail=True
        )


def test_rolled_back_tail_is_recorded(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    assert header(pg, "tail_state") == "applied"
    rebuild.rollback(pg, manifest, sha, windows, report, only={"2024-05-tail"})
    assert header(pg, "tail_state") == "rolled_back"


def test_an_older_ledger_is_upgraded(pg, tmp_path):
    with pg, pg.cursor() as cur:
        cur.execute(
            "CREATE TABLE ingestion.entsoe_coal_rebuilds (repair_id text PRIMARY KEY,"
            " manifest_sha256 text NOT NULL, plan_sha256 text NOT NULL,"
            " created_at timestamptz NOT NULL DEFAULT now())"
        )
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    rebuild.apply(pg, manifest, sha, windows, report)
    assert (header(pg, "tail_state"), header(pg, "views_state")) == (
        "applied",
        "pending",
    )


def test_a_different_scope_config_is_refused(pg, tmp_path):
    insert(pg, stored(MARCH))
    manifest, sha, windows, report = prepared(tmp_path, pg, MONTHS)
    report = dict(report, etl=dict(report["etl"], scope_config_sha256="0" * 64))
    report["plan_sha256"] = rebuild.digest(
        {k: v for k, v in report.items() if k != "plan_sha256"}
    )
    with pytest.raises(ValueError, match="differs from the one the plan was made with"):
        rebuild.apply(pg, manifest, sha, windows, report)


def test_unknown_or_modified_checkout_blocks_the_plan(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(rebuild, "etl_commit", lambda: None)
    insert(pg, stored(MARCH))
    _, _, _, report = prepared(tmp_path, pg, MONTHS)
    assert report["blocking"]["etl_checkout_unknown_or_modified"]


def test_etl_commit_sees_changes_in_src_and_config(tmp_path):
    def git(*args):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)

    for name in ("src/tool.py", "config/scopes.json", "README.md"):
        (tmp_path / name).parent.mkdir(exist_ok=True)
        (tmp_path / name).write_text("x\n")
    git("init", "-q")
    git("add", ".")
    git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    commit = REAL_ETL_COMMIT(tmp_path)
    assert commit and len(commit) == 40
    (tmp_path / "README.md").write_text("elsewhere\n")
    assert REAL_ETL_COMMIT(tmp_path) == commit
    (tmp_path / "src/tool.py").write_text("changed\n")
    assert REAL_ETL_COMMIT(tmp_path) is None
    git("checkout", "-q", "--", "src")
    (tmp_path / "config/new.json").write_text("{}\n")
    assert REAL_ETL_COMMIT(tmp_path) is None
    # By default it checks the repository root, where src/ and config/ live.
    root = REAL_ETL_COMMIT.__defaults__[0]
    assert (root / "src").is_dir() and (root / "config").is_dir()
