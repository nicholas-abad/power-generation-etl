"""The certified OCCTO rebuild on a disposable local PostgreSQL (never Neon)."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

import psycopg2
import psycopg2.extensions
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import rebuild_occto as rebuild  # noqa: E402
from local_postgres import local_settings, verify_local_connection  # noqa: E402

TARGET = "local/test"
JAN, FEB, MAR = 1704067200000, 1706745600000, 1709251200000  # 2024-01/02/03-01 UTC
HALF = 1_800_000
OLD_RUN = "11111111-2222-3333-4444-555555555555"


class RecordingCursor(psycopg2.extensions.cursor):
    statements = []

    def execute(self, query, vars=None):
        RecordingCursor.statements.append(query)
        return super().execute(query, vars)

    def copy_expert(self, sql, file, size=8192):
        RecordingCursor.statements.append(sql)
        return super().copy_expert(sql, file, size)


@pytest.fixture
def pg():
    # CI provides COAL_TEST_PG_DSN (power_generation_test); locally any
    # occto_rebuild_tests_* database. The fixture recreates the schema.
    dsn = os.getenv("OCCTO_TEST_PG_DSN") or os.getenv("COAL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Set OCCTO_TEST_PG_DSN to a disposable local database")
    with patch.dict(os.environ):
        parts = local_settings(
            dsn, r"(?:occto_rebuild_tests_[a-z0-9_]+|power_generation_test)"
        )
        conn = psycopg2.connect(
            **parts, connect_timeout=5, cursor_factory=RecordingCursor
        )
        try:
            with conn:
                verify_local_connection(conn, parts["dbname"])
                with conn.cursor() as cur:
                    cur.execute("DROP SCHEMA IF EXISTS ingestion CASCADE")
                    cur.execute((ROOT / "schema/occto_generation.sql").read_text())
            RecordingCursor.statements = []
            yield conn
        finally:
            conn.close()


def canonical_line(
    ts, plant="砂川発電所", unit="３号機", kwh=1000, fuel=("03", "coal")
):
    return (
        "\t".join(
            map(
                str, (ts, plant, unit, "123", fuel[0], fuel[1], "01", "北海道", 30, kwh)
            )
        )
        + "\n"
    )


def coal_plant_lines(lines):
    """The lines of plants with a coal line, computed here independently of the tool."""
    coal = {line.split("\t")[1] for line in lines if line.split("\t")[5] == "coal"}
    return [line for line in lines if line.split("\t")[1] in coal]


def make_inputs(
    tmp_path,
    lines,
    *,
    b_digest=None,
    b_coal_digest=None,
    rejected=(),
    several_fuels=(),
    cut_report=True,
):
    canonical = tmp_path / "A.tsv.gz"
    with gzip.open(canonical, "wt", encoding="utf-8", newline="") as out:
        out.writelines(lines)
    digest = hashlib.sha256("".join(lines).encode()).hexdigest()
    certificate = {"ok": True, "digest": "cert", "end": "2024-02-29"}
    (tmp_path / "cert.json").write_text(json.dumps(certificate))
    report = {
        "digest": b_digest or digest,
        "certificate_digest": "cert",
        "load_start": "2024-01-01",
        "rejected": list(rejected),
        "duplicate_keys": [],
        "unit_months_with_several_fuels": list(several_fuels),
        "excluded_days": [],
        "gaps": 0,
    }
    if cut_report:
        kept = "".join(coal_plant_lines(lines))
        report["coal_plants_digest"] = (
            b_coal_digest or hashlib.sha256(kept.encode()).hexdigest()
        )
    (tmp_path / "B.json").write_text(json.dumps(report))
    return canonical, tmp_path / "B.json", tmp_path / "cert.json"


def build(tmp_path, lines, repair_id="occto-test-1", **kw):
    canonical, b, cert = make_inputs(tmp_path, lines, **kw)
    out = tmp_path / f"payload-{repair_id}"
    rebuild.build_payload(canonical, b, cert, out, repair_id, "2024-01-01")
    return rebuild.load_payload(out)


SOURCE = [canonical_line(JAN + i * HALF, kwh=1000 + i) for i in range(1, 4)] + [
    canonical_line(FEB + i * HALF, kwh=2000 + i) for i in range(1, 3)
]


def seed(conn, rows):
    """Stored rows: (timestamp_ms, plant, unit, mwh)."""
    with conn, conn.cursor() as cur:
        for ts, plant, unit, mwh in rows:
            cur.execute(
                f"INSERT INTO {rebuild.TABLE} (extraction_run_id, created_at_ms, plant, unit, plant_code,"
                " fuel_code, fuel_type, area_code, area_name, timestamp_ms, generation_mwh, resolution_minutes)"
                " VALUES (%s, 1, %s, %s, '123', '03', 'coal', NULL, '北海道', %s, %s, 30)",
                (OLD_RUN, plant, unit, ts, mwh),
            )


def table(conn):
    with conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT id, {rebuild.COLS} FROM {rebuild.TABLE} ORDER BY timestamp_ms, plant, unit"
        )
        return cur.fetchall()


# Stored history keyed one hour early (the Berlin-local bulk load), so its
# keys differ from the payload's: a rebuild must remove them, not add beside.
STORED = [
    (JAN + i * HALF - 2 * HALF, "砂川発電所", "３号機", 1.0 + i) for i in range(1, 4)
] + [(FEB + HALF, "砂川発電所", "３号機", 9.0)]


def planned(conn, manifest, sha, windows):
    return rebuild.plan(conn, manifest, sha, windows, TARGET)


# --- payload --------------------------------------------------------------


def test_payload_splits_months_and_converts_exactly(tmp_path):
    manifest, _, windows = build(tmp_path, SOURCE)
    assert [m["month"] for m in manifest["months"]] == ["2024-01", "2024-02"]
    assert [m["rows"] for m in manifest["months"]] == [3, 2]
    assert manifest["months"][0]["mwh"] == "3.006"
    assert windows[0]["start"] == 0 and windows[0]["end"] == FEB
    # exclusive end just past the archive end day's 24:00 slot
    assert windows[1]["end"] == MAR + 1


@pytest.mark.parametrize(
    "kw,message",
    [
        ({"b_digest": "0" * 64}, "disagree"),
        ({"rejected": [{"reason": "negative value"}]}, "rejected"),
        (
            {"several_fuels": [[["2024-01", "p", "u"], ["coal", "unspecified"]]]},
            "unit_months_with_several_fuels",
        ),
    ],
)
def test_payload_refuses_without_independent_agreement(tmp_path, kw, message):
    with pytest.raises(ValueError, match=message):
        build(tmp_path, SOURCE, **kw)


GAS = ("04", "gas")
# Two coal plants and one gas-only plant: 碧南 burns coal and gas, 新潟 has gas in
# January and coal only in February, 横浜 never burns coal.
MIXED = sorted(
    [
        canonical_line(JAN + HALF, "碧南火力発電所", "１号機", 500),
        canonical_line(JAN + HALF, "碧南火力発電所", "Ｇ１", 40, GAS),
        canonical_line(JAN + HALF, "横浜火力発電所", "７号機", 70, GAS),
        canonical_line(JAN + HALF, "新潟火力発電所", "Ｇ１", 30, GAS),
        canonical_line(FEB + HALF, "碧南火力発電所", "１号機", 510),
        canonical_line(FEB + HALF, "横浜火力発電所", "７号機", 71, GAS),
        canonical_line(FEB + HALF, "新潟火力発電所", "１号機", 300),
    ],
    key=lambda line: (
        int(line.split("\t")[0]),
        line.split("\t")[1],
        line.split("\t")[2],
    ),
)


def test_payload_keeps_every_row_of_coal_plants_and_leaves_out_the_rest(tmp_path):
    manifest, _, windows = build(tmp_path, MIXED)
    kept = coal_plant_lines(MIXED)
    assert manifest["plants"] == "coal" and manifest["coal_plants"] == 2
    assert sum(m["rows"] for m in manifest["months"]) == len(kept) == 5
    assert (
        manifest["kept_canonical_sha256"]
        == hashlib.sha256("".join(kept).encode()).hexdigest()
    )
    # the gas-only plant is left out; the coal plants' gas units stay
    assert manifest["left_out"] == {"gas": {"rows": 2, "mwh": "0.141"}}
    assert manifest["source_coal"] == {"rows": 3, "mwh": "1.31"}
    payload_plants = set()
    for w in windows:
        with gzip.open(w["file"], "rt", encoding="utf-8") as f:
            next(f)
            payload_plants |= {line.split("\t")[1] for line in f}
    assert payload_plants == {"碧南火力発電所", "新潟火力発電所"}


def test_payload_refuses_without_agreement_on_the_cut(tmp_path):
    with pytest.raises(ValueError, match="coal-plant cut"):
        build(tmp_path, MIXED, b_coal_digest="0" * 64)


def test_payload_refuses_a_parser_b_report_without_the_cut(tmp_path):
    with pytest.raises(ValueError, match="predates the coal-plant cut"):
        build(tmp_path, MIXED, cut_report=False)


# --- plan / apply / rollback --------------------------------------------


def test_apply_replaces_windows_and_rollback_restores_exactly(pg, tmp_path):
    seed(pg, STORED)
    before = table(pg)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    assert [w["stored_rows"] for w in approved["windows"]] == [3, 1]

    results = rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert [r["action"] for r in results] == ["applied", "applied"]
    rows = table(pg)
    assert [(r[1], r[10]) for r in rows] == [
        (JAN + HALF, 1.001),
        (JAN + 2 * HALF, 1.002),
        (JAN + 3 * HALF, 1.003),
        (FEB + HALF, 2.001),
        (FEB + 2 * HALF, 2.002),
    ]
    assert {r[4] for r in rows} == {"123"} and {r[7] for r in rows} == {
        "01"
    }  # area_code filled

    # repeated apply is a no-op
    again = rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert [r["action"] for r in again] == ["already applied"] * 2

    rebuild.rollback(pg, manifest, sha, windows, approved, TARGET)
    assert table(pg) == before  # same ids and values


def test_a_coal_plant_payload_loads_exactly_and_rolls_back(pg, tmp_path):
    seed(pg, STORED)
    before = table(pg)
    manifest, sha, windows = build(tmp_path, MIXED)
    approved = planned(pg, manifest, sha, windows)
    rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    # what verify_load.sh exports must equal parser B's coal-plant lines
    with pg, pg.cursor() as cur:
        cur.execute(
            "SELECT timestamp_ms, plant, unit, plant_code, fuel_code, fuel_type, area_code,"
            " area_name, resolution_minutes, round(generation_mwh::numeric * 1000)::bigint"
            f" FROM {rebuild.TABLE} ORDER BY {rebuild.ORDER}"
        )
        exported = "".join("\t".join(map(str, r)) + "\n" for r in cur.fetchall())
        cur.execute(f"SELECT count(*) FROM {rebuild.TABLE} WHERE fuel_type = 'coal'")
        coal_rows = cur.fetchone()[0]
    assert exported == "".join(coal_plant_lines(MIXED))
    assert coal_rows == manifest["source_coal"]["rows"]
    rebuild.rollback(pg, manifest, sha, windows, approved, TARGET)
    assert table(pg) == before


def test_apply_refuses_when_stored_rows_changed_after_plan(pg, tmp_path):
    seed(pg, STORED)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    seed(pg, [(FEB + 5 * HALF, "新規発電所", "1号機", 3.0)])  # another session writes
    with pytest.raises(ValueError, match="changed since the plan"):
        rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert len(table(pg)) == len(STORED) + 1  # nothing written
    # the refusal did not bind the repair id: a fresh plan applies
    fresh = planned(pg, manifest, sha, windows)
    results = rebuild.apply(pg, manifest, sha, windows, fresh, TARGET)
    assert [r["action"] for r in results] == ["applied", "applied"]


def test_failed_window_leaves_earlier_windows_and_resumes(pg, tmp_path):
    seed(pg, STORED)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    real = rebuild.apply_window

    def fail_february(cursor, manifest_, window, expected):
        if window["month"] == "2024-02":
            raise ValueError("simulated failure")
        return real(cursor, manifest_, window, expected)

    with patch.object(rebuild, "apply_window", fail_february):
        with pytest.raises(ValueError, match="simulated"):
            rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert [r[10] for r in table(pg)] == [
        1.001,
        1.002,
        1.003,
        9.0,
    ]  # Jan applied, Feb untouched
    results = rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert [r["action"] for r in results] == ["already applied", "applied"]


def test_rollback_refuses_after_a_later_write(pg, tmp_path):
    seed(pg, STORED)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    seed(pg, [(JAN + 20 * HALF, "新規発電所", "1号機", 3.0)])  # the older window
    after_write = table(pg)
    with pytest.raises(ValueError, match="later writes"):
        rebuild.rollback(pg, manifest, sha, windows, approved, TARGET)
    assert table(pg) == after_write  # February was not rolled back either


def test_plan_bound_to_its_manifest_and_target(pg, tmp_path):
    seed(pg, STORED)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    with pytest.raises(ValueError, match="another target"):
        rebuild.apply(pg, manifest, sha, windows, approved, "staging/other/neondb")
    tampered = dict(approved, retained_rows_after_end=7)
    with pytest.raises(ValueError, match="fingerprint"):
        rebuild.apply(pg, manifest, sha, windows, tampered, TARGET)


def test_rows_after_the_certified_end_are_retained(pg, tmp_path):
    late = (MAR + 7 * HALF, "砂川発電所", "３号機", 5.0)
    seed(pg, STORED + [late])
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    assert approved["retained_rows_after_end"] == 1
    rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    assert table(pg)[-1][1] == late[0]


def test_every_statement_targets_only_occto_objects(pg, tmp_path):
    seed(pg, STORED)
    manifest, sha, windows = build(tmp_path, SOURCE)
    approved = planned(pg, manifest, sha, windows)
    RecordingCursor.statements = []
    rebuild.apply(pg, manifest, sha, windows, approved, TARGET)
    rebuild.rollback(pg, manifest, sha, windows, approved, TARGET)
    allowed = {
        rebuild.TABLE,
        rebuild.HEADER,
        rebuild.WINDOWS,
        rebuild.BACKUP,
        "_occto_payload",
        "information_schema.columns",
        "pg_roles",
    }
    pattern = re.compile(
        r"\b(?:FROM|INTO|UPDATE|ON|TABLE(?:\s+IF(?:\s+NOT)?\s+EXISTS)?)\s+([a-z_][a-z0-9_.]*)",
        re.I,
    )
    targets = set()
    for statement in RecordingCursor.statements:
        text = statement.decode() if isinstance(statement, bytes) else statement
        targets |= {
            m
            for m in pattern.findall(text)
            if m.upper() not in {"STDIN", "PUBLIC", "SELECT"}
        }
    targets -= {"moved"}  # the DELETE ... RETURNING CTE inside apply_window
    # REVOKE ... FROM <role> names roles, not tables (only where the roles exist)
    targets -= {"etl_writer", "dashboard_ro"}
    assert targets <= allowed, targets - allowed


# --- target guard --------------------------------------------------------


def staging_env(**overrides):
    config = json.loads((ROOT / "config/environments.json").read_text())
    st = config["environments"]["staging"]
    env = {
        "ETL_ENVIRONMENT": "staging",
        "NEON_ENDPOINT_ID": st["endpoint_id"],
        "NEON_BRANCH_ID": st["branch_id"],
        "DIRECT_DATABASE_URL": f"postgresql://{config['roles']['owner']}:x@{st['endpoint_id']}."
        f"{config['proxy_host']}:5432/{config['database']}?sslmode=require",
    }
    env.update(overrides)
    return env, config


def production_env(**overrides):
    _, config = staging_env()
    prod = config["environments"]["production"]
    return staging_env(
        ETL_ENVIRONMENT="production",
        NEON_ENDPOINT_ID=prod["endpoint_id"],
        NEON_BRANCH_ID=prod["branch_id"],
        DIRECT_DATABASE_URL=f"postgresql://{config['roles']['owner']}:x@{prod['endpoint_id']}."
        f"{config['proxy_host']}:5432/{config['database']}?sslmode=require",
        **overrides,
    )[0]


def guard(environment, allow_production=False):
    return rebuild.checked_dsn(environment, allow_production, "DIRECT_DATABASE_URL")


def test_guard_takes_the_pinned_owner_and_production_only_when_allowed():
    with patch.dict(os.environ, staging_env()[0], clear=True):
        assert guard("staging")[1].startswith("staging/")
    with patch.dict(os.environ, production_env(), clear=True):
        assert guard("production", True)[1].startswith("production/")
        with pytest.raises(ValueError, match="allow-production"):
            guard("production")
    # the wrapper pinned one environment; the command names the other
    with patch.dict(os.environ, staging_env()[0], clear=True):
        with pytest.raises(ValueError, match="pinned environment owner wrapper"):
            guard("production", True)
    with patch.dict(os.environ, production_env(), clear=True):
        with pytest.raises(ValueError, match="pinned environment owner wrapper"):
            guard("staging")


def run_plan(tmp_path, payload, *flags):
    return rebuild.main(
        [
            "plan",
            "--payload",
            str(payload),
            "--report",
            str(tmp_path / "r.json"),
            *flags,
        ]
    )


def test_main_refuses_production_before_connecting(tmp_path):
    manifest, _, _ = build(tmp_path, SOURCE)
    payload = tmp_path / f"payload-{manifest['repair_id']}"
    for env, flags in (
        (production_env(), ("--environment", "production")),
        (staging_env()[0], ("--environment", "production", "--allow-production")),
        (production_env(), ("--environment", "staging")),
    ):
        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(rebuild.psycopg2, "connect") as connect,
        ):
            assert run_plan(tmp_path, payload, *flags) == 1
            connect.assert_not_called()


def test_an_all_plants_payload_is_refused_on_production_only(tmp_path, capsys):
    manifest, _, _ = build(tmp_path, SOURCE)
    payload = tmp_path / f"payload-{manifest['repair_id']}"
    old = json.loads((payload / "manifest.json").read_text())
    del old["plants"]  # a manifest written before the cut (the 2026-10 staging repair)
    (payload / "manifest.json").write_text(json.dumps(old, indent=1) + "\n")
    with (
        patch.dict(os.environ, production_env(), clear=True),
        patch.object(rebuild.psycopg2, "connect") as connect,
    ):
        assert (
            run_plan(
                tmp_path, payload, "--environment", "production", "--allow-production"
            )
            == 1
        )
        connect.assert_not_called()
    assert "coal-plant payload" in capsys.readouterr().out
    # on staging it still loads (its rollback must keep working): it reaches the guard
    with (
        patch.dict(os.environ, staging_env()[0], clear=True),
        patch.object(
            rebuild, "checked_dsn", side_effect=ValueError("reached the guard")
        ),
    ):
        assert run_plan(tmp_path, payload, "--environment", "staging") == 1
    assert "reached the guard" in capsys.readouterr().out


def test_guard_refuses_pooler_and_non_owner():
    env, config = staging_env()
    st = config["environments"]["staging"]
    for url in (
        env["DIRECT_DATABASE_URL"].replace(
            st["endpoint_id"] + ".", st["endpoint_id"] + "-pooler."
        ),
        env["DIRECT_DATABASE_URL"].replace(
            config["roles"]["owner"], config["roles"]["writer"]
        ),
    ):
        with patch.dict(os.environ, dict(env, DIRECT_DATABASE_URL=url), clear=True):
            with pytest.raises(ValueError, match="pinned direct owner"):
                guard("staging")
