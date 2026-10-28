"""Parser B's coal-plant cut and verify_load.sh's target handling (no database)."""

import gzip
import hashlib
import importlib.util
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
VALIDATION = ROOT / "docs" / "validation" / "occto"


def parser_b():
    spec = importlib.util.spec_from_file_location(
        "independent_parse", VALIDATION / "independent_parse.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parser_b_cut_keeps_every_line_of_the_named_plants(tmp_path):
    lines = [
        "1\t碧南火力発電所\t１号機\t1\t03\tcoal\t05\t中部\t30\t500\n",
        "1\t碧南火力発電所\tＧ１\t1\t04\tgas\t05\t中部\t30\t40\n",
        "1\t横浜火力発電所\t７号機\t2\t04\tgas\t03\t東京\t30\t70\n",
    ]
    canonical = tmp_path / "canonical.tsv.gz"
    with gzip.open(canonical, "wt", encoding="utf-8", newline="") as out:
        out.writelines(lines)
    cut = tmp_path / "cut.tsv.gz"
    digest, rows = parser_b().coal_plant_cut(canonical, cut, {"碧南火力発電所"})
    assert rows == 2
    assert digest == hashlib.sha256("".join(lines[:2]).encode()).hexdigest()
    assert gzip.open(cut, "rt", encoding="utf-8").read() == "".join(lines[:2])


def run_verify(*args):
    # PATH without uv: the script must refuse before it reaches any database call
    return subprocess.run(
        ["bash", str(VALIDATION / "verify_load.sh"), *args],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
        cwd=ROOT,
    )


def test_verify_load_needs_allow_production_before_any_query(tmp_path):
    result = run_verify("production", "b", "p", str(tmp_path / "out"))
    assert result.returncode == 2
    assert "--allow-production" in result.stdout
    assert not (tmp_path / "out").exists()


def test_verify_load_refuses_an_unknown_target():
    assert run_verify("prod", "b", "p", "o").returncode == 2
    assert run_verify("staging").returncode == 2
