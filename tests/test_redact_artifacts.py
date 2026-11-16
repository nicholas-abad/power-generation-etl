"""Secrets never reach uploaded workflow artifacts (src/redact_artifacts.py)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from redact_artifacts import main, redact, secret_values

FAKE_KEY = "0f1e2d3c-aaaa-bbbb-cccc-1234567890ab"


def test_a_failed_entsoe_request_url_loses_its_key(tmp_path, monkeypatch):
    monkeypatch.setenv("ENTSOE_API_KEY", FAKE_KEY)
    failures = tmp_path / "extracted_data" / "failures_2026-11-02_2026-11-04.json"
    failures.parent.mkdir()
    failures.write_text(
        '{"error": "599 for url: https://web-api.tp.entsoe.eu/api?'
        f'securityToken={FAKE_KEY}&documentType=A73"}}'
    )

    assert main([str(tmp_path / "extracted_data")]) == 0

    text = failures.read_text()
    assert FAKE_KEY not in text
    assert "securityToken=***&documentType=A73" in text


def test_a_key_outside_a_url_is_masked_when_it_is_in_the_environment(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("CL_API_KEY", FAKE_KEY)
    log = tmp_path / "logs" / "run.log"
    log.parent.mkdir()
    log.write_text(f"header Authorization: {FAKE_KEY}\n")

    main([str(tmp_path)])

    assert log.read_text() == "header Authorization: ***\n"


def test_a_token_parameter_is_masked_without_the_key_in_the_environment():
    assert redact(b"?securityToken=abc123&x=1", []) == b"?securityToken=***&x=1"


def test_short_or_unset_variables_are_not_treated_as_secrets():
    assert secret_values({"POSTGRES_PASSWORD": "short", "EIA_API_KEY": ""}) == []


def test_files_without_secrets_are_left_unchanged(tmp_path):
    data = tmp_path / "entsoe.jsonl"
    data.write_bytes(b'{"plant_name": "Turow 4", "generation_mw": 230.0}\n')
    before = data.stat().st_mtime_ns

    main([str(tmp_path)])

    assert data.stat().st_mtime_ns == before


def test_a_missing_path_is_not_an_error(tmp_path):
    assert main([str(tmp_path / "not-created")]) == 0
