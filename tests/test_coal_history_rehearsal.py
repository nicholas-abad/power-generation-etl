"""Reject a substituted source snapshot before any local database is created."""

import gzip
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("mismatch", ["sha256", "snapshot_at", "branch_id"])
def test_rehearsal_requires_the_audited_seed_before_connecting(
    tmp_path, monkeypatch, mismatch
):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    module = importlib.import_module("rehearse_coal_history_candidates")
    audit_dir, staging = tmp_path / "audit", tmp_path / "staging"
    audit_dir.mkdir()
    staging.mkdir()
    payload = audit_dir / "changes.gz"
    with gzip.open(payload, "wt") as handle:
        handle.write(json.dumps({"before": {"id": "1"}}) + "\n")
    expected = audit_dir / "source.csv.gz"
    expected.write_bytes(b"fixture")
    seed_csv = staging / "2025.csv.gz"
    seed_csv.write_bytes(b"fixture")
    identity = {
        "sha256": module.sha(seed_csv),
        "snapshot_at": "fixture",
        "branch_id": "fixture",
        "endpoint_id": "fixture",
        "transaction_read_only": "on",
    }
    audit = {
        "status": "source_reconciled",
        "unresolved_difference_keys": 0,
        "repair_candidates": {
            "rows": 1,
            "path": payload.name,
            "sha256": module.sha(payload),
        },
        "source_observations": {"path": expected.name, "sha256": module.sha(expected)},
        "alias_configuration_sha256": module.sha(
            ROOT / "config/entsoe-coal-aliases.json"
        ),
        "start": "2025-01-01T00:00Z",
        "staging": {**identity, mismatch: "different"},
    }
    (audit_dir / "audit.json").write_text(json.dumps(audit))
    seed = {
        **identity,
        "files": {"2025": {"path": seed_csv.name, "sha256": identity["sha256"]}},
    }
    (staging / "manifest.json").write_text(json.dumps(seed))

    def forbidden(*args, **kwargs):
        pytest.fail("Substituted seed reached the database connector")

    monkeypatch.setattr(module.psycopg2, "connect", forbidden)
    with pytest.raises(ValueError, match="seed"):
        module.main(
            SimpleNamespace(
                audit=audit_dir,
                staging=staging,
                dsn="host=/private/tmp user=postgres dbname=coal_hotfix_rehearsal_history_test",
                output=tmp_path / "output",
            )
        )
