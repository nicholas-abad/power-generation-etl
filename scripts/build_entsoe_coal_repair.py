"""Rebuild the pinned coal repair from retained audit exports and source XML.

Offline only. Run with the hotfix extractor's src directory on PYTHONPATH.
The archived 2024 source is fully replayed through the hotfix period parser.
"""

import argparse
from collections import Counter
import csv
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

from energy_extractors.entsoe.periods import parse_coal_periods

ROOT = Path(__file__).resolve().parents[1]
COAL = {"B02", "B03", "B05"}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def key(row, name=None):
    return int(row["timestamp_ms"]), row["psr_type"], name or row["plant_name"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--etl-audit", type=Path, required=True)
    parser.add_argument("--source-archive", type=Path, required=True)
    args = parser.parse_args()
    alias_path = ROOT / "config/entsoe-coal-aliases.json"
    aliases = json.loads(alias_path.read_text())["aliases"]
    names = {(a["psr_type"], a["source_name"]): a["legacy_name"] for a in aliases}
    year_dir = args.source_archive / "year2024-full"
    manifest_path = year_dir / "entsoe_unit_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    expected = {}
    source_hashes = {}
    for request in manifest["source_requests"]:
        assert request["status"] == "success"
        xml = year_dir / request["path"]
        assert sha(xml) == request["sha256"]
        source_hashes[xml.name] = sha(xml)
        rows, _ = parse_coal_periods(
            xml.read_text(), "CZ", "10YCZ-CEPS-----N", request["start"], request["end"]
        )
        for row in rows:
            name = names.get((row["psr_type"], row["plant_name"]), row["plant_name"])
            k = key(row, name)
            assert k not in expected
            expected[k] = row
    assert len(source_hashes) == 366 and len(expected) == 530160
    total = sum(
        Decimal(str(r["generation_mw"])) * r["resolution_minutes"] / 60
        for r in expected.values()
    )
    assert total == Decimal("18782731.275")
    boundary_xml = (
        args.source_archive / "year-interval-screen/source_xml/CZ_20231231.xml"
    )
    assert (
        sha(boundary_xml)
        == "8acce3e454b715307ce6028a7335e2e59ca6577d2b8ea90df9e898f6ffa0b75f"
    )
    boundary, _ = parse_coal_periods(
        boundary_xml.read_text(),
        "CZ",
        "10YCZ-CEPS-----N",
        "2023-12-31T23:00Z",
        "2024-01-01T00:00Z",
    )
    assert len(boundary) == 24
    for row in boundary:
        expected[
            key(row, names.get((row["psr_type"], row["plant_name"]), row["plant_name"]))
        ] = row
    source_hashes[boundary_xml.name] = sha(boundary_xml)

    changes = []
    original_path = args.etl_audit / "deep-dive/backup.csv"
    originals = [
        r for r in csv.DictReader(original_path.open()) if r["psr_type"] in COAL
    ]
    sample_path = (
        args.etl_audit / "year-interval-screen/staging-sample-observations.csv"
    )
    originals += [
        r
        for r in csv.DictReader(sample_path.open())
        if int(r["timestamp_ms"]) == 1704063600000
    ]
    for row in originals:
        k = key(row)
        canonical = names.get((row["psr_type"], row["plant_name"]), row["plant_name"])
        source = expected.get(k)
        action = "interval"
        if source is None:
            source = expected[key(row, canonical)]
            assert canonical != row["plant_name"]
            action = "duplicate"
        assert Decimal(row["generation_mw"]) == Decimal(str(source["generation_mw"]))
        before, after = int(row["resolution_minutes"]), source["resolution_minutes"]
        assert (before, after) == ((15, 60) if action == "interval" else (15, 15))
        changes.append(
            {
                "action": action,
                "timestamp_ms": k[0],
                "psr_type": k[1],
                "plant_name": k[2],
                "generation_mw": row["generation_mw"],
                "before_minutes": before,
                "after_minutes": after,
                "canonical_name": canonical if action == "duplicate" else k[2],
            }
        )
    counts = Counter(r["action"] for r in changes)
    assert counts == {"interval": 104208, "duplicate": 12}
    assert len({key(r) for r in changes}) == len(changes)
    changes.sort(key=key)
    payload = "".join(
        json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n" for r in changes
    ).encode()
    out = ROOT / "config/repairs"
    out.mkdir(parents=True, exist_ok=True)
    artifact = out / "entsoe-cz-coal-2023-2024.jsonl.gz"
    artifact.write_bytes(gzip.compress(payload, mtime=0))
    change_mwh = sum(
        Decimal(r["generation_mw"]) * (r["after_minutes"] - r["before_minutes"]) / 60
        for r in changes
        if r["action"] == "interval"
    )
    result = {
        "repair_id": "entsoe-cz-coal-2023-2024-v1",
        "country_code": "CZ",
        "artifact": artifact.name,
        "artifact_sha256": sha(artifact),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "counts": dict(counts),
        "interval_mwh_increase": str(change_mwh),
        "duplicate_mwh_removed": "0",
        "coal_2024_source_mwh": str(total),
        "excluded": ["2025", "non-coal", "other countries", "plant mappings"],
        "evidence_sha256": {
            "backup.csv": sha(original_path),
            "staging-sample-observations.csv": sha(sample_path),
            "source_manifest.json": sha(manifest_path),
            "aliases.json": sha(alias_path),
        },
        "source_xml_sha256": source_hashes,
    }
    (out / "entsoe-cz-coal-2023-2024.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(
        json.dumps(
            {k: v for k, v in result.items() if k != "source_xml_sha256"}, indent=2
        )
    )


if __name__ == "__main__":
    main()
