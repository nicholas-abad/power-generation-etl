"""Replay the pinned hotfix extractor on the two audited annual XML archives.

Use the extractor's Python environment and put its src directory on PYTHONPATH.
This command is offline and never connects to a database or the ENTSO-E API.
"""

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess

from energy_extractors.entsoe.periods import parse_coal_periods

ROOT = Path(__file__).resolve().parents[1]
FIELDS = (
    "plant_name",
    "production_unit_eic",
    "fuel_type",
    "data_type",
    "generation_mw",
    "resolution_minutes",
)


def sha(path):
    with path.open("rb") as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def key(row):
    return row["timestamp_ms"], row["psr_type"], row["unit_eic"]


def replay(archive, output, extractor):
    release = json.loads((ROOT / "config/entsoe-coal-release.json").read_text())
    commit = subprocess.check_output(
        ["git", "-C", str(extractor), "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != release["extractor_commit"]:
        raise ValueError("Use the pinned extractor revision")
    import inspect

    if (
        Path(inspect.getfile(parse_coal_periods)).resolve()
        != extractor.resolve() / "src/energy_extractors/entsoe/periods.py"
    ):
        raise ValueError("PYTHONPATH did not select the pinned extractor")
    for diff in (
        ["diff", "--exit-code", "--", "src"],
        ["diff", "--cached", "--exit-code", "--", "src"],
    ):
        subprocess.run(["git", "-C", str(extractor), *diff], check=True)
    output.mkdir(parents=True, exist_ok=False)
    benchmarks = json.loads(
        (ROOT / "docs/validation/entsoe-coal-hotfix-2026-10-05.json").read_text()
    )["parser_reconciliation"]
    results = {"extractor_commit": commit, "years": {}}
    for year, directory in ((2019, "year2019"), (2024, "year2024-full")):
        folder = archive / directory
        (reference_path,) = folder.glob("*_etl.jsonl")
        benchmark = benchmarks[str(year)]
        if sha(reference_path) != benchmark["reference_jsonl_sha256"]:
            raise ValueError("Audited annual reference hash changed")
        reference = {}
        with reference_path.open() as source:
            for line in source:
                row = json.loads(line)
                if row["country_code"] == "CZ" and row["psr_type"] in {
                    "B02",
                    "B03",
                    "B05",
                }:
                    if key(row) in reference:
                        raise ValueError("Duplicate audited source key")
                    reference[key(row)] = tuple(row.get(field) for field in FIELDS)
                    metadata = {
                        k: row[k] for k in ("extraction_run_id", "created_at_ms")
                    }
        manifest = json.loads((folder / "entsoe_unit_manifest.json").read_text())
        requests = manifest["source_requests"]
        if len(requests) != benchmark["archived_xml_days"]:
            raise ValueError("Incomplete daily archive")
        path = output / f"coal{year}.jsonl"
        count, energy = 0, Decimal(0)
        archive_hashes = {}
        with path.open("w") as destination:
            for request in requests:
                xml = folder / request["path"]
                if request["status"] != "success" or sha(xml) != request["sha256"]:
                    raise ValueError("Daily archive hash/status mismatch")
                archive_hashes[xml.name] = request["sha256"]
                rows, _ = parse_coal_periods(
                    xml.read_text(),
                    "CZ",
                    "10YCZ-CEPS-----N",
                    request["start"],
                    request["end"],
                )
                for row in rows:
                    if reference.pop(key(row), None) != tuple(
                        row.get(field) for field in FIELDS
                    ):
                        raise ValueError("Hotfix parser disagrees with audited source")
                    destination.write(
                        json.dumps({**row, **metadata}, separators=(",", ":")) + "\n"
                    )
                    energy += (
                        Decimal(str(row["generation_mw"]))
                        * row["resolution_minutes"]
                        / 60
                    )
                    count += 1
        if (
            reference
            or count != benchmark["coal_observations"]
            or energy != Decimal(benchmark["coal_mwh"])
        ):
            raise ValueError("Incomplete or incorrect annual replay")
        results["years"][str(year)] = {
            "path": path.name,
            "sha256": sha(path),
            "rows": count,
            "generation_mwh": str(energy),
            "reference_sha256": sha(reference_path),
            "source_xml_sha256": archive_hashes,
            "missing_extra_or_mismatched_observations": 0,
        }
        print(
            f"Replayed {year}: {count:,} coal observations, {energy} MWh; exact source match",
            flush=True,
        )
    (output / "manifest.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--extractor", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    replay(args.archive, args.output, args.extractor)
