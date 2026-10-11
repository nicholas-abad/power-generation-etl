"""Verify retained UTC year-boundary days, including the first 2018 spillover hour.

File-only comparison; no API or database connection. Main annual audits establish
full-year coverage. This additional check joins their adjoining source days to
the separately exported staging boundary records.
"""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import gzip
import json
from pathlib import Path

from audit_entsoe_coal_history import FUEL, ROOT, independent_day, ms, require, sha
from energy_extractors.entsoe.periods import parse_coal_periods


def archived_day(date, history, original):
    year = int(date[:4])
    filename = f"CZ_{date.replace('-', '')}.xml"
    if year in {2019, 2024}:
        directory = original / ("year2019" if year == 2019 else "year2024-full")
        manifest = json.loads((directory / "entsoe_unit_manifest.json").read_text())
        requests = manifest["source_requests"]
    else:
        directory = (
            history / ("boundary-2018" if year == 2018 else str(year)) / "source"
        )
        manifest = json.loads((directory / "manifest.json").read_text())
        requests = list(manifest["requests"].values())
    matches = [r for r in requests if Path(r["path"]).name == filename]
    require(
        len(matches) == 1 and matches[0]["status"] == "success",
        f"Boundary day unavailable: {date}",
    )
    entry = matches[0]
    path = directory / entry["path"]
    require(sha(path) == entry["sha256"], "Boundary XML hash mismatch")
    return path, entry


def audit(history, original, output):
    aliases_path = ROOT / "config/entsoe-coal-aliases.json"
    aliases = json.loads(aliases_path.read_text())["aliases"]
    names = {(a["psr_type"], a["source_name"]): a["legacy_name"] for a in aliases}
    report = {
        "method": "Independent Decimal XML block integration and extractor agreement; staging exports only",
        "script_sha256": sha(Path(__file__)),
        "independent_parser_sha256": sha(ROOT / "scripts/audit_entsoe_coal_history.py"),
        "alias_configuration_sha256": sha(aliases_path),
        "boundaries": {},
        "excluded": {},
    }
    for staging in (history / "staging", history / "staging-2019-boundary"):
        manifest = json.loads((staging / "manifest.json").read_text())
        for label, entry in manifest["files"].items():
            if not label.endswith(("_prior_day", "_next_day")):
                continue
            year = int(label[:4])
            date = (
                datetime(year, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                if label.endswith("_prior_day")
                else datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            )
            if date >= datetime.now(timezone.utc):
                require(
                    entry["rows"] == 0, "Future boundary unexpectedly has observations"
                )
                report["excluded"][label] = (
                    "Future boundary outside the 2026 YTD audit; no stored rows"
                )
                continue
            csv_path = staging / entry["path"]
            require(sha(csv_path) == entry["sha256"], "Boundary CSV hash mismatch")
            with gzip.open(csv_path, "rt") as handle:
                stored = list(csv.DictReader(handle))
            require(len(stored) == entry["rows"], "Boundary export row count mismatch")
            path, request = archived_day(date.strftime("%Y-%m-%d"), history, original)
            start, end = date.isoformat(), (date + timedelta(days=1)).isoformat()
            note = "Full adjoining UTC day"
            if label == "2019_prior_day":
                start = "2018-12-31T23:00:00+00:00"
                note = "Only the first retained hour: local 2019 begins at 2018-12-31 23:00 UTC; earlier 2018 data is outside scope"
            source, energy, _ = independent_day(path.read_bytes(), ms(start), ms(end))
            parsed, _ = parse_coal_periods(
                path.read_bytes(), "CZ", "10YCZ-CEPS-----N", start, end
            )
            replay = {
                (r["timestamp_ms"], r["unit_eic"]): (
                    r["psr_type"],
                    r["plant_name"],
                    D(str(r["generation_mw"])),
                    r["resolution_minutes"],
                    r["production_unit_eic"],
                )
                for r in parsed
            }
            require(
                source == replay and len(replay) == len(parsed),
                "Boundary extractor mismatch",
            )
            expected = {}
            for (timestamp, eic), (psr, name, power, minutes, _) in source.items():
                for alias in aliases:
                    spellings = {alias["source_name"], alias["legacy_name"]}
                    if eic == alias["unit_eic"] or name in spellings:
                        require(
                            eic == alias["unit_eic"]
                            and psr == alias["psr_type"]
                            and name in spellings,
                            "Boundary identity drift",
                        )
                key = (timestamp, psr, names.get((psr, name), name))
                require(key not in expected, "Boundary canonical key collision")
                expected[key] = (power, minutes)
            actual = defaultdict(list)
            before = D(0)
            for row in stored:
                timestamp, psr = int(row["timestamp_ms"]), row["psr_type"]
                require(
                    ms(start) <= timestamp < ms(end),
                    "Stored boundary row outside comparison",
                )
                require(
                    row["fuel_type"] == FUEL[psr]
                    and row["data_type"] in {"Actual Aggregated", "Unknown", FUEL[psr]},
                    "Boundary metric/fuel conflict",
                )
                actual[
                    (
                        timestamp,
                        psr,
                        names.get((psr, row["plant_name"]), row["plant_name"]),
                    )
                ].append(row)
                before += D(row["generation_mw"]) * int(row["resolution_minutes"]) / 60
            require(actual.keys() == expected.keys(), "Missing or extra boundary keys")
            counts = Counter()
            for key, copies in actual.items():
                power, minutes = expected[key]
                require(
                    all(D(row["generation_mw"]) == power for row in copies),
                    "Boundary MW mismatch",
                )
                counts["duration_mismatches"] += sum(
                    int(row["resolution_minutes"]) != minutes for row in copies
                )
                counts["extra_alias_copies"] += len(copies) - 1
            report["boundaries"][label] = {
                "start": start,
                "end": end,
                "scope": note,
                "stored_rows": len(stored),
                "source_rows": len(source),
                "stored_mwh": str(before),
                "source_mwh": str(energy),
                "source_minus_stored_mwh": str(energy - before),
                "differences": dict(counts),
                "source_xml_sha256": request["sha256"],
                "staging_csv_sha256": entry["sha256"],
                "snapshot_at": manifest["snapshot_at"],
            }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["boundaries"], indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    audit(args.history, args.original, args.output)
