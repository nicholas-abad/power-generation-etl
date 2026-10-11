"""Certify one country's archived ENTSO-E coal source before it can feed a rebuild.

Reads an archive written by the rebuild downloader (`<root>/<COUNTRY>/manifest.jsonl`
plus `xml/<day>_<psr>.xml.gz`) and requires, for every (UTC day, PSR) in the
window:
- the inventory is complete: each request is `ok` or verified `no_data`, never
  `failed`/`parse_failed`, with no gaps or duplicates;
- each archived body matches its recorded sha256;
- the independent stdlib/Decimal parser (`audit_entsoe_coal_history.independent_day`)
  and the extractor's per-Period parser yield identical observations;
- the Decimal block integral equals the sum of the expanded observations.

Writes per unit-month observations and energy (Decimal strings) with the
evidence hashes. Never opens a database connection.

Run with the extractors venv (it imports the extractor's parser):
  PYTHONPATH=<extractor>/src python scripts/certify_entsoe_coal_source.py \
      --archive <root>/PL --psrs B02,B05 --start 2019-01-01 --end 2026-10-01 \
      --extractor <extractor checkout> --output <report.json>
"""

import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from decimal import Decimal as D
import gzip
import hashlib
import inspect
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from audit_entsoe_coal_history import FUEL, independent_day, iso, ms, require  # noqa: E402


def load_inventory(archive, psrs, start, end):
    """The last manifest entry per (day, PSR), checked for completeness."""
    entries = {}
    for line in (archive / "manifest.jsonl").read_text().splitlines():
        entry = json.loads(line)
        entries[(entry["day"], entry["psr"])] = entry
    expected, day = [], start
    while day < end:
        expected += [(day.isoformat(), psr) for psr in psrs]
        day += timedelta(days=1)
    missing = [key for key in expected if key not in entries]
    require(not missing, f"Archive lacks {len(missing)} requests, e.g. {missing[:3]}")
    bad = [
        (k, entries[k]["status"])
        for k in expected
        if entries[k]["status"] not in {"ok", "no_data"}
    ]
    require(not bad, f"Archive has {len(bad)} failed requests, e.g. {bad[:3]}")
    domains = {entries[k]["domain"] for k in expected}
    require(len(domains) == 1, f"Archive mixes bidding domains {domains}")
    return [entries[k] for k in expected], domains.pop()


def certify_request(args):
    """Independent vs extractor observations for one archived (day, PSR)."""
    archive, entry, country, domain = args
    from energy_extractors.entsoe.periods import parse_coal_periods

    body = gzip.decompress(
        (Path(archive) / "xml" / f"{entry['day']}_{entry['psr']}.xml.gz").read_bytes()
    )
    require(
        hashlib.sha256(body).hexdigest() == entry["sha256"],
        f"Source hash mismatch {entry['day']} {entry['psr']}",
    )
    lower, upper = ms(entry["start"]), ms(entry["end"])
    independent, block_mwh, counters = independent_day(body, lower, upper, domain)
    parsed, _ = parse_coal_periods(
        body, country, domain, entry["start"], entry["end"], {entry["psr"]}
    )
    replay = {}
    for row in parsed:
        key = (row["timestamp_ms"], row["unit_eic"])
        require(key not in replay, "Duplicate extractor output")
        require(
            row["data_type"] == "Actual Aggregated"
            and row["resolution_source"] == "xml_period"
            and row["fuel_type"] == FUEL[row["psr_type"]]
            and row["country_code"] == country,
            "Wrong extractor record contract",
        )
        replay[key] = (
            row["psr_type"],
            row["plant_name"],
            D(str(row["generation_mw"])),
            row["resolution_minutes"],
            row["production_unit_eic"],
        )
    require(
        independent == replay,
        f"Independent parser disagrees with extractor on {entry['day']} {entry['psr']}",
    )
    require(
        block_mwh == sum((v[2] * v[3] / 60 for v in independent.values()), D(0)),
        f"Block integral disagrees on {entry['day']} {entry['psr']}",
    )
    return entry["day"], entry["psr"], independent, dict(counters)


def certify(archive, country, psrs, start, end, extractor, workers=8):
    from energy_extractors.entsoe.periods import parse_coal_periods

    require(
        Path(inspect.getfile(parse_coal_periods)).resolve()
        == (extractor / "src/energy_extractors/entsoe/periods.py").resolve(),
        "Wrong extractor import",
    )
    for args in (
        ["diff", "--exit-code", "--", "src"],
        ["diff", "--cached", "--exit-code", "--", "src"],
    ):
        subprocess.run(["git", "-C", str(extractor), *args], check=True)
    commit = subprocess.check_output(
        ["git", "-C", str(extractor), "rev-parse", "HEAD"], text=True
    ).strip()
    entries, domain = load_inventory(archive, psrs, start, end)
    ok = [e for e in entries if e["status"] == "ok"]
    unit_months = defaultdict(
        lambda: {
            "observations": 0,
            "mwh": D(0),
            "names": set(),
            "psr": set(),
            "minutes": set(),
        }
    )
    seen, counters = set(), defaultdict(int)
    with ProcessPoolExecutor(workers) as pool:
        jobs = [(str(archive), e, country, domain) for e in ok]
        for day, psr, observations, stats in pool.map(
            certify_request, jobs, chunksize=16
        ):
            for key, (p, name, power, minutes, _) in observations.items():
                require(key not in seen, f"Observation repeated across requests {key}")
                seen.add(key)
                cell = unit_months[(key[1], iso(key[0])[:7])]
                cell["observations"] += 1
                cell["mwh"] += power * minutes / 60
                cell["names"].add(name)
                cell["psr"].add(p)
                cell["minutes"].add(minutes)
            for name, value in stats.items():
                counters[name] += value
    rows = [
        {
            "unit_eic": eic,
            "month": month,
            "psr": sorted(c["psr"]),
            "names": sorted(c["names"]),
            "minutes": sorted(c["minutes"]),
            "observations": c["observations"],
            "mwh": str(c["mwh"]),
        }
        for (eic, month), c in sorted(unit_months.items())
    ]
    yearly = defaultdict(lambda: D(0))
    for row in rows:
        yearly[row["month"][:4]] += D(row["mwh"])
    manifest = archive / "manifest.jsonl"
    return {
        "country": country,
        "domain": domain,
        "psrs": psrs,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "requests": len(entries),
        "ok": len(ok),
        "no_data": len(entries) - len(ok),
        "observations": len(seen),
        "yearly_mwh": {y: str(v) for y, v in sorted(yearly.items())},
        "parser_counts": dict(counters),
        "independent_parser_mismatches": 0,
        "extractor_commit": commit,
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "unit_months": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--psrs", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--extractor", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)
    report = certify(
        args.archive,
        args.archive.name,
        sorted(args.psrs.split(",")),
        args.start,
        args.end,
        args.extractor,
        args.workers,
    )
    args.output.write_text(json.dumps(report, indent=1, ensure_ascii=False))
    print(json.dumps({k: v for k, v in report.items() if k != "unit_months"}, indent=1))


if __name__ == "__main__":
    main()
