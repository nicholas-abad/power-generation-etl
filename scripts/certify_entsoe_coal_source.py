"""Certify one country's archived ENTSO-E coal source before it can feed a rebuild.

Reads an archive written by the rebuild downloader (`<root>/<COUNTRY>/manifest.jsonl`
plus `xml/<day>_<psr>.xml.gz`) and requires, for every (UTC day, PSR) in the
window:
- the inventory is complete: each request is `ok` or verified `no_data`, never
  `failed`/`parse_failed`, with no gaps or duplicates;
- each `no_data` request keeps the source's own answer: ENTSO-E's reason-999
  acknowledgement ("No matching data found") for this domain and exactly this
  UTC day, with its recorded sha256;
- each archived body matches its recorded sha256;
- the independent stdlib/Decimal parser (`audit_entsoe_coal_history.independent_day`)
  and the extractor's per-Period parser yield identical observations;
- the Decimal block integral equals the sum of the expanded observations.

Series with a unit EIC but no unit name are included (name None) and listed
per (EIC, PSR, day) under `unnamed`; the payload names them by EIC from the
reviewed identity table or lists them as exclusions.

Writes per unit-month observations, energy (Decimal strings) and an
observation digest (sorted `observation_line`s, sha256) with the evidence
hashes. Never opens a database connection.

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
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parent))
from audit_entsoe_coal_history import FUEL, independent_day, iso, ms, require  # noqa: E402


def observation_line(timestamp_ms, unit_eic, psr, power, minutes, parent, name):
    """The fixed serialization behind every observation digest.

    MW is the source decimal value without trailing zeros; the payload builder
    and later checks must reproduce exactly this line for each observation.
    """
    return json.dumps(
        [
            timestamp_ms,
            unit_eic,
            psr,
            format(D(power).normalize(), "f"),
            minutes,
            parent,
            name,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


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
    # The label is not trusted: each request must cover exactly its UTC day,
    # for this archive's country and the labelled PSR.
    country = archive.name
    for day, psr in expected:
        entry = entries[(day, psr)]
        lower = ms(f"{day}T00:00:00+00:00")
        require(
            ms(entry["start"]) == lower
            and ms(entry["end"]) == lower + 86400000
            and entry["country"] == country
            and entry["psr"] == psr,
            f"Request {day} {psr} does not cover exactly that UTC day for {country}",
        )
    domains = {entries[k]["domain"] for k in expected}
    require(len(domains) == 1, f"Archive mixes bidding domains {domains}")
    domain = domains.pop()
    for key in expected:
        if entries[key]["status"] == "no_data":
            check_no_data(archive, entries[key], domain)
    return [entries[k] for k in expected], domain


def check_no_data(archive, entry, domain):
    """No data is certified only from the source's archived acknowledgement."""
    label = f"{entry['day']} {entry['psr']}"
    path = archive / "xml" / f"{entry['day']}_{entry['psr']}.xml.gz"
    require(
        "sha256" in entry and path.exists(),
        f"No-data request {label} has no archived source answer",
    )
    body = gzip.decompress(path.read_bytes())
    require(
        hashlib.sha256(body).hexdigest() == entry["sha256"],
        f"Source hash mismatch {label}",
    )
    root = ElementTree.fromstring(body)
    require(
        root.tag.rsplit("}", 1)[-1] == "Acknowledgement_MarketDocument",
        f"No-data request {label} is not an acknowledgement",
    )
    start = iso(ms(entry["start"])).replace("+00:00", "Z")
    end = iso(ms(entry["end"])).replace("+00:00", "Z")
    reasons = [
        (r.findtext("{*}code", ""), r.findtext("{*}text", ""))
        for r in root.findall(".//{*}Reason")
    ]
    require(
        any(
            code == "999"
            and text.startswith("No matching data found")
            and f"({domain})" in text
            and f"interval {start}/{end}" in text
            for code, text in reasons
        ),
        f"No-data request {label} lacks a reason-999 answer for {domain} and that day",
    )


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
    independent, block_mwh, counters = independent_day(
        body, lower, upper, domain, include_unnamed=True
    )
    parsed, _ = parse_coal_periods(
        body,
        country,
        domain,
        entry["start"],
        entry["end"],
        {entry["psr"]},
        include_unnamed=True,
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


def summarize(results):
    """Per unit-month totals and digests, unnamed series and counts from
    certified (day, psr, observations, stats) results."""
    unit_months = defaultdict(
        lambda: {
            "observations": 0,
            "mwh": D(0),
            "names": set(),
            "psr": set(),
            "minutes": set(),
            "lines": [],
        }
    )
    unnamed = defaultdict(lambda: {"observations": 0, "mwh": D(0)})
    seen, counters = set(), defaultdict(int)
    for day, psr, observations, stats in results:
        for key, (p, name, power, minutes, parent) in observations.items():
            require(key not in seen, f"Observation repeated across requests {key}")
            seen.add(key)
            cell = unit_months[(key[1], iso(key[0])[:7])]
            cell["observations"] += 1
            cell["mwh"] += power * minutes / 60
            if name is not None:
                cell["names"].add(name)
            cell["psr"].add(p)
            cell["minutes"].add(minutes)
            cell["lines"].append(
                observation_line(key[0], key[1], p, power, minutes, parent, name)
            )
            if name is None:
                gap = unnamed[(key[1], p, day)]
                gap["observations"] += 1
                gap["mwh"] += power * minutes / 60
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
            "observation_sha256": hashlib.sha256(
                "\n".join(sorted(c["lines"])).encode()
            ).hexdigest(),
        }
        for (eic, month), c in sorted(unit_months.items())
    ]
    return rows, unnamed, dict(counters), len(seen)


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
    with ProcessPoolExecutor(workers) as pool:
        jobs = [(str(archive), e, country, domain) for e in ok]
        rows, unnamed, counters, observation_count = summarize(
            pool.map(certify_request, jobs, chunksize=16)
        )
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
        "observations": observation_count,
        "yearly_mwh": {y: str(v) for y, v in sorted(yearly.items())},
        "parser_counts": counters,
        "independent_parser_mismatches": 0,
        "extractor_commit": commit,
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "unnamed": [
            {
                "unit_eic": eic,
                "psr": psr,
                "day": day,
                "observations": g["observations"],
                "mwh": str(g["mwh"]),
            }
            for (eic, psr, day), g in sorted(unnamed.items())
        ],
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
    summary = {k: v for k, v in report.items() if k not in {"unit_months", "unnamed"}}
    summary["unnamed_series_days"] = len(report["unnamed"])
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
