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

Fuel exceptions (config/entsoe-fuel-exceptions.json): the non-coal series of a
`count_as_coal` record are read from the same archived responses, by both
parsers, in the record's months only. ENTSO-E returns other-PSR series inside a
response, so the same series can arrive in several requests: identical copies
are counted once, conflicting copies fail. Their unit-months carry the record's
`exception_id`; they are kept out of the coal `yearly_mwh` and summed in
`relabelled_yearly_mwh` (stored after the rebuild = coal - excluded +
relabelled). Every generation series neither parser reads is listed under
`out_of_scope_series` per (EIC, PSR) with its months, so a unit whose label
changed is visible rather than silently dropped (tracker #10).

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
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from audit_entsoe_coal_history import (  # noqa: E402
    COAL,
    FUEL,
    independent_day,
    iso,
    ms,
    require,
    skipped_series,
)
import entsoe_fuel_exceptions  # noqa: E402


def listed_windows(country, records=None):
    """{(source psr, unit EIC): (exception id, from month, to month)} of the
    country's count_as_coal records, in a form worker processes can take."""
    return {
        (record.source_psr, record.unit_eic): (
            record.id,
            record.from_month,
            record.to_month,
        )
        for record in (
            entsoe_fuel_exceptions.exceptions() if records is None else records
        )
        if record.area == country and record.treatment == "count_as_coal"
    }


def in_window(listed, psr, eic, month):
    """True for a listed non-coal series in one of its record's months."""
    window = listed.get((psr, eic))
    return (
        window is not None
        and window[1] <= month
        and (window[2] is None or month <= window[2])
    )


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
    archive, entry, country, domain, listed = args
    from energy_extractors.entsoe.constants import PSR_TO_FUEL_TYPE
    from energy_extractors.entsoe.periods import parse_coal_periods

    body = gzip.decompress(
        (Path(archive) / "xml" / f"{entry['day']}_{entry['psr']}.xml.gz").read_bytes()
    )
    require(
        hashlib.sha256(body).hexdigest() == entry["sha256"],
        f"Source hash mismatch {entry['day']} {entry['psr']}",
    )
    lower, upper = ms(entry["start"]), ms(entry["end"])
    month = entry["day"][:7]
    pairs = frozenset(pair for pair in listed if in_window(listed, *pair, month))
    independent, block_mwh, counters = independent_day(
        body, lower, upper, domain, include_unnamed=True, listed_series=pairs
    )
    parsed, _ = parse_coal_periods(
        body,
        country,
        domain,
        entry["start"],
        entry["end"],
        {entry["psr"]} | {psr for psr, _ in pairs},
        include_unnamed=True,
    )
    replay = {}
    for row in parsed:
        if (
            row["psr_type"] not in COAL
            and (row["psr_type"], row["unit_eic"]) not in pairs
        ):
            continue
        key = (row["timestamp_ms"], row["unit_eic"])
        require(key not in replay, "Duplicate extractor output")
        fuel = FUEL.get(row["psr_type"]) or PSR_TO_FUEL_TYPE[row["psr_type"]]
        require(
            row["data_type"] == "Actual Aggregated"
            and row["resolution_source"] == "xml_period"
            and row["fuel_type"] == fuel
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
    skipped = skipped_series(body, lambda psr, eic: psr in COAL or (psr, eic) in pairs)
    return entry["day"], entry["psr"], independent, dict(counters), skipped


def summarize(results, listed=None):
    """Per unit-month totals and digests, unnamed series, skipped series and
    counts from certified (day, psr, observations, stats, skipped) results."""
    listed = listed or {}
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
    out_of_scope = defaultdict(lambda: {"months": set(), "series_days": 0})
    seen, counters = {}, defaultdict(int)
    for day, psr, observations, stats, skipped in results:
        for key, value in observations.items():
            p, name, power, minutes, parent = value
            if key in seen:
                # A listed non-coal series can arrive in every request PSR's
                # response; coal observations must come from one request.
                require(
                    p not in COAL and seen[key] == value,
                    f"Observation repeated across requests {key}",
                )
                counters["listed_identical_copies"] += 1
                continue
            seen[key] = value
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
        for (eic, p), series in skipped.items():
            cell = out_of_scope[(eic, p)]
            cell["months"].add(day[:7])
            cell["series_days"] += series
    rows = []
    for (eic, month), c in sorted(unit_months.items()):
        non_coal = sorted(c["psr"] - COAL)
        require(
            not non_coal or len(c["psr"]) == 1,
            f"{eic} {month} mixes a listed non-coal series with coal",
        )
        row = {
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
        if non_coal:
            row["exception_id"] = listed[(non_coal[0], eic)][0]
        rows.append(row)
    skipped_rows = [
        {
            "unit_eic": eic,
            "psr": p,
            "months": sorted(c["months"]),
            "series_days": c["series_days"],
        }
        for (eic, p), c in sorted(
            out_of_scope.items(), key=lambda i: (str(i[0][0]), i[0][1])
        )
    ]
    return rows, unnamed, dict(counters), len(seen), skipped_rows


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
    listed = listed_windows(country)
    with ProcessPoolExecutor(workers) as pool:
        jobs = [(str(archive), e, country, domain, listed) for e in ok]
        rows, unnamed, counters, observation_count, skipped = summarize(
            pool.map(certify_request, jobs, chunksize=16), listed
        )
    yearly, relabelled = defaultdict(lambda: D(0)), defaultdict(lambda: D(0))
    for row in rows:
        ledger = relabelled if "exception_id" in row else yearly
        ledger[row["month"][:4]] += D(row["mwh"])
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
        # Listed non-coal series (count_as_coal), written as coal by the payload.
        "relabelled_yearly_mwh": {y: str(v) for y, v in sorted(relabelled.items())},
        "fuel_exceptions_sha256": entsoe_fuel_exceptions.sha256(),
        "listed_series": [
            {"psr": psr, "unit_eic": eic, "exception_id": window[0]}
            for (psr, eic), window in sorted(listed.items())
        ],
        "out_of_scope_series": skipped,
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
    summary = {
        k: v
        for k, v in report.items()
        if k not in {"unit_months", "unnamed", "out_of_scope_series"}
    }
    summary["unnamed_series_days"] = len(report["unnamed"])
    summary["out_of_scope_series"] = len(report["out_of_scope_series"])
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
