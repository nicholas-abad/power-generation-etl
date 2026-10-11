"""Independently reconcile a complete Czech coal source archive with a staging export.

XML calculations use only stdlib + Decimal. Every expanded observation is then
compared with the hotfix extractor. This tool never opens a database connection.
Only exact, source-proven duplicate/interval changes become repair candidates;
missing rows or changed MW values remain explicit unresolved differences.
"""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal as D
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
COAL = {"B02", "B03", "B05"}
FUEL = {
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B05": "Fossil Hard coal",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ms(value):
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(date.tzinfo is not None, "Source date lacks timezone")
    return int(date.timestamp() * 1000)


def iso(value):
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def field(element, path):
    item = element.find("/".join("{*}" + part for part in path.split("/")))
    return item.text.strip() if item is not None and item.text else None


def independent_day(xml, lower, upper):
    root = ET.fromstring(xml)
    require(
        field(root, "type") == "A73" and field(root, "process.processType") == "A16",
        "Unexpected document type",
    )
    observations = {}
    counters = Counter()
    block_mwh = D(0)
    for series in root.findall("{*}TimeSeries"):
        psr = field(series, "MktPSRType/psrType")
        if psr not in COAL:
            continue
        if series.find("{*}outBiddingZone_Domain.mRID") is not None:
            counters["consumption_series"] += 1
            continue
        name = field(series, "MktPSRType/PowerSystemResources/name")
        if not name:
            counters["nameless_series"] += 1
            continue
        unit = field(series, "MktPSRType/PowerSystemResources/mRID")
        parent = field(series, "registeredResource.mRID")
        require(
            unit is not None and re.fullmatch(r"[A-Z0-9-]{16}", unit),
            "Named source unit lacks valid EIC",
        )
        for path in ("MktPSRType/PowerSystemResources/mRID", "registeredResource.mRID"):
            element = series.find("/".join("{*}" + part for part in path.split("/")))
            if element is not None and element.text:
                require(element.get("codingScheme") == "A01", "Non-EIC identity coding")
        require(
            field(series, "inBiddingZone_Domain.mRID") == "10YCZ-CEPS-----N",
            "Wrong bidding domain",
        )
        require(
            field(series, "businessType") == "A01"
            and field(series, "objectAggregation") == "A06",
            "Unexpected source business/aggregation",
        )
        require(
            field(series, "quantity_Measure_Unit.name") == "MAW",
            "Expected MW source values",
        )
        curve = field(series, "curveType")
        require(curve in {"A01", "A03"}, "Unsupported source curve")
        counters["curve_" + curve] += 1
        for period in series.findall("{*}Period"):
            start, end = (
                ms(field(period, "timeInterval/start")),
                ms(field(period, "timeInterval/end")),
            )
            resolution = re.fullmatch(
                r"PT(\d+)([MH])", field(period, "resolution") or ""
            )
            require(resolution is not None, "Invalid period duration")
            minutes = int(resolution[1]) * (60 if resolution[2] == "H" else 1)
            step = minutes * 60000
            require(
                minutes in {15, 30, 60} and end > start and (end - start) % step == 0,
                "Unaligned period",
            )
            counters[f"periods_{minutes}m"] += 1
            points = sorted(
                (int(field(p, "position")), field(p, "quantity"))
                for p in period.findall("{*}Point")
            )
            require(
                len(points) == len({p for p, _ in points}), "Duplicate point positions"
            )
            for index, (position, quantity) in enumerate(points):
                require(1 <= position <= (end - start) // step, "Point outside period")
                left = start + (position - 1) * step
                right = (
                    left + step
                    if curve == "A01"
                    else (
                        start + (points[index + 1][0] - 1) * step
                        if index + 1 < len(points)
                        else end
                    )
                )
                a, b = max(left, lower), min(right, upper)
                if b <= a:
                    continue
                require(
                    (a - left) % step == 0 and (b - a) % step == 0,
                    "Window cuts an observation",
                )
                if quantity is None:
                    counters["missing_intervals"] += (b - a) // step
                    continue
                power = D(quantity)
                require(power.is_finite() and power >= 0, "Invalid source MW")
                block_mwh += power * D(b - a) / 3600000
                for timestamp in range(a, b, step):
                    key = (timestamp, unit)
                    value = (psr, name, power, minutes, parent)
                    if key in observations:
                        require(observations[key] == value, "Conflicting source copies")
                        counters["identical_copies"] += 1
                        block_mwh -= power * minutes / 60
                    observations[key] = value
    return observations, block_mwh, counters


def source_records(archive, extractor, compare_end=None):
    from energy_extractors.entsoe.periods import parse_coal_periods
    import inspect

    require(
        Path(inspect.getfile(parse_coal_periods)).resolve()
        == extractor.resolve() / "src/energy_extractors/entsoe/periods.py",
        "Wrong extractor import",
    )
    commit = subprocess.check_output(
        ["git", "-C", str(extractor), "rev-parse", "HEAD"], text=True
    ).strip()
    for args in (
        ["diff", "--exit-code", "--", "src"],
        ["diff", "--cached", "--exit-code", "--", "src"],
    ):
        subprocess.run(["git", "-C", str(extractor), *args], check=True)
    manifest = json.loads((archive / "manifest.json").read_text())
    lower, upper = ms(manifest["start"]), ms(manifest["end"])
    effective_upper = upper if compare_end is None else ms(compare_end)
    require(lower < effective_upper <= upper, "Comparison end is outside archive")
    requests = sorted(manifest["requests"].values(), key=lambda x: x["start"])
    require(len(requests) == (upper - lower) // 86400000, "Incomplete daily archive")
    records, counters, direct_mwh = {}, Counter(), D(0)
    next_start = lower
    for request in requests:
        start, end = ms(request["start"]), ms(request["end"])
        require(
            request["status"] == "success"
            and start == next_start
            and end - start == 86400000,
            "Failed, missing or overlapping source day",
        )
        next_start = end
        path = archive / request["path"]
        require(sha(path) == request["sha256"], "Source XML hash mismatch")
        xml = path.read_bytes()
        if start >= effective_upper:
            continue
        clipped_end = min(end, effective_upper)
        independent, energy, stats = independent_day(xml, start, clipped_end)
        parsed, _ = parse_coal_periods(
            xml, "CZ", "10YCZ-CEPS-----N", request["start"], iso(clipped_end)
        )
        replay = {}
        for row in parsed:
            key = (row["timestamp_ms"], row["unit_eic"])
            require(key not in replay, "Duplicate extractor output")
            require(
                row["data_type"] == "Actual Aggregated"
                and row["resolution_source"] == "xml_period"
                and row["fuel_type"] == FUEL[row["psr_type"]],
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
            f"Independent parser disagrees with extractor on {request['date']}",
        )
        require(not records.keys() & independent.keys(), "Overlapping daily archives")
        records.update(independent)
        direct_mwh += energy
        counters.update(stats)
    require(next_start == upper, "Archive ends before requested boundary")
    require(
        direct_mwh == sum((row[2] * row[3] / 60 for row in records.values()), D(0)),
        "Block integral disagrees with expanded intervals",
    )
    return records, {
        "start": manifest["start"],
        "end": iso(effective_upper),
        "archive_end": manifest["end"],
        "source_days": len(requests),
        "source_rows": len(records),
        "direct_block_mwh": str(direct_mwh),
        "parser_counts": dict(counters),
        "extractor_commit": commit,
        "manifest_sha256": sha(archive / "manifest.json"),
        "independent_parser_mismatches": 0,
    }


def source_summary(records):
    monthly, units = defaultdict(lambda: D(0)), {}
    for (timestamp, eic), (psr, name, power, minutes, _) in sorted(records.items()):
        energy = power * minutes / 60
        monthly[iso(timestamp)[:7]] += energy
        unit = units.setdefault(
            eic,
            {
                "names": set(),
                "psrs": set(),
                "rows": 0,
                "hours": D(0),
                "mwh": D(0),
                "max_mw": D(0),
                "first": iso(timestamp),
                "last_end_ms": timestamp,
                "internal_gaps": [],
            },
        )
        require(timestamp >= unit["last_end_ms"], "Source intervals overlap")
        if timestamp > unit["last_end_ms"]:
            unit["internal_gaps"].append(
                {
                    "start": iso(unit["last_end_ms"]),
                    "end": iso(timestamp),
                    "hours": str(D(timestamp - unit["last_end_ms"]) / 3600000),
                }
            )
        unit["last_end_ms"] = timestamp + minutes * 60000
        unit["names"].add(name)
        unit["psrs"].add(psr)
        unit["rows"] += 1
        unit["hours"] += D(minutes) / 60
        unit["mwh"] += energy
        unit["max_mw"] = max(unit["max_mw"], power)
    for unit in units.values():
        unit["last_end"] = iso(unit.pop("last_end_ms"))
        unit["names"] = sorted(unit["names"])
        unit["psrs"] = sorted(unit["psrs"])
    return {"monthly_mwh": dict(monthly), "units": units}


def audit(archive, staging, label, output, extractor, compare_end=None):
    output.mkdir(parents=True, exist_ok=False)
    source, report = source_records(archive, extractor, compare_end)
    if label.isdigit():
        year = int(label)
        require(
            ms(report["start"]) == ms(f"{year}-01-01T00:00Z"),
            "Annual audit must start on January 1 UTC",
        )
        if compare_end is None:
            require(
                ms(report["end"]) == ms(f"{year + 1}-01-01T00:00Z"),
                "Partial annual audit requires an explicit comparison cutoff",
            )
    report.update(
        method="Independent stdlib XML/Decimal block integration, full extractor replay, and read-only staging CSV reconciliation",
        source_summary=source_summary(source),
    )
    seed = json.loads((staging / "manifest.json").read_text())
    entry = seed["files"][label]
    csv_path = staging / entry["path"]
    require(sha(csv_path) == entry["sha256"], "Staging export hash mismatch")
    report["staging"] = {
        "snapshot_at": seed["snapshot_at"],
        "branch_id": seed["branch_id"],
        "endpoint_id": seed["endpoint_id"],
        "transaction_read_only": seed["transaction_read_only"],
        "sha256": entry["sha256"],
    }
    aliases = json.loads((ROOT / "config/entsoe-coal-aliases.json").read_text())[
        "aliases"
    ]
    names = {
        (a["unit_eic"], a["psr_type"], a["source_name"]): a["legacy_name"]
        for a in aliases
    }
    stored_names = {
        (a["psr_type"], a["source_name"]): a["legacy_name"] for a in aliases
    }
    # Validate both spellings before allowing the name-keyed table to inherit
    # identity from reviewed aliases. Equal power does not prove identity.
    identities = {(eic, row[0], row[1]) for (_, eic), row in source.items()}
    for eic, psr, name in identities:
        for alias in aliases:
            spellings = {alias["source_name"], alias["legacy_name"]}
            if eic == alias["unit_eic"] or name in spellings:
                require(
                    eic == alias["unit_eic"]
                    and psr == alias["psr_type"]
                    and name in spellings,
                    "Source identity conflicts with reviewed EIC/name/fuel mapping",
                )
    canonical = {}
    for (timestamp, eic), value in source.items():
        psr, name, power, minutes, parent = value
        name = names.get((eic, psr, name), name)
        key = (timestamp, psr, name)
        require(key not in canonical, "Two source units collide at a legacy key")
        canonical[key] = (power, minutes, eic)
    stored = defaultdict(list)
    before_mwh, before_monthly, labels, fuel_errors = (
        D(0),
        defaultdict(lambda: D(0)),
        Counter(),
        Counter(),
    )
    with gzip.open(csv_path, "rt", newline="") as handle:
        rows = list(csv.DictReader(handle))
    require(len(rows) == entry["rows"], "Staging row-count mismatch")
    lower, upper = ms(report["start"]), ms(report["end"])
    excluded_rows = 0
    for row in rows:
        timestamp, psr, name = (
            int(row["timestamp_ms"]),
            row["psr_type"],
            row["plant_name"],
        )
        require(
            row["country_code"] == "CZ" and psr in COAL, "Export includes other scope"
        )
        if not lower <= timestamp < upper:
            excluded_rows += 1
            continue
        energy = D(row["generation_mw"]) * int(row["resolution_minutes"]) / 60
        before_mwh += energy
        before_monthly[iso(timestamp)[:7]] += energy
        labels[row["data_type"]] += 1
        if row["fuel_type"] != FUEL[psr]:
            fuel_errors[row["fuel_type"]] += 1
        stored[(timestamp, psr, stored_names.get((psr, name), name))].append(row)
    candidates, differences = [], []
    counts = Counter()
    candidate_delta = D(0)
    for key in sorted(canonical.keys() | stored.keys()):
        expected, copies = canonical.get(key), stored.get(key, [])
        if expected is None:
            counts["stored_without_source"] += len(copies)
            differences.append(
                {"kind": "stored_without_source", "key": key, "rows": copies}
            )
            continue
        power, minutes, eic = expected
        if not copies:
            counts["source_without_stored"] += 1
            differences.append(
                {"kind": "source_without_stored", "key": key, "source": expected}
            )
            continue
        # Legacy files put the fuel label (or Unknown) into data_type. Those
        # labels may be retained, but consumption and fuel conflicts block repair.
        if any(
            row["fuel_type"] != FUEL[key[1]]
            or row["data_type"] not in {"Actual Aggregated", "Unknown", FUEL[key[1]]}
            for row in copies
        ):
            counts["metric_or_fuel_conflict_keys"] += 1
            differences.append(
                {"kind": "metric_or_fuel_conflict", "key": key, "rows": copies}
            )
            continue
        if any(D(row["generation_mw"]) != power for row in copies):
            counts["mw_mismatch_keys"] += 1
            differences.append(
                {"kind": "mw_mismatch", "key": key, "source": expected, "rows": copies}
            )
            continue
        # Keep the exact canonical spelling where it already exists; otherwise
        # leave the retained row's historical spelling unchanged.
        copies.sort(key=lambda row: (row["plant_name"] != key[2], int(row["id"])))
        retained = copies[0]
        counts["source_matched_keys"] += 1
        for copy in copies[1:]:
            # Different durations are a conflict, not a proven duplicate.
            if int(copy["resolution_minutes"]) != int(retained["resolution_minutes"]):
                counts["conflicting_duplicate_durations"] += 1
                differences.append(
                    {
                        "kind": "conflicting_duplicate_durations",
                        "key": key,
                        "rows": copies,
                    }
                )
                break
        else:
            for copy in copies[1:]:
                delta = -D(copy["generation_mw"]) * int(copy["resolution_minutes"]) / 60
                candidates.append(
                    {
                        "action": "duplicate",
                        "before": copy,
                        "retained_id": retained["id"],
                        "unit_eic": eic,
                        "canonical_name": key[2],
                        "source_mw": str(power),
                        "source_minutes": minutes,
                        "delta_mwh": str(delta),
                    }
                )
                counts["duplicate_rows"] += 1
                candidate_delta += delta
            if int(retained["resolution_minutes"]) != minutes:
                delta = power * (minutes - int(retained["resolution_minutes"])) / 60
                candidates.append(
                    {
                        "action": "interval",
                        "before": retained,
                        "unit_eic": eic,
                        "source_mw": str(power),
                        "source_minutes": minutes,
                        "delta_mwh": str(delta),
                    }
                )
                counts["interval_rows"] += 1
                candidate_delta += delta
    candidate_path = output / "repair-candidates.jsonl.gz"
    expected_path = output / "source-observations.csv.gz"
    with gzip.open(expected_path, "wt", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "timestamp_ms",
                "psr_type",
                "plant_name",
                "generation_mw",
                "resolution_minutes",
                "unit_eic",
            ]
        )
        for key, value in sorted(canonical.items()):
            writer.writerow([*key, *value])
    with candidate_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
            for item in candidates:
                compressed.write((json.dumps(item, sort_keys=True) + "\n").encode())
    with (output / "unresolved-differences.jsonl").open("w") as handle:
        for item in differences:
            handle.write(json.dumps(item, default=str) + "\n")
    source_mwh = D(report["direct_block_mwh"])
    if not differences:
        require(
            before_mwh + candidate_delta == source_mwh,
            "Candidate changes do not reconcile energy",
        )
    report.update(
        status="source_reconciled" if not differences else "unresolved_differences",
        stored_rows=sum(map(len, stored.values())),
        exported_rows=len(rows),
        excluded_stored_rows_outside_comparison=excluded_rows,
        stored_mwh=str(before_mwh),
        source_mwh=str(source_mwh),
        source_minus_stored_mwh=str(source_mwh - before_mwh),
        delta_percent=str((source_mwh / before_mwh - 1) * 100) if before_mwh else None,
        stored_monthly_mwh=dict(before_monthly),
        comparison=dict(counts),
        stored_data_type_labels=dict(labels),
        unexpected_fuel_labels=dict(fuel_errors),
        audit_script_sha256=sha(Path(__file__)),
        alias_configuration_sha256=sha(ROOT / "config/entsoe-coal-aliases.json"),
        repair_candidates={
            "path": candidate_path.name,
            "rows": len(candidates),
            "sha256": sha(candidate_path),
            "delta_mwh": str(candidate_delta),
            "applied": False,
        },
        source_observations={
            "path": expected_path.name,
            "sha256": sha(expected_path),
            "rows": len(canonical),
        },
        unresolved_difference_keys=len(differences),
        limitations=[
            "Czech identified coal units only; not all national generation or other countries.",
            "Source gaps remain missing and are not filled with zero.",
            "Historical data_type labels are inventoried separately; candidates change only proven duplicate copies or interval durations.",
            "No production access, staging mutations, or frontend changes.",
        ],
    )
    (output / "audit.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "source_days",
                    "source_rows",
                    "stored_rows",
                    "stored_mwh",
                    "source_mwh",
                    "source_minus_stored_mwh",
                    "comparison",
                    "unresolved_difference_keys",
                )
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--extractor", type=Path, required=True)
    parser.add_argument(
        "--compare-end", help="Optional exclusive UTC cutoff inside the archived days"
    )
    args = parser.parse_args()
    audit(
        args.source,
        args.staging,
        args.label,
        args.output,
        args.extractor,
        args.compare_end,
    )
