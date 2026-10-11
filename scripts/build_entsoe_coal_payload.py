"""Build one country's rebuild payload from its certified coal source archive.

For every month of the certified window, re-parse the archived responses with
the extractor's per-Period parser (unnamed series included) and write the rows
to insert, one gzip TSV per month with a fixed column order. The only change
to a source observation is its database name:
- a unit in config/entsoe-unit-names.json gets its canonical plant_name;
- any other named unit keeps its literal source name;
- an unnamed series takes the name its EIC has in the reviewed identity table,
  else the stored name the country's identity map links to that EIC
  (an exact name match); otherwise it is excluded and listed in the manifest.
Each month's per-unit observation digests must equal the certified ones
(`certify_entsoe_coal_source.observation_line`), and both unique keys must hold
within the payload. Output is byte-identical for the same inputs.

Run with the extractors venv:
  PYTHONPATH=<extractor>/src python scripts/build_entsoe_coal_payload.py \
      --archive <root>/PL --certified certified/PL.json --identity identity/PL.json \
      --repair-id entsoe-coal-rebuild-PL-v1 --created-at-ms 1791676800000 \
      --output payload/PL
"""

import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from decimal import Decimal as D
import gzip
import hashlib
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))
from audit_entsoe_coal_history import FUEL, require  # noqa: E402
from certify_entsoe_coal_source import observation_line  # noqa: E402
import entsoe_names  # noqa: E402

COLUMNS = (
    "timestamp_ms",
    "country_code",
    "psr_type",
    "plant_name",
    "fuel_type",
    "data_type",
    "generation_mw",
    "resolution_minutes",
    "unit_eic",
    "production_unit_eic",
    "source_unit_name",
    "extraction_run_id",
    "created_at_ms",
)


def mw_text(value):
    """Source MW as decimal text without trailing zeros (as in observation_line)."""
    return format(D(repr(float(value))).normalize(), "f")


def tsv(value):
    if value is None:
        return "\\N"
    text = str(value)
    require(not any(c in text for c in "\t\n\\"), f"Unsafe TSV value {text!r}")
    return text


def build_month(args):
    (
        archive,
        country,
        domain,
        month,
        entries,
        units,
        identity_names,
        certified,
        run_id,
        created_at_ms,
        out_dir,
    ) = args
    from energy_extractors.entsoe.periods import parse_coal_periods

    rows, lines = [], defaultdict(list)
    excluded = defaultdict(lambda: {"observations": 0, "mwh": D(0)})
    for entry in entries:
        body = gzip.decompress(
            (
                Path(archive) / "xml" / f"{entry['day']}_{entry['psr']}.xml.gz"
            ).read_bytes()
        )
        require(
            hashlib.sha256(body).hexdigest() == entry["sha256"],
            f"Source hash mismatch {entry['day']} {entry['psr']}",
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
        for r in parsed:
            source_name = r["plant_name"]
            eic = r["unit_eic"]
            lines[eic].append(
                observation_line(
                    r["timestamp_ms"],
                    eic,
                    r["psr_type"],
                    mw_text(r["generation_mw"]),
                    r["resolution_minutes"],
                    r["production_unit_eic"],
                    source_name,
                )
            )
            reviewed = units.get(eic)
            if reviewed:
                require(
                    reviewed["psr_type"] == r["psr_type"],
                    f"{eic} arrives as {r['psr_type']}, reviewed as {reviewed['psr_type']}",
                )
                require(
                    source_name is None or source_name in reviewed["source_names"],
                    f"{eic} arrives under unreviewed name {source_name!r}",
                )
                name = reviewed["plant_name"]
            elif source_name is not None:
                name = source_name
            elif eic in identity_names:
                name = identity_names[eic]
            else:
                gap = excluded[(eic, r["psr_type"], entry["day"])]
                gap["observations"] += 1
                gap["mwh"] += (
                    D(mw_text(r["generation_mw"])) * r["resolution_minutes"] / 60
                )
                continue
            require(r["fuel_type"] == FUEL[r["psr_type"]], "Unexpected fuel label")
            rows.append(
                (
                    r["timestamp_ms"],
                    country,
                    r["psr_type"],
                    name,
                    r["fuel_type"],
                    "Actual Aggregated",
                    mw_text(r["generation_mw"]),
                    r["resolution_minutes"],
                    eic,
                    r["production_unit_eic"],
                    source_name,
                    run_id,
                    created_at_ms,
                )
            )
    for eic, unit_lines in lines.items():
        digest = hashlib.sha256("\n".join(sorted(unit_lines)).encode()).hexdigest()
        require(
            certified.get(eic) == digest,
            f"{month} {eic}: payload observations differ from the certified digest",
        )
    require(set(lines) == set(certified), f"{month}: units differ from certification")
    rows.sort(key=lambda r: (r[0], r[2], r[3], r[8]))
    natural = {(r[0], r[1], r[2], r[3]) for r in rows}
    require(len(natural) == len(rows), f"{month}: duplicate (time, country, PSR, name)")
    by_eic = {(r[1], r[8], r[0]) for r in rows}
    require(len(by_eic) == len(rows), f"{month}: duplicate (country, EIC, time)")
    data = (
        "\t".join(COLUMNS)
        + "\n"
        + "".join("\t".join(tsv(v) for v in r) + "\n" for r in rows)
    ).encode()
    path = Path(out_dir) / f"{month}.tsv.gz"
    path.write_bytes(gzip.compress(data, mtime=0))
    mwh = sum((D(r[6]) * r[7] / 60 for r in rows), D(0))
    return {
        "month": month,
        "file": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "rows": len(rows),
        "mwh": str(mwh),
        "excluded": [
            {
                "unit_eic": e,
                "psr": p,
                "day": d,
                "observations": g["observations"],
                "mwh": str(g["mwh"]),
            }
            for (e, p, d), g in sorted(excluded.items())
        ],
    }


def build(
    archive, certified_path, identity_path, repair_id, created_at_ms, out_dir, workers=8
):
    certified = json.loads(certified_path.read_text())
    country = certified["country"]
    require(
        archive.name == country, "Archive and certification are for different countries"
    )
    manifest_sha = hashlib.sha256((archive / "manifest.jsonl").read_bytes()).hexdigest()
    require(
        manifest_sha == certified["manifest_sha256"],
        "Archive manifest changed since certification",
    )
    entries = {}
    for line in (archive / "manifest.jsonl").read_text().splitlines():
        entry = json.loads(line)
        entries[(entry["day"], entry["psr"])] = entry
    ok = [
        e
        for e in entries.values()
        if e["status"] == "ok"
        and certified["start"] <= e["day"] < certified["end"]
        and e["psr"] in certified["psrs"]
    ]
    # Every month of the certified window gets a file, also one whose requests
    # were all verified no_data (header only: its stored rows are replaced by
    # nothing).
    by_month = {}
    last = (date.fromisoformat(certified["end"]) - timedelta(days=1)).isoformat()[:7]
    year, number = map(int, certified["start"][:7].split("-"))
    while f"{year:04d}-{number:02d}" <= last:
        by_month[f"{year:04d}-{number:02d}"] = []
        year, number = (year + 1, 1) if number == 12 else (year, number + 1)
    for entry in sorted(ok, key=lambda e: (e["day"], e["psr"])):
        by_month[entry["day"][:7]].append(entry)
    digests = defaultdict(dict)
    for row in certified["unit_months"]:
        digests[row["month"]][row["unit_eic"]] = row["observation_sha256"]
    units = {eic: e for (c, eic), e in entsoe_names.units().items() if c == country}
    identity = json.loads(identity_path.read_text())
    identity_names = {}
    for item in identity["stored_names"]:
        if item["rule"] == "exact" and len(item["eics"]) == 1:
            identity_names.setdefault(item["eics"][0], set()).add(item["stored_name"])
    identity_names = {e: n.pop() for e, n in identity_names.items() if len(n) == 1}
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"power-generation-etl/{repair_id}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = [
        (
            str(archive),
            country,
            certified["domain"],
            month,
            month_entries,
            units,
            identity_names,
            digests[month],
            run_id,
            created_at_ms,
            str(out_dir),
        )
        for month, month_entries in sorted(by_month.items())
    ]
    if workers <= 1:
        months = [build_month(job) for job in jobs]
    else:
        with ProcessPoolExecutor(workers) as pool:
            months = list(pool.map(build_month, jobs))
    for month in months:
        cert = sum(
            (
                D(r["mwh"])
                for r in certified["unit_months"]
                if r["month"] == month["month"]
            ),
            D(0),
        )
        excl = sum((D(x["mwh"]) for x in month["excluded"]), D(0))
        require(
            D(month["mwh"]) + excl == cert,
            f"{month['month']}: payload + exclusions differ from certified MWh",
        )
    return {
        "repair_id": repair_id,
        "country": country,
        "psrs": certified["psrs"],
        "start": certified["start"],
        "end": certified["end"],
        "columns": list(COLUMNS),
        "extraction_run_id": run_id,
        "created_at_ms": created_at_ms,
        "inputs": {
            "certification_sha256": hashlib.sha256(
                certified_path.read_bytes()
            ).hexdigest(),
            "archive_manifest_sha256": manifest_sha,
            "identity_table_sha256": hashlib.sha256(
                entsoe_names.PATH.read_bytes()
            ).hexdigest(),
            "identity_map_sha256": hashlib.sha256(
                identity_path.read_bytes()
            ).hexdigest(),
            "extractor_commit": certified["extractor_commit"],
        },
        "months": months,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--certified", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--repair-id", required=True)
    parser.add_argument("--created-at-ms", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)
    manifest = build(
        args.archive,
        args.certified,
        args.identity,
        args.repair_id,
        args.created_at_ms,
        args.output,
        args.workers,
    )
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    rows = sum(m["rows"] for m in manifest["months"])
    excluded = [x for m in manifest["months"] for x in m["excluded"]]
    print(
        json.dumps(
            {
                "months": len(manifest["months"]),
                "rows": rows,
                "excluded_unit_days": len(excluded),
                "excluded_mwh": str(sum((D(x["mwh"]) for x in excluded), D(0))),
            }
        )
    )


if __name__ == "__main__":
    main()
