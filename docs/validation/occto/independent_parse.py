"""Independent parser for the OCCTO archive (parser B). Standard library only.

Written separately from energy_extractors.occto.parse (parser A) and sharing
no code with it, so that agreement between the two is evidence. It reads the
archive's manifest and raw files itself (it does not trust A's certificate
code), derives every stored observation, and writes:

  canonical.tsv.gz   one line per observation, in the load's column order and
                     sort order; its sha256 must equal parser A's digest
  unit_month.csv     per stored month x unit: kWh, slots, gaps
  report.json        digest, counts, every exclusion, and the structural checks
                     the rebuild depends on

Usage (from any directory; -I keeps the archive directory off sys.path):
  python3 -I independent_parse.py ARCHIVE_DIR CERTIFICATE OUT_DIR [--load-start 2024-03-25]

Conventions it re-derives, not imports:
  - slot htdnRsltHHMM is the half-hour ENDING at HH:MM JST; stored key is that
    JST wall-clock read as UTC, so 24:00 is the next day's 00:00 (calendar.timegm,
    never the host time zone);
  - month of an observation = calendar month of its stored key;
  - labels = WebDriver visible text (nbsp -> space, ASCII whitespace runs
    collapsed) then str.strip();
  - '' = gap (omitted), negative or non-integer = rejected, fuel '' = unspecified.
"""

import argparse
import calendar
import csv
import gzip
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

FUELS = {
    "原子力": ("01", "nuclear"), "水力": ("02", "hydro"), "火力（石炭）": ("03", "coal"),
    "火力（ガス）": ("04", "gas"), "火力（石油）": ("05", "oil"), "地熱": ("06", "geothermal"),
    "風力": ("07", "wind"), "太陽光・太陽熱": ("08", "solar"), "その他": ("09", "other"),
    "": ("10", "unspecified"),
}
WS = re.compile(r"[ \t\n\r\f\v]+")
INTEGER = re.compile(r"^-?[0-9]{1,3}(,[0-9]{3})*$|^-?[0-9]+$")


def label(value):
    return WS.sub(" ", ("" if value is None else str(value)).replace(" ", " ")).strip()


def slots():
    """(field name, minutes after JST midnight) for the 48 period-ending slots."""
    out = []
    for n in range(1, 49):
        h, m = divmod(n * 30, 60)
        out.append(("htdnRslt%02d%02d" % (h, m), n * 30))
    return out


SLOTS = slots()


def key_ms(day_text, minutes):
    y, mo, d = (int(x) for x in day_text.split("/"))
    wall = datetime(y, mo, d) + timedelta(minutes=minutes)
    return calendar.timegm(wall.timetuple()) * 1000


def month_of(ms):
    t = datetime(1970, 1, 1) + timedelta(milliseconds=ms)
    return "%04d-%02d" % (t.year, t.month)


def select_leaves(archive_dir, certificate):
    """Re-derive the loadable leaves from the manifest and check them against the certificate."""
    entries = [json.loads(l) for l in open(archive_dir / "manifest.jsonl", encoding="utf-8") if l.strip()]
    good = {}
    for e in entries:
        if e["status"] not in ("data", "verified_no_data", "truncated") or not e.get("file"):
            continue
        try:
            raw = gzip.open(archive_dir / e["file"], "rb").read()
        except (OSError, EOFError):
            continue
        if hashlib.sha256(raw).hexdigest() == e["sha256"]:
            good.setdefault((e["date"], e["partition"]), []).append(e)
    leaves = []
    day = date.fromisoformat(certificate["start"])
    while day <= date.fromisoformat(certificate["end"]):
        k = day.isoformat()
        whole = [e for e in good.get((k, "all"), []) if e["status"] != "truncated"]
        if len(whole) == 1:
            leaves.append(whole[0])
        else:
            kids = [good.get((k, "%02d" % a), []) for a in range(1, 11)]
            kids = [[e for e in ks if e["status"] != "truncated"] for ks in kids]
            if whole or any(len(ks) != 1 for ks in kids):
                raise SystemExit("parser B: coverage not exact on " + k)
            leaves.extend(ks[0] for ks in kids)
        day += timedelta(days=1)
    mine = sorted((e["date"], e["partition"], e["sha256"]) for e in leaves)
    theirs = sorted((l["date"], l["partition"], l["sha256"]) for l in certificate["leaves"])
    if mine != theirs:
        raise SystemExit("parser B: leaf selection differs from the certificate")
    return leaves


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive_dir", type=Path)
    ap.add_argument("certificate", type=Path)
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--load-start", default="2024-03-25")
    a = ap.parse_args()
    cert = json.loads(a.certificate.read_text(encoding="utf-8"))
    a.out_dir.mkdir(parents=True, exist_ok=True)

    leaves = select_leaves(a.archive_dir, cert)
    by_day = defaultdict(list)
    for e in leaves:
        by_day[e["date"]].append(e)

    digest = hashlib.sha256()
    rows = 0
    unit_month = defaultdict(lambda: [Decimal(0), 0, 0])  # kwh, slots, gaps
    fuels_per_unit_month = defaultdict(set)
    codes_per_name = defaultdict(set)
    names_per_code = defaultdict(set)
    gaps_total = 0
    rejected, mismatches, excluded_days, duplicate_keys = [], [], [], []
    fuel_kwh = defaultdict(Decimal)

    with gzip.open(a.out_dir / "canonical.tsv.gz", "wt", encoding="utf-8", newline="") as out:
        for day in sorted(by_day):
            if day < a.load_start:
                excluded_days.append(day)
                continue
            obs = []
            for e in by_day[day]:
                body = json.loads(gzip.open(a.archive_dir / e["file"], "rb").read().decode("utf-8"))
                for it in ((body.get("data") or {}).get("items") or []):
                    plant, unit = label(it.get("htdnsNm")), label(it.get("unitNm"))
                    code = label(it.get("htdnsCd"))
                    fname = str(it.get("htdnNm") or "").strip()
                    if fname not in FUELS:
                        rejected.append({"reason": "unknown fuel name", "date": it.get("tgtDate"),
                                         "plant_code": code, "plant": plant, "unit": unit, "fuel_name": fname})
                        continue
                    fcode, ftype = FUELS[fname]
                    acode, aname = label(it.get("areaCd")), label(it.get("areaCdNm"))
                    codes_per_name[(plant, unit)].add(code)
                    names_per_code[(code, unit)].add(plant)
                    total = Decimal(0)
                    for field, minutes in SLOTS:
                        text = str(it.get(field) if it.get(field) is not None else "").strip()
                        ms = key_ms(it["tgtDate"], minutes)
                        um = (month_of(ms), code, plant, unit, ftype)
                        if text == "":
                            gaps_total += 1
                            unit_month[um][2] += 1
                            continue
                        if not INTEGER.match(text):
                            rejected.append({"reason": "unreadable value", "date": it["tgtDate"], "plant": plant,
                                             "unit": unit, "slot": field, "raw_value": text})
                            continue
                        value = Decimal(text.replace(",", ""))
                        total += value
                        if value < 0:
                            rejected.append({"reason": "negative value", "date": it["tgtDate"], "plant": plant,
                                             "unit": unit, "slot": field, "raw_value": text})
                            continue
                        obs.append((ms, plant, unit, code, fcode, ftype, acode, aname, 30, int(value)))
                        unit_month[um][0] += value
                        unit_month[um][1] += 1
                        fuels_per_unit_month[(month_of(ms), plant, unit)].add(ftype)
                        fuel_kwh[ftype] += value
                    day_total = str(it.get("dayElr") or "").replace(",", "").strip()
                    if day_total:
                        try:
                            if Decimal(day_total) != total:
                                mismatches.append({"date": it["tgtDate"], "plant": plant, "unit": unit,
                                                   "day_total": day_total, "slot_sum": str(total)})
                        except InvalidOperation:
                            mismatches.append({"date": it["tgtDate"], "plant": plant, "unit": unit,
                                               "day_total": day_total, "slot_sum": "unreadable dayElr"})
            obs.sort(key=lambda r: (r[0], r[1], r[2]))
            for prev, cur in zip(obs, obs[1:]):
                if prev[:3] == cur[:3]:
                    duplicate_keys.append(list(cur[:3]))
            for r in obs:
                line = "\t".join(str(x) for x in r) + "\n"
                digest.update(line.encode("utf-8"))
                out.write(line)
            rows += len(obs)

    with open(a.out_dir / "unit_month.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["month", "plant_code", "plant", "unit", "fuel_type", "kwh", "slots", "gaps"])
        for (m, code, plant, unit, ftype), (kwh, n, g) in sorted(unit_month.items()):
            w.writerow([m, code, plant, unit, ftype, kwh, n, g])

    report = {
        "rows": rows,
        "digest": digest.hexdigest(),
        "load_start": a.load_start,
        "excluded_days": excluded_days,
        "gaps": gaps_total,
        "rejected": rejected,
        "day_total_mismatches": mismatches,
        "duplicate_keys": duplicate_keys[:50],
        "duplicate_key_count": len(duplicate_keys),
        "names_with_several_codes": [[list(k), sorted(v)] for k, v in codes_per_name.items() if len(v) > 1],
        "codes_with_several_names": [[list(k), sorted(v)] for k, v in names_per_code.items() if len(v) > 1],
        "unit_months_with_several_fuels": [[list(k), sorted(v)] for k, v in fuels_per_unit_month.items() if len(v) > 1],
        "kwh_by_fuel": {k: str(v) for k, v in sorted(fuel_kwh.items())},
        "certificate_digest": cert["digest"],
    }
    (a.out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("rows", "digest", "gaps", "duplicate_key_count")}
                     | {"rejected": len(rejected), "day_total_mismatches": len(mismatches),
                        "names_with_several_codes": len(report["names_with_several_codes"]),
                        "codes_with_several_names": len(report["codes_with_several_names"]),
                        "unit_months_with_several_fuels": len(report["unit_months_with_several_fuels"])},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
