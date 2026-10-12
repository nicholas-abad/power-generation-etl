"""Step 4: OCCTO unit-level totals vs independent published totals. Stdlib only.

  python3 -I compare_benchmarks.py PARSER_B_DIR BENCHMARK_DIR > benchmarks_comparison.txt

Aggregates parser B's canonical rows (the rows the rebuild loads) to
month x area x fuel and compares them with
  (a) TSO area actuals (area_month_fuel.csv), only where the TSO month is
      complete for that fuel; both sides bucket months by period end;
  (b) METI 電力調査統計 national monthly totals (meti_month_fuel.csv).
Ratios are OCCTO / benchmark. OCCTO covers units >= 100 MW that agreed to
publish, so ratios below 1 are expected for coal and gas; nuclear should be ~1.
Hydro is approximate: OCCTO's hydro includes pumped-storage generation, TSO
hydro excludes it and TSO pumped storage is net of pumping.
"""

import csv
import gzip
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

AREAS = {
    "01": "hokkaido", "02": "tohoku", "03": "tokyo", "04": "chubu", "05": "hokuriku",
    "06": "kansai", "07": "chugoku", "08": "shikoku", "09": "kyushu", "10": "okinawa",
}
# OCCTO fuel -> benchmark fuels it is compared against
TSO_FUELS = {"coal": ["coal"], "gas": ["lng"], "oil": ["oil"], "nuclear": ["nuclear"],
             "hydro": ["hydro", "pumped_storage_net"]}
METI_FUELS = {"coal": ["coal"], "gas": ["lng"], "oil": ["oil"], "nuclear": ["nuclear"],
              "hydro": ["hydro", "pumped_storage"]}


def month_of(ms):
    t = datetime(1970, 1, 1) + timedelta(milliseconds=ms)
    return "%04d-%02d" % (t.year, t.month)


def ratio(a, b):
    return "%.3f" % (a / b) if b else "n/a"


def main():
    b_dir, bench = Path(sys.argv[1]), Path(sys.argv[2])
    occto = defaultdict(int)  # (month, area, fuel) -> kWh
    with gzip.open(b_dir / "canonical.tsv.gz", "rt", encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            occto[(month_of(int(p[0])), AREAS[p[6]], p[5])] += int(p[9])

    tso, complete = defaultdict(float), {}
    for r in csv.DictReader(open(bench / "area_month_fuel.csv", encoding="utf-8")):
        if r["mwh"] != "":
            tso[(r["month"], r["area"], r["fuel"])] = float(r["mwh"])
        complete[(r["month"], r["area"], r["fuel"])] = r["complete"] == "True"
    meti = defaultdict(float)
    for r in csv.DictReader(open(bench / "meti_month_fuel.csv", encoding="utf-8")):
        meti[(r["month"], r["fuel"])] = float(r["mwh"])

    print("== (a) OCCTO vs TSO area actuals, months complete in the TSO data, TWh")
    print("fuel      occto      tso   ratio  area-months")
    by_area = defaultdict(lambda: [0.0, 0.0])
    for fuel, tso_fuels in TSO_FUELS.items():
        o = t = 0.0
        n = 0
        for (month, area, tf), ok in complete.items():
            if tf != tso_fuels[0] or not ok or area == "okinawa":
                continue
            if not all(complete.get((month, area, x)) for x in tso_fuels):
                continue
            ov = occto.get((month, area, fuel), 0) / 1000.0
            bv = tso[(month, area, tso_fuels[0])]
            if fuel == "hydro":
                bv += max(tso[(month, area, "pumped_storage_net")], 0.0)
            o, t, n = o + ov, t + bv, n + 1
            if fuel == "coal":
                by_area[area][0] += ov
                by_area[area][1] += bv
        print(f"{fuel:8s} {o / 1e6:8.2f} {t / 1e6:8.2f} {ratio(o, t):>7s}  {n}")
    print("\ncoal by area (complete months)")
    for area, (o, t) in sorted(by_area.items()):
        print(f"  {area:9s} {o / 1e6:7.2f} {t / 1e6:7.2f} {ratio(o, t):>7s}")

    print("\ncoal by month, 9 areas (complete area-months only)")
    months = sorted({m for (m, _, f) in complete if f == "coal"})
    for month in months:
        areas = [a for a in AREAS.values() if a != "okinawa" and complete.get((month, a, "coal"))]
        o = sum(occto.get((month, a, "coal"), 0) for a in areas) / 1000.0
        t = sum(tso[(month, a, "coal")] for a in areas)
        if areas:
            print(f"  {month} areas={len(areas)} occto={o / 1e6:6.3f} tso={t / 1e6:6.3f} ratio={ratio(o, t)}")

    print("\n== (b) OCCTO (all 10 areas) vs METI national, TWh")
    meti_months = sorted({m for (m, _) in meti})
    print("fuel      occto     meti   ratio")
    for fuel, meti_fuels in METI_FUELS.items():
        o = sum(occto.get((m, a, fuel), 0) for m in meti_months for a in AREAS.values()) / 1000.0
        t = sum(meti[(m, x)] for m in meti_months for x in meti_fuels)
        print(f"{fuel:8s} {o / 1e6:8.2f} {t / 1e6:8.2f} {ratio(o, t):>7s}")
    print("\ncoal by month vs METI")
    for m in meti_months:
        o = sum(occto.get((m, a, "coal"), 0) for a in AREAS.values()) / 1000.0
        print(f"  {m} occto={o / 1e6:6.3f} meti={meti[(m, 'coal')] / 1e6:6.3f} ratio={ratio(o, meti[(m, 'coal')])}")
    print("\nOCCTO other/unspecified (no benchmark column), TWh:",
          {f: round(sum(v for (m, a, ff), v in occto.items() if ff == f) / 1e9, 2) for f in ("other", "unspecified")})


if __name__ == "__main__":
    main()
