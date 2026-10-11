"""Independent monthly benchmarks of Japanese generation by fuel (no database).

Two public sources, neither of which is the OCCTO unit-level dataset:

  (a) TSO area supply-demand actuals (エリア需給実績), the common FY2024 format
      ``eria_jukyu_YYYYMM_NN.csv`` published by each transmission operator.
      Rows are 30-minute periods labelled by period START (JST); values are MWh
      per 30 minutes (published header unit; equal to 0.5 x average MW).
  (b) METI / 資源エネルギー庁 電力調査統計 table 2-(1) 発電実績, the
      machine-readable layout (``2-1-YYYYn.xlsx``), monthly national totals in
      MWh (published unit: 千kWh = MWh).

Subcommands:
  fetch  download every source file once into RAW_DIR, keeping the bytes as
         served and appending one line per request to RAW_DIR/sources.jsonl
  build  parse RAW_DIR and write area_month_fuel.csv and meti_month_fuel.csv

Usage (-I keeps the download directory off sys.path; files are untrusted data):
  python3 -I benchmarks.py fetch RAW_DIR [--last-month 2026-10]
  python3 -I benchmarks.py build RAW_DIR OUT_DIR

Needs pandas and openpyxl for ``build`` (METI workbooks); ``fetch`` is stdlib.
"""

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Several TSO servers return 403 to a bare "Mozilla/5.0" or Python agent but
# serve a normal browser string; the files are public downloads.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)
REQUEST_GAP_S = 1.5
FIRST_MONTH = (2024, 3)
# Benchmarks start where the unit-level load starts; earlier periods are dropped.
WINDOW_START = datetime(2024, 3, 25)

AREAS = {  # code -> (name, monthly CSV URL template)
    "01": ("hokkaido", "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/csv/eria_jukyu_{ym}_01.csv"),
    "02": ("tohoku", "https://setsuden.nw.tohoku-epco.co.jp/common/demand/eria_jukyu_{ym}_02.csv"),
    "03": ("tokyo", "https://www.tepco.co.jp/forecast/html/images/eria_jukyu_{ym}_03.csv"),
    "04": ("chubu", "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{ym}_04.csv"),
    "05": ("hokuriku", "https://www.rikuden.co.jp/nw/denki-yoho/csv/eria_jukyu_{ym}_05.csv"),
    "06": ("kansai", "https://www.kansai-td.co.jp/interchange/denkiyoho/area-performance/eria_jukyu_{ym}_06.csv"),
    "07": ("chugoku", "https://www.energia.co.jp/nw/jukyuu/sys/eria_jukyu_{ym}_07.csv"),
    "08": ("shikoku", "https://www.yonden.co.jp/nw/supply_demand/csv/eria_jukyu_{ym}_08.csv"),
    "09": ("kyushu", "https://www.kyuden.co.jp/td_area_jukyu/csv/eria_jukyu_{ym}_09.csv"),
    "10": ("okinawa", None),  # www.okiden.co.jp answers 403 (Incapsula) to every request from here
}
OKINAWA_PAGE = "https://www.okiden.co.jp/business-support/service/supply-and-demand/index.html"
# Chubu only links the latest months individually; earlier months come as
# fiscal-year zips of the same monthly CSVs.
CHUBU_FY_ZIP = "https://powergrid.chuden.co.jp/denki_yoho_content_data/eria_jukyu_{fy}.zip"
METI_URL = "https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/{fy}/2-1-{fy}n.xlsx"


# ---------------------------------------------------------------- fetch


def months(first, last):
    y, m = first
    while (y, m) <= last:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def fiscal_year(y, m):
    return y if m >= 4 else y - 1


def load_manifest(raw):
    path = raw / "sources.jsonl"
    if not path.exists():
        return {}
    done = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if rec["http_status"] == 200:
            done[rec["url"]] = rec
    return done


def fetch_one(url, dest, raw, done):
    """Download url to dest unless already fetched; append a manifest line."""
    if url in done:
        return done[url]["http_status"]
    time.sleep(REQUEST_GAP_S)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.5"})
    rec = {"url": url, "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body, status = resp.read(), resp.status
            ctype = resp.headers.get("Content-Type")
    except urllib.error.HTTPError as exc:
        body, status, ctype = None, exc.code, exc.headers.get("Content-Type")
    except urllib.error.URLError as exc:
        body, status, ctype = None, None, None
        rec["error"] = str(exc.reason)
    rec.update(http_status=status, content_type=ctype, sha256=None, bytes=None, local_path=None)
    if status == 200 and body is not None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        rec.update(sha256=hashlib.sha256(body).hexdigest(), bytes=len(body),
                   local_path=str(dest.relative_to(raw)))
    with (raw / "sources.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"{status} {url}", file=sys.stderr)
    return status


def cmd_fetch(args):
    raw = Path(args.raw_dir).resolve()
    raw.mkdir(parents=True, exist_ok=True)
    done = load_manifest(raw)
    last = tuple(int(x) for x in args.last_month.split("-"))
    span = list(months(FIRST_MONTH, last))

    for code, (name, template) in AREAS.items():
        if template is None:
            fetch_one(OKINAWA_PAGE, raw / "tso" / name / "index.html", raw, done)
            continue
        if code == "04":
            for fy in sorted({fiscal_year(y, m) for y, m in span}):
                url = CHUBU_FY_ZIP.format(fy=fy)
                fetch_one(url, raw / "tso" / name / url.rsplit("/", 1)[1], raw, done)
            # the current fiscal year's latest months are not yet in the zip
            current_fy = fiscal_year(*last)
            wanted = [(y, m) for y, m in span if fiscal_year(y, m) == current_fy]
        else:
            wanted = span
        for y, m in wanted:
            url = template.format(ym=f"{y}{m:02d}")
            fetch_one(url, raw / "tso" / name / url.rsplit("/", 1)[1], raw, done)

    for fy in range(2024, fiscal_year(*last) + 1):
        url = METI_URL.format(fy=fy)
        fetch_one(url, raw / "meti" / url.rsplit("/", 1)[1], raw, done)


# ---------------------------------------------------------------- build (TSO)

# Column header (whitespace removed, unit suffix stripped) -> benchmark fuel.
# Columns not listed (demand, curtailment, interconnector flows, totals) are
# not generation and are deliberately ignored; unknown headers abort the build.
TSO_COLUMNS = {
    "原子力": "nuclear",
    "火力(LNG)": "lng",
    "火力(石炭)": "coal",
    "火力(石油)": "oil",
    "火力(その他)": "thermal_other",
    "水力": "hydro",
    "地熱": "geothermal",
    "バイオマス": "biomass",
    "太陽光発電実績": "solar",
    "風力発電実績": "wind",
    "揚水": "pumped_storage_net",
    "蓄電池": "battery",
    "その他": "other",
}
TSO_IGNORED = {
    "エリア需要", "連系線", "合計",
    # curtailed output (not generated), added per source in the FY2025 layout
    "火力出力制御量", "バイオマス出力制御量", "太陽光出力制御量", "風力出力制御量",
}
FUEL_ORDER = ["nuclear", "coal", "lng", "oil", "thermal_other", "hydro", "pumped_storage_net",
              "geothermal", "biomass", "solar", "wind", "battery", "other"]


def norm_header(h):
    # NFKC folds the full-width （ＬＮＧ） some TSOs use into (LNG)
    h = re.sub(r"\s+", "", unicodedata.normalize("NFKC", h))
    return re.sub(r"\[[^\]]*\]$", "", h)


def mwh_factor(unit_text, source):
    """MWh per published value for one 30-minute period, from the unit row."""
    unit = re.sub(r"\s+", "", unit_text)
    if "MW平均" in unit:  # average MW over the half hour
        return 0.5
    if "MWh" in unit:
        return 1.0
    raise ValueError(f"{source}: unrecognised unit row {unit_text!r}")


def decode(raw_bytes):
    for enc in ("utf-8-sig", "cp932"):
        try:
            return raw_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("neither UTF-8 nor CP932")


def parse_tso_csv(text, source):
    """Yield (period_start, {fuel: MWh}) from one eria_jukyu CSV."""
    rows = list(csv.reader(io.StringIO(text, newline="")))
    hdr_i = next(i for i, r in enumerate(rows) if r and r[0].strip() in ("DATE", "日付"))
    header = rows[hdr_i]
    factor = mwh_factor(" ".join(c for r in rows[:hdr_i] for c in r), source)
    cols = {}
    for j, h in enumerate(header[2:], start=2):
        key = norm_header(h)
        if not key:
            continue
        if key in TSO_COLUMNS:
            cols[j] = TSO_COLUMNS[key]
        elif key not in TSO_IGNORED:
            raise ValueError(f"{source}: unmapped column {h!r}")
    for r in rows[hdr_i + 1:]:
        if not r or not r[0].strip():
            continue
        start = datetime.strptime(f"{r[0].strip()} {r[1].strip()}", "%Y/%m/%d %H:%M")
        vals = {}
        for j, fuel in cols.items():
            cell = r[j].strip().replace(",", "") if j < len(r) else ""
            if cell not in ("", "-"):
                vals[fuel] = vals.get(fuel, 0.0) + float(cell) * factor
        if vals:  # rows for periods not yet reported carry a timestamp only
            yield start, vals


def tso_files(raw):
    """Yield (area, source_label, text) for every downloaded area-month CSV."""
    for code, (name, _) in AREAS.items():
        folder = raw / "tso" / name
        if not folder.is_dir():
            continue
        for path in sorted(folder.iterdir()):
            if path.suffix == ".csv":
                yield name, path.name, decode(path.read_bytes())
            elif path.suffix == ".zip":
                with zipfile.ZipFile(path) as zf:
                    for member in sorted(zf.namelist()):
                        if member.endswith(".csv"):
                            yield name, f"{path.name}:{member}", decode(zf.read(member))


def build_area(raw, out):
    # (area, start) -> values; a later file wins only if it is the same period
    # republished, and identical duplicates are tolerated.
    periods = {}
    conflicts = 0
    for area, source, text in tso_files(raw):
        for start, vals in parse_tso_csv(text, source):
            if start < WINDOW_START:
                continue
            key = (area, start)
            if key in periods and periods[key] != vals:
                conflicts += 1
            periods[key] = vals
    if conflicts:
        print(f"warning: {conflicts} periods appeared twice with different values; last file wins",
              file=sys.stderr)

    agg = defaultdict(float)
    present = defaultdict(int)
    last_start = {}
    for (area, start), vals in periods.items():
        month = f"{start.year}-{start.month:02d}"
        present[(area, month)] += 1
        last_start[area] = max(last_start.get(area, start), start)
        for fuel, v in vals.items():
            agg[(area, month, fuel)] += v

    rows = []
    for (area, month), n in sorted(present.items()):
        y, m = map(int, month.split("-"))
        first = max(datetime(y, m, 1), WINDOW_START)
        nxt = datetime(y + (m == 12), m % 12 + 1, 1)
        expected = int((nxt - first) / timedelta(minutes=30))
        for fuel in FUEL_ORDER:
            rows.append({"month": month, "area": area, "fuel": fuel,
                         "mwh": round(agg.get((area, month, fuel), 0.0), 3),
                         "half_hours_present": n, "half_hours_expected": expected,
                         "complete": n == expected})
    write_csv(out / "area_month_fuel.csv", rows)
    for area, ts in sorted(last_start.items()):
        print(f"{area}: last period start {ts:%Y-%m-%d %H:%M}", file=sys.stderr)


# ---------------------------------------------------------------- build (METI)

def build_meti(raw, out):
    import pandas as pd  # noqa: PLC0415  (only build needs pandas)

    rows = []
    for path in sorted((raw / "meti").glob("2-1-*n.xlsx")):
        rows.extend(parse_meti_workbook(pd, path))
    write_csv(out / "meti_month_fuel.csv", rows)


def parse_meti_workbook(pd, path):  # filled in once the layout is inspected
    raise NotImplementedError


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {path}", file=sys.stderr)


def cmd_build(args):
    raw, out = Path(args.raw_dir).resolve(), Path(args.out_dir).resolve()
    build_area(raw, out)
    build_meti(raw, out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("raw_dir")
    f.add_argument("--last-month", default=date.today().strftime("%Y-%m"))
    f.set_defaults(func=cmd_fetch)
    b = sub.add_parser("build")
    b.add_argument("raw_dir")
    b.add_argument("out_dir")
    b.set_defaults(func=cmd_build)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
