"""Per-fuel accounting of audited ONS source files, independent of the extractor.

Reads the raw Parquet files listed for one year in config/ons-benchmarks.json,
verifies their hashes, and counts rows and MWh per source fuel label: all rows,
rows in individual-plant modalities, accepted rows, and eligible rows rejected
by reason. The selection rules are re-implemented here from the documented
policy (energy-extractors ONS README and migration 017), deliberately without
importing the extractor, so the two implementations check each other.

No database connection. The ETL environment has no Parquet engine, so run:

    uv run --with pyarrow python scripts/audit_ons_fuels.py --year 2026 \
        --source-dir <dir with the year's Parquet files> [--write]

--write stores the result as the year's "fuels" table in the benchmark file.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "config/ons-benchmarks.json"

INDIVIDUAL = {"TIPO I", "TIPO II-A", "TIPO II-B"}
FORECAST = {"TIPO III", "PEQUENAS USINAS (TIPO III)"}
GROUP = {"TIPO II-C", "CONJUNTO DE USINAS", "PEQUENAS USINAS (MMGD)"}
MISSING = "<missing>"


def blank(series):
    text = series.astype("string").str.strip()
    return text.isna() | text.isin(["", "-"])


def classify(df):
    """Return (modality category, rejection reason or None) per raw row."""
    mode = df["cod_modalidadeoperacao"].astype("string").str.strip().str.upper()
    category = pd.Series("unresolved", index=df.index, dtype="object")
    category[mode.isin(INDIVIDUAL)] = "individual"
    category[mode.isin(FORECAST)] = "forecast"
    category[mode.isin(GROUP)] = "group"

    reason = pd.Series(None, index=df.index, dtype="object")
    eligible = category == "individual"
    generation = pd.to_numeric(df["val_geracao"], errors="coerce")
    checks = [
        (
            "unresolved_identity",
            blank(df["id_ons"])
            | blank(df["nom_usina"])
            | df["ceg"].astype("string").str.contains(r"[;,|]", regex=True, na=False),
        ),
        ("unresolved_fuel", blank(df["nom_tipousina"]) | blank(df["nom_tipocombustivel"])),
        ("invalid_timestamp", pd.to_datetime(df["din_instante"], errors="coerce").isna()),
        (
            "invalid_generation",
            ~np.isfinite(generation) | (generation < 0),
        ),
    ]
    for name, failed in checks:
        reason[eligible & reason.isna() & failed] = name
    candidate = eligible & reason.isna()
    copies = df[candidate].groupby(
        [df["din_instante"], df["id_ons"].astype("string").str.strip()]
    )["din_instante"].transform("size")
    reason[copies[copies > 1].index] = "duplicate_observation"
    return category, reason


def account(df):
    """Per-fuel table for one or more source files."""
    category, reason = classify(df)
    fuel = df["nom_tipocombustivel"].astype("string").str.strip().fillna(MISSING)
    fuel = fuel.mask(fuel.isin(["", "-"]), MISSING)
    mwh = pd.to_numeric(df["val_geracao"], errors="coerce")
    finite = mwh.where(np.isfinite(mwh), 0.0)
    accepted = (category == "individual") & reason.isna()
    table = {}
    for label in sorted(fuel.unique()):
        rows = fuel == label
        entry = {
            "raw_rows": int(rows.sum()),
            "raw_mwh": float(finite[rows].sum()),
            "rows_by_modality": {
                key: int(value)
                for key, value in category[rows].value_counts().sort_index().items()
            },
            "eligible_rows": int((rows & (category == "individual")).sum()),
            "accepted_rows": int((rows & accepted).sum()),
            "accepted_mwh": float(finite[rows & accepted].sum()),
            "rejected_eligible": {
                key: int(value)
                for key, value in reason[rows].dropna().value_counts().sort_index().items()
            },
        }
        table[label] = entry
    return table


def merge(tables):
    total = {}
    for table in tables:
        for label, entry in table.items():
            into = total.setdefault(
                label,
                {
                    "raw_rows": 0,
                    "raw_mwh": 0.0,
                    "rows_by_modality": {},
                    "eligible_rows": 0,
                    "accepted_rows": 0,
                    "accepted_mwh": 0.0,
                    "rejected_eligible": {},
                },
            )
            for key in ("raw_rows", "raw_mwh", "eligible_rows", "accepted_rows", "accepted_mwh"):
                into[key] += entry[key]
            for key in ("rows_by_modality", "rejected_eligible"):
                for name, count in entry[key].items():
                    into[key][name] = into[key].get(name, 0) + count
    return {label: total[label] for label in sorted(total)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(BENCHMARKS.read_text())
    entry = manifest["years"][str(args.year)]
    tables = []
    for item in entry["source_files"]:
        path = args.source_dir / item["url"].rsplit("/", 1)[1]
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != item["sha256"]:
            raise SystemExit(f"{path.name}: sha256 differs from the audited file")
        df = pd.read_parquet(path)
        if len(df) != item["raw_records"]:
            raise SystemExit(f"{path.name}: row count differs from the audited file")
        tables.append(account(df))
        print(f"{item['label']}: {len(df):,} rows", flush=True)
    fuels = merge(tables)
    accepted = sum(f["accepted_rows"] for f in fuels.values())
    if accepted != entry["expected_observations"]:
        raise SystemExit(
            f"Independent count {accepted:,} differs from the extractor's "
            f"{entry['expected_observations']:,}"
        )
    print(json.dumps(fuels, indent=2, ensure_ascii=False))
    if args.write:
        entry["fuels"] = fuels
        BENCHMARKS.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
