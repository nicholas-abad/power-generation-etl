"""Per-fuel accounting of audited ONS source files, independent of the extractor.

Reads the raw Parquet files listed for one year in config/ons-benchmarks.json,
verifies their hashes, and counts rows and MWh per source fuel label: all rows,
rows in individual-plant modalities, accepted rows, and eligible rows rejected
by reason. Missing (null) generation in eligible plants is also listed as
source gaps: contiguous hourly runs per ONS ID. The selection rules are
re-implemented here from the documented policy (energy-extractors ONS README
and migration 017), deliberately without importing the extractor, so the two
implementations check each other.

No database connection. The ETL environment has no Parquet engine, so run:

    uv run --with pyarrow python scripts/audit_ons_fuels.py --year 2026 \
        --source-dir <dir with the year's Parquet files> [--write]

--write stores the result as the year's "fuels" table and "source_gaps" list in
the benchmark file.
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
    """Return (modality category, rejection reason or None) per raw row.

    Expects a unique index (account and gaps reset it).
    """
    mode = df["cod_modalidadeoperacao"].astype("string").str.strip().str.upper()
    category = pd.Series("unresolved", index=df.index, dtype="object")
    category[mode.isin(INDIVIDUAL)] = "individual"
    category[mode.isin(FORECAST)] = "forecast"
    category[mode.isin(GROUP)] = "group"

    reason = pd.Series(None, index=df.index, dtype="object")
    eligible = category == "individual"
    generation = pd.to_numeric(df["val_geracao"], errors="coerce")
    raw_value = df["val_geracao"].astype("string").str.strip()
    checks = [
        (
            "unresolved_identity",
            blank(df["id_ons"])
            | blank(df["nom_usina"])
            | df["ceg"].astype("string").str.contains(r"[;,|]", regex=True, na=False),
        ),
        (
            "unresolved_fuel",
            blank(df["nom_tipousina"]) | blank(df["nom_tipocombustivel"]),
        ),
        (
            "invalid_timestamp",
            pd.to_datetime(df["din_instante"], errors="coerce").isna(),
        ),
        ("missing_generation", raw_value.isna() | raw_value.eq("")),
        (
            "invalid_generation",
            ~np.isfinite(generation) | (generation < 0),
        ),
    ]
    for name, failed in checks:
        reason[eligible & reason.isna() & failed] = name
    candidate = eligible & reason.isna()
    copies = (
        df[candidate]
        .groupby([df["din_instante"], df["id_ons"].astype("string").str.strip()])[
            "din_instante"
        ]
        .transform("size")
    )
    reason[copies[copies > 1].index] = "duplicate_observation"
    return category, reason


def account(df):
    """Per-fuel table, source gaps and candidate plant-hours for one source file."""
    df = df.reset_index(drop=True)
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
                for key, value in reason[rows]
                .dropna()
                .value_counts()
                .sort_index()
                .items()
            },
        }
        table[label] = entry
    # Plant-hours that reached the collision check: must not recur in another file.
    candidate = (category == "individual") & (
        reason.isna() | (reason == "duplicate_observation")
    )
    stamps = pd.to_datetime(df.loc[candidate, "din_instante"]).astype("int64")
    ids = df.loc[candidate, "id_ons"].astype("string").str.strip()
    plant_hours = set(zip(stamps.tolist(), ids.tolist()))
    return table, gaps(df, reason), plant_hours


def gaps(df, reason):
    """Contiguous hourly runs of null generation in eligible plants.

    Each plant-hour counts once; an hour that also has an accepted value is
    not a gap (same rule as the extractor's source_gaps).
    """
    missing = df[reason == "missing_generation"]
    if missing.empty:
        return []
    frame = pd.DataFrame(
        {
            "ons_plant_id": missing["id_ons"].astype("string").str.strip(),
            "fuel_type": missing["nom_tipocombustivel"].astype("string").str.strip(),
            "start": pd.to_datetime(missing["din_instante"]),
        }
    ).drop_duplicates(["ons_plant_id", "start"])
    accepted = df[
        reason.isna()
        & df["cod_modalidadeoperacao"]
        .astype("string")
        .str.strip()
        .str.upper()
        .isin(INDIVIDUAL)
    ]
    valued = set(
        zip(
            accepted["id_ons"].astype("string").str.strip(),
            pd.to_datetime(accepted["din_instante"]),
        )
    )
    frame = frame[[k not in valued for k in zip(frame["ons_plant_id"], frame["start"])]]
    if frame.empty:
        return []
    frame = frame.sort_values(["ons_plant_id", "start"])
    new_run = frame.groupby("ons_plant_id")["start"].diff() != pd.Timedelta(hours=1)
    runs = frame.groupby(new_run.cumsum()).agg(
        ons_plant_id=("ons_plant_id", "first"),
        fuel_type=("fuel_type", "first"),
        start=("start", "min"),
        last=("start", "max"),
        hours=("start", "size"),
    )
    hour = pd.Timedelta(hours=1)
    return [
        {
            "ons_plant_id": run.ons_plant_id,
            "fuel_type": run.fuel_type,
            "start_ms": run.start.value // 1_000_000,
            "end_ms": (run.last + hour).value // 1_000_000,
            "hours": int(run.hours),
        }
        for run in runs.itertuples()
    ]


def join_gaps(parts):
    """Join runs that continue across file boundaries."""
    joined = []
    for gap in sorted(
        (g for part in parts for g in part),
        key=lambda g: (g["ons_plant_id"], g["start_ms"]),
    ):
        last = joined[-1] if joined else None
        if (
            last
            and last["ons_plant_id"] == gap["ons_plant_id"]
            and last["end_ms"] == gap["start_ms"]
        ):
            last["end_ms"] = gap["end_ms"]
            last["hours"] += gap["hours"]
        else:
            joined.append(dict(gap))
    return joined


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
            for key in (
                "raw_rows",
                "raw_mwh",
                "eligible_rows",
                "accepted_rows",
                "accepted_mwh",
            ):
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
    gap_parts = []
    seen = set()
    for item in entry["source_files"]:
        path = args.source_dir / item["url"].rsplit("/", 1)[1]
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != item["sha256"]:
            raise SystemExit(f"{path.name}: sha256 differs from the audited file")
        df = pd.read_parquet(path)
        if len(df) != item["raw_records"]:
            raise SystemExit(f"{path.name}: row count differs from the audited file")
        table, file_gaps, plant_hours = account(df)
        if plant_hours & seen:
            # The collision rule rejects both copies; per-file counts would not.
            raise SystemExit(f"{path.name} repeats plant-hours of another audited file")
        seen |= plant_hours
        tables.append(table)
        gap_parts.append(file_gaps)
        print(f"{item['label']}: {len(df):,} rows", flush=True)
    fuels = merge(tables)
    accepted = sum(f["accepted_rows"] for f in fuels.values())
    if accepted != entry["expected_observations"]:
        raise SystemExit(
            f"Independent count {accepted:,} differs from the extractor's "
            f"{entry['expected_observations']:,}"
        )
    source_gaps = join_gaps(gap_parts)
    print(
        json.dumps(
            {"fuels": fuels, "source_gaps": source_gaps}, indent=2, ensure_ascii=False
        )
    )
    if args.write:
        entry["fuels"] = fuels
        entry["source_gaps"] = source_gaps
        BENCHMARKS.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
