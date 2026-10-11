# Chile coal-unit plants, all fuels: staging pilot (tracker issue #33)

Scope (decided by the user on 2026-10-11): every plant that has had a **coal unit** since 2016 — the
14 plants coal mode already selects plus the coal units its plant-level filter misses — with **every
unit** of those plants loaded under its own fuel (e.g. Mejillones' gas CTM3 next to its coal
CTM1/CTM2). Not every Chilean plant. The rows go into a **separate staging-only table**. The coal table
`ingestion.chile_generation_data`, its loader, `mv_chile_plant_monthly`, the weekly job and the
tracker's `fuel_type = 'Carbón'` contract are unchanged. Production is out of scope.

## Why a separate table

Coal mode selects plants by the plant master's free-text `combus_termo` and sums **all** units of a
selected plant as coal. Unit-level labels disagree with that in both directions:

- coal units under non-coal plant labels, found by sampling thermal units monthly 2016-2026:
  TER TARAPACÁ (378, master 'Diésel', to 2019-12), TER MEJILLONES CTM1/CTM2 (386, mixed),
  TER TOCOPILLA U12-U15 (387, mixed, to 2022-09), TER BOCAMINA I (117, 'Sin información', to 2022-02);
  PetCoke at TER PETROPOWER (182);
- non-coal units inside coal plants are counted as coal today.

The plant list is `COAL_UNIT_PLANT_IDS` in the extractor (19 plants), extracted with
`energy-extract chile --all-fuels --plants coal-units`. Until Christine settles which units count as
coal, these rows stay apart so the coal history cannot change underneath the tracker. Do not add this
table's totals to the coal table's: coal plants appear in both.

## Objects (migration 019)

- `ingestion.chile_generation_all_fuels`: plant × source technology × hour. Natural key
  `(timestamp_ms, plant_key, tipo_tecnologia, subtipo_tecnologia)`, all source values. `plant_key`
  is the CEN `id_central`, or `name:<plant>` for battery rows that have none. `plant` and `fuel_type`
  are revisable columns (updated on conflict). A missing subtype is stored as `''`.
- `public.mv_chile_all_fuels_plant_monthly`: monthly per `plant_key` × technology, owned by
  `etl_writer`, not granted to `dashboard_ro`. Refreshed only with
  `refresh_views.py --source chile_all_fuels`; it is not in `ALL_VIEWS`, so databases without 019
  still refresh cleanly.
- `fuel_type` is provisional and source-faithful: the thermal subtype ('Carbón', 'Gas Natural',
  'PetCoke', 'Cogeneración', ...) or the technology ('Solar', 'Eólica', 'Hidráulica', 'Geotérmica',
  'BESS'). Battery 'Retiro' (charging) is its own row, never netted against 'Inyección'.

Rollback: `schema/migrations/rollback/019_chile_all_fuels.sql` (drops only the new view and table).

## Source behaviour the extractor works around

- **Unstable pagination.** Rows are sorted by hour only and their order within an hour changes per
  request, so a page boundary inside an hour returns some unit-hours twice and as many never.
  Per-hour counts stay exact, so the extractor re-fetches each straddled hour with one page that
  holds it whole (`chile/pagination.py`). Coal mode has the same exposure; it is fixed on a
  separate branch.
- **DST nights** (`chile/hours.py`): the fall-back hour arrives as `hora` 25 labelled with the next
  day's 00:00, and the spring-forward day carries a zero placeholder at the nonexistent 00:00.
- **Identical duplicate rows**: 2019-12-31 is served two or three times over; exact copies are dropped
  and counted, conflicting repeats fail the plant-year.
- History starts between 2015-01-01 (no rows) and 2016-01-01 (rows).

## Procedure (staging)

```bash
# 0. Before: coal fingerprint (read-only)
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_chile_all_fuels.py fingerprint before.json

# 1. Extract (resumable; rerun the same command after an interruption). ~40 requests a year.
energy-extract chile --all-fuels --plants coal-units --start-date 2024-01-01 --end-date 2024-12-31 --output ./out

# 2. Load each plant-year file atomically (nothing is written for a file with any invalid or duplicate record)
for f in ./out/chile_all_fuels/parts/*.jsonl; do
  run_in_environment.py --environment staging --role writer -- \
    uv run python src/database_management.py load-data chile_all_fuels "$f" || echo "FAILED $f"
done

# 3. Refresh the pilot view, then verify
run_in_environment.py --environment staging --role writer -- \
  uv run python src/refresh_views.py --source chile_all_fuels
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_chile_all_fuels.py reconcile ./out/chile_all_fuels/manifest.jsonl 2024-01-01 2024-12-31
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_chile_all_fuels.py fingerprint after.json
uv run python src/check_chile_all_fuels.py compare before.json after.json
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_chile_all_fuels.py coal-report 2024-01-01 2024-12-31 coal_2024.csv
```

A failed plant-year is replayed by rerunning steps 1-2; reconciliation fails on any failed part or
mismatched day, so a partial year cannot pass.

## Open methodology questions (Christine)

- Do the newly found coal units (Tarapacá, Mejillones CTM1/CTM2, Bocamina I) count as coal?
- Do non-coal units inside coal plants keep counting as coal (current coal-mode behaviour)?
- PetCoke: coal or other? How to categorise 'Cogeneración', 'Termosolar' and BESS injection/withdrawal?

## Results

### Three-day live pilot, 2024-06-10..12 (2026-10-11)

- Migration 019 applied to staging; `dashboard_ro` surface unchanged (27 relations), no shadow tables.
- Extraction: 15 min, 15 requests (3 pages + 2 straddled-hour repairs per day), peak RSS 270 MB,
  exit 0. 90,936 raw unit-hours → 80,496 rows; 1,079 plant keys; 15 fuel labels; 0 repeated
  unit-hours after repair; no API key in any output.
- Load: 3 day files, all valid; replaying a day wrote 0 rows. Reconciliation: every day matched its
  manifest, view matched table. Coal table fingerprint identical before and after
  (755,878 rows, md5 `f49643a17fd9c72fe629ee51142a6c0f`).
- Coal report: all 10 coal-table plants with data match unit-level 'Carbón' exactly; TER MEJILLONES
  appears as a new coal plant (0 MWh on these days). Both tables show the same coal drop from
  2024-06-08 (7 of 10 plants at 0), i.e. it is in the source.
- Size: 235 bytes/row including indexes → ~2.3 GB per year at 2024 volume (~26.6k rows/day).

### Full-year benchmarks 2024 and 2019

(running)
