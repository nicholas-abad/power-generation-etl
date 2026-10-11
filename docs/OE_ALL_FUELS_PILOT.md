# Australia (NEM + WEM) all fuels: staging pilot (tracker issue #32)

Scope: every OpenElectricity unit and fueltech (coal, gas, distillate, hydro, pumped hydro, wind,
utility solar, batteries, bioenergy) for NEM and WEM, unit grain, daily, loaded into a **separate
staging-only table**. `ingestion.oe_facility_generation_data`, its loader, `mv_oe_plant_monthly`,
the weekly job, the crosswalk and the tracker's coal filter are unchanged. Production is out of
scope. OpenElectricity use for this work is permitted (decided by the user, 2026-10-11).

## Objects (migration 022)

- `ingestion.oe_unit_generation_all_fuels`: one row per unit × network-local day × direction; key
  `(timestamp_ms, network_code, unit_code, direction)`. Directions:
  - `injection`: generation, including battery discharging;
  - `withdrawal`: battery charging and pumping (OE reports them as positive amounts);
  - `auxiliary`: a generating unit's negative values, stored as positive magnitudes;
  - `net`: a battery's signed aggregate where no components exist;
  - `anomaly`: a negative value on a withdrawal fueltech, kept signed (none seen so far).
  `basis = 'derived'` marks a battery component derived from OE's aggregate
  (aggregate = discharging − charging). Series the facility master does not list are kept with
  fueltech `unmatched`.
- `public.mv_oe_all_fuels_unit_monthly`: monthly per unit × direction in **network-local months**,
  owned by `etl_writer`, not granted to `dashboard_ro`, refreshed only with
  `refresh_views.py --source oe_all_fuels`.
- Rollback: `schema/migrations/rollback/022_oe_all_fuels.sql`.

Do not add this table's totals to the facility table's: fossil units appear in both.

## Procedure (staging)

```bash
# 0. Before: fingerprint of the existing OE facility table
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_oe_all_fuels.py fingerprint before.json

# 1. Extract per network (resumable; rerun the same command after an interruption)
energy-extract openelectricity --all-fuels --network NEM --start-date 2024-01-01 --end-date 2024-12-31 --output ./out
energy-extract openelectricity --all-fuels --network WEM --start-date 2024-01-01 --end-date 2024-12-31 --output ./out

# 2. Load each network-month atomically
for f in ./out/oe_all_fuels/*/parts/*.jsonl; do
  run_in_environment.py --environment staging --role writer -- \
    uv run python src/database_management.py load-data oe_all_fuels "$f" || echo "FAILED $f"
done

# 3. Refresh and verify
run_in_environment.py --environment staging --role writer -- uv run python src/refresh_views.py --source oe_all_fuels
run_in_environment.py --environment staging --role writer -- uv run python src/check_oe_all_fuels.py \
  reconcile 2024-01-01 2024-12-31 ./out/oe_all_fuels/NEM/manifest.jsonl ./out/oe_all_fuels/WEM/manifest.jsonl
run_in_environment.py --environment staging --role writer -- uv run python src/check_oe_all_fuels.py fingerprint after.json
uv run python src/check_oe_all_fuels.py compare before.json after.json
run_in_environment.py --environment staging --role writer -- \
  uv run python src/check_oe_all_fuels.py fuel-report 2024-01-01 2024-12-31 fuel_2024.csv
```

## Checks built into the extraction

- Per network-month, before a month is accepted: valid unit-days per fueltech group against the
  days OE's master expects (groups of ≥10 units must reach 90%; smaller groups are reported only),
  and series outside the master below 1% of the month's energy. A month that fails is recorded failed
  and fetched again on the next run.
- Every month also records our per-fueltech sums against OE's own network totals (a plumbing check;
  main fuels flagged beyond 1%, nothing fails on it).

## Results (staging, 2026-10-11)

Migration 022 applied; `dashboard_ro` surface unchanged (27 relations), no shadow tables. 2019 and
2024, NEM and WEM: 48 network-months, 433 requests (~1 API credit), 319,647 rows (87 MB), all loaded
with zero invalid or duplicate records. Reconciliation matched every month, every local day and the
view at its full key. The OE facility table fingerprint was identical before and after (232,206 rows,
md5 `255e7e6cadaf5eeb0e2fa196e15ec037`).

| TWh, injection | NEM 2019 | NEM 2024 | WEM 2019 | WEM 2024 |
|---|---:|---:|---:|---:|
| Coal | 140.06 | 120.26 | 7.95 | 6.23 |
| Gas | 18.72 | 11.52 | 7.26 | 7.23 |
| Wind | 16.94 | 28.20 | 2.13 | 3.74 |
| Utility solar | 5.12 | 15.93 | 0.05 | 0.32 |
| Hydro | 13.73 | 13.54 | — | — |
| Battery discharge | 0.07 | 0.65 | — | 0.15 |
| Bioenergy | 0.24 | 0.39 | 0.11 | 0.07 |
| Withdrawal: battery charging / pumping | 0.08 / 0.72 | 0.86 / 1.91 | — | 0.17 / — |

- Coal matches the existing OE facility table exactly in all four network-years; gas is within
  −0.6% to +0.3% (the new table includes commissioning units and splits negative values out as
  auxiliary).
- OE network-total check: every main fuel within 1% except NEM OCGT gas in Aug and Sep 2019
  (+1.2%, +1.3%).
- Rooftop solar is network-level only in OE and is not in this table.
- Batteries: most facility-days have both components (the aggregate is a cross-check, max difference
  10.6 MWh/day in NEM 2025-10); others have a component derived from the aggregate, a few only the net
  aggregate.

## Known issue in the existing coal view (not changed here)

`mv_oe_plant_monthly` buckets months in UTC, but OE daily rows are stamped at network-local midnight
(14:00/16:00 UTC the day before), so each tracker month gains the next month's first day and loses its
own (−0.4% to +0.7% per month on staging; yearly totals almost unchanged). The new view uses local
months. Fixing the existing view changes tracker numbers and needs a separate decision.

## Open methodology questions (Christine)

- Rooftop solar (network-level estimate only): in or out?
- Batteries and pumped hydro: discharge only, net, or both charge and discharge?
- Mapping of OE fueltechs to reporting categories (e.g. pumped-hydro generation is reported as hydro).
