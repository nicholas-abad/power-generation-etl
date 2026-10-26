# Daily and weekly generation drill-down

Daily and weekly coal generation for ENTSO-E, OCCTO, ONS, Chile, NPP and Open Electricity; EIA and
Climate TRACE stay monthly, since neither reports anything finer.

## Timestamp conventions (step 1, checked on staging 2026-10-18)

Days follow the monthly views' clock: `DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms/1000))` under
`SET TIME ZONE 'UTC'`, except Australia (local date). Read-only probes on staging confirmed what
each source stores:

| Source | What `timestamp_ms` holds | Evidence | Daily view's day |
|---|---|---|---|
| ENTSO-E | True UTC instant, interval start; 15/30/60-minute intervals | Neurath G has 24 hourly rows on every UTC day across the 2026-03-29 CET→CEST change; stamps at :00/:15/:30/:45 | UTC day (a CET day starts 23:00/22:00 UTC the previous calendar day) |
| OCCTO | JST wall-clock written as UTC, period-end label; 30-minute slots | Slots at :00 and :30; a JST day's 24:00 slot is stored as the next day's 00:00 (`docs/OCCTO_ALL_FUELS_REBUILD.md`) | The stored date, as monthly does: the 24:00 slot counts on the next day |
| ONS | Brazilian wall-clock written as UTC, hourly | Stamps on every hour 0–23 (Brazil has no DST since 2019); start vs end of the hour is not determinable from the data and does not affect matching the monthly view | The stored date (= Brazilian local date) |
| Chile | True UTC instant (extractor localizes from `America/Santiago`), hourly | 24 rows per Santiago local day across the 2026-04-05 DST change; stamps evenly on every UTC hour | UTC day (a Chilean day starts 03:00/04:00 UTC) |
| NPP (India) | The report date at 00:00 UTC, one row per unit per day | All rows at 00:00 UTC | The report date |
| Open Electricity (Australia) | The UTC instant of network-local midnight, one row per facility-fueltech per day | NEM rows at 14:00 UTC (00:00 AEST), WEM at 16:00 UTC (00:00 AWST) | **Network-local date** (`AT TIME ZONE CASE network_code WHEN 'WEM' THEN 'Australia/Perth' ELSE 'Australia/Brisbane' END`, as migration 022); a UTC day would label every day one day early |

## Other findings that shape the views

- **ONS and Chile:** no hour has two rows under the same coal view key (ONS `plant, state,
  state_name, fuel_type`; Chile `plant, region, comuna, fuel_type`), so coverage can be counted per
  view key; no per-ID split is needed.
- **ENTSO-E:** 19 coal series-days since 2025-01-01 mix interval lengths within one day; their
  completeness is `unknown`.
- **India unlabelled rows:** the 5 plants without a fuel label (7.9 TWh in 2025) are Bhutanese hydro
  plants (Mangdechhu, Tala, Chukha, Kurichu, Punatsangchhu-II), so leaving them out of coal
  (`fuel_type = 'THERMAL'`) is correct.
- **Coal = the tracker's predicates** (`COAL_PREDICATES` in the tracker's `plant-generation.ts`);
  India's `THERMAL` is coal (user decision 2026-10-18; every THERMAL plant sits at a GEM coal plant,
  gas and diesel have their own labels `THER (GT)` / `THER (DG)`).

## Views (migrations 024 and 025)

| Source | Daily view (materialized) | Weekly view (plain) | Grain | Completeness |
|---|---|---|---|---|
| ENTSO-E | `mv_entsoe_plant_daily` | `entsoe_plant_weekly` | plant_name, country_code, fuel_type | complete / partial / unknown |
| ONS | `mv_ons_plant_daily` | `ons_plant_weekly` | plant, state, state_name, fuel_type | complete / partial / unknown |
| Chile | `mv_chile_plant_daily` | `chile_plant_weekly` | plant, region, comuna, fuel_type | always unknown (plant sums) |
| NPP | `mv_npp_unit_daily` | `npp_unit_weekly` | plant, unit | complete (one report per unit-day) |
| Open Electricity | `mv_oe_plant_daily` | `oe_plant_weekly` | facility_code, fueltech | always unknown (facility sums) |
| OCCTO (025) | `mv_occto_unit_daily` | `occto_unit_weekly` | plant, unit | complete / partial / unknown |

No row for a day means not reported (never zero). Weekly views: `week_start` (ISO Monday),
`days_reported`, `days_complete`, `days_in_period`, `is_complete`.

## Staging results (2026-10-18)

- Migration 024 applied in 1 min 52 s (default `work_mem`), 025 in 43 s. Sizes: NPP 244 MB, ENTSO-E
  147 MB, OCCTO 16 MB, OE 7.6 MB, Chile 5.8 MB, ONS 4.3 MB (~425 MB total).
- Reconciliation: daily sums equal the coal-filtered monthly views for every series-month (ENTSO-E
  29,490, ONS 836, Chile 1,054, NPP 59,088, OCCTO 3,080; none missing, none mismatched), and OE's
  all-time totals per facility match (its months differ at the edges by design).
- Weekly refresh of the six daily views (`src/refresh_views.py`, `work_mem` 256 MB): about 3.5 min
  (ENTSO-E 78 s, OCCTO 79 s, NPP 22 s, ONS 22 s, Chile 3 s, OE 1 s). Production's whole
  `refresh-views` step took 2 min 46 s on 2026-10-11, so the job stays well inside its 30 minutes.
- One-plant queries use the unique indexes: Neurath G all days 31 ms, BARH STPS all unit-days
  32 ms, Neurath G weekly 6 ms. A weekly query that reads `days_in_period` also looks up the
  view's latest day, through each daily view's index on `day` (without it, 166–331 ms).

## For the tracker (read contract)

**What exists.** Each source with daily data has a daily view next to the monthly view the
tracker already reads, at the same keys, plus a weekly view over it (table under "Views"). A
plant page reads them exactly as it reads the monthly views, swapping the view name and `month`
for `day` / `week_start`. Japan's views appear only where migration 025 is applied (staging now;
production after the OCCTO history rebuild): until then they do not exist, which
`TRANSITIONAL` in `scripts/neon-surface-check.mjs` already treats as "skip".

**Columns.** Daily: the monthly view's keys, `day` (date), `generation_mwh`, `observation_count`,
`covered_minutes`, `completeness`. Weekly: the keys, `week_start` (ISO Monday), `generation_mwh`,
`days_reported`, `days_complete`, `days_in_period` (7, or fewer in the latest week),
`is_complete`.

**Coal.** The views already contain only coal, filtered exactly as `COAL_PREDICATES` does (please
confirm this table matches your code):

| Source | Predicate |
|---|---|
| ENTSO-E | `fuel_type in ('Fossil Hard coal', 'Fossil Brown coal/Lignite')` |
| NPP | `fuel_type = 'THERMAL'` (per unit-month, as `mv_npp_unit_monthly` carries it) |
| ONS | `fuel_type = 'Carvão'` |
| OCCTO | `fuel_type = 'coal'` (per unit-month, as `mv_occto_unit_monthly` carries it) |
| Chile | `fuel_type = 'Carbón'` |
| OE | `fueltech in ('coal_black', 'coal_brown')` |

**Gaps and completeness.** No row for a day means the source did not report it: draw a gap, never
a zero. A row with `generation_mwh = 0` is a reported zero. `completeness`:
- `complete`: every slot of the day is present;
- `partial`: some slots missing (the day's total is an undercount);
- `unknown`: coverage cannot be established. Chile and Australia are always `unknown` (their
  rows are plant/facility sums built by skipping missing units); ENTSO-E days that mix interval
  lengths too.

Compare only `is_complete` weeks, or label partial ones. Chile and Australia weeks are never `is_complete` (their days are always `unknown`), so for them show the totals with an "unverified completeness" label rather than hiding them. A month view should use daily rows
(weeks cross month ends).

**Days.** Days follow the monthly views' clock, so a month's days add up to its monthly bar:
UTC days for ENTSO-E and Chile, local dates for ONS (Brazil) and OCCTO (Japan, whose 24:00 slot
counts on the next day), the report date for India, and the network-local date for Australia
(so Australian daily bars do not sum to the UTC-month `mv_oe_plant_monthly` bar at month edges).

**Query shapes** (each hits the view's unique index; ~30 ms for a plant's full history on
staging):

```sql
SELECT day, generation_mwh, completeness FROM mv_entsoe_plant_daily
WHERE plant_name = $1 ORDER BY day;
SELECT week_start, generation_mwh, is_complete FROM entsoe_plant_weekly
WHERE plant_name = $1 ORDER BY week_start;
SELECT day, unit, generation_mwh, completeness FROM mv_npp_unit_daily
WHERE plant = $1 ORDER BY day, unit;
```

**GEM-unit pages.** There are no GEM-unit daily views yet. Join the source daily views through
`unit_gem_map`, as `gem_unit_monthly` does (ENTSO-E on `plant_key = plant_name`; India and Japan
on `plant_key = plant` and `unit_key = unit`). Australia's unit map joins unit codes the facility
rows lack, so use the facility rows. Dedicated GEM-unit daily views, and their place in the
surface check, come with a later change.

**Surface lists.** Add the new names to `TRANSITIONAL` first, then to `SURFACE` once production
has them.
