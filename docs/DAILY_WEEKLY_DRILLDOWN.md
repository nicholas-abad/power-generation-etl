# Daily and weekly generation drill-down

Plan: `~/.claude/plans/daily-weekly-drilldown.md` (option 1: daily and weekly coal generation for
ENTSO-E, OCCTO, ONS, Chile, NPP and Open Electricity; EIA and Climate TRACE stay monthly).

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
