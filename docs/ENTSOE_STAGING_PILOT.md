# ENTSO-E identified-unit staging pilot

The pilot covers **CZ (Czech Republic), March 2019, then full 2019 and 2024**. Production is out of scope, including read-only queries, migrations, deployments and releases. Work stays on `feat/entsoe-all-fuels` in the extractor and ETL repositories and Neon staging branch `br-sweet-voice-aglj4sd7`.

The frontend going forward is `chienleng/global-coal-generation-tracker`. Do not push to that repository. This pilot does not change frontend code, its queries, or reader permissions.

## Source and coverage

The extractor uses the existing ENTSO-E API with the [actual generation per generation unit (16.1.A)](https://transparencyplatform.zendesk.com/hc/en-us/articles/16648326220564-Actual-Generation-per-Generation-Unit-16-1-A) product: A73, A16, business A01, aggregation A06, generation direction and MW. Requests cover complete UTC days within complete months. All generation PSR types B01–B20 and B25 are requested. Each response is archived and hashed.

The generation-unit EIC comes from `MktPSRType/PowerSystemResources/mRID`; `registeredResource.mRID` identifies its parent production unit. Both require EIC coding scheme A01. The original source name is retained. Consumption and unidentified series are excluded and counted. Unknown encodings, conflicting copies or identities, overlapping intervals and legacy-key collisions fail the extraction. An identical repeated source observation is counted and deduplicated.

[A01 fixed intervals and A03 variable blocks](https://transparencyplatform.zendesk.com/hc/en-us/articles/30262342482961-CurveType-A01-vs-CurveType-A03) are parsed explicitly. Missing A01 positions and missing A03 quantities stay missing. Generation MWh is the sum of MW × source interval minutes / 60. In this pilot, 2024 changes from hourly to 15-minute reporting during the year.

These are **reported generation units, not national totals or every plant**. The Czech responses include brown coal, hard coal, gas, nuclear and pumped-hydro generation. Requesting all fuels does not fabricate wind, solar or other absent source observations. Reporting thresholds and source gaps still apply.

## Schema and compatibility

Migration [018](../schema/migrations/018_entsoe_unit_identifiers.sql) adds three nullable columns to `ingestion.entsoe_generation_data`:

| Column | Meaning |
|---|---|
| `unit_eic VARCHAR(16)` | Stable source generation-unit ID |
| `production_unit_eic VARCHAR(16)` | Optional parent production-unit ID |
| `source_unit_name TEXT` | Unmodified API name |

The existing timestamp/country/PSR/plant-name unique key remains. A new partial unique index on `(country_code, unit_eic, timestamp_ms)` prevents duplicate identified observations. Existing rows retain null IDs until an audited source file backfills them. The loader rejects attempts to replace a known EIC; legacy replays retain IDs and the known generation metric.

Twenty-six Czech names differ from stored legacy names by one trailing underscore. [Explicit reviewed aliases](../config/entsoe-unit-aliases.json) connect the source EIC/name/PSR combination to the existing `plant_name`, preserving legacy joins while retaining `source_unit_name`. There is no general trimming or fuzzy matching. An unexpected name or PSR for a known alias fails validation. New countries require their own identity/coverage review; two EICs sharing the old name/PSR/time key require a separate key migration.

`public.mv_entsoe_unit_monthly` groups by UTC month, country, both EICs, legacy/source names, PSR and fuel. It contains interval-integrated `generation_mwh`, `observation_count` and `observed_minutes` for identified actual generation only. It is owned by `etl_writer`, with no new grants to `dashboard_ro` or PUBLIC. The old `mv_entsoe_plant_monthly` stays available. The two views summarize overlapping raw observations; **do not add their totals together**.

Apply migration 018 before running this branch's loader or refresher. The migration has been applied to staging only. It was tested twice on a disposable PostgreSQL database, alongside identified backfills, legacy replays, interval energy, conflicting identities, concurrent view refresh and unchanged reader access. Migration rollback removes the new view, index, constraints, columns and ledger entry; it does not undo loaded generation observations. A data rollback requires the recorded input keys and a review of subsequent revisions, not a whole-year delete.

## Running the staging benchmark

From the extractor repository, use a separate directory for each year:

```bash
uv run energy-extract entsoe --countries CZ --start-year 2019 --end-year 2019 \
  --all-fuels --yes --output output/entsoe-cz-2019
```

`--months 3` limits this to the March sample. `--resume-source-archive` explicitly reuses archived responses after an interrupted download; the parser rechecks every response. For this pilot the 2024 download is split into four independent month ranges; all 366 archived day responses must then be replayed as one full-year extraction before loading. Only full-year manifests qualify for the fixed benchmarks.

From the ETL repository, apply the migration through the pinned staging wrapper:

```bash
uv run python src/run_in_environment.py --environment staging --role owner -- \
  psql -X -v ON_ERROR_STOP=1 -f schema/migrations/018_entsoe_unit_identifiers.sql
```

The [manual staging workflow](../.github/workflows/entsoe-staging.yml) takes one year (2019 or 2024) and a full extractor commit SHA. It checks migration availability, captures existing coal measurements, extracts the year, checks source hashes and coverage, loads twice, requires zero repeat writes, refreshes ENTSO-E views, and reconciles every observation and monthly group. Its data job runs only on `workflow_dispatch` with the `staging` environment. Feature-branch pushes run offline parser tests and the regular ETL CI against disposable PostgreSQL; they do not load any database. No production credentials are selected by the staging wrapper.

For local verification, run `src/check_entsoe_staging.py` through the same wrapper, using `--phase preflight` before loading and `--phase verify --baseline <preflight-report>` afterwards. Pass the exact JSONL file with `--input`, its `entsoe_unit_manifest.json` with `--manifest`, and `--year`. Full benchmarks additionally use `--require-audited`. The fixed [canonical hashes](../config/entsoe-benchmarks.json) exclude volatile response-document IDs, retrieval timestamps and run metadata, but cover every observation's identity, name, fuel, metric, value and interval. Source revisions fail the pinned benchmark and require review before updating it.

Preflight refuses missing existing observations, changed measurements and identity collisions before any load. Verification compares source to stored observations, all new monthly unit groups, the old coal monthly view, exact before/after coal measurement hashes and reader permissions. `--strict` reports validation errors; a multi-batch load is not atomic, so this rehearsal remains on staging. `rows_written` counts both inserts and updates. The identical file must produce zero repeat writes; a new extraction run can update provenance on existing rows.

## Evidence and storage

The [2019 verification summary](validation/entsoe-cz-2019-2026-10-04.json) records 356,083 observations, 41 units, 492 monthly unit groups and 57,621,966.75 MWh across five reported fuel types. All 224,876 existing coal observations and 312 coal monthly groups were preserved. Both loads had zero invalid or duplicate input rows; the repeat wrote zero rows. The earlier March sample also passed with 30,432 observations, 41 monthly groups and 19,272 unchanged coal observations.

Raw observations are stored on staging in `ingestion.entsoe_generation_data`; aggregates are in `public.mv_entsoe_unit_monthly` and the existing ENTSO-E views. Complete XML archives, JSONL/CSV and manifests are retained locally under the extractor checkout's ignored `output/entsoe_cz_pilot_2026-10-04/`. Load reports, baseline hashes and reconciliation reports are under the ETL checkout's matching ignored output directory. Permanent summaries belong in `docs/validation/`. GitHub benchmark artifacts retain source XML and verification reports for 30 days.

The metadata date-range query exposed an unrelated planner problem during the sample: PostgreSQL walked the timestamp index across the full ENTSO-E table to find the run's first/last observation. Filtering the run in a materialized CTE made it use the existing run-ID index; the staging query dropped from nine minutes to 0.16 seconds. A PostgreSQL regression test checks this execution-plan shape and correct date boundaries.
