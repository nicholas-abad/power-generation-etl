# ONS all-fuel individual-plant pilot: 2019

Scope: expand one complete ONS year, 2019, using the verified individual-plant extractor. The existing raw table already stores fuel and plant-type labels, and the live 2019 population includes six thermal fuel labels. Coal-only display is a dashboard filter (`fuel_type = 'Carvão'`), not a raw-table restriction.

## Verified result — 2026-10-03

Migration 017 was applied to Neon and the existing loader inserted all 1,493,161 additions with zero invalid or duplicate records. All three ONS views were refreshed. Post-load reconciliation verified every stable field and generation value for all 2,424,001 expected qualifying observations, plus preservation of all 1,385,616 pre-existing raw observations. All 77,544 coal observations remained exactly equal; the twelve dashboard coal totals matched within floating-point aggregation tolerance.

The qualified view contains 3,327 plant/month/type/fuel/state rows for 2019, covering 291 ONS IDs and ten fuel labels. Its observation counts and generation sums match the source aggregation. The dashboard role and no-shadow-table checks passed. Only ONS's twelve 2019 drift baselines were updated after verification; the load's source URL, source hash and input hash were recorded in extraction metadata. Run ID: `08a29718-1f13-4541-9fe2-68402a8a3805`. The local `verification.json` records the final checks. All 72 ETL tests passed, including both PostgreSQL integration tests.

## Migration

Apply `schema/migrations/017_ons_individual_plant_view.sql` before deploying the updated `src/refresh_views.py`:

```bash
psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f schema/migrations/017_ons_individual_plant_view.sql
```

The canonical definition is `schema/ons_individual_plant_monthly.sql`. It adds `public.mv_ons_individual_plant_monthly`, owned by `etl_writer`, with:

- Original ONS ID, plant name, source plant type and fuel label, state, monthly MWh and observation count.
- Individual modes I, II-A and II-B, usable identifiers/fuel/type, single or null CEG, hourly resolution, and finite nonnegative generation.
- All collisions among otherwise eligible timestamp/ONS-ID pairs excluded before aggregation.
- Explicit UTC month bucketing of the existing source-clock timestamp encoding. This does not establish the source's timezone/DST semantics.

The existing raw table and `mv_ons_plant_monthly` retain their contracts. Historical group/forecast rows remain in raw ingestion and are excluded from the new qualified view. The new view has no `dashboard_ro` grant: exposing all fuels in the dashboard, including crosswalk coverage and emissions methods, is a separate change. No schema surface-check change or weekly extraction flag change is required for this pilot.

The view can contain thermal history outside 2019. Non-thermal coverage has only been backfilled for 2019; presence of a month does not imply complete coverage of every fuel. Even within 2019, this is the ONS qualifying reporting population, not Brazil's national total.

## Input and reconciliation

The audited source is `GERACAO_USINA-2_2019.parquet`, downloaded 2026-10-03:

- SHA-256: `4fee764a541f11c47abaa3add1a7797ddb79683d383475b7c5834acb9709bf2b`.
- 4,394,570 source rows; 2,424,001 qualifying observations; 1,970,569 excluded group/forecast rows.
- 291 qualifying ONS IDs, 10 fuel labels and all 12 months.

The database comparison matched every stable field for the 930,840 qualifying thermal observations already stored (numeric tolerance 1e-9 MWh). The load therefore contains only the 1,493,161 missing non-thermal observations, from 171 ONS IDs:

| Source fuel | New observations |
|---|---:|
| Hidráulica | 1,370,521 |
| Eólica | 96,360 |
| Nuclear | 17,520 |
| Fotovoltaica | 8,760 |

The existing raw 2019 population is 1,385,616 rows, including 454,776 legacy observations outside the qualified view. Expected raw population after this additive load: 2,878,777. Expected qualified population: 2,424,001. No coal rows are in the additions file.

Local, gitignored evidence is under the extractor checkout's `output/ons_2019_load_2026-10-03/`: source comparison and input hash in `load_manifest.json`, the exact additions JSONL, full pre-load 2019 CSV, coal snapshot, validation reports, migration log and post-load checks. The earlier `output/ons_validation_2026-10-03/` retains the original Parquet, excluded rows, full accepted output and source reconciliation.

## Load and verification

The pilot uses the existing `database_management.py load-data ons` staging/upsert path. Only its prepared, validated additions JSONL is loaded; never load `ons_excluded_*.jsonl`. After loading:

1. Refresh the three ONS views with `python src/refresh_views.py --source ons`.
2. Match the new raw observations back to their source fields and values; confirm the new view reconciles by month, ONS ID, plant/type/fuel and state, including observation counts.
3. Compare all 77,544 stored 2019 coal observations and the existing dashboard view's coal totals against the saved baseline.
4. After reconciliation succeeds, update only ONS's twelve 2019 rows in `ingestion.monthly_totals_snapshot`. Adding non-thermal generation intentionally changes the legacy all-fuel totals used by the weekly drift check; other periods and sources keep their baselines.

Migration and loader integration tests use a disposable local PostgreSQL database, refuse remote test connections, and cover fuel preservation, invalid/group/forecast exclusion, duplicate identity collisions, UTC month boundaries, re-running the load/migration, and view permissions. CI supplies `ONS_TEST_PG_DSN` explicitly; local unit-test runs without it skip those two integration tests.

Rollback of migration 017 removes only the new materialized view and its ledger entry (and its refresher registration). It does not remove the 2019 generation backfill. Any data rollback must use the pilot's run ID and recorded input keys after checking for later revisions; do not delete an entire year or fuel category.
