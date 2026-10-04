# Staging and production

**Frontend correction — 2026-10-04:** the frontend going forward is [chienleng/global-coal-generation-tracker](https://github.com/chienleng/global-coal-generation-tracker). Plan frontend CI, staging, and release integration against that repository's contracts. **The user prohibits pushing commits, branches, or tags to Chien's repository, including equivalent GitHub API writes.** Read-only inspection and local proposals are allowed; publishing frontend changes remains with Chien. References below to `energy-generation-dashboard`, its GitHub environments, and its driver checks record the earlier legacy setup; they do not configure or verify the replacement frontend. The ETL and Neon staging setup remains applicable.

The Neon project `damp-wildflower-34076536` now has two independent branches:

| Environment | Neon branch | Endpoint | Purpose |
| --- | --- | --- | --- |
| production | `br-holy-tooth-agibelcu` | `ep-empty-star-ag0bt9wd` | Scheduled ETL and the public dashboard |
| staging | `br-sweet-voice-aglj4sd7` | `ep-sparkling-night-agj1zen6` | Migration and ingestion rehearsals |

Staging was cloned from production on 2026-10-03. It has its own compute, scales between 0.25 and 2 CU, and suspends after five idle minutes. Its writer, reader and owner passwords were rotated separately from production. The endpoint mapping is versioned in `config/environments.json`.

## Local commands

Use `src/run_in_environment.py` for database operations. It checks the selected endpoint, database, role and TLS settings, then performs a read-only connection probe before running the command. It supplies consistent PostgreSQL, libpq and URL variables, replacing inherited connection settings. It requires complete credentials and does not use the legacy `.env` as a fallback.

Credentials are in ignored, mode-0600 files `.env.<environment>.<role>`:

- `writer`: `etl_writer`, for ingestion and materialized-view refreshes.
- `reader`: `dashboard_ro`, for dashboard queries.
- `owner`: `neondb_owner`, for migrations only; kept local, not stored in GitHub Actions.

Copy `.env.staging.writer.example` when setting up another checkout and obtain the actual staging credentials from Neon. An explicit production command uses `--environment production` and its separate credential file.

```bash
# Check the staging writer connection without changing the database.
uv run python src/run_in_environment.py --environment staging

# Apply the migration with the staging owner over its direct connection.
uv run python src/run_in_environment.py --environment staging --role owner -- \
  psql -X -v ON_ERROR_STOP=1 -f schema/migrations/017_ons_individual_plant_view.sql

# Load a prepared file with the staging writer.
uv run python src/run_in_environment.py --environment staging -- \
  uv run python src/database_management.py load-data ons PATH_TO_ADDITIONS.jsonl \
    --strict --validation-report output/staging-load.json

uv run python src/run_in_environment.py --environment staging -- \
  uv run python src/refresh_views.py --source ons
```

The existing direct CLIs still support their legacy `.env` behavior. Use the wrapper whenever choosing between these two databases. `--strict` reports validation failures after processing; it does not make a multi-chunk load atomic. Rehearsals belong in staging.

## GitHub environments

Both `power-generation-etl` and `energy-generation-dashboard` have `staging` and `production` environments. Production allows deployments from the `main` branch only. Staging permits feature branches. No reviewer gate was added to interrupt scheduled production jobs.

ETL environment secrets: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`. The username is `etl_writer` in both environments, with distinct passwords. Dashboard environments have a `DATABASE_URL` secret using `dashboard_ro`. Both repositories have an environment variable `NEON_ENDPOINT_ID`.

The feature branch's weekly ETL workflow declares `environment: production`; publishing the feature branch does not change the scheduled workflow on `main`. The separate `ons-staging.yml` workflow uses `environment: staging` and is now **manual only**. Select one audited year with the `year` input, or select `benchmarks` to run the fixed **2019 and 2024** regression years sequentially. Every dispatch supplies a full extractor commit SHA. Ordinary code pushes run the normal CI checks without loading data.

Before loading, the workflow checks every downloaded file against the audited hashes and row counts in `config/ons-benchmarks.json`. It loads the same extracted file twice and requires the repeat's machine-readable `rows_written` to equal zero. After refreshing ONS views it reconciles every source observation and monthly group, verifies all twelve months, and checks coal preservation and database permissions. Each year produces its own artifact with code revisions, source hashes, baseline, validation reports, load logs, and reconciliation results. Migration 017 must already be applied to staging. The workflow neither rebuilds crosswalks nor changes production data or drift baselines.

An upstream restatement fails the benchmark before ingestion. Review the changed source before updating the committed hashes or expected counts; a larger or different result must not silently become the new benchmark.

The dashboard's manual `database-check.yml` reads the selected environment and verifies its endpoint, role, 2019 coal totals and coverage. GitHub environments govern jobs that explicitly reference them. Creating the environments does not deploy workflow files or change Cloudflare Pages settings.

Existing repository-level production secrets remain for compatibility with the currently published workflow. After the environment-aware workflow is merged and a production run succeeds, remove the redundant repository-level database secrets. Until then, old workflows can still access those repository secrets.

## Release sequence

1. Develop on feature branches and run CI. Pin the extractor commit used for the rehearsal.
2. Apply the exact proposed migration to staging as owner; load as writer. Keep input hashes, validation reports and reconciliation output.
3. Check all intended source rows, coal regressions, monthly aggregates, and a repeat load. Verify the read-only database contract, then test the relevant integration with `chienleng/global-coal-generation-tracker`; legacy dashboard checks alone do not verify that frontend.
4. Review the results and merge the tested code. Apply the migration to production before deploying code that requires the new view, then run the narrowly scoped production load.
5. Reconcile production and update only drift baselines affected by the verified expansion.

Promote reviewed code and migrations; do not replace production with staging data. Staging is a snapshot and does not automatically receive later production loads. Resetting it from production discards its experiments and can restore production passwords; verify the branch/endpoint and rotate staging credentials again before reconnecting jobs.

## ONS 2019 rehearsal evidence

The initial rehearsal used the already audited additions file from `ONS_2019_PILOT.md`. On the newly created staging branch only, it reversed the pilot's run ID, migration-017 view/ledger and twelve ONS drift baselines, retaining all pre-existing 2019 observations. It then replayed the real migration and writer-role load.

The [2026-10-03 verification report](validation/ons-staging-2019-2026-10-03.json) records a successful rehearsal:

- Migration 017 applied and reapplied successfully.
- First load inserted 1,493,161 rows; the identical repeat wrote zero rows. Both validated every record, with zero invalid or duplicate records.
- Every stable field and generation value for all 2,424,001 qualifying source observations matched staging; all 3,327 monthly groups reconciled.
- Fingerprints of all 1,385,616 pre-existing rows, including their metadata, were identical before and after. All 77,544 coal rows and twelve coal monthly totals were preserved.
- Final raw 2019 population: 2,878,777, with no duplicate natural keys. Qualified coverage: 291 ONS IDs, ten fuels, twelve months, 478,334,817.266 MWh.
- Dashboard-driver checks passed against both staging and production using their respective read-only roles. Production was only read during this staging task.
- Provenance and only the twelve ONS 2019 staging drift baselines were updated after reconciliation.
- All 83 ETL tests passed, including the two local PostgreSQL integration tests. Ruff, Actionlint in both repositories, and dashboard TypeScript checks passed.

Full ignored evidence, source comparison input, validation reports and logs are in `output/staging_ons_2019_2026-10-03/`. The first published [2019 GitHub rehearsal](https://github.com/nicholas-abad/power-generation-etl/actions/runs/37178579505) passed on 2026-10-04, including source reconciliation, unchanged coal results, and a repeat load that wrote zero rows. The corresponding [ETL CI run](https://github.com/nicholas-abad/power-generation-etl/actions/runs/37178579495) also passed. These runs used the feature branch; no production merge was needed.

## ONS benchmark years — 2026-10-04

| Year | Source files | Source observations | Qualifying observations |
| --- | ---: | ---: | ---: |
| 2019 | 1 annual Parquet | 4,394,570 | 2,424,001 |
| 2024 | 12 monthly Parquets | 6,089,616 | 2,842,224 |

The benchmark includes every qualifying individual-plant fuel label, excluding groups and forecasts. These are ONS reporting populations, not national totals. The 2024 source audit checked all twelve monthly files, including February 29. Source hashes are committed in `config/ons-benchmarks.json`; local raw Parquets and the detailed source audit are under the extractor's ignored `output/ons_benchmark_2024_2026-10-04/` directory.

Both years passed in the [two-year GitHub Actions run](https://github.com/nicholas-abad/power-generation-etl/actions/runs/37179237503). The [permanent verification summary](validation/ons-staging-benchmarks-2019-2024-2026-10-04.json) records the tested code revisions and source manifest hash:

| Year | Monthly groups reconciled | Coal observations preserved | Repeat rows written |
| --- | ---: | ---: | ---: |
| 2019 | 3,327 | 77,544 | 0 |
| 2024 | 3,887 | 79,056 | 0 |

All qualifying observations matched their source fields and generation values; both years covered twelve months and had zero invalid or duplicate records. Coal observations and monthly coal totals remained unchanged. The matching ETL CI run passed 103 tests, including four PostgreSQL integration tests using PostgreSQL 17; the local ONS extractor suite passed 57 tests. First-load `rows_written` includes inserts and updates, including provenance updates, and must not be interpreted as a count of new observations. Production and the frontend repository were unchanged.

Full downloaded artifacts are retained locally under `output/github_ons_benchmarks_37179237503/`; GitHub retains the uploaded artifacts for thirty days. The committed verification summary and source manifest persist beyond that artifact retention window.

Run both years again from the published ETL feature branch:

```bash
gh workflow run ons-staging.yml --repo nicholas-abad/power-generation-etl \
  --ref feat/ons-all-fuels-2019 \
  -f year=benchmarks \
  -f extractors_commit=db56cb9023b9844356fc633651d49f7027ac3274
```

The branch name retains the original pilot year. The GitHub Actions UI's manual-run button requires the workflow on the default branch. The workflow was registered by its initial feature-branch push; the CLI can select that branch explicitly.

## Year-by-year expansion

Following the successful 2019/2024 benchmarks, the user authorized one additional year at a time, starting with **2020**. The 2020 annual source was audited on 2026-10-04: 4,479,000 source observations, 2,438,376 qualifying observations, 281 ONS plant IDs, ten fuel labels and all twelve months. Its exact source-file hash and expected count are recorded in `config/ons-benchmarks.json`. The checker accepts only years present in that committed manifest.

Run 2020 alone:

```bash
gh workflow run ons-staging.yml --repo nicholas-abad/power-generation-etl \
  --ref feat/ons-all-fuels-2019 \
  -f year=2020 \
  -f extractors_commit=db56cb9023b9844356fc633651d49f7027ac3274
```

Before adding another year, audit its full source inventory, commit its hashes and counts, add it to the workflow's year choices, and run the same staging checks. Complete historical years require all twelve months; a partial current-year run needs a separately defined cutoff and is not covered by this full-year check.

The [2020-only GitHub run](https://github.com/nicholas-abad/power-generation-etl/actions/runs/37181952314) passed on 2026-10-04. Its [permanent verification summary](validation/ons-staging-2020-2026-10-04.json) records 2,438,376 source observations and 3,334 monthly groups reconciled across twelve months and ten fuel labels. All 79,056 coal observations and the monthly coal totals were preserved. Both loads had zero invalid or duplicate records; the identical repeat wrote zero rows. The matching [ETL CI run](https://github.com/nicholas-abad/power-generation-etl/actions/runs/37181868831) passed all 107 tests, including five PostgreSQL integration tests. First-load writes include updates and are not a count of new observations.

Full downloaded evidence is retained locally under `output/github_ons_2020_37181952314/`; the GitHub artifact expires after thirty days. The summary hashes the source manifest from that artifact, preserving the exact manifest used by the run. Validated all-fuel years are now **2019, 2020 and 2024**. **2021 is the next year to audit and load**; it has not yet been run through this expansion workflow. This 2020 run changed staging only; production and the frontend repository were unchanged.

Loaded observations are saved to `ingestion.ons_generation_data` on Neon staging branch `br-sweet-voice-aglj4sd7`. The qualifying monthly aggregates are in `public.mv_ons_individual_plant_monthly` on that same branch. The local 2020 source Parquet and source audit are under the extractor checkout's ignored `output/ons_yearly_audits_2026-10-04/2020/` directory. GitHub artifacts contain verification reports and logs, not the complete generation dataset.

The legacy dashboard's Cloudflare preview instructions are historical. Its pending preview setup and CI rollout are no longer part of the active plan. Inspect `chienleng/global-coal-generation-tracker` before defining the replacement frontend's environment and release configuration.
