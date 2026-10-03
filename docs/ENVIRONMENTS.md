# Staging and production

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

The weekly ETL workflow declares `environment: production`. The separate `ons-staging.yml` workflow is manual and fixed to 2019. It requires the full extractor commit SHA, uses `environment: staging`, loads the same extracted file twice, refreshes ONS views, and reconciles every source observation and monthly group while checking coal preservation and permissions. It neither rebuilds crosswalks nor updates production drift baselines. Migration 017 must already be applied to staging. Its expected observation count is pinned to the audited 2019 release; an upstream restatement must be reviewed before changing that count.

The dashboard's manual `database-check.yml` reads the selected environment and verifies its endpoint, role, 2019 coal totals and coverage. GitHub environments govern jobs that explicitly reference them. Creating the environments does not deploy workflow files or change Cloudflare Pages settings.

Existing repository-level production secrets remain for compatibility with the currently published workflow. After the environment-aware workflow is merged and a production run succeeds, remove the redundant repository-level database secrets. Until then, old workflows can still access those repository secrets.

## Release sequence

1. Develop on feature branches and run CI. Pin the extractor commit used for the rehearsal.
2. Apply the exact proposed migration to staging as owner; load as writer. Keep input hashes, validation reports and reconciliation output.
3. Check all intended source rows, coal regressions, monthly aggregates, a repeat load, and dashboard queries as reader.
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
- All 82 ETL tests passed, including the two local PostgreSQL integration tests. Ruff, Actionlint in both repositories, and dashboard TypeScript checks passed.

Full ignored evidence, source comparison input, validation reports and logs are in `output/staging_ons_2019_2026-10-03/`. The new workflows are prepared locally; they have not yet run in GitHub Actions.

Cloudflare preview routing is documented in the dashboard repository's `docs/ENVIRONMENTS.md`.
