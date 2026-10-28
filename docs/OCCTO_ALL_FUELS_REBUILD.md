# OCCTO (Japan) rebuild — runbook (staging and production)

Tracker issue #31. Replaces every stored OCCTO row with rows parsed from a certified archive of
OCCTO's unit-generation source, one key convention (JST wall-clock written as UTC, period-end).
**Coal plants only** (decision 2026-10-13): the payload keeps every row (all fuels) of each plant
that reports coal anywhere in the certified range, and leaves out plants with no coal, the rule the
weekly loader applies. `src/rebuild_occto.py` takes `--environment staging|production`; production
also needs `--allow-production`, and both check the pinned direct owner connection before connecting.

Why a rebuild and not an append: the 2026-03-09 bulk load keyed timestamps as Europe/Berlin local
time (−1 h winter, −2 h summer; spring-forward days lost two half-hours per unit), the first week
(2024-03-25..31) was loaded only for Tokyo, and some units are missing from history. Step 5 below
measures all of this before anything changes.

Paths: `EXT` = energy-extractors checkout, `ARCHIVE`/`EVID` = evidence directories outside git.

1. **Archive** (extractor): `uv run python -m energy_extractors.occto.archive fetch --archive-dir ARCHIVE`
   — resumable; every raw response kept with its sha256.
2. **Certify**: `... archive certify --archive-dir ARCHIVE --end YYYY-MM-DD --out EVID/certificate.json`
   — must report `ok: true` (every day covered exactly once, no failures or truncation).
3. **Two parsers**:
   - A: `uv run python -m energy_extractors.occto.parse --archive-dir ARCHIVE --certificate EVID/certificate.json --out EVID/A.tsv.gz`
   - B: `python3 -I docs/validation/occto/independent_parse.py ARCHIVE EVID/certificate.json EVID/B`
   — digests must be equal, for the whole source and for the coal-plant cut (B writes its own
   `canonical_coal_plants.tsv.gz` and `coal_plants_digest`); B's `report.json` lists every gap,
   rejection and exclusion, and the source's coal rows and kWh.
4. **Benchmarks** (external totals): `docs/validation/occto/benchmarks.py fetch|build` (TSO area
   actuals, METI 電力調査統計).
5. **Stored vs source** (local scratch database only): `docs/validation/occto/compare_stored.sh
   load-stored`, `load-source EVID/B`, `run EVID/compare.txt`.
6. **Payload and plan** (owner wrapper; `ENV` = staging or production):
   ```bash
   uv run python src/rebuild_occto.py build-payload --canonical EVID/A.tsv.gz \
     --parser-b-report EVID/B/report.json --certificate EVID/certificate.json \
     --out EVID/payload --repair-id occto-coal-plants-YYYYMMDD
   uv run python src/run_in_environment.py --environment ENV --role owner -- \
     uv run python src/rebuild_occto.py plan --environment ENV [--allow-production] \
       --payload EVID/payload --report EVID/plan.json
   ```
   The manifest records the cut: coal plants kept, rows and MWh left out per fuel, and the source's
   coal (every coal row is kept, or the build refuses). Production refuses a manifest from before
   the cut (no `"plants"`); staging still loads one, so the 2026-10 staging repair can be rolled back.
7. **Apply** with the reviewed plan: `... rebuild_occto.py apply --payload EVID/payload
   --approved-plan EVID/plan.json --expect-plan-sha256 <plan_sha256> --report EVID/apply.json`.
   Refreshes the three OCCTO views; if that fails, rerun `src/refresh_views.py --source occto`
   through the same environment's wrapper.
8. **Verify**: `docs/validation/occto/verify_load.sh ENV EVID/B EVID/payload EVID/verify
   [--allow-production]` (read-only): no rows after the certified end, database rows equal parser B's
   coal-plant lines, every source coal row is stored, views equal the raw table, the tracker's coal
   reads equal source coal.
9. **Rollback** (only on request): same as apply with `rollback`; refused per window if anything
   wrote to it after apply.

**Production.** Keep the weekly workflow disabled from the archive cut until verification is
signed off: a weekly OCCTO run writes rows after the certified end (step 8 refuses) and revises rows
in the last windows (rollback then refuses them), so rollback is possible only until the first
weekly run after apply. Certify the archive through production's newest stored OCCTO day. The first
weekly `check-totals-drift` after the rebuild is expected to be red (the history's monthly totals
change); it re-baselines the week after.

Ledger: `ingestion.occto_rebuilds`, `ingestion.occto_rebuild_windows`; original rows in
`ingestion.occto_rebuild_backup` (by `repair_id`, `window_start`).

## Weekly job after the rebuild

The weekly `extract-occto` job now runs `energy-extract occto --backend json` (all fuels, the
same client and parser as the rebuild), refetches a 14-day lookback and ends at the last fully
published JST day (two days back). This fixes the lost day per run: OCCTO's 24:00 slot is
stored as the next day's 00:00, so resuming from `latest + 1` skipped every Sunday.

- **Deploy order.** CI checks out the extractors' default branch unpinned. Merge the extractors
  change first, then the ETL change, between two Sunday runs (cron 06:23 UTC). Old ETL with the new
  extractor still skips a day; new ETL with the old extractor fails on `--backend json`.
- **Persistent gaps.** A day that fails (no data, unresolved request, certificate problem) turns
  the job red, but only while it is inside the 14-day lookback. Repair older holes with
  `START_OVERRIDE`/`END_OVERRIDE`.
- **Coal plants only (2026-10-18).** Per the 2026-10-13 decision (other fuels only at plants that
  also burn coal), `incremental_extract.py` cuts each file to the plants with a coal unit, known from
  the file or from coal already stored, before loading. On 2026-10-10 that kept 4,656 of 22,017 rows
  (68 of 264 plants): every coal row, plus the gas and oil units of mixed plants such as Toyama Shinko.
- **Production before its rebuild.** Production's weekly runs load all fuels at coal plants, while
  production history stays coal/thermal-only with the Berlin-keyed bulk rows until the production
  rebuild (separate sign-off). Coal reads are unaffected (they filter `fuel_type = 'coal'`); other
  fuels and `mv_occto_row_counts` only cover the weeks loaded since the switch. The rebuild applies
  the same cut to the whole history (steps 6–8).
- **Revisions** update values, fuel, codes and area labels in place. The key is
  `(timestamp_ms, plant, unit)`, so a plant or unit renamed retroactively inside the lookback
  adds rows under the new name next to the old ones. A value the source later blanks is not
  deleted (upserts never delete).
