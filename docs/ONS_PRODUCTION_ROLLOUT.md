# ONS production rollout

Two changes, in this order. **Every step marked ⚑ needs explicit authorization
before it runs.** Nothing here has been executed in production; the only
production access so far was one read-only check (2026-10-11, `dashboard_ro`).

| Part | PR | What it fixes or adds |
|---|---|---|
| A | power-generation-etl #117 | Rows left by republished 2026 files: plant renames stored twice (May 2026 coal doubled, June +36 %), withdrawn and now-null rows. Adds the plant-hour key (migration 020). |
| B | energy-extractors #19, power-generation-etl #116 | ONS individual plants across all fuels, 2019 → 2026 (Jan–Aug), source gaps (migration 021). |

Part A is independent and urgent: Steven's tracker shows the inflated May/June
coal today. Part B depends on A (its loads use the plant-hour key) and on the
open decisions listed at the end.

All database commands go through `src/run_in_environment.py --environment
production --role <role>`; schema changes use the owner role, data changes the
writer role.

## Part A — remove republication leftovers (#117)

Rehearsed on staging 2026-10-11 with the final code, from production's state:
plan, apply, migration, rollback (exact, identical fingerprint on re-plan),
re-apply. One plan: 162,720 rows (111,720 duplicates, 24,792 withdrawn,
26,208 now-missing). The repair runs as the owner role; every production
command, including the read-only plan, needs `--allow-production`.

1. ⚑ Disable the weekly workflow (`gh workflow disable weekly-extraction.yml`)
   and confirm no run is in progress. Needed because the repair takes a table
   lock and the plan's row ids change with every load.
2. ⚑ **Plan** (read-only, owner role): `repair_ons_rename_duplicates.py plan
   --source-dir <audited 2026 files> --plan-file prod-plan.json
   --allow-production` (run with `uv run --with pyarrow`; about 3 minutes). Review the summary: coal rows only under
   `duplicate` for 2026-01/05/06, about 427,455 MWh (May) and 282,312 MWh
   (June); `withdrawn`/`now_missing` without coal.
3. ⚑ **Apply** (owner role): `apply --plan-file prod-plan.json --fingerprint
   <fp> --allow-production`. About 2 minutes under lock on staging; records the
   repair in `ingestion.ons_republication_repairs`.
4. ⚑ **Migration 020** (owner role): `schema/migrations/020_ons_plant_hour_key.sql`
   (refuses while duplicates remain; built concurrently).
5. ⚑ Merge #117, then `refresh_views.py --source ons`.
6. **Check:** May 2026 coal = 427,454.9 MWh and June = 785,088.7 MWh in
   `mv_ons_plant_monthly` (the audited source); 9 coal plant names per month;
   no duplicate (hour, ONS ID); the dashboard_ro surface check passes. Update
   only the affected ONS months in the totals-drift baseline.
7. ⚑ Re-enable the weekly workflow.

**Rollback:** revert the loader (#117), `DROP INDEX CONCURRENTLY
ingestion.uq_ons_plant_hour`, delete ledger `020`, `rollback --fingerprint
<fp> --allow-production` (owner role), refresh ONS views. Rollback refuses rows
that would collide with later loads; the archive is kept and marked.

## Part B — ONS all fuels (#19, #116)

Staging evidence: 2019–2025 (`docs/validation/ons-staging-*.json`), 2026 Jan–Aug
(`ons-staging-2026-2026-10-11.json`), regressions 2019 and 2024 re-run
2026-10-11.

1. **Preconditions:** Part A done; the open decisions below settled; staging
   view refresh times within the weekly `refresh-views` timeout.
2. ⚑ **Read-only inventory:** ledger (017 is expected present from the 2019
   pilot of 2026-10-03, per `docs/ONS_2019_PILOT.md`), definition of
   `mv_ons_individual_plant_monthly`, rows per year by plant type, fuel and
   modality, current ONS drift baselines. Adjust the steps below to what exists.
3. ⚑ Disable the weekly workflow for the whole rollout window.
4. ⚑ Merge a one-line ETL PR pinning `extract-ons` to the current extractor
   commit, so merging #19 cannot change production by itself; then merge #19.
5. ⚑ Migration 017 if absent (otherwise verify its definition), then migration
   021 (owner role). Merge #116 with the pin moved to #19's merge commit.
6. ⚑ **Per year, 2019 → 2026, additions only.** Extract with
   `check_ons_staging.py --print-extract-args`; preflight against the audited
   hashes, the per-fuel audit and the gap list; compare with stored rows and
   load only plant-hours that are missing (differing rows are held for review,
   never overwritten without approval). Persist the input hash, inserted keys
   with value hashes, and the year's drift-baseline rows before the first
   chunk. Record the year's source gaps (`ons_source_gaps.py`). 2019 is
   expected to need nothing beyond verification.
7. `refresh_views.py --source ons`.
8. **Checks** (production-capable reconciliation; `check_ons_staging.py`
   refuses production by design): per-year `expected_observations` and per-fuel
   tables; recorded gaps equal the approved audit (2026: 24 gaps, 50,352 h);
   coal observations and coal totals unchanged; `mv_ons_plant_monthly`
   definition and grants unchanged, its added volume equal to the inserted
   non-thermal rows; dashboard_ro surface check passes; update only the
   verified ONS months in the drift baseline.
9. ⚑ Re-enable the weekly workflow; switch its ONS mode only per decision D6.

**Rollback (before D6 changes the weekly mode):** under a pause and a table
lock, verify every recorded inserted key still has its loaded values (refuse on
a later revision), then in one transaction delete those keys, delete the gap
rows this rollout recorded, and restore the drift-baseline rows; refresh.
Migrations 017/021 and `SOURCE_VIEWS` stay (ETL-only). After D6 switches the
weekly job to all fuels, a rollback must also remove non-thermal keys it loaded.

## Open decisions before Part B

- **D3** Non-thermal ONS plants become visible to `dashboard_ro` through
  `mv_ons_plant_monthly` (that view has no fuel filter; likely already true for
  2019). Accept, or restrict that view to its legacy thermal population first.
  Steven's coal queries filter `Carvão` and are unaffected either way.
- **D5** Pin `extract-ons` before merging #19 (recommended).
- **D6** Weekly ONS mode after the rollout: stay thermal-only, switch to
  all-types individual plants (coal then excludes group/forecast rows from the
  switch date), or run both.
- **Christine:** individual-plant coverage per fuel (wind ~1–3 %, solar
  ~0.2–1.7 %, biomass ~11–27 % of source MWh), the excluded forecast-only fuel,
  and the mapping to tracker fuel categories.

## Follow-up (not in either PR)

The weekly loader never deletes: when ONS republishes a month and drops or
re-identifies rows, the old ones stay. #117 cleans 2026 and stops renames from
duplicating, but withdrawn rows can recur. A lasting fix is to replace each
fully re-downloaded month (delete rows of that month absent from the file,
with an archive) — to be designed separately.
