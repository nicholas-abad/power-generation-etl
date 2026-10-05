# Czech coal correction: release preparation and rehearsal

Scope authorized on 2026-10-05: prepare the extractor/ETL hotfix and the
production-compatible repair, then rehearse them in an isolated local database.
Production access, merges to main and production execution remain unauthorized.
Staging was read only to export rehearsal inputs. No staging or production data
was modified. The frontend is `chienleng/global-coal-generation-tracker`;
publication to that repository is prohibited.

This branch starts from production ETL main
`d8c6d781a34726e87001e2c43ec09b66aa9e0595`. It does not require migrations 017/018
or the all-fuel feature branches. The raw table and existing dashboard view
definitions retain their existing columns and keys. The repair creates only
internal owner-controlled audit/backup tables when an actual repair is applied.

## Hotfix behavior

The matching extractor reads each Czech coal observation's XML Period duration.
The ETL recognizes only the reviewed Czech coal name/EIC aliases in
`config/entsoe-coal-aliases.json`. If one name already exists at that timestamp,
the loader retains that key. Otherwise it uses the reviewed legacy name. It
does not trim arbitrary names. If both names exist, it stops for an audited
repair instead of choosing or deleting a copy. Concurrent name lookup/insertion
is serialized within the same PostgreSQL transaction using a table lock.

Source-period records carry their unit EIC and `resolution_source=xml_period`
in JSONL. These fields are used for validation without adding columns to the
existing table. Recognized EIC/name mismatches and conflicting input copies fail.
Legacy files cannot change a stored Czech coal interval duration. Other
countries and fuels retain their existing loading behavior. The generic loader
still commits in batches; use the repair command for the atomic historical fix.

The weekly ENTSO-E job pins the matching extractor commit. Both hotfix branches
are published for review in
[extractor PR #17](https://github.com/nicholas-abad/energy-extractors/pull/17) and
[ETL PR #114](https://github.com/nicholas-abad/power-generation-etl/pull/114).
The GitHub CI jobs run offline tests and disposable PostgreSQL tests, with no
Neon access; inspect each PR's current-head checks for remote results. Local
checks are recorded in `docs/validation/entsoe-coal-hotfix-2026-10-05.json`.
The subsequent full
rehearsal is recorded in
[`docs/validation/entsoe-coal-rehearsal-2026-10-05.json`](validation/entsoe-coal-rehearsal-2026-10-05.json).

## Exact historical repair

The committed gzip JSONL payload and manifest are under `config/repairs/`.
The manifest pins the compressed and uncompressed hashes, audit export hashes
and all 367 supporting daily XML hashes. The command rejects a modified payload.

| Scope (UTC) | Duration corrections | Duplicate copies removed | Coal energy change |
| --- | ---: | ---: | ---: |
| 2023-12-31 23:00 | 24 | 0 | +941.925 MWh |
| 2024 | 104,184 | 12 | +6,563,611.575 MWh |
| Total | 104,208 | 12 | +6,564,553.500 MWh |

The twelve deleted copies contain zero generation. Every deletion requires the
retained canonical observation to exist and match the audited MW, duration and
fuel. Only the listed keys can be changed. Non-coal rows, other countries,
2025 observations and plant mappings are excluded. The separately discovered
2025 overlap still requires full reconciliation of the affected source period.

If production still matches the audited original measurements, Czech 2024 coal
changes from 12,219,119.700 to 18,782,731.275 MWh. This is a reported-unit total,
not national generation or the whole frontend's displayed total. Production has
not been inspected; its exact preflight and frontend query checks remain pending.

## Plan, apply and rollback

`src/repair_entsoe_coal.py` defaults to a read-only, repeatable-read plan. It
checks original measurements, required canonical rows, unexpected alias copies
and the complete current contents of every protected row. The resulting
`plan_sha256` binds that database state to the artifact and selected environment.
Already-correct observations are counted and left unchanged.

Apply requires the reviewed `--expect-plan-sha256`. It acquires a writer-blocking
table lock, repeats the checks, archives every affected full original row, and
updates/deletes the exact rows in one transaction. A mismatch, lock timeout or
failed post-check rolls back the entire transaction, including backup creation.
An unchanged repeat writes zero rows. Routine ETL and dashboard roles receive no
write access to the backup or repair ledger, including through default grants.

Rollback restores only this repair's archived rows, including deleted row IDs
and original metadata. It first checks that the repaired rows and protected
canonical copies have not subsequently changed. Later revisions require a new
review; they are never overwritten by a blind restore. Backups remain retained.
Reapplying an already rolled-back repair requires a new reviewed release ID.

Use the versioned environment wrapper and owner role, with complete ignored
environment credentials installed locally. The role credential files are ignored
by Git, and the owner credential is not added to GitHub Actions. Example
**staging plan**:

```sh
uv run python src/run_in_environment.py --environment staging --role owner -- \
  uv run python src/repair_entsoe_coal.py --environment staging \
    --report output/coal-plan.json
```

For apply, pass `--apply --expect-plan-sha256 <reviewed-plan-hash>` and a distinct
report filename. For rollback, generate a fresh plan and pass `--rollback` with
that plan's hash. Hashes cannot be reused across environments or changed rows.

Production additionally requires `--allow-production` on the repair command;
this is an explicit operator assertion, not a substitute for the user's approval.
The environment wrapper itself probes its selected database, so even a
production plan command must wait for that approval. No production command was
run while preparing this package.

## Full rehearsal results — 2026-10-05

**Passed** on 1,676,016 reconstructed records using the existing raw-table
schema and materialized-view definitions. No all-fuel migration was applied.
The actual repair archived 104,220 complete original rows, corrected 104,208
durations and removed the twelve audited zero-valued duplicate copies.

| Czech reported-unit coal scope | Before (MWh) | After (MWh) |
| --- | ---: | ---: |
| Full 2019 | 22,827,602.150 | 22,827,602.150 |
| 2023-12-31 23:00 UTC only | 313.975 | 1,255.900 |
| Full 2024 | 12,219,119.700 | 18,782,731.275 |

Both full annual datasets matched every independently audited observation:
224,876 for 2019 and 530,160 for 2024. All 600 source plant-month groups matched
the refreshed view; its maximum floating-point difference was 0.000000004 MWh.
The row-count view matched the raw table. Raw-row checks include exact IDs and
metadata; derived view totals use an absolute tolerance of 0.00001 MWh because
parallel floating-point sums are not byte-stable across refreshes.

All 1,571,796 records outside the repair keys remained identical, including
852,492 Czech 2025 coal records, 66,316 Czech 2024 gas records and 2,136 Polish
control records. Plant mappings remained identical. Routine writer and reader
roles had no access to the repair backup or ledger.

The actual rollback restored every original record, including deleted IDs and
metadata, and restored the views. Repeating apply and rollback each wrote zero
rows. On a separate repaired copy, the actual loader ran as `etl_writer` for
both full years twice: source quantities and durations stayed correct, no new
observations appeared, and the second replay left raw records identical.

Using the pinned frontend query and staging mappings, the two mapped Czech
units increased from 2,004,460.450 to 2,863,828.750 MWh (+42.8728%). The full
reported-unit Czech coal increase is +53.7159%; the frontend projection covers
the two mapped units. This is a query-level rehearsal, not a live frontend check.

Local apply took 9.532 seconds, rollback 7.259 seconds, and the post-apply view
refresh 0.614 seconds. These timings cover the local subset and PostgreSQL 14;
they are not a production maintenance-window estimate. All 110 ETL tests passed,
including twelve real PostgreSQL integration tests and the rehearsal target
guards. The runtime extractor/repair/loader code did not need further changes.

The complete local evidence is under `output/coal-rehearsal-2026-10-05/`:
`seed/` contains the read-only staging exports, `source/` contains the offline
parser replay, and `run3/report.json` contains the successful complete run.
The committed validation report pins their hashes and records monthly totals.
Earlier failed rehearsal checks are retained under `run1/` and `run2/`.

## Reproducing the isolated rehearsal

The three rehearsal scripts have separate purposes:

1. `scripts/export_entsoe_coal_rehearsal.py` accepts only the pinned staging
   owner wrapper and uses a read-only repeatable-read transaction. It exports
   the old-schema columns, including IDs and metadata, plus the original 2024
   repair backup and Czech frontend mappings. Run it from an already configured
   staging checkout, passing the hotfix script's absolute path and `--output`.
2. `scripts/replay_entsoe_coal_archives.py` runs offline using the pinned
   extractor checkout and its Python environment. It verifies every daily XML
   hash, regenerates both years through the hotfix parser, and compares every
   coal observation with the independently audited, hash-pinned annual JSONL.
3. `scripts/rehearse_entsoe_coal_repair.py` accepts only an explicit local socket
   or loopback connection and a new `coal_hotfix_rehearsal_*` database name.
   It reconstructs the pre-repair population, applies the actual repair,
   refreshes the existing views, checks rollback, and replays the actual loader
   twice on a separate repaired database. Existing databases are never dropped
   or overwritten. Evidence directories must also be new.

Example offline replay and local rehearsal, using existing Python environments:

```sh
PYTHONPATH=/path/to/hotfix-extractor/src /path/to/extractor-python \
  scripts/replay_entsoe_coal_archives.py \
  --archive /path/to/retained-extractor-pilot \
  --extractor /path/to/hotfix-extractor --output output/rehearsal/source

/path/to/etl-python scripts/rehearse_entsoe_coal_repair.py \
  --dsn 'host=/private/tmp port=5432 dbname=coal_hotfix_rehearsal_fresh user=postgres' \
  --seed output/rehearsal/seed --source output/rehearsal/source \
  --output output/rehearsal/run
```

The seed is a reconstruction from staging, not a current production snapshot.
Original backed-up metadata is restored for affected 2024 rows; other metadata
comes from the exported staging snapshot. The controls include all stored Czech
2025 coal observations, Czech 2024 gas, and one Polish day. Unchanged controls
are not certification that those populations are source-correct.

The local PostgreSQL 14 rehearsal refreshes views as owner. The versioned Neon
role migration relies on PostgreSQL 17's `pg_maintain` privilege for writer
refresh. Production role
permissions and full-table lock/refresh timings still require production
preflight after explicit authorization. The frontend check replays the ENTSOE
portion of `period-generation.ts` at commit
`26f6b71b825958db483d199a437bb9104a5fe21e` with staging mappings; it does not verify
the live browser, other providers, or current production mappings.

## Remaining release work

Review the two draft PRs, exact revisions, source hashes, affected rows,
before/after monthly totals and rollback evidence. Confirm passing GitHub CI
for both approved head revisions before release. Publication for review does
not authorize merging, production access or running the extraction workflow.
There is no frontend publication step. Following explicit production
approval, coordinate with scheduled ENTSO-E ingestion, perform the production
preflight, deploy the pinned hotfixes, apply the repair, refresh existing ENTSO-E
materialized views, and verify the frontend's actual queries. Only then update
affected monitoring baselines and resume ingestion. The repair command does not
refresh views or reset monitoring baselines automatically.

Rebuild the payload offline from retained evidence with
`scripts/build_entsoe_coal_repair.py --etl-audit <pilot-output> --source-archive
<extractor-pilot-output>`, using the pinned extractor's `src` on `PYTHONPATH`.
The builder verifies the complete 2024 XML archive and the 2023 boundary response
before deriving the exact repair keys from the retained staging audit.
