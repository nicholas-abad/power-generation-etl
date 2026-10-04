# Czech coal correction: prepared release

Scope authorized on 2026-10-05: prepare the extractor/ETL hotfix and the
production-compatible repair. Production access, merges to main and production
execution remain unauthorized. No staging or production data was modified while
preparing this release. The frontend is `chienleng/global-coal-generation-tracker`;
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

The weekly ENTSO-E job pins the matching extractor commit. Both repository
revisions must be published before that workflow is merged or run. The GitHub
CI jobs run offline tests and disposable PostgreSQL tests, with no Neon access.
Their remote results are still pending publication; local checks are recorded
in `docs/validation/entsoe-coal-hotfix-2026-10-05.json`.

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
environment credentials installed locally. The owner credential is not added to
GitHub Actions. Example **staging plan**, once the next rehearsal is authorized:

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

## Remaining release work

The next step is a full rehearsal using an isolated reconstruction of the old
schema and pre-repair records. Current all-fuel staging already contains the
2024 correction, so a no-op there is not sufficient evidence for the full repair.
Local representative integration tests establish implementation behavior; the
full data rehearsal, migration timing and frontend results are still pending.

After rehearsal, review the exact revisions, source hashes, affected rows,
before/after monthly totals and rollback evidence. Following explicit production
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
