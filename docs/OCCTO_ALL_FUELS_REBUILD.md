# OCCTO (Japan) all-fuel rebuild — staging runbook

Tracker issue #31. Replaces every stored OCCTO row with rows parsed from a certified archive of
OCCTO's unit-generation source, all nine fuels, one key convention (JST wall-clock written as UTC,
period-end). Staging only: `src/rebuild_occto.py` refuses any other connection before connecting.

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
   — digests must be equal; B's `report.json` lists every gap, rejection and exclusion.
4. **Benchmarks** (external totals): `docs/validation/occto/benchmarks.py fetch|build` (TSO area
   actuals, METI 電力調査統計).
5. **Stored vs source** (local scratch database only): `docs/validation/occto/compare_stored.sh
   load-stored`, `load-source EVID/B`, `run EVID/compare.txt`.
6. **Payload and plan** (owner wrapper, staging):
   ```bash
   uv run python src/rebuild_occto.py build-payload --canonical EVID/A.tsv.gz \
     --parser-b-report EVID/B/report.json --certificate EVID/certificate.json \
     --out EVID/payload --repair-id occto-all-fuels-YYYYMMDD
   uv run python src/run_in_environment.py --environment staging --role owner -- \
     uv run python src/rebuild_occto.py plan --payload EVID/payload --report EVID/plan.json
   ```
7. **Apply** with the reviewed plan: `... rebuild_occto.py apply --payload EVID/payload
   --approved-plan EVID/plan.json --expect-plan-sha256 <plan_sha256> --report EVID/apply.json`.
   Refreshes the three OCCTO views; if that fails, rerun `src/refresh_views.py --source occto`
   through the staging wrapper.
8. **Verify**: `docs/validation/occto/verify_load.sh EVID/B EVID/payload EVID/verify` — database rows
   equal parser B, views equal the raw table, the tracker's coal reads equal source coal.
9. **Rollback** (only on request): same as apply with `rollback`; refused per window if anything
   wrote to it after apply.

Ledger: `ingestion.occto_rebuilds`, `ingestion.occto_rebuild_windows`; original rows in
`ingestion.occto_rebuild_backup` (by `repair_id`, `window_start`).
