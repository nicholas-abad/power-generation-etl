# Certified ENTSO-E coal rebuild

Stored ENTSO-E coal history is wrong in ways an interval relabel cannot fix:
truncated unit names collapsed several units onto one key, whole months and
units were never loaded, interval labels are wrong, and some units ENTSO-E has
since reclassified to other fuels are still stored as coal. The rebuild
replaces one country's stored coal rows with rows rebuilt from its archived,
independently certified source. Units are keyed by EIC (migration 018); the
reviewed names come from `config/entsoe-unit-names.json`.

## Steps, per country

1. **Download** every UTC day from 2019-01-01 to the first day of the current
   month, one request per coal PSR the country reports, archiving each response
   with its bounds, status and sha256 (`manifest.jsonl` plus `xml/`).
2. **Certify** (`scripts/certify_entsoe_coal_source.py`). The inventory must be
   complete (`ok` or verified `no_data`, each request covering exactly its UTC
   day); the independent stdlib/Decimal parser and the extractor's per-Period
   parser must agree on every observation; per unit-month it records energy and
   an observation digest. Series with a unit EIC but no name are included and
   listed.
3. **Identity map** for the country: stored names to source EICs (exact,
   cleanup, whitespace, encoding, truncated). Reviewed renames go into
   `config/entsoe-unit-names.json` (canonical name = longest stored history).
4. **Build the payload** (`scripts/build_entsoe_coal_payload.py`): one gzip TSV
   per month, byte-identical for the same inputs. Its observation digests must
   equal the certified ones; unnamed series without a reviewed or identity-map
   name are excluded and listed in the manifest.
5. **Plan** (`src/rebuild_entsoe_coal.py`, read-only) and review the report:
   stored rows, exact MWh and hash per month, and blocking findings (stored coal
   in uncertified PSRs, unit/time key collisions, country not configured as
   rebuilt).
6. **Cutover**, within the certified end's own UTC month:
   - release the extractor allow-list and the ETL scope config with the country
     in `countries` and `rebuilt_countries` (legacy coal rows are then rejected
     for it), and the pinned extractor commit;
   - pause the weekly ENTSO-E job;
   - `--apply --approved-plan <plan> --expect-plan-sha256 <sha>`; months are
     applied one transaction at a time, and an interrupted run resumes;
   - resume the weekly job: it refetches from the newest month in the table,
     which refills the tail window (certified end onwards). The tool refuses the
     tail outside that month, because a tail deleted later would never be
     refetched.
7. **Verify**: view refresh (UTC, `CONCURRENTLY`; `--refresh-views` retries a
   failure), totals per month equal the certified payload, other countries and
   fuels unchanged, crosswalk diff (no lost GEM links, site capacity conserved).

## Rollback

`--rollback` with the same approved plan restores each month's original rows
and ids, per month under the table lock, only while nothing has written to that
month since apply. Once the weekly job has refilled the tail, roll back the
certified months with `--months <start>..<last certified month>`; the tail then
holds only rows the weekly job wrote. Backups live in
`ingestion.entsoe_coal_rebuild_backup` (owner only) until an agreed acceptance
period ends.

## Not yet covered

- The weekly path still skips unnamed series (`include_unnamed` is off in the
  extractor), so after cutover their months would show gaps again; name them by
  EIC in the loader before a country goes live.
- Units the source now lists under another fuel leave coal history when the
  source is followed (pending a methodology decision).
- Production apply needs explicit authorization per country.
