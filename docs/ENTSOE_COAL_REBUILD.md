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
5. **Release, then pause.** Release the extractor allow-list and the ETL scope
   config with the country in `countries` and `rebuilt_countries` (legacy coal
   rows are then rejected for it), with the weekly workflow pinned to those
   commits; then pause the weekly ENTSO-E job.
6. **Plan** (`src/rebuild_entsoe_coal.py`, read-only) from that released
   checkout and review the report: stored rows, exact MWh and hash per month;
   the ETL commit and scope-config hash it was checked against (they must equal
   the released ones); and blocking findings (stored coal in uncertified PSRs,
   unit/time key collisions, country not configured as rebuilt).
   **Apply**, within the certified end's own month:
   `--apply --approved-plan <plan> --expect-plan-sha256 <sha>`. Months are
   applied one transaction at a time; an interrupted run resumes. The tail
   (certified end onwards) is applied only if the weekly job will resume within
   it: the newest stored day in the whole table plus one day must not fall in a
   later month, checked under the lock after the tail is deleted, and the
   resume month is recorded. Completing the certified months without the tail
   needs `--defer-tail` (recorded in the ledger); then re-certify through the
   current month before resuming. Finally resume the weekly job, which refills
   the tail.
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

## Unnamed series on the weekly path

For allow-listed countries the extractor keeps series that carry a unit EIC
but no name (plant_name null). The loader names them from the reviewed
identity table, else from the name the same unit (country, EIC) has in the
database; a unit never stored under a name is left out with a warning. After a
rebuild every unit's rows carry their EIC, so known units are named.

## Not yet covered

- Units the source now lists under another fuel leave coal history when the
  source is followed (pending a methodology decision).
- Production apply needs explicit authorization per country.
