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
   `config/entsoe-unit-names.json` (canonical name = latest correct source spelling;
   broken encodings excluded; older spellings kept as source and legacy names).
4. **Build the payload** (`scripts/build_entsoe_coal_payload.py`): one gzip TSV
   per month, byte-identical for the same inputs. Its observation digests must
   equal the certified ones; unnamed series without a reviewed or identity-map
   name are excluded and listed in the manifest.
5. **Release, then pause.** Apply migration 018 first (the rebuild tool's
   preflight requires it; before it, the loader writes no identity columns).
   Release the extractor allow-list and the ETL scope
   config with the country in `countries` and `rebuilt_countries` (legacy coal
   rows are then rejected for it), with the weekly workflow pinned to those
   commits. The extractor pin must be at or after `6d9f512` (energy-extractors),
   which emits unnamed series for the ETL to name by EIC; older pins drop them.
   Then pause the weekly ENTSO-E job.
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
   current month before resuming. Before resuming the weekly job, the report's
   `tail_state` (also `ingestion.entsoe_coal_rebuilds.tail_state`) must be
   `applied`; with `deferred` or `rolled_back`, the weekly job would write new
   rows beside the legacy tail rows. Then resume the weekly job, which refills
   the tail. The weekly job starts at the table's latest stored day (minus a
   7-day lookback), which other areas may already have passed: if the tail
   starts earlier, dispatch "ENTSOE (Europe)" with `start_override` set to the
   tail's first day, and repeat with the reported first unfinished day while
   it ends `ENTSOE BEHIND`.
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
database at that timestamp, else its latest stored name; a unit never stored
under a name is left out with a warning. Distinct EICs that would share a name
at one timestamp fail the batch for identity review. After a rebuild every
unit's rows carry their EIC, so known units are named. This needs the
extractor pin at or after `6d9f512`.

## Fuel exceptions

ENTSO-E's per-unit fuel label is followed, except for the reviewed records in
`config/entsoe-fuel-exceptions.json` (loader `src/entsoe_fuel_exceptions.py`;
decisions on tracker #48 and #10). Each record covers one unit and a range of
UTC months:

- `count_as_coal`: a non-coal series that is coal for the tracker. Example:
  München Nord 2 ("Nord 2 T20", `11WD2NOR20001055`), labelled gas (B04) for
  2019-01..2024-12. Certification reads the listed series from the same
  archived responses with both parsers (identical copies in several requests
  count once; only coal and listed series reach the strict parser); the
  payload writes it as `counted_psr` and lists it under `relabelled`. A
  unit-month with listed observations carries `relabelled_mwh` and
  `exception_ids` (a unit can switch label within a month). The certificate
  keeps that energy out of the coal `yearly_mwh` and sums it in
  `relabelled_yearly_mwh`: stored after the rebuild = coal - excluded +
  relabelled.
- `exclude`: coal rows that are not coal (e.g. a unit converted to biomass that
  is still labelled hard coal). The payload lists them under `excluded` with
  the exception id, before any name rule.

Every load (weekly, `START_OVERRIDE` replays, prepared files) drops the
matching rows by the row's UTC month: excluded coal, and the source-PSR rows of
a `count_as_coal` unit inside its months, which come only from the rebuild. A
coal row without a unit EIC from an area with an `exclude` record fails the
load. The plan blocks (`retained_rows_conflicting_with_relabel`, rechecked
under the apply lock) while rows outside the deletion PSRs sit at a relabelled
unit's times with its EIC or, without an EIC, one of its `reviewed_names`;
those rows are removed in their own reviewed step, never by widening the
rebuild. The certificate's `out_of_scope_series` lists every generation series
neither parser reads, so a unit whose label changes shows up. A record covering
the window that matches nothing (a mistyped area, EIC or month range) fails
certification and the payload. Payloads record the list's sha256; the builder
refuses a list changed since certification, and the rebuild tool one changed
since the payload was built, or a payload older than the list when the list
has a record for its country.

## Not yet covered

- Production apply needs explicit authorization per country.
