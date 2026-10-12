#!/usr/bin/env bash
# Step 5 driver: copy stored OCCTO rows from STAGING (read-only) and parser B's
# canonical rows into a LOCAL scratch database, then run compare_stored.sql.
#
#   compare_stored.sh load-stored          # staging -> local table "stored"
#   compare_stored.sh load-source DIR      # DIR/canonical.tsv.gz -> local table "src"
#   compare_stored.sh run OUT_FILE         # compare_stored.sql -> OUT_FILE
#
# LOCAL_PG is a libpq connection string for the scratch database; it must be
# local (127.0.0.1 or a /tmp socket). Run from the ETL repository root.
set -euo pipefail
LOCAL_PG="${LOCAL_PG:-host=127.0.0.1 port=55432 dbname=occto_compare user=postgres}"
# Nothing inherited may route libpq elsewhere; then the repository's local
# guard validates the effective parameters (local host, disposable database).
for var in $(env | sed -n 's/^\(PG[A-Z]*\)=.*/\1/p'); do unset "$var"; done
unset DATABASE_URL DIRECT_DATABASE_URL
uv run python -I -c 'import sys; sys.path.insert(0, "src")
from local_postgres import local_settings
local_settings(sys.argv[1], r"occto_compare[a-z0-9_]*")' "$LOCAL_PG" \
  || { echo "LOCAL_PG must be a local occto_compare* database" >&2; exit 1; }
here="$(cd "$(dirname "$0")" && pwd)"
local_psql() { psql -X -q -v ON_ERROR_STOP=1 "$LOCAL_PG" "$@"; }

case "${1:-}" in
  load-stored)
    local_psql -c "DROP TABLE IF EXISTS stored; CREATE TABLE stored (id bigint, run text,
      created_at_ms bigint, ts bigint, plant text, unit text, plant_code text, fuel_type text,
      area_name text, mwh double precision);"
    uv run python src/run_in_environment.py --environment staging --role writer -- \
      psql -X -q -v ON_ERROR_STOP=1 -c "SET default_transaction_read_only = on" -c "\copy (SELECT id,
        extraction_run_id, created_at_ms, timestamp_ms, plant, coalesce(unit, ''), plant_code,
        fuel_type, area_name, generation_mwh FROM ingestion.occto_generation_data) TO STDOUT" \
      | sed '1{/^Target: /d;}' | local_psql -c "\copy stored FROM STDIN"
    local_psql -At -c "SELECT count(*), round(sum(mwh)::numeric / 1e6, 3) FROM stored"
    ;;
  load-source)
    local_psql -c "DROP TABLE IF EXISTS src; CREATE TABLE src (ts bigint, plant text, unit text,
      plant_code text, fuel_code text, fuel_type text, area_code text, area_name text, res int, kwh bigint);"
    gzip -dc "$2/canonical.tsv.gz" | local_psql -c "\copy src FROM STDIN"
    local_psql -At -c "SELECT count(*), round(sum(kwh)::numeric / 1e9, 3) FROM src"
    ;;
  run)
    local_psql -f "$here/compare_stored.sql" > "$2"
    ;;
  *) echo "usage: $0 load-stored | load-source DIR | run OUT_FILE" >&2; exit 2 ;;
esac
