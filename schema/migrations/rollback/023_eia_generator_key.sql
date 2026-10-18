-- Rollback of migration 023: restore EIA's own generator IDs.
--
-- Run plant-data's eia_generator_info load from a commit before the key (or
-- leave it: its id_as_registered column then simply goes unused) BEFORE this
-- file if anything still needs the old register spellings; the register load
-- also stops working without the function. Then refresh mv_eia_unit_monthly.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/rollback/023_eia_generator_key.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP TRIGGER IF EXISTS eia_generation_key ON ingestion.eia_generation_data;
DROP FUNCTION IF EXISTS ingestion.eia_generation_key_row();

UPDATE ingestion.eia_generation_data
SET generator_id = generator_id_reported,
    eia_plant_unit_id = plant_code || '|' || generator_id_reported
WHERE generator_id_reported IS NOT NULL
  AND (generator_id <> generator_id_reported
       OR eia_plant_unit_id IS DISTINCT FROM plant_code || '|' || generator_id_reported);

ALTER TABLE ingestion.eia_generation_data DROP COLUMN IF EXISTS generator_id_reported;
DROP FUNCTION IF EXISTS public.eia_generator_key(TEXT, TEXT);

DELETE FROM ingestion.schema_migrations WHERE version = '023';

COMMIT;
