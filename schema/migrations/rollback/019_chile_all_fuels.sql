-- Rollback of migration 019: drops the Chile all-fuel view and table.
-- Deletes all-fuel observations only; ingestion.chile_generation_data (coal)
-- and mv_chile_plant_monthly are not touched. Remove `chile_all_fuels` from
-- refresh_views.STAGING_VIEWS first if it is deployed anywhere.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/rollback/019_chile_all_fuels.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP MATERIALIZED VIEW IF EXISTS public.mv_chile_all_fuels_plant_monthly;
DROP TABLE IF EXISTS ingestion.chile_generation_all_fuels;
DELETE FROM ingestion.schema_migrations WHERE version = '019';

COMMIT;
