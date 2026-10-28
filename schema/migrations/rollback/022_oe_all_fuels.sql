-- Rollback of migration 022: drops the OpenElectricity all-fuel view and table.
-- Deletes all-fuel observations only; ingestion.oe_facility_generation_data and
-- mv_oe_plant_monthly are not touched. Remove `oe_all_fuels` from
-- refresh_views.STAGING_VIEWS first if it is deployed anywhere.
-- plant-data's gem_unit_monthly (and India's npp_gem_unit_monthly over it) read
-- the view this drops: drop those views first, then re-run plant-data's
-- `bootstrap_neon_db.py --unit-map-only`, which re-creates them without it.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/rollback/022_oe_all_fuels.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP MATERIALIZED VIEW IF EXISTS public.mv_oe_all_fuels_unit_monthly;
DROP TABLE IF EXISTS ingestion.oe_unit_generation_all_fuels;
DELETE FROM ingestion.schema_migrations WHERE version = '022';

COMMIT;
