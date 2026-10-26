-- Rollback for migration 024. Drop plant-data's daily views first (they read
-- these); no CASCADE, so a remaining dependant stops the rollback.
\set ON_ERROR_STOP on
BEGIN;
DROP VIEW IF EXISTS public.entsoe_plant_weekly, public.ons_plant_weekly,
    public.chile_plant_weekly, public.npp_unit_weekly, public.oe_plant_weekly;
DROP MATERIALIZED VIEW IF EXISTS public.mv_entsoe_plant_daily, public.mv_ons_plant_daily,
    public.mv_chile_plant_daily, public.mv_npp_unit_daily, public.mv_oe_plant_daily;
DELETE FROM ingestion.schema_migrations WHERE version = '024';
COMMIT;
