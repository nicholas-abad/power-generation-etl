-- Migration 017: ONS individual-plant monthly observations across all fuels.
--
-- The raw ONS table already supports every source fuel and plant type; its
-- existing population is thermal, including some groups and forecasts. No
-- fuel-column conversion or rewrite of historical coal is needed. Add a
-- separate qualified view that retains the ONS ID, with the same conservative
-- selection as energy-extract ons --all-types --individual-plants-only.
--
-- Deploy before using the updated refresh_views.py. The first backfill is
-- restricted to 2019. Keep the existing dashboard view and grants unchanged;
-- this new view is initially readable by the ETL only. Dashboard expansion
-- (including crosswalk coverage and the read-only surface check) is separate.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/017_ons_individual_plant_view.sql
--
-- Rollback: remove this view from SOURCE_VIEWS, DROP MATERIALIZED VIEW
-- public.mv_ons_individual_plant_monthly, and delete ledger version '017'.
-- That does not delete any generation observations or affect the coal view.

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

\ir ../ons_individual_plant_monthly.sql

ALTER MATERIALIZED VIEW public.mv_ons_individual_plant_monthly OWNER TO etl_writer;
REVOKE ALL ON public.mv_ons_individual_plant_monthly FROM PUBLIC, dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('017', 'ONS individual-plant monthly view across loaded fuels; preserve existing coal dashboard surface; first non-thermal backfill 2019')
ON CONFLICT (version) DO NOTHING;

COMMIT;
