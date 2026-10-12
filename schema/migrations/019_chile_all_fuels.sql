-- Migration 019: Chile all-fuel generation table and ETL-only monthly view.
--
-- Adds ingestion.chile_generation_all_fuels (plant × source technology × hour)
-- and public.mv_chile_all_fuels_plant_monthly for the all-fuel staging pilot.
-- Nothing existing changes: the coal table, its key and loader,
-- mv_chile_plant_monthly and the dashboard grants stay as they are.
--
-- The new view is readable by the ETL only, and is deliberately absent from
-- refresh_views.ALL_VIEWS (refresh it with --source chile_all_fuels), so a
-- database without this migration still refreshes cleanly.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/019_chile_all_fuels.sql
--
-- Rollback: schema/migrations/rollback/019_chile_all_fuels.sql (drops only the
-- new view and table; the coal table is not touched).

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

\ir ../chile_generation_all_fuels.sql

GRANT SELECT, INSERT, UPDATE, DELETE ON ingestion.chile_generation_all_fuels TO etl_writer;
GRANT USAGE ON SEQUENCE ingestion.chile_generation_all_fuels_id_seq TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_chile_all_fuels_plant_monthly OWNER TO etl_writer;
REVOKE ALL ON public.mv_chile_all_fuels_plant_monthly FROM PUBLIC, dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('019', 'Chile all-fuel table (plant x source technology x hour) and ETL-only monthly view; coal table and dashboard surface unchanged')
ON CONFLICT (version) DO NOTHING;

COMMIT;
