-- Migration 026: fuel groups and Chile's fuel-group view for the tracker.
--
-- Adds ingestion.fuel_groups (copy of config/fuel-groups.json) and
-- public.mv_chile_fuel_group_monthly (UTC month x Chile plant x fuel group,
-- from migration 019's all-fuel table), readable by dashboard_ro. Nothing
-- existing changes. Needs 019. The view is created empty; the first
-- refresh_views run (it is in SOURCE_VIEWS['chile']) syncs the groups from the
-- file, fails on any unmapped label, and fills it.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/026_chile_fuel_groups.sql
--
-- Rollback: schema/migrations/rollback/026_chile_fuel_groups.sql.

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM ingestion.schema_migrations WHERE version = '019') THEN
    RAISE EXCEPTION 'migration 019 (Chile all-fuel table) must be applied first';
  END IF;
END $$;

\ir ../fuel_groups.sql

ALTER TABLE ingestion.fuel_groups OWNER TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_chile_fuel_group_monthly OWNER TO etl_writer;
REVOKE ALL ON public.mv_chile_fuel_group_monthly FROM PUBLIC;
GRANT SELECT ON public.mv_chile_fuel_group_monthly TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('026', 'Fuel groups (copy of config/fuel-groups.json) and Chile fuel-group monthly view; readable by dashboard_ro')
ON CONFLICT (version) DO NOTHING;

COMMIT;
