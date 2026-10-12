-- Migration 022: OpenElectricity all-fuel table (unit grain) and ETL-only view.
--
-- Adds ingestion.oe_unit_generation_all_fuels (unit × day × direction, every
-- fueltech) and public.mv_oe_all_fuels_unit_monthly for the Australia
-- all-fuels staging pilot (tracker issue #32). Nothing existing changes:
-- oe_facility_generation_data, its loader, mv_oe_plant_monthly and the
-- dashboard grants stay as they are.
--
-- The view is readable by the ETL only and is not in refresh_views.ALL_VIEWS
-- (refresh with --source oe_all_fuels), so databases without 022 still refresh.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/022_oe_all_fuels.sql
--
-- Rollback: schema/migrations/rollback/022_oe_all_fuels.sql.

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

\ir ../oe_unit_generation_all_fuels.sql

GRANT SELECT, INSERT, UPDATE, DELETE ON ingestion.oe_unit_generation_all_fuels TO etl_writer;
GRANT USAGE ON SEQUENCE ingestion.oe_unit_generation_all_fuels_id_seq TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_oe_all_fuels_unit_monthly OWNER TO etl_writer;
REVOKE ALL ON public.mv_oe_all_fuels_unit_monthly FROM PUBLIC, dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('022', 'OpenElectricity all-fuel table (unit x day x direction) and ETL-only monthly view in network-local months; existing OE table and dashboard surface unchanged')
ON CONFLICT (version) DO NOTHING;

COMMIT;
