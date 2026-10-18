-- Migration 024: daily and weekly coal generation views for five sources
-- (ENTSO-E, ONS, Chile, NPP, Open Electricity). Japan is migration 025.
--
-- Adds one daily materialized view and one weekly plain view per source
-- (schema/daily_generation_views.sql), readable by dashboard_ro. Nothing existing
-- changes. src/refresh_views.py refreshes the daily views once this ledger row
-- exists (and fails if it exists but a view is missing).
--
-- The CREATE populates each view inside this transaction (a full scan of the raw
-- tables; ENTSO-E is the largest): no statement_timeout shorter than that.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/024_daily_weekly_generation.sql
--
-- Rollback: schema/migrations/rollback/024_daily_weekly_generation.sql (after
-- plant-data's daily views, which read these, are dropped).

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

\ir ../daily_generation_views.sql

ALTER MATERIALIZED VIEW public.mv_entsoe_plant_daily OWNER TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_ons_plant_daily OWNER TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_chile_plant_daily OWNER TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_npp_unit_daily OWNER TO etl_writer;
ALTER MATERIALIZED VIEW public.mv_oe_plant_daily OWNER TO etl_writer;
ALTER VIEW public.entsoe_plant_weekly OWNER TO etl_writer;
ALTER VIEW public.ons_plant_weekly OWNER TO etl_writer;
ALTER VIEW public.chile_plant_weekly OWNER TO etl_writer;
ALTER VIEW public.npp_unit_weekly OWNER TO etl_writer;
ALTER VIEW public.oe_plant_weekly OWNER TO etl_writer;

GRANT SELECT ON public.mv_entsoe_plant_daily, public.mv_ons_plant_daily,
    public.mv_chile_plant_daily, public.mv_npp_unit_daily, public.mv_oe_plant_daily,
    public.entsoe_plant_weekly, public.ons_plant_weekly, public.chile_plant_weekly,
    public.npp_unit_weekly, public.oe_plant_weekly
TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('024', 'Daily (materialized) and weekly (plain) coal generation views for ENTSO-E, ONS, Chile, NPP, OE; readable by dashboard_ro')
ON CONFLICT (version) DO NOTHING;

COMMIT;
