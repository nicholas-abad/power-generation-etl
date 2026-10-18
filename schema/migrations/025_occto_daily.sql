-- Migration 025: daily and weekly coal generation views for Japan (OCCTO).
--
-- Same design as 024 (schema/daily_generation_occto.sql). Separate so production
-- takes it only after its OCCTO history rebuild is done, its views refreshed and
-- reconciled; staging takes it straight away. The ledger row is the switch every
-- consumer keys on (refresh, plant-data, both surface checks).
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/025_occto_daily.sql
--
-- Rollback: schema/migrations/rollback/025_occto_daily.sql (after plant-data's
-- daily views are dropped or rebuilt without the OCCTO branch).

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

\ir ../daily_generation_occto.sql

ALTER MATERIALIZED VIEW public.mv_occto_unit_daily OWNER TO etl_writer;
ALTER VIEW public.occto_unit_weekly OWNER TO etl_writer;
GRANT SELECT ON public.mv_occto_unit_daily, public.occto_unit_weekly TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('025', 'Daily (materialized) and weekly (plain) coal generation views for OCCTO; readable by dashboard_ro')
ON CONFLICT (version) DO NOTHING;

COMMIT;
