-- Migration 004: a read-only database role for the dashboard
--
-- Until now the dashboard connected as `neondb_owner` — the role that OWNS
-- every table. A leaked Cloudflare DATABASE_URL therefore meant full write
-- access to 91M rows and every extraction record. The frontend never writes,
-- and it reads exactly 18 of the 33 relations in the schema: six tables and
-- twelve materialized views (audited from nextjs/lib/queries.ts, 2026-08-30).
-- This role can SELECT those 18 and touch nothing else. The raw ENTSO-E, ONS,
-- OCCTO, NPP and Chile tables (the 5 largest, 36 GB of 37) are invisible to it.
--
-- Postgres defaults already deny PUBLIC any table access here (verified on
-- prod), so nothing needs revoking — grants alone define the surface.
--
-- Deliberately NOT using ALTER DEFAULT PRIVILEGES: it would auto-grant every
-- future table, including raw ones, and defeat the point. The consequence is
-- that the surface must be maintained BY HAND:
--   * a migration that DROPs and re-creates a materialized view (as 002 did)
--     loses its grant — re-GRANT in the same migration;
--   * a new view the dashboard reads needs a GRANT line here or in its own
--     migration (005 will add the EIA / OE / Climate TRACE plant views).
-- The assertion block at the end fails the whole migration if the surface is
-- not exactly the intended set, so drift is caught on re-run.
--
-- Re-runnable: re-applying rotates the password and re-asserts the grants.
--
-- Usage (the password is never committed — generate one at apply time):
--   psql "$DATABASE_URL" \
--        -v dashboard_ro_password="$(openssl rand -base64 32)" \
--        -f schema/migrations/004_dashboard_readonly_role.sql
--
-- Then set the dashboard's Cloudflare Pages DATABASE_URL to
--   postgresql://dashboard_ro:<password>@<same host>/<same db>?sslmode=require
-- and redeploy. The ETL keeps using neondb_owner; nothing here changes it.

\set ON_ERROR_STOP on

\if :{?dashboard_ro_password}
\else
  -- A deliberate error (not \quit, which cannot set an exit status) so a
  -- script that forgot the variable fails loudly under ON_ERROR_STOP.
  DO $$ BEGIN RAISE EXCEPTION 'pass the password as a psql variable: -v dashboard_ro_password=...'; END $$;
\endif

BEGIN;

-- Create if missing (CREATE ROLE has no IF NOT EXISTS), then set login +
-- password outside the DO block, where psql can substitute the variable.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    CREATE ROLE dashboard_ro NOINHERIT;
  END IF;
END $$;

ALTER ROLE dashboard_ro LOGIN PASSWORD :'dashboard_ro_password';
-- Belt and braces: even a SELECT-only role should never open a writable
-- transaction. (Grants are the real control; this catches mistakes early.)
ALTER ROLE dashboard_ro SET default_transaction_read_only = on;

-- Neon databases are named differently per branch; grant on whichever this is.
DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO dashboard_ro', current_database());
END $$;
GRANT USAGE ON SCHEMA public TO dashboard_ro;

-- ---- The 18 relations the dashboard reads -----------------------------------
-- Raw tables (the 3 sources not yet behind a materialized view + reference data)
GRANT SELECT ON eia_generation_data          TO dashboard_ro;  -- USA, monthly grain
GRANT SELECT ON eia_generator_info           TO dashboard_ro;  -- coal-unit filter + capacity
GRANT SELECT ON oe_facility_generation_data  TO dashboard_ro;  -- Australia
GRANT SELECT ON climatetrace_generation_data TO dashboard_ro;  -- ~50 modeled countries
GRANT SELECT ON plant_crosswalk              TO dashboard_ro;  -- coords / capacity / coal type
GRANT SELECT ON gcpt_coal_metadata           TO dashboard_ro;  -- US coal type + technology
-- Plant-month materialized views
GRANT SELECT ON mv_entsoe_plant_monthly      TO dashboard_ro;
GRANT SELECT ON mv_npp_plant_monthly         TO dashboard_ro;
GRANT SELECT ON mv_ons_plant_monthly         TO dashboard_ro;
GRANT SELECT ON mv_occto_plant_monthly       TO dashboard_ro;
GRANT SELECT ON mv_chile_plant_monthly       TO dashboard_ro;
-- Row-count views behind /data-quality
GRANT SELECT ON mv_eia_row_counts            TO dashboard_ro;
GRANT SELECT ON mv_entsoe_row_counts         TO dashboard_ro;
GRANT SELECT ON mv_npp_row_counts            TO dashboard_ro;
GRANT SELECT ON mv_ons_row_counts            TO dashboard_ro;
GRANT SELECT ON mv_oe_row_counts             TO dashboard_ro;
GRANT SELECT ON mv_occto_row_counts          TO dashboard_ro;
GRANT SELECT ON mv_chile_row_counts          TO dashboard_ro;

-- ---- Assert the surface is exactly what was intended -------------------------
DO $$
DECLARE
  readable TEXT[];
  expected TEXT[] := ARRAY[
    'eia_generation_data', 'eia_generator_info', 'oe_facility_generation_data',
    'climatetrace_generation_data', 'plant_crosswalk', 'gcpt_coal_metadata',
    'mv_entsoe_plant_monthly', 'mv_npp_plant_monthly', 'mv_ons_plant_monthly',
    'mv_occto_plant_monthly', 'mv_chile_plant_monthly',
    'mv_eia_row_counts', 'mv_entsoe_row_counts', 'mv_npp_row_counts',
    'mv_ons_row_counts', 'mv_oe_row_counts', 'mv_occto_row_counts', 'mv_chile_row_counts'];
BEGIN
  SELECT array_agg(relname ORDER BY relname) INTO readable
  FROM pg_class
  WHERE relnamespace = 'public'::regnamespace
    AND relkind IN ('r', 'm', 'v', 'p')
    AND has_table_privilege('dashboard_ro', oid, 'SELECT');

  IF readable IS DISTINCT FROM (SELECT array_agg(e ORDER BY e) FROM unnest(expected) e) THEN
    RAISE EXCEPTION 'dashboard_ro surface mismatch. readable=% expected=%', readable, expected;
  END IF;
  IF has_table_privilege('dashboard_ro', 'entsoe_generation_data', 'SELECT') THEN
    RAISE EXCEPTION 'dashboard_ro can read entsoe_generation_data — grants are wrong';
  END IF;
  IF has_table_privilege('dashboard_ro', 'plant_crosswalk', 'INSERT, UPDATE, DELETE') THEN
    RAISE EXCEPTION 'dashboard_ro has write privileges — grants are wrong';
  END IF;
  RAISE NOTICE 'dashboard_ro: SELECT on exactly % relations, no writes', array_length(readable, 1);
END $$;

COMMIT;
