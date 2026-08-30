-- Migration 004: a read-only database role for the dashboard
--
-- Until now the dashboard connected as `neondb_owner` — the role that OWNS
-- every table. A leaked Cloudflare DATABASE_URL therefore meant full write
-- access to 91M rows and every extraction record. The frontend never writes,
-- and it reads exactly 18 of the 33 relations in the schema: six tables and
-- twelve materialized views (audited from nextjs/lib/queries.ts, 2026-08-30).
-- This role can SELECT those 18 and touch nothing else. The CONTENTS of the raw
-- ENTSO-E, ONS, OCCTO, NPP and Chile tables (the 5 largest, 36 GB of 37) are
-- unreadable to it; like any role it can still see catalog metadata (table
-- and column names, row estimates) — Postgres does not hide those.
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
-- The canonical list of what the role may read is schema/checks/
-- dashboard_ro_surface.sql; this migration runs it at the end (and fails if the
-- surface is not exactly that set), and the weekly workflow runs it after every
-- load — the plant_crosswalk rebuild swaps the table via DROP + RENAME, which is
-- exactly how a grant silently disappears (plant-data's swap now re-applies it).
--
-- Re-runnable. The password is a psql variable and is ONLY changed when one is
-- passed; re-running WITHOUT it re-asserts the grants and leaves the live
-- password alone. (Rotating it invalidates the dashboard's connection string
-- until Cloudflare is updated — never do that as a "check".)
--
-- Use Neon's DIRECT endpoint (host without "-pooler"). Through the pooler
-- (pgbouncer, transaction mode) this script intermittently lands on a backend
-- in a read-only state and CREATE ROLE fails — 1 in 5 dry-runs; 0 in 5 direct.
-- The pre-flight after BEGIN turns that into a clear error instead.
--
-- First apply (the password is never committed — generate it, keep it):
--   PW="$(openssl rand -hex 32)"; echo "$PW"
--   psql "$DIRECT_DATABASE_URL" -v dashboard_ro_password="$PW" \
--        -f schema/migrations/004_dashboard_readonly_role.sql
-- Hex only: base64 can emit '/', '+' and '=' which break the connection URI.
-- ALTER ROLE ... PASSWORD is logged in plaintext by log_statement — rotate if
-- Neon's log retention matters to you.
--
-- Then set the dashboard's Cloudflare Pages DATABASE_URL to
--   postgresql://dashboard_ro:<password>@<same host>/<same db>?sslmode=require
-- and redeploy. The ETL keeps using neondb_owner; nothing here changes it.
--
-- Re-assert grants later (no password, nothing rotated):
--   psql "$DIRECT_DATABASE_URL" -f schema/migrations/004_dashboard_readonly_role.sql

\set ON_ERROR_STOP on

\if :{?dashboard_ro_password}
\else
  -- No password given: fine for a re-run, fatal for a first apply (a LOGIN role
  -- with no password would be useless). A deliberate error rather than \quit,
  -- which cannot set an exit status, so ON_ERROR_STOP exits 3.
  DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
      RAISE EXCEPTION 'dashboard_ro does not exist yet: first apply needs -v dashboard_ro_password=...';
    END IF;
  END $$;
\endif

BEGIN;

DO $$ BEGIN
  IF current_setting('transaction_read_only') = 'on' THEN
    RAISE EXCEPTION 'this session is read-only — connect to the direct (non-pooler) endpoint and re-run';
  END IF;
END $$;

-- Create if missing (CREATE ROLE has no IF NOT EXISTS); the password is set
-- outside the DO block, where psql can substitute the variable, and only when
-- one was passed.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    CREATE ROLE dashboard_ro LOGIN;
  END IF;
END $$;

\if :{?dashboard_ro_password}
ALTER ROLE dashboard_ro PASSWORD :'dashboard_ro_password';
\endif
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
-- Shared with the weekly workflow; the expected list lives there, not here.
\ir ../checks/dashboard_ro_surface.sql

COMMIT;
