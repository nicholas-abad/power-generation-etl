-- Migration 005: materialized views for EIA, OpenElectricity and Climate TRACE
--
-- Step 2 of 3 in taking raw generation tables off the dashboard's surface
-- (004 created the read-only role; PR 3 re-points the dashboard; 006 revokes
-- the raw tables). Definitions and rationale live in schema/materialized_views.sql
-- — this file creates them in an existing database, grants them to
-- dashboard_ro, and proves they are lossless before committing.
--
--   mv_eia_unit_monthly           projection of eia_generation_data (already monthly)
--   mv_oe_plant_monthly           daily facility rows → facility × fueltech × month
--   mv_climatetrace_coal_monthly  coal-only projection of climatetrace_generation_data
--
-- Deploy order: merge + apply in the SAME sitting. schema/checks/
-- dashboard_ro_surface.sql now lists the three views, and the weekly
-- check-dashboard-ro-surface job fails (and files an issue) every Sunday
-- until they exist and are granted. The dashboard itself keeps reading the raw
-- tables until PR 3 deploys, so nothing user-facing changes here.
--
-- Rollback: DROP MATERIALIZED VIEW <name>; remove its line from the check
-- file; remove it from src/refresh_views.py. Note CREATE … IF NOT EXISTS is
-- not idempotent-with-changes: re-applying an EDITED definition needs the DROP
-- first, or the old definition silently stays.
--
-- Use Neon's DIRECT endpoint (host without "-pooler") — see 004.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/005_mv_eia_oe_climatetrace.sql

\set ON_ERROR_STOP on
BEGIN;

DO $$ BEGIN
  IF current_setting('transaction_read_only') = 'on' THEN
    RAISE EXCEPTION 'this session is read-only — connect to the direct (non-pooler) endpoint and re-run';
  END IF;
END $$;

-- The existing views are CREATE … IF NOT EXISTS and merely NOTICE that they
-- already exist; keep the output readable.
SET LOCAL client_min_messages = warning;
\ir ../materialized_views.sql
RESET client_min_messages;

-- Grant to the dashboard role when it exists (004 applied). Guarded so this
-- migration also works on a fresh local database that never ran 004.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    GRANT SELECT ON mv_eia_unit_monthly, mv_oe_plant_monthly, mv_climatetrace_coal_monthly TO dashboard_ro;
  ELSE
    RAISE NOTICE 'dashboard_ro does not exist — views created without grants';
  END IF;
END $$;

-- ---- Post-conditions: the views lose nothing ---------------------------------
-- Energy totals are compared to 1e-9 RELATIVE tolerance, not with =: the views
-- store double-precision sums, and re-grouping changes summation order, so the
-- totals differ in the ~15th digit on prod (measured: OE 3e-16 relative) with
-- the data identical. Row counts and timestamps are exact.
DO $$
DECLARE raw_n BIGINT; view_n BIGINT; raw_s NUMERIC; view_s NUMERIC; raw_t BIGINT; view_t BIGINT; raw_v TEXT; view_v TEXT;
BEGIN
  -- EIA: same row count (already monthly), same total energy
  SELECT count(*), SUM(net_generation_mwh::numeric) INTO raw_n, raw_s FROM eia_generation_data;
  SELECT count(*), SUM(net_generation_mwh::numeric) INTO view_n, view_s FROM mv_eia_unit_monthly;
  IF raw_n <> view_n OR abs(raw_s - view_s) > abs(raw_s) * 1e-9 THEN
    RAISE EXCEPTION 'mv_eia_unit_monthly is lossy: rows % vs %, MWh % vs %', raw_n, view_n, raw_s, view_s;
  END IF;

  -- OE: same total energy, same newest day (the Australia window bound)
  SELECT SUM(generation_mwh::numeric), MAX(timestamp_ms) INTO raw_s, raw_t FROM oe_facility_generation_data;
  SELECT SUM(generation_mwh::numeric), MAX(timestamp_ms) INTO view_s, view_t FROM mv_oe_plant_monthly;
  IF abs(raw_s - view_s) > abs(raw_s) * 1e-9 OR raw_t <> view_t THEN
    RAISE EXCEPTION 'mv_oe_plant_monthly is lossy: MWh % vs %, max ts % vs %', raw_s, view_s, raw_t, view_t;
  END IF;

  -- Climate TRACE: exactly the coal rows, same cited release
  SELECT count(*), MAX(ct_version) INTO raw_n, raw_v FROM climatetrace_generation_data WHERE fuel_type ILIKE '%coal%';
  SELECT count(*), MAX(ct_version) INTO view_n, view_v FROM mv_climatetrace_coal_monthly;
  IF raw_n <> view_n OR raw_v IS DISTINCT FROM view_v THEN
    RAISE EXCEPTION 'mv_climatetrace_coal_monthly mismatch: rows % vs %, version % vs %', raw_n, view_n, raw_v, view_v;
  END IF;

  RAISE NOTICE 'post-conditions OK: ct % coal rows = view; oe newest day % = view; eia rows + totals equal', raw_n, to_timestamp(view_t / 1000.0)::date;
END $$;

\ir ../checks/dashboard_ro_surface.sql

COMMIT;
