-- Check: the dashboard's read-only role can SELECT exactly the intended
-- relations and nothing else. THE canonical list of what the dashboard may
-- read lives here — migrations that add or remove a view edit this file.
--
-- Read-only and safe to run any time (no BEGIN, no DDL, no password). Fails
-- with a non-zero exit under ON_ERROR_STOP if the surface has drifted; skips
-- with a NOTICE if the role does not exist yet (so the weekly workflow can
-- carry this check before migration 004 is applied).
--
-- Run by:
--   * migration 004 (via \ir) right after it grants,
--   * the weekly workflow (job check-dashboard-ro-surface) after every load
--     and after the plant_crosswalk rebuild — the rebuild swaps the table
--     via DROP + RENAME, which is exactly how a grant silently disappears.
--
-- Usage:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/checks/dashboard_ro_surface.sql

DO $$
DECLARE
  expected TEXT[] := ARRAY[
    -- raw tables (the 3 sources not yet behind a plant-month view + reference data)
    'public.eia_generation_data', 'public.eia_generator_info',
    'public.oe_facility_generation_data', 'public.climatetrace_generation_data',
    'public.plant_crosswalk', 'public.gcpt_coal_metadata',
    -- plant-month materialized views
    'public.mv_entsoe_plant_monthly', 'public.mv_npp_plant_monthly',
    'public.mv_ons_plant_monthly', 'public.mv_occto_plant_monthly',
    'public.mv_chile_plant_monthly',
    -- row-count views behind /data-quality
    'public.mv_eia_row_counts', 'public.mv_entsoe_row_counts', 'public.mv_npp_row_counts',
    'public.mv_ons_row_counts', 'public.mv_oe_row_counts', 'public.mv_occto_row_counts',
    'public.mv_chile_row_counts'];
  readable  TEXT[];
  writable  TEXT[];
  missing   TEXT[];
  extra     TEXT[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    RAISE NOTICE 'dashboard_ro does not exist — migration 004 not applied yet; nothing to check';
    RETURN;
  END IF;

  -- Every relation kind has_table_privilege understands, in every non-system
  -- schema: a grant hiding in another schema or on a sequence must show up.
  SELECT array_agg(n.nspname || '.' || c.relname ORDER BY 1)
    INTO readable
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'm', 'v', 'p', 'f', 'S')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND has_table_privilege('dashboard_ro', c.oid, 'SELECT');

  SELECT array_agg(n.nspname || '.' || c.relname ORDER BY 1)
    INTO writable
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'm', 'v', 'p', 'f', 'S')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND has_table_privilege('dashboard_ro', c.oid,
          'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER');

  SELECT array_agg(e) INTO missing FROM unnest(expected) e WHERE NOT e = ANY(COALESCE(readable, '{}'));
  SELECT array_agg(r) INTO extra   FROM unnest(COALESCE(readable, '{}')) r WHERE NOT r = ANY(expected);

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'dashboard_ro CANNOT read (grant lost — a DROP/recreate or a missing GRANT?): %', missing;
  END IF;
  IF extra IS NOT NULL THEN
    RAISE EXCEPTION 'dashboard_ro can read relations outside the intended surface: %', extra;
  END IF;
  IF writable IS NOT NULL THEN
    RAISE EXCEPTION 'dashboard_ro has write privileges on: %', writable;
  END IF;
  RAISE NOTICE 'dashboard_ro surface OK: SELECT on exactly % relations, no writes', array_length(expected, 1);
END $$;
