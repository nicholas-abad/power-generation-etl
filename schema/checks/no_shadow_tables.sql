-- Check: no raw generation table exists in the public schema.
--
-- After migration 006 the raw tables live in the `ingestion` schema and the
-- ETL reaches them through neondb_owner's search_path (public, ingestion).
-- The one way this design loses data: something runs pre-006 DDL
-- (CREATE TABLE IF NOT EXISTS entsoe_generation_data …) and creates an EMPTY
-- table in `public`, which then SHADOWS the real one for every unqualified
-- statement — inserts land in the shadow, MAX(timestamp) reads 1970, and the
-- materialized views keep reading the frozen real table. No error anywhere.
-- Most likely trigger: plant-data's bootstrap_neon_db.py reads this repo's
-- schema files from a SIBLING CHECKOUT, which must be on a post-006 commit.
--
-- Read-only; safe any time. Run by migration 006 and by the weekly
-- check-dashboard-ro-surface job.
--
-- Usage:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/checks/no_shadow_tables.sql

DO $$
DECLARE shadow TEXT[];
BEGIN
  IF to_regnamespace('ingestion') IS NULL THEN
    RAISE NOTICE 'ingestion schema does not exist — migration 006 not applied yet; nothing to check';
    RETURN;
  END IF;
  SELECT array_agg(c.relname) INTO shadow
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'public'
    AND c.relkind IN ('r', 'p')
    AND c.relname IN (
      'entsoe_generation_data', 'ons_generation_data', 'occto_generation_data',
      'npp_generation', 'chile_generation_data', 'eia_generation_data',
      'oe_facility_generation_data', 'oe_generation_data',
      'climatetrace_generation_data', 'extraction_metadata');
  IF shadow IS NOT NULL THEN
    RAISE EXCEPTION 'shadow tables in public (pre-006 DDL ran? they swallow writes silently): %', shadow;
  END IF;
  RAISE NOTICE 'no shadow tables: raw generation tables exist only in ingestion';
END $$;
