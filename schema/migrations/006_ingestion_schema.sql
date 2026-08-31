-- Migration 006: split the database into `ingestion` (raw) and `public` (frontend)
--
-- The frontend engineer's ask, completed: `public` becomes the dashboard's
-- whole world — materialized views, GEM reference tables, plant_crosswalk —
-- while every raw generation table, the extraction bookkeeping and their
-- sequences move to a new `ingestion` schema the dashboard role cannot even
-- see into. Nothing is copied; views keep reading the moved tables by OID.
--
-- The ETL keeps working unchanged through ONE setting:
--   ALTER ROLE neondb_owner SET search_path = "$user", public, ingestion;
-- Unqualified reads/writes of raw tables fall through to `ingestion`;
-- unqualified CREATEs (views, plant_crosswalk staging) land in `public`.
-- No Python in any repo qualifies a schema (verified 2026-08-30).
--
-- READ BEFORE APPLYING
--   * Apply MID-WEEK on the DIRECT endpoint. Each SET SCHEMA takes a brief
--     ACCESS EXCLUSIVE lock (~30 ms measured); lock_timeout below makes an
--     overlap with a load fail fast instead of queueing behind it.
--   * LIVE SESSIONS DO NOT PICK UP THE NEW search_path (role settings apply at
--     login; the pooler keeps long-lived backends). After COMMIT, connect
--     through the POOLER and check `SHOW search_path`; if stale, let the Neon
--     compute idle-suspend or restart it before the next load.
--   * The dashboard must already read only views + reference tables
--     (dashboard PR #36) — this migration revokes and moves the raw tables.
--   * plant-data's bootstrap_neon_db.py reads THIS repo's schema files from a
--     sibling checkout: that checkout must be on a post-006 commit, or its
--     pre-006 DDL creates SHADOW TABLES in public that silently swallow
--     writes. schema/checks/no_shadow_tables.sql guards this weekly.
--
-- Rollback (mirror, same session rules):
--   ALTER TABLE ingestion.<t> SET SCHEMA public;  -- x10 tables, x2 views
--   ALTER ROLE neondb_owner RESET search_path;
--   GRANT SELECT ON eia_generation_data, oe_facility_generation_data,
--     climatetrace_generation_data TO dashboard_ro;
--   restore the 3 names in schema/checks/dashboard_ro_surface.sql
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/006_ingestion_schema.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';        -- a QUEUED access-exclusive lock blocks all readers: fail fast
SET LOCAL statement_timeout = '60s';

DO $$ BEGIN
  IF current_setting('transaction_read_only') = 'on' THEN
    RAISE EXCEPTION 'read-only session — connect to the direct (non-pooler) endpoint and re-run';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    RAISE EXCEPTION 'migration 004 not applied';
  END IF;
  IF to_regclass('public.mv_eia_unit_monthly') IS NULL
     OR to_regclass('public.mv_oe_plant_monthly') IS NULL
     OR to_regclass('public.mv_climatetrace_coal_monthly') IS NULL THEN
    RAISE EXCEPTION 'migration 005 not applied';
  END IF;
  IF to_regclass('public.gem_locations') IS NULL THEN
    RAISE EXCEPTION 'migration 007 not applied (gem_* tables)';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS ingestion AUTHORIZATION neondb_owner;
REVOKE ALL ON SCHEMA ingestion FROM PUBLIC;    -- a new schema grants nothing anyway; explicit

ALTER TABLE public.entsoe_generation_data       SET SCHEMA ingestion;
ALTER TABLE public.ons_generation_data          SET SCHEMA ingestion;
ALTER TABLE public.occto_generation_data        SET SCHEMA ingestion;
ALTER TABLE public.npp_generation               SET SCHEMA ingestion;
ALTER TABLE public.chile_generation_data        SET SCHEMA ingestion;
ALTER TABLE public.eia_generation_data          SET SCHEMA ingestion;
ALTER TABLE public.oe_facility_generation_data  SET SCHEMA ingestion;
ALTER TABLE public.oe_generation_data           SET SCHEMA ingestion;
ALTER TABLE public.climatetrace_generation_data SET SCHEMA ingestion;
ALTER TABLE public.extraction_metadata          SET SCHEMA ingestion;
ALTER VIEW  public.extraction_summary           SET SCHEMA ingestion;
ALTER VIEW  public.recent_extractions           SET SCHEMA ingestion;
-- public.npp_llm_test deliberately STAYS: plant-data's unqualified DROP+RENAME
-- swap would relocate it back to public anyway (verified in review).

-- Dead weight: refreshed weekly off millions of rows, read by nothing in any
-- repo (grepped 2026-08-30). mv_*_plant_monthly stay; these five go.
DROP MATERIALIZED VIEW IF EXISTS public.mv_entsoe_monthly, public.mv_ons_monthly,
     public.mv_npp_monthly, public.mv_occto_monthly, public.mv_chile_monthly;

-- The grants travelled with the moved tables, and has_table_privilege ignores
-- schema USAGE (verified) — revoke by the NEW qualified names or the surface
-- check keeps listing them as readable.
REVOKE ALL ON ingestion.eia_generation_data, ingestion.oe_facility_generation_data,
              ingestion.climatetrace_generation_data FROM dashboard_ro;

ALTER ROLE neondb_owner SET search_path = "$user", public, ingestion;
SET LOCAL search_path = "$user", public, ingestion;

-- ---- Post-conditions ----------------------------------------------------------
DO $$ DECLARE n int; BEGIN
  SELECT count(*) INTO n FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace
    WHERE s.nspname = 'ingestion' AND c.relkind = 'S';
  IF n <> 10 THEN RAISE EXCEPTION 'expected 10 sequences in ingestion, found %', n; END IF;
  SELECT count(*) INTO n FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace
    WHERE s.nspname = 'public' AND c.relkind = 'S';
  IF n <> 0 THEN RAISE EXCEPTION '% sequences left behind in public', n; END IF;
  IF has_schema_privilege('dashboard_ro', 'ingestion', 'USAGE') THEN
    RAISE EXCEPTION 'dashboard_ro has USAGE on ingestion';
  END IF;
  SELECT count(*) INTO n FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace
    WHERE s.nspname = 'public' AND c.relkind = 'm' AND NOT c.relispopulated;
  IF n <> 0 THEN RAISE EXCEPTION '% materialized views unpopulated after the move', n; END IF;
END $$;
SELECT 1 FROM entsoe_generation_data LIMIT 1;                  -- resolves via search_path
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_chile_row_counts;    -- views still work after the move

\ir ../checks/no_shadow_tables.sql
\ir ../checks/dashboard_ro_surface.sql

COMMIT;
