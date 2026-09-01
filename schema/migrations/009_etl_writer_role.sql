-- Migration 009: scoped writer role for the weekly pipeline
--
-- The GitHub Actions pipeline connected as neondb_owner — the schema owner
-- with CREATEDB/CREATEROLE. A leaked CI secret or a buggy extractor could
-- drop anything. dashboard_ro (004) scoped the read side; this scopes the
-- write side: etl_writer can load ingestion, refresh the public views, and
-- perform the weekly crosswalk/GEM staging-and-swap — and nothing else.
--
-- What the weekly pipeline actually does, mapped to grants:
--   * INSERT/UPDATE ingestion.* + sequences        → table/sequence grants
--   * REFRESH MATERIALIZED VIEW CONCURRENTLY        → pg_maintain (PG17)
--   * staging-and-swap of plant_crosswalk / gem_*   → CREATE on schemas +
--     OWNERSHIP of the swapped tables, the review view and the xw_guard
--     function (bootstrap re-CREATEs all of them each swap)
--   * re-GRANT to dashboard_ro after a swap          → owner of the new table
--
-- neondb_owner is granted etl_writer, so it retains full control of the
-- transferred objects and this migration can probe with SET ROLE.
--
-- Usage (generate a strong password first; store it ONLY in the CI secret):
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--        -v etl_password="'<password>'" \
--        -f schema/migrations/009_etl_writer_role.sql
--
-- Cutover: set repo secrets POSTGRES_USER=etl_writer / POSTGRES_PASSWORD,
-- then dispatch weekly-extraction.yml and watch it end-to-end BEFORE Sunday.
-- Rollback: point the secrets back at neondb_owner (objects stay usable —
-- owner is a member of etl_writer); DROP ROLE requires re-owning first.

\set ON_ERROR_STOP on

CREATE ROLE etl_writer LOGIN PASSWORD :etl_password;
COMMENT ON ROLE etl_writer IS
    'Weekly ETL pipeline (GitHub Actions). Writes ingestion, refreshes public MVs, owns the weekly-swapped crosswalk/GEM objects. Created by migration 009.';

GRANT etl_writer TO neondb_owner;

BEGIN;

-- Ingestion: full DML + sequences, now and for future tables.
GRANT USAGE, CREATE ON SCHEMA ingestion TO etl_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ingestion TO etl_writer;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA ingestion TO etl_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE neondb_owner IN SCHEMA ingestion
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE neondb_owner IN SCHEMA ingestion
    GRANT USAGE ON SEQUENCES TO etl_writer;

-- Public: read everything (reference joins, verify gates), CREATE for the
-- staging tables the atomic swap builds, plus decision uploads on the
-- crosswalk (import_decisions.py runs UPDATEs).
GRANT USAGE, CREATE ON SCHEMA public TO etl_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE neondb_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO etl_writer;
GRANT INSERT, UPDATE, DELETE ON public.plant_crosswalk TO etl_writer;

-- REFRESH MATERIALIZED VIEW / ANALYZE / VACUUM on everything (PG17 predefined
-- role carrying the MAINTAIN privilege). Maintenance only — no data access.
GRANT pg_maintain TO etl_writer;

-- The weekly swap re-CREATEs these objects, which requires owning the current
-- ones (DROP ... / CREATE OR REPLACE fail for non-owners).
ALTER TABLE public.plant_crosswalk            OWNER TO etl_writer;
ALTER TABLE public.gem_locations              OWNER TO etl_writer;
ALTER TABLE public.gem_units                  OWNER TO etl_writer;
ALTER TABLE public.gem_unit_status_snapshots  OWNER TO etl_writer;
ALTER TABLE public.gem_external_ids           OWNER TO etl_writer;
ALTER VIEW  public.plant_crosswalk_review     OWNER TO etl_writer;
ALTER FUNCTION public.xw_guard()              OWNER TO etl_writer;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('009', 'etl_writer role: ingestion DML, pg_maintain, ownership of weekly-swapped crosswalk/GEM objects');

COMMIT;

-- ── probes: exercise the pipeline's permission surface as etl_writer ─────────
SET ROLE etl_writer;

-- write path (permission is checked even with no matching rows)
UPDATE ingestion.extraction_metadata SET source = source WHERE false;
UPDATE public.plant_crosswalk SET decided_by = decided_by WHERE false;

-- staging-and-swap mechanics incl. re-granting to the reader
CREATE TABLE public._probe_etl_writer_swap (id int);
ALTER TABLE public._probe_etl_writer_swap RENAME TO _probe_etl_writer_swapped;
GRANT SELECT ON public._probe_etl_writer_swapped TO dashboard_ro;
DROP TABLE public._probe_etl_writer_swapped;

-- the schema split still holds for the reader
DO $$
BEGIN
    IF has_schema_privilege('dashboard_ro', 'ingestion', 'USAGE') THEN
        RAISE EXCEPTION 'dashboard_ro must not gain ingestion access';
    END IF;
END $$;

RESET ROLE;

-- refresh path (CONCURRENTLY is disallowed in a transaction; plain REFRESH on
-- the 90-row counts view locks it for milliseconds)
SET ROLE etl_writer;
REFRESH MATERIALIZED VIEW mv_chile_row_counts;
RESET ROLE;

DO $$ BEGIN RAISE NOTICE '009 applied: etl_writer created, probes passed'; END $$;
