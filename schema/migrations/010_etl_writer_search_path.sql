-- Migration 010: give etl_writer the same search_path 006 gave neondb_owner
--
-- 009's dispatch test failed on every extract job with
--   relation "chile_generation_data" does not exist
-- because the ETL code uses unqualified table names, resolved through the
-- connecting role's search_path — which 006 extended with `ingestion` for
-- neondb_owner only. New role, same requirement. (fetch-gem passed: it only
-- touches public. The freshness check failed too, but correctly — it flagged
-- the genuinely stale ENTSO-E source.)
--
-- Role settings apply at connection time; the pipeline opens fresh
-- connections every job, so no backend recycling is needed for CI.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/010_etl_writer_search_path.sql

\set ON_ERROR_STOP on

ALTER ROLE etl_writer SET search_path = "$user", public, ingestion;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('010', 'etl_writer search_path += ingestion (unqualified names in ETL code, same as 006 did for neondb_owner)');

DO $$ BEGIN RAISE NOTICE '010 applied: etl_writer search_path set'; END $$;
