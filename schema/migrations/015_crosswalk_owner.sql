-- Migration 015: plant_crosswalk (+ review view) owned by etl_writer again
--
-- Migration 009 made etl_writer the owner of the crosswalk so the weekly
-- rebuild-crosswalk job can swap it (DROP + RENAME needs ownership). The
-- one-off GPPD-purge load of 2026-09-15 ran locally as neondb_owner, and the
-- swap recreated both relations under that role, so every CI load since
-- failed with "must be owner of table plant_crosswalk" (hidden until
-- 2026-09-29 behind the verify-gate failure fixed in plant-data #19).
-- plant-data's loader now re-asserts etl_writer ownership after each swap.
-- Idempotent. Direct endpoint.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/015_crosswalk_owner.sql

\set ON_ERROR_STOP on
BEGIN;

ALTER TABLE public.plant_crosswalk OWNER TO etl_writer;
ALTER VIEW public.plant_crosswalk_review OWNER TO etl_writer;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('015', 'plant_crosswalk + plant_crosswalk_review owner -> etl_writer (CI swap needs ownership)');

COMMIT;
DO $$ BEGIN RAISE NOTICE '015 applied'; END $$;
