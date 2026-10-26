-- Rollback for migration 025. Drop or rebuild plant-data's daily views without
-- the OCCTO branch first; no CASCADE.
\set ON_ERROR_STOP on
BEGIN;
DROP VIEW IF EXISTS public.occto_unit_weekly;
DROP MATERIALIZED VIEW IF EXISTS public.mv_occto_unit_daily;
DELETE FROM ingestion.schema_migrations WHERE version = '025';
COMMIT;
