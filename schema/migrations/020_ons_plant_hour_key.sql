-- Migration 020: one ONS row per plant-hour, keyed on the ONS ID (non-transactional)
--
-- The natural key (timestamp_ms, plant, ONS ID) let renamed plants be stored
-- twice: ONS republished 2026 files with new names and the weekly load added
-- them beside the old rows (May 2026 coal doubled). This adds a unique key on
-- (timestamp_ms, ONS ID), falling back to the plant name for rows without an
-- ID, which the loader uses as its conflict target, so a rename updates the
-- row in place. The old index stays (it is implied by the new one).
--
-- Order: run src/repair_ons_rename_duplicates.py apply FIRST (this migration
-- refuses while duplicates exist), then this file, then deploy the loader that
-- targets uq_ons_plant_hour. Pause the weekly workflow across the three steps.
--
-- CONCURRENTLY cannot run in a transaction: do NOT wrap in BEGIN/COMMIT. If the
-- build fails, DROP INDEX CONCURRENTLY ingestion.uq_ons_plant_hour and re-run.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/020_ons_plant_hour_key.sql
--
-- Rollback: revert the loader first, then
--   DROP INDEX CONCURRENTLY ingestion.uq_ons_plant_hour; delete ledger '020'.

\set ON_ERROR_STOP on

DO $$
DECLARE duplicates BIGINT;
BEGIN
    SELECT count(*) INTO duplicates FROM (
        SELECT 1 FROM ingestion.ons_generation_data
        GROUP BY timestamp_ms, COALESCE(NULLIF(ons_plant_id, ''), 'name:' || plant)
        HAVING count(*) > 1) d;
    IF duplicates > 0 THEN
        RAISE EXCEPTION '% duplicate ONS plant-hours: run repair_ons_rename_duplicates.py first', duplicates;
    END IF;
END $$;

SET lock_timeout = '5s';
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_ons_plant_hour
ON ingestion.ons_generation_data (timestamp_ms, (COALESCE(NULLIF(ons_plant_id, ''), 'name:' || plant)));

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.oid = 'ingestion.uq_ons_plant_hour'::regclass AND i.indisvalid AND i.indisunique
    ) THEN
        RAISE EXCEPTION 'uq_ons_plant_hour is missing or invalid';
    END IF;
END $$;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('020', 'ONS unique plant-hour key on ONS ID (name fallback); renames update in place')
ON CONFLICT (version) DO NOTHING;
