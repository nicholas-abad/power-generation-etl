-- Migration 001b: Add UNIQUE index to ONS (non-transactional)
-- Date: 2026-02-15
--
-- ONS has ~12.9M rows. CREATE INDEX CONCURRENTLY avoids locking the table
-- during index creation, but CANNOT run inside a transaction.
--
-- Run AFTER 001a completes:
--   psql $DATABASE_URL -f 001b_dedup_and_constrain_ons.sql
--
-- This file must NOT be wrapped in BEGIN/COMMIT.
-- If the index creation fails, re-run this file — IF NOT EXISTS makes it safe.

-- ============================================================================
-- ONS_GENERATION_DATA  (natural key: timestamp_ms, plant, COALESCE(ons_plant_id, ''))
-- ============================================================================

-- Count duplicates (informational)
DO $$
DECLARE dup_count BIGINT;
BEGIN
    WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY timestamp_ms, plant, COALESCE(ons_plant_id, '')
                   ORDER BY id
               ) AS rn
        FROM ons_generation_data
    )
    SELECT COUNT(*) INTO dup_count FROM ranked WHERE rn > 1;
    RAISE NOTICE 'ons_generation_data: % duplicate rows to remove', dup_count;
END $$;

-- Delete duplicates using CTE (more efficient for large tables)
DELETE FROM ons_generation_data
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY timestamp_ms, plant, COALESCE(ons_plant_id, '')
                   ORDER BY id
               ) AS rn
        FROM ons_generation_data
    ) sub
    WHERE rn > 1
);

-- Expression-based unique index (COALESCE handles nullable ons_plant_id)
-- CONCURRENTLY avoids locking the table during index creation
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_ons_natural_key
ON ons_generation_data (timestamp_ms, plant, COALESCE(ons_plant_id, ''));

-- Verify: should return 0
SELECT COUNT(*) - COUNT(DISTINCT (timestamp_ms, plant, COALESCE(ons_plant_id, '')))
AS remaining_duplicates FROM ons_generation_data;
