-- Migration 001: Add UNIQUE constraints to prevent duplicate rows
-- Date: 2026-02-15
--
-- Context: 4 of 5 data tables have no duplicate protection at the database level.
-- Re-running an extraction for the same date range silently inserts duplicate rows,
-- inflating all SUM/COUNT aggregations in the dashboard.
--
-- This migration:
--   1. Counts existing duplicates (informational)
--   2. Deletes duplicates keeping the row with the smallest id
--   3. Adds UNIQUE constraint/index to each table
--
-- Run with: psql $DATABASE_URL -f 001_add_unique_constraints.sql

BEGIN;

-- ============================================================================
-- 1. NPP_GENERATION  (natural key: timestamp_ms, plant_and_unit)
--    ~931K rows — small enough for DELETE USING
-- ============================================================================

-- Count duplicates (informational)
DO $$
DECLARE dup_count BIGINT;
BEGIN
    SELECT COUNT(*) - COUNT(DISTINCT (timestamp_ms, plant_and_unit))
    INTO dup_count
    FROM npp_generation;
    RAISE NOTICE 'npp_generation: % duplicate rows to remove', dup_count;
END $$;

-- Delete duplicates, keeping the row with the smallest id
DELETE FROM npp_generation a
USING npp_generation b
WHERE a.timestamp_ms = b.timestamp_ms
  AND a.plant_and_unit = b.plant_and_unit
  AND a.id > b.id;

-- Add UNIQUE constraint
ALTER TABLE npp_generation
ADD CONSTRAINT uq_npp_natural_key UNIQUE (timestamp_ms, plant_and_unit);

-- ============================================================================
-- 2. EIA_GENERATION_DATA  (natural key: timestamp_ms, plant_code, generator_id)
--    ~276K rows — small enough for DELETE USING
-- ============================================================================

DO $$
DECLARE dup_count BIGINT;
BEGIN
    SELECT COUNT(*) - COUNT(DISTINCT (timestamp_ms, plant_code, generator_id))
    INTO dup_count
    FROM eia_generation_data;
    RAISE NOTICE 'eia_generation_data: % duplicate rows to remove', dup_count;
END $$;

DELETE FROM eia_generation_data a
USING eia_generation_data b
WHERE a.timestamp_ms = b.timestamp_ms
  AND a.plant_code = b.plant_code
  AND a.generator_id = b.generator_id
  AND a.id > b.id;

ALTER TABLE eia_generation_data
ADD CONSTRAINT uq_eia_natural_key UNIQUE (timestamp_ms, plant_code, generator_id);

-- ============================================================================
-- 3. OE_GENERATION_DATA  (natural key: timestamp_ms, fueltech, network_code)
--    ~148K rows — small enough for DELETE USING
-- ============================================================================

DO $$
DECLARE dup_count BIGINT;
BEGIN
    SELECT COUNT(*) - COUNT(DISTINCT (timestamp_ms, fueltech, network_code))
    INTO dup_count
    FROM oe_generation_data;
    RAISE NOTICE 'oe_generation_data: % duplicate rows to remove', dup_count;
END $$;

DELETE FROM oe_generation_data a
USING oe_generation_data b
WHERE a.timestamp_ms = b.timestamp_ms
  AND a.fueltech = b.fueltech
  AND a.network_code = b.network_code
  AND a.id > b.id;

ALTER TABLE oe_generation_data
ADD CONSTRAINT uq_oe_natural_key UNIQUE (timestamp_ms, fueltech, network_code);

-- ============================================================================
-- 4. OE_FACILITY_GENERATION_DATA  (natural key: timestamp_ms, facility_code, fueltech)
--    Small table — DELETE USING is fine
-- ============================================================================

DO $$
DECLARE dup_count BIGINT;
BEGIN
    SELECT COUNT(*) - COUNT(DISTINCT (timestamp_ms, facility_code, fueltech))
    INTO dup_count
    FROM oe_facility_generation_data;
    RAISE NOTICE 'oe_facility_generation_data: % duplicate rows to remove', dup_count;
END $$;

DELETE FROM oe_facility_generation_data a
USING oe_facility_generation_data b
WHERE a.timestamp_ms = b.timestamp_ms
  AND a.facility_code = b.facility_code
  AND a.fueltech = b.fueltech
  AND a.id > b.id;

ALTER TABLE oe_facility_generation_data
ADD CONSTRAINT uq_oe_facility_natural_key UNIQUE (timestamp_ms, facility_code, fueltech);

COMMIT;

-- ============================================================================
-- 5. ONS_GENERATION_DATA  (natural key: timestamp_ms, plant, COALESCE(ons_plant_id, ''))
--    ~12.9M rows — use CTE with ROW_NUMBER for dedup, CREATE INDEX CONCURRENTLY
--    to avoid long table locks.
--    NOTE: Must run outside transaction for CONCURRENTLY.
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
