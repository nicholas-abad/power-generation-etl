-- Migration 002: fuel_type on npp_generation + mv_npp_plant_monthly
--
-- The NPP DGR-2 source is fuel-sectioned (TYPE: dividers — THERMAL, HYDRO,
-- NUCLEAR, 'THER (GT)', 'THER (DG)') but neither the extractor nor this
-- schema carried it, so ~213 TWh/yr of hydro/nuclear/gas flowed into "India
-- coal" downstream. The extractor now emits fuel_type; this migration adds
-- the column and rebuilds the plant-monthly view to expose it.
--
-- MUST run in prod BEFORE the extractor change is merged: the loader COPYs
-- the JSONL's full column list into a staging table shaped like
-- npp_generation, so a fuel_type-bearing extract against a column-less table
-- fails the load.
--
-- Historical rows keep fuel_type NULL until the name-mapping backfill runs
-- (plant -> fuel harvested from sampled DGR-2 files across 2019-2026; a
-- plant's fuel never changes in the source).
--
-- Usage:
--   psql "$DATABASE_URL" -f schema/migrations/002_npp_fuel_type.sql

BEGIN;

ALTER TABLE npp_generation
    ADD COLUMN IF NOT EXISTS fuel_type VARCHAR(50);

COMMIT;

-- Rebuild the plant-monthly view with fuel_type (MAX keeps the (month, plant)
-- key stable across months that mix backfilled and pre-backfill rows).
-- Outside the transaction: DROP ... CASCADE + CREATE cannot run inside one on
-- some managed Postgres setups, and the dashboard tolerates the brief gap.
DROP MATERIALIZED VIEW IF EXISTS mv_npp_plant_monthly;

CREATE MATERIALIZED VIEW mv_npp_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    MAX(fuel_type) AS fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM npp_generation
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_plant_monthly
ON mv_npp_plant_monthly (month, plant);

SELECT 'Migration 002 (npp fuel_type) complete' AS status;
