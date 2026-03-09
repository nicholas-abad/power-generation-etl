-- Materialized Views for Dashboard Performance
-- Pre-aggregates large tables (ENTSOE 47.5M rows, ONS 12.9M rows, NPP 931K rows)
-- into monthly summaries so the dashboard reads hundreds of rows instead of millions.
--
-- Usage:
--   psql power_generation -c "\i schema/materialized_views.sql"
--
-- Refresh (run after ETL completes):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_plant_monthly;

-- ============================================================================
-- ENTSOE MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + fuel_type (for time-series chart)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_entsoe_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    fuel_type,
    SUM(generation_mw * COALESCE(resolution_minutes, 60) / 60.0) AS generation_mwh
FROM entsoe_generation_data
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_monthly
ON mv_entsoe_monthly (month, fuel_type);

-- Aggregated by month + plant + country + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_entsoe_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant_name,
    country_code,
    fuel_type,
    SUM(generation_mw * COALESCE(resolution_minutes, 60) / 60.0) AS generation_mwh
FROM entsoe_generation_data
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_plant_monthly
ON mv_entsoe_plant_monthly (month, plant_name, country_code, fuel_type);

-- ============================================================================
-- ONS MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + fuel_type (for time-series chart)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ons_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ons_generation_data
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_monthly
ON mv_ons_monthly (month, fuel_type);

-- Aggregated by month + plant + state + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ons_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    state,
    state_name,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ons_generation_data
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_plant_monthly
ON mv_ons_plant_monthly (month, plant, state, state_name, fuel_type);

-- ============================================================================
-- NPP MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month (for time-series chart)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    SUM(generation_mwh) AS generation_mwh
FROM npp_generation
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_monthly
ON mv_npp_monthly (month);

-- Aggregated by month + plant (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    SUM(generation_mwh) AS generation_mwh
FROM npp_generation
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_plant_monthly
ON mv_npp_plant_monthly (month, plant);

-- ============================================================================
-- OCCTO MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + fuel_type (for time-series chart)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM occto_generation_data
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_monthly
ON mv_occto_monthly (month, fuel_type);

-- Aggregated by month + plant + area + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    area_name,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM occto_generation_data
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_plant_monthly
ON mv_occto_plant_monthly (month, plant, area_name, fuel_type);

-- ============================================================================
SELECT 'Materialized views created successfully!' AS status;
