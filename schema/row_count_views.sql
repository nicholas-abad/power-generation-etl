-- Row-Count Materialized Views for Data Quality Page
-- One row per (source, month) with COUNT(*) of raw rows. Powers the dashboard's
-- /data-quality coverage matrix without scanning multi-million-row raw tables
-- on every page load (ENTSOE alone is 50M+ rows).
--
-- Each view is ~90 rows total — refresh is near-instant.
--
-- Usage:
--   psql $DATABASE_URL -f schema/row_count_views.sql
--
-- Refresh (run after ETL completes — handled by src/refresh_views.py):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_<source>_row_counts;

-- ============================================================================
-- EIA (USA)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_eia_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM eia_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_eia_row_counts ON mv_eia_row_counts (month);

-- ============================================================================
-- ENTSOE (Europe)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_entsoe_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM entsoe_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_row_counts ON mv_entsoe_row_counts (month);

-- ============================================================================
-- NPP (India)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM npp_generation
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_row_counts ON mv_npp_row_counts (month);

-- ============================================================================
-- ONS (Brazil)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ons_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM ons_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_row_counts ON mv_ons_row_counts (month);

-- ============================================================================
-- OE_FACILITY (Australia)
-- The dashboard's REGION_TABLE maps australia → oe_facility_generation_data.
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_oe_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM oe_facility_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_oe_row_counts ON mv_oe_row_counts (month);

-- ============================================================================
-- OCCTO (Japan)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM occto_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_row_counts ON mv_occto_row_counts (month);

-- ============================================================================
-- CHILE (Coordinador)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chile_row_counts AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000))::date AS month,
    COUNT(*) AS row_count
FROM chile_generation_data
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_chile_row_counts ON mv_chile_row_counts (month);

SELECT 'Row-count materialized views created successfully!' AS status;
