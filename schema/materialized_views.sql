-- Materialized Views for Dashboard Performance
-- Pre-aggregates the large tables (ENTSOE ~62M rows, OCCTO ~12.6M, ONS ~11.9M,
-- NPP ~2.6M) into monthly summaries so the dashboard reads thousands of rows
-- instead of millions — and, since migration 005, projects the three small or
-- already-monthly sources (EIA, OpenElectricity, Climate TRACE) into views too,
-- so the dashboard's read-only role never touches a raw generation table.
-- Prod runs with TimeZone = GMT, so DATE_TRUNC('month', …) here truncates on
-- the same UTC boundaries the dashboard used when it truncated at query time.
--
-- Usage:
--   psql power_generation -c "\i schema/materialized_views.sql"
--
-- Refresh (run after ETL completes):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_chile_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_eia_unit_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_oe_plant_monthly;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_climatetrace_coal_monthly;
-- (src/refresh_views.py does all of this; the weekly workflow calls it.)

-- ============================================================================
-- ENTSOE MATERIALIZED VIEWS
-- ============================================================================
-- NOTE: the per-fuel aggregate views (mv_<source>_monthly) were DROPPED by
-- migration 006 — the dashboard reads only *_plant_monthly and *_row_counts
-- since PR #36. Do not re-add them here without re-granting and updating
-- refresh_views.py and the dashboard_ro surface check.

-- Aggregated by month + plant + country + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_entsoe_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant_name,
    country_code,
    fuel_type,
    SUM(generation_mw * COALESCE(resolution_minutes, 60) / 60.0) AS generation_mwh
FROM ingestion.entsoe_generation_data
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_plant_monthly
ON mv_entsoe_plant_monthly (month, plant_name, country_code, fuel_type);

-- ============================================================================
-- ONS MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + plant + state + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ons_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    state,
    state_name,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.ons_generation_data
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_plant_monthly
ON mv_ons_plant_monthly (month, plant, state, state_name, fuel_type);

-- ============================================================================
-- NPP MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + plant (for map)
-- fuel_type via MAX() keeps the (month, plant) key stable while months mix
-- backfilled and pre-backfill rows: a plant has exactly one fuel in the DGR-2
-- source, so MAX just surfaces the label whenever any row that month has it.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    MAX(fuel_type) AS fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.npp_generation
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_plant_monthly
ON mv_npp_plant_monthly (month, plant);

-- ============================================================================
-- OCCTO MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + plant + area + fuel_type (for map)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    area_name,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.occto_generation_data
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_plant_monthly
ON mv_occto_plant_monthly (month, plant, area_name, fuel_type);

-- ============================================================================
-- CHILE MATERIALIZED VIEWS
-- ============================================================================

-- Aggregated by month + plant + region + comuna + fuel_type (for map).
-- Coords are NOT carried here — the dashboard joins plant_crosswalk via
-- get_plant_coordinates('CHILE'), matching the ONS/NPP/OCCTO pattern.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chile_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    region,
    comuna,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.chile_generation_data
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_chile_plant_monthly
ON mv_chile_plant_monthly (month, plant, region, comuna, fuel_type);

-- ============================================================================
-- EIA (USA) — projection, NOT an aggregation
-- ============================================================================
-- EIA-923 is already monthly per generator, so this view aggregates nothing. It
-- exists so the dashboard reads a view instead of the raw table (dropping the
-- ETL metadata columns), and so eia_generation_data can leave the dashboard
-- role's surface. Deliberately NO joins to eia_generator_info /
-- gcpt_coal_metadata: plant-data replaces those tables with DROP … CASCADE,
-- which would destroy any view defined over them. The dashboard keeps joining
-- them at read time (they are ~3 MB reference tables, same class as
-- plant_crosswalk). Grain is the GENERATOR because the drilldown shows units.
-- timestamp_ms is carried (identical to month start here) so the dashboard's
-- timestamp_ms predicates and MIN/MAX bounds work unchanged and use the index.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_eia_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000.0)) AS month,
    timestamp_ms,
    plant_code,
    generator_id,
    state,
    prime_mover,
    eia_plant_unit_id,                       -- join key to gcpt_coal_metadata
    SUM(net_generation_mwh) AS net_generation_mwh
FROM ingestion.eia_generation_data
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_eia_unit_monthly
ON mv_eia_unit_monthly (timestamp_ms, plant_code, generator_id);
CREATE INDEX IF NOT EXISTS ix_mv_eia_unit_monthly_plant_time
ON mv_eia_unit_monthly (plant_code, timestamp_ms);

-- ============================================================================
-- OpenElectricity (Australia) — the one view here that genuinely aggregates
-- ============================================================================
-- Daily facility rows → one row per facility × fueltech × month. Capacity is a
-- standing quantity: MAX per facility-month, never summed across days (summing
-- the daily rows once overstated the Australian fleet ~30×). Facility
-- attributes can change over time (56 facility×fueltech pairs do), so they are
-- taken per month rather than once per facility. timestamp_ms is the LAST day
-- with data in the month (min_timestamp_ms the first): Australia is the only
-- daily-grain source and the dashboard's default window deliberately ends at
-- its newest DAY, not at the end of its newest month.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_oe_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000.0)) AS month,
    MIN(timestamp_ms) AS min_timestamp_ms,
    MAX(timestamp_ms) AS timestamp_ms,
    facility_code,
    fueltech,
    MAX(facility_name) AS facility_name,
    MAX(network_region) AS network_region,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude,
    MAX(capacity_registered_mw) AS capacity_registered_mw,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.oe_facility_generation_data
GROUP BY 1, 4, 5
ORDER BY 1, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_oe_plant_monthly
ON mv_oe_plant_monthly (month, facility_code, fueltech);
CREATE INDEX IF NOT EXISTS ix_mv_oe_plant_monthly_fueltech_time
ON mv_oe_plant_monthly (fueltech, timestamp_ms);

-- ============================================================================
-- Climate TRACE (global, MODELED) — coal slice, projection only
-- ============================================================================
-- The table is already one row per plant-month, so there is no GROUP BY (one
-- would only hide a future loader bug). What this view does is drop the 73% of
-- rows that are not coal — the dashboard applies fuel_type ILIKE '%coal%' at
-- every call site (mixed-fuel plants such as 'gas, coal' are KEPT, following
-- the GEM coal-inventory convention) — and the ETL metadata columns. Named
-- _coal_monthly, not _plant_monthly: that suffix means "all fuels, fuel as a
-- column" everywhere else in this file. `gas` is dropped (single value,
-- co2e_100yr). ct_version is kept: the dashboard cites MAX(ct_version).
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_climatetrace_coal_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000.0)) AS month,
    timestamp_ms,
    climatetrace_id,
    plant_name,
    country_code,
    fuel_type,
    latitude,
    longitude,
    capacity_mw,
    activity_confidence,
    generation_mwh,
    emissions_tonnes,
    ct_version
FROM ingestion.climatetrace_generation_data
WHERE fuel_type ILIKE '%coal%'
ORDER BY country_code, timestamp_ms;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_climatetrace_coal_monthly
ON mv_climatetrace_coal_monthly (climatetrace_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS ix_mv_climatetrace_coal_monthly_country_time
ON mv_climatetrace_coal_monthly (country_code, timestamp_ms);

-- ============================================================================
SELECT 'Materialized views created successfully!' AS status;
