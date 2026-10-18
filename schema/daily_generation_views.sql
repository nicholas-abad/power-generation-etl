-- Daily and weekly coal generation (migration 024): five sources. Japan (OCCTO)
-- is in daily_generation_occto.sql (migration 025), applied to production only
-- after its history rebuild. Design: docs/DAILY_WEEKLY_DRILLDOWN.md.
--
-- One daily materialized view per source, at the grain of the monthly view the
-- tracker reads, coal only (the tracker's COAL_PREDICATES), refreshed in full by
-- src/refresh_views.py. Days follow the monthly views' clock
-- (DATE_TRUNC under SET TIME ZONE 'UTC'), so a source's daily rows add up to its
-- monthly figures; Australia uses its network-local date instead (its rows are
-- stamped at local midnight). Weeks are plain views over the days (ISO weeks,
-- Monday start).
--
-- Sparse: a day with no observation has no row (never a zero). Each row carries
-- observation_count, covered_minutes (distinct slots x their length) and
-- completeness:
--   complete  every slot of the day is present, once
--   partial   some slots missing
--   unknown   coverage cannot be established: mixed interval lengths,
--             misaligned stamps, or two observations in one slot (which could
--             hide a missing one) in that day, or a source whose stored rows are
--             sums the extractor builds while skipping missing units (Chile
--             plant rows, Australia facility rows)

-- ENTSO-E: per plant name, country and fuel, like mv_entsoe_plant_monthly.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_entsoe_plant_daily AS
SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000))::date AS day,
    plant_name,
    country_code,
    fuel_type,
    SUM(generation_mw * COALESCE(resolution_minutes, 60) / 60.0) AS generation_mwh,
    COUNT(*) AS observation_count,
    -- with no slot observed twice, the slots' own lengths; otherwise distinct slots
    (CASE WHEN COUNT(*) = COUNT(DISTINCT timestamp_ms) THEN SUM(COALESCE(resolution_minutes, 60))
          ELSE COUNT(DISTINCT timestamp_ms) * MIN(COALESCE(resolution_minutes, 60)) END)::int AS covered_minutes,
    CASE
        WHEN COUNT(DISTINCT COALESCE(resolution_minutes, 60)) > 1
          OR BOOL_OR(timestamp_ms % (COALESCE(resolution_minutes, 60) * 60000) <> 0)
          OR COUNT(*) > COUNT(DISTINCT timestamp_ms) THEN 'unknown'
        WHEN SUM(COALESCE(resolution_minutes, 60)) = 1440 THEN 'complete'
        ELSE 'partial'
    END AS completeness
FROM ingestion.entsoe_generation_data
WHERE fuel_type IN ('Fossil Hard coal', 'Fossil Brown coal/Lignite')
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_plant_daily
ON mv_entsoe_plant_daily (plant_name, country_code, fuel_type, day);

-- ONS (Brazil): per plant, state and fuel, like mv_ons_plant_monthly. Hourly.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ons_plant_daily AS
SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000))::date AS day,
    plant,
    state,
    state_name,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count,
    -- with no slot observed twice, the slots' own lengths; otherwise distinct slots
    (CASE WHEN COUNT(*) = COUNT(DISTINCT timestamp_ms) THEN SUM(COALESCE(resolution_minutes, 60))
          ELSE COUNT(DISTINCT timestamp_ms) * MIN(COALESCE(resolution_minutes, 60)) END)::int AS covered_minutes,
    CASE
        WHEN COUNT(DISTINCT COALESCE(resolution_minutes, 60)) > 1
          OR BOOL_OR(timestamp_ms % (COALESCE(resolution_minutes, 60) * 60000) <> 0)
          OR COUNT(*) > COUNT(DISTINCT timestamp_ms) THEN 'unknown'
        WHEN SUM(COALESCE(resolution_minutes, 60)) = 1440 THEN 'complete'
        ELSE 'partial'
    END AS completeness
FROM ingestion.ons_generation_data
WHERE fuel_type = 'Carvão'
GROUP BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_plant_daily
ON mv_ons_plant_daily (plant, state, state_name, fuel_type, day);

-- Chile: per plant, region and fuel, like mv_chile_plant_monthly. Hourly. Rows are
-- plant sums the extractor builds while skipping missing units, so completeness
-- is always unknown.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chile_plant_daily AS
SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000))::date AS day,
    plant,
    region,
    comuna,
    fuel_type,
    SUM(generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count,
    -- with no slot observed twice, the slots' own lengths; otherwise distinct slots
    (CASE WHEN COUNT(*) = COUNT(DISTINCT timestamp_ms) THEN SUM(COALESCE(resolution_minutes, 60))
          ELSE COUNT(DISTINCT timestamp_ms) * MIN(COALESCE(resolution_minutes, 60)) END)::int AS covered_minutes,
    'unknown'::text AS completeness
FROM ingestion.chile_generation_data
WHERE fuel_type = 'Carbón'
GROUP BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_chile_plant_daily
ON mv_chile_plant_daily (plant, region, comuna, fuel_type, day);

-- NPP (India): per plant and unit, like mv_npp_unit_monthly. One report per unit
-- per day. Coal is decided per unit-month exactly as the tracker reads
-- mv_npp_unit_monthly (MAX(fuel_type) = 'THERMAL'), and every row of such a
-- unit-month is kept, so daily never drops energy the month counts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_unit_daily AS
WITH coal_unit_months AS (
    SELECT plant, COALESCE(unit, '') AS unit,
           DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month
    FROM ingestion.npp_generation
    GROUP BY 1, 2, 3
    HAVING MAX(fuel_type) = 'THERMAL'
)
SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(g.timestamp_ms / 1000))::date AS day,
    g.plant,
    COALESCE(g.unit, '') AS unit,
    'THERMAL'::text AS fuel_type,
    SUM(g.generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count,
    1440 AS covered_minutes,
    'complete'::text AS completeness
FROM ingestion.npp_generation g
JOIN coal_unit_months c
  ON c.plant = g.plant
 AND c.unit = COALESCE(g.unit, '')
 AND c.month = DATE_TRUNC('month', TO_TIMESTAMP(g.timestamp_ms / 1000))
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_unit_daily
ON mv_npp_unit_daily (plant, unit, day);

-- Open Electricity (Australia): per facility and fueltech, like
-- mv_oe_plant_monthly, on the network-local date (as migration 022's unit view).
-- Facility rows are sums the extractor builds while skipping missing units, so
-- completeness is always unknown.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_oe_plant_daily AS
SELECT
    (TO_TIMESTAMP(timestamp_ms / 1000.0) AT TIME ZONE
        CASE network_code WHEN 'WEM' THEN 'Australia/Perth' ELSE 'Australia/Brisbane' END
    )::date AS day,
    facility_code,
    fueltech,
    MAX(facility_name) AS facility_name,
    MAX(network_code) AS network_code,
    SUM(generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count,
    1440 AS covered_minutes,
    'unknown'::text AS completeness
FROM ingestion.oe_facility_generation_data
WHERE fueltech IN ('coal_black', 'coal_brown')
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_oe_plant_daily
ON mv_oe_plant_daily (facility_code, fueltech, day);

-- Weekly views: ISO weeks (Monday start) summed from the days. days_in_period is
-- 7, or fewer for the week holding the source's latest day; a week is complete
-- only when all its days are present and complete.
CREATE OR REPLACE VIEW entsoe_plant_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, plant_name, country_code, fuel_type,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_entsoe_plant_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_entsoe_plant_daily
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW ons_plant_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, plant, state, state_name, fuel_type,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_ons_plant_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_ons_plant_daily
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW chile_plant_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, plant, region, comuna, fuel_type,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_chile_plant_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_chile_plant_daily
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW npp_unit_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, plant, unit, fuel_type,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_npp_unit_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_npp_unit_daily
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW oe_plant_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, facility_code, fueltech,
       MAX(facility_name) AS facility_name,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_oe_plant_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_oe_plant_daily
GROUP BY 1, 2, 3;
