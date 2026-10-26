-- Daily and weekly coal generation for Japan (OCCTO), migration 025. Same design
-- as daily_generation_views.sql (migration 024); separate so production can take
-- it only after its OCCTO history rebuild (the 2026-03-09 bulk load is keyed to
-- Berlin local time, docs/OCCTO_ALL_FUELS_REBUILD.md).
--
-- Per plant and unit, like mv_occto_unit_monthly; 30-minute slots stamped at
-- period end in JST wall-clock time, so a day's 24:00 slot falls on the next
-- stored date, exactly as the monthly view counts it. Coal is decided per
-- unit-month as the tracker reads mv_occto_unit_monthly (MAX(fuel_type) =
-- 'coal'), and every row of such a unit-month is kept.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_unit_daily AS
WITH coal_unit_months AS (
    SELECT plant, COALESCE(unit, '') AS unit,
           DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month
    FROM ingestion.occto_generation_data
    GROUP BY 1, 2, 3
    HAVING MAX(fuel_type) = 'coal'
)
SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(g.timestamp_ms / 1000))::date AS day,
    g.plant,
    COALESCE(g.unit, '') AS unit,
    'coal'::text AS fuel_type,
    SUM(g.generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count,
    -- with no slot observed twice, the slots' own lengths; otherwise distinct slots
    (CASE WHEN COUNT(*) = COUNT(DISTINCT g.timestamp_ms) THEN SUM(COALESCE(g.resolution_minutes, 30))
          ELSE COUNT(DISTINCT g.timestamp_ms) * MIN(COALESCE(g.resolution_minutes, 30)) END)::int AS covered_minutes,
    CASE
        WHEN COUNT(DISTINCT COALESCE(g.resolution_minutes, 30)) > 1
          OR BOOL_OR(g.timestamp_ms % (COALESCE(g.resolution_minutes, 30) * 60000) <> 0)
          OR COUNT(*) > COUNT(DISTINCT g.timestamp_ms) THEN 'unknown'
        WHEN SUM(COALESCE(g.resolution_minutes, 30)) = 1440 THEN 'complete'
        ELSE 'partial'
    END AS completeness
FROM ingestion.occto_generation_data g
JOIN coal_unit_months c
  ON c.plant = g.plant
 AND c.unit = COALESCE(g.unit, '')
 AND c.month = DATE_TRUNC('month', TO_TIMESTAMP(g.timestamp_ms / 1000))
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_unit_daily
ON mv_occto_unit_daily (plant, unit, day);

CREATE OR REPLACE VIEW occto_unit_weekly AS
SELECT DATE_TRUNC('week', day)::date AS week_start, plant, unit, fuel_type,
       SUM(generation_mwh) AS generation_mwh,
       COUNT(*) AS days_reported,
       COUNT(*) FILTER (WHERE completeness = 'complete') AS days_complete,
       LEAST(7, (SELECT MAX(day) FROM mv_occto_unit_daily) - DATE_TRUNC('week', day)::date + 1) AS days_in_period,
       COUNT(*) FILTER (WHERE completeness = 'complete') = 7 AS is_complete
FROM mv_occto_unit_daily
GROUP BY 1, 2, 3, 4;
