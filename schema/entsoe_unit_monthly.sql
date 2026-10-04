-- Identified 16.1.A generation units. Legacy name-only rows remain available
-- through mv_entsoe_plant_monthly; do not combine the two views' totals.
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_entsoe_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000.0) AT TIME ZONE 'UTC')
        AT TIME ZONE 'UTC' AS month,
    country_code,
    unit_eic,
    production_unit_eic,
    plant_name,
    source_unit_name,
    psr_type,
    fuel_type,
    SUM(generation_mw * resolution_minutes / 60.0) AS generation_mwh,
    COUNT(*) AS observation_count,
    SUM(resolution_minutes) AS observed_minutes
FROM ingestion.entsoe_generation_data
WHERE unit_eic IS NOT NULL
  AND data_type = 'Actual Aggregated'
  AND generation_mw >= 0 AND generation_mw < 'Infinity'::double precision
  AND resolution_minutes IN (15, 30, 60)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_entsoe_unit_monthly
ON public.mv_entsoe_unit_monthly
    (month, country_code, unit_eic, production_unit_eic, plant_name, source_unit_name, psr_type, fuel_type);

COMMENT ON MATERIALIZED VIEW public.mv_entsoe_unit_monthly IS
    'Actual generation for identified ENTSO-E units across loaded PSR types; MW integrated using source interval duration. Excludes consumption and legacy unidentified rows. Reporting thresholds and source gaps apply; not national totals. ETL validation only until frontend integration is reviewed.';
