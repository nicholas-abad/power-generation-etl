-- All-fuel ONS observations at the identified plant/month grain.
--
-- The raw table also contains legacy forecasts and plant groups. Apply the
-- individual-plant extractor policy before aggregating; never add those
-- records to measured plant totals. Keep original IDs and source fuel labels.
-- Coverage follows what has actually been loaded, not the whole SIN fleet.
-- The first non-thermal backfill is 2019; other years remain partial by fuel.
-- This view is initially for ETL validation, with no dashboard_ro grant.

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_ons_individual_plant_monthly AS
WITH eligible AS (
    SELECT *,
        COUNT(*) OVER (PARTITION BY timestamp_ms, ons_plant_id) AS observation_copies
    FROM ingestion.ons_generation_data
    WHERE operation_mode ~* '^\s*(TIPO I|TIPO II-A|TIPO II-B)\s*$'
      AND ons_plant_id !~ '^\s*(-)?\s*$'
      AND plant !~ '^\s*(-)?\s*$'
      AND plant_type !~ '^\s*(-)?\s*$'
      AND fuel_type !~ '^\s*(-)?\s*$'
      AND COALESCE(ceg, '') !~ '[;,|]'
      AND timestamp_ms > 0
      AND generation_mwh >= 0
      AND generation_mwh < 'Infinity'::double precision
      AND resolution_minutes = 60
)
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000) AT TIME ZONE 'UTC')
        AT TIME ZONE 'UTC' AS month,
    ons_plant_id,
    plant,
    plant_type,
    fuel_type,
    state,
    state_name,
    SUM(generation_mwh) AS generation_mwh,
    COUNT(*) AS observation_count
FROM eligible
WHERE observation_copies = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_ons_individual_plant_monthly
ON public.mv_ons_individual_plant_monthly
    (month, ons_plant_id, plant, plant_type, fuel_type, state, state_name);

COMMENT ON MATERIALIZED VIEW public.mv_ons_individual_plant_monthly IS
    'ONS qualifying individual plants, all loaded fuels; excludes forecasts, groups, unresolved identities and colliding observations. Source labels and IDs retained. Coverage is limited to loaded periods and the ONS reporting population; not a national total.';
