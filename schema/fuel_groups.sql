-- Fuel groups at the coal plants (migration 026; tracker #32, #33, #51 theme 4).
--
-- ingestion.fuel_groups is the database copy of config/fuel-groups.json:
-- one row per (source, source fuel label), either a reporting group or an
-- exclusion with its reason. src/fuel_groups.py rewrites it from the file
-- before the views that read it are refreshed; edit the file, never the table.
--
-- public.mv_chile_fuel_group_monthly: Chile's all-fuel generation (every unit
-- of the plants that have had a coal unit, migration 019) per UTC month,
-- plant and fuel group, excluded labels left out. Example: TER MEJILLONES in
-- 2024-03 has one row for coal (CTM1/CTM2) and one for gas (CTM3). Readable
-- by dashboard_ro. Coal here is every coal unit by its own fuel label, so it
-- includes units the coal table (mv_chile_plant_monthly) never had: Tarapacá
-- CTTAR, Bocamina I, Mejillones CTM1/CTM2, Tocopilla U12-U15. Do not add the
-- two views together.

CREATE TABLE IF NOT EXISTS ingestion.fuel_groups (
    source_system TEXT NOT NULL,
    source_label TEXT NOT NULL,
    fuel_group TEXT,
    excluded_reason TEXT,
    PRIMARY KEY (source_system, source_label),
    CONSTRAINT fuel_group_or_exclusion CHECK ((fuel_group IS NULL) <> (excluded_reason IS NULL))
);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_chile_fuel_group_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(a.timestamp_ms / 1000.0), 'UTC') AS month,
    a.plant_key,
    MAX(a.chile_plant_id) AS chile_plant_id,
    MAX(a.plant) AS plant,
    g.fuel_group,
    SUM(a.generation_mwh) AS generation_mwh,
    COUNT(DISTINCT a.timestamp_ms) AS hours
FROM ingestion.chile_generation_all_fuels a
JOIN ingestion.fuel_groups g
    ON g.source_system = 'CHILE' AND g.source_label = a.fuel_type
WHERE g.fuel_group IS NOT NULL
GROUP BY 1, 2, 5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_chile_fuel_group_monthly
ON public.mv_chile_fuel_group_monthly (month, plant_key, fuel_group);
