-- Chile Coordinador all-fuel generation (staging pilot, tracker issue #33).
-- Hourly per plant × source technology from the SIP API (`energy-extract chile
-- --all-fuels`): every unit of the plants that have had a coal unit, each
-- unit's fuel taken from the generation rows (`tipo_tecnologia` /
-- `subtipo_tecnologia`) rather than the plant master.
--
-- Deliberately separate from ingestion.chile_generation_data: the coal table,
-- its loader, mv_chile_plant_monthly and the tracker's `fuel_type = 'Carbón'`
-- contract stay untouched until the coal rules are settled. Do not add this
-- table's totals to the coal table's: coal plants appear in both.
--
-- Identity is all source values: (timestamp_ms, plant_key, tipo_tecnologia,
-- subtipo_tecnologia). `plant` (current master name) and `fuel_type`
-- (provisional derived label) are updated on conflict, so a rename or a
-- recategorisation updates rows instead of adding them. A missing source
-- label is stored as '' so the key needs no COALESCE.

CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.chile_generation_all_fuels (
    id BIGSERIAL PRIMARY KEY,

    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- `id_central`, or 'name:<cleaned plant name>' for rows without one (batteries)
    plant_key TEXT NOT NULL,
    chile_plant_id VARCHAR(100),
    plant TEXT NOT NULL,
    region VARCHAR(100),
    comuna VARCHAR(100),

    tipo_tecnologia TEXT NOT NULL,
    subtipo_tecnologia TEXT NOT NULL,
    fuel_type TEXT NOT NULL,

    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 60,
    unit_count INTEGER,

    CONSTRAINT valid_timestamps_chile_all_fuels CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_chile_all_fuels CHECK (generation_mwh >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_chile_all_fuels_natural_key
ON ingestion.chile_generation_all_fuels (timestamp_ms, plant_key, tipo_tecnologia, subtipo_tecnologia);

CREATE INDEX IF NOT EXISTS idx_chile_all_fuels_extraction_run
ON ingestion.chile_generation_all_fuels (extraction_run_id);

-- Monthly per plant × technology, for the ETL only (not granted to dashboard_ro).
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_chile_all_fuels_plant_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant_key,
    tipo_tecnologia,
    subtipo_tecnologia,
    MAX(fuel_type) AS fuel_type,
    MAX(chile_plant_id) AS chile_plant_id,
    MAX(plant) AS plant,
    MAX(region) AS region,
    MAX(comuna) AS comuna,
    SUM(generation_mwh) AS generation_mwh,
    COUNT(*) AS hours
FROM ingestion.chile_generation_all_fuels
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_chile_all_fuels_plant_monthly
ON public.mv_chile_all_fuels_plant_monthly (month, plant_key, tipo_tecnologia, subtipo_tecnologia);
