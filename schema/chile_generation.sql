-- Chile Coordinador Power Generation Data
-- Hourly per-plant coal generation from the Coordinador Eléctrico Nacional SIP
-- API (sipubv2). Coal plants are identified via combus_termo == "Carbón" on
-- /centrales/v4/findByDate; hourly values come from /generacion-real/v3.
-- Data source: https://sipub.api.coordinador.cl

CREATE TABLE IF NOT EXISTS chile_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Plant identification
    plant TEXT NOT NULL,
    chile_plant_id VARCHAR(100),
    fuel_type VARCHAR(100),

    -- Location (regional admin division + commune; lat/lon live in plant_crosswalk)
    region VARCHAR(100),
    comuna VARCHAR(100),

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 60,

    -- Data quality constraints
    CONSTRAINT valid_timestamps_chile CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_chile CHECK (generation_mwh >= 0)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_chile_gen_timestamp ON chile_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_chile_gen_plant_time ON chile_generation_data (plant, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_chile_gen_fuel_time ON chile_generation_data (fuel_type, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_chile_gen_region_time ON chile_generation_data (region, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_chile_gen_extraction_run ON chile_generation_data (extraction_run_id);
CREATE INDEX IF NOT EXISTS idx_chile_gen_plant_id ON chile_generation_data (chile_plant_id);

-- Natural key uniqueness (prevents cross-batch and re-load duplicates)
-- Uses expression index because chile_plant_id is nullable (NULL != NULL in UNIQUE)
CREATE UNIQUE INDEX IF NOT EXISTS uq_chile_natural_key
ON chile_generation_data (timestamp_ms, plant, COALESCE(chile_plant_id, ''));
