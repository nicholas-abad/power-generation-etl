-- Brazil ONS Power Generation Data
-- Hourly generation data from thermal/fossil fuel plants
-- Data source: ONS Geração por Usina em Base Horária

CREATE TABLE IF NOT EXISTS ons_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Plant identification
    plant TEXT NOT NULL,
    ons_plant_id VARCHAR(100),
    plant_type VARCHAR(50),
    fuel_type VARCHAR(100),

    -- Location
    subsystem_id VARCHAR(10),
    subsystem VARCHAR(50),
    state VARCHAR(5),
    state_name VARCHAR(100),

    -- Operation
    operation_mode VARCHAR(100),
    ceg VARCHAR(100),

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 60,

    -- Data quality constraints
    CONSTRAINT valid_timestamps_ons CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_ons CHECK (generation_mwh >= 0)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_ons_gen_timestamp ON ons_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ons_gen_plant_time ON ons_generation_data (plant, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ons_gen_fuel_time ON ons_generation_data (fuel_type, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ons_gen_state_time ON ons_generation_data (state, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ons_gen_subsystem_time ON ons_generation_data (subsystem_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ons_gen_extraction_run ON ons_generation_data (extraction_run_id);
CREATE INDEX IF NOT EXISTS idx_ons_gen_plant_id ON ons_generation_data (ons_plant_id);
