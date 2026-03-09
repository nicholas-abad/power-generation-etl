-- Japan OCCTO Power Generation Data
-- Half-hourly unit-level generation data from thermal/fossil fuel plants
-- Data source: OCCTO Unit-based Generation Results Disclosure System
-- https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks

CREATE TABLE IF NOT EXISTS occto_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Plant identification
    plant TEXT NOT NULL,
    unit TEXT,
    plant_code VARCHAR(20),
    fuel_code VARCHAR(10),
    fuel_type VARCHAR(50),

    -- Location (grid area)
    area_code VARCHAR(5),
    area_name VARCHAR(50),

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 30,

    -- Data quality constraints
    CONSTRAINT valid_timestamps_occto CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_occto CHECK (generation_mwh >= 0)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_occto_gen_timestamp ON occto_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_occto_gen_plant_time ON occto_generation_data (plant, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_occto_gen_fuel_time ON occto_generation_data (fuel_type, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_occto_gen_area_time ON occto_generation_data (area_code, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_occto_gen_extraction_run ON occto_generation_data (extraction_run_id);
CREATE INDEX IF NOT EXISTS idx_occto_gen_plant_code ON occto_generation_data (plant_code);

-- Natural key uniqueness (prevents cross-batch and re-load duplicates)
CREATE UNIQUE INDEX IF NOT EXISTS uq_occto_natural_key
ON occto_generation_data (timestamp_ms, plant, COALESCE(unit, ''));
