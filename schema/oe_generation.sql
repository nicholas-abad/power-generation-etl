-- Australia OpenElectricity Power Generation Data
-- Daily generation data by fuel technology from the NEM (National Electricity Market)
-- Data source: OpenElectricity API v4 (https://api.openelectricity.org.au/v4)

CREATE TABLE IF NOT EXISTS oe_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Network identification
    network_code VARCHAR(10) NOT NULL,
    network_region VARCHAR(20),

    -- Fuel technology
    fueltech VARCHAR(50) NOT NULL,
    fueltech_group VARCHAR(30),

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 1440,

    -- Data quality constraints
    CONSTRAINT valid_timestamps_oe CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_oe CHECK (generation_mwh >= 0)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_oe_gen_timestamp ON oe_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_gen_fueltech_time ON oe_generation_data (fueltech, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_gen_region_time ON oe_generation_data (network_region, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_gen_extraction_run ON oe_generation_data (extraction_run_id);
CREATE INDEX IF NOT EXISTS idx_oe_gen_fueltech_group_time ON oe_generation_data (fueltech_group, timestamp_ms);
