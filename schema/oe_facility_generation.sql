-- Australia OpenElectricity Facility-Level Generation Data
-- Daily generation data by facility from the NEM (National Electricity Market)
-- Data source: OpenElectricity API v4 (https://api.openelectricity.org.au/v4)
-- Includes facility coordinates (latitude/longitude) from the API

CREATE TABLE IF NOT EXISTS oe_facility_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Network identification
    network_code VARCHAR(10) NOT NULL,
    network_region VARCHAR(20),

    -- Facility identification
    facility_code VARCHAR(50) NOT NULL,
    facility_name TEXT NOT NULL,

    -- Coordinates (from OpenElectricity API)
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    -- Fuel technology
    fueltech VARCHAR(50) NOT NULL,
    fueltech_group VARCHAR(30),

    -- Capacity
    capacity_registered_mw DOUBLE PRECISION,

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER DEFAULT 1440,

    -- Data quality constraints
    CONSTRAINT valid_timestamps_oe_fac CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_oe_fac CHECK (generation_mwh >= 0),
    CONSTRAINT valid_latitude_oe_fac CHECK (latitude IS NULL OR (latitude >= -90 AND latitude <= 90)),
    CONSTRAINT valid_longitude_oe_fac CHECK (longitude IS NULL OR (longitude >= -180 AND longitude <= 180))
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_timestamp ON oe_facility_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_facility_time ON oe_facility_generation_data (facility_code, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_fueltech_time ON oe_facility_generation_data (fueltech, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_region_time ON oe_facility_generation_data (network_region, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_extraction_run ON oe_facility_generation_data (extraction_run_id);
CREATE INDEX IF NOT EXISTS idx_oe_fac_gen_coords ON oe_facility_generation_data (latitude, longitude);
