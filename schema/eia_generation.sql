-- EIA Generation Data - PostgreSQL Schema
-- Self-contained schema for USA electricity generation data
-- Can be executed independently
--
-- Usage:
--   psql power_generation -c "\i schema/eia_generation.sql"

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- EIA TABLES
-- ============================================================================

-- EIA generation data table
CREATE TABLE IF NOT EXISTS eia_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Location and facility identifiers
    utility_id VARCHAR(20) NOT NULL,
    plant_code VARCHAR(20) NOT NULL,
    generator_id VARCHAR(20) NOT NULL,
    state VARCHAR(5) NOT NULL,

    -- Technology and fuel information (optional in ETL format)
    fuel_source VARCHAR(100),
    prime_mover VARCHAR(50) NOT NULL,
    energy_source VARCHAR(50),

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    net_generation_mwh DOUBLE PRECISION NOT NULL,
    resolution_minutes INTEGER,  -- Data granularity (NULL = monthly)

    -- Crosswalk data
    in_gcpt_crosswalk BOOLEAN,  -- TRUE if plant matches GCPT database
    eia_plant_unit_id VARCHAR(100),  -- Combined plant+unit ID for matching

    -- Data quality constraints
    CONSTRAINT valid_timestamps_eia CHECK (timestamp_ms > 0 AND created_at_ms > 0)
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

-- Time-series analysis indexes
CREATE INDEX IF NOT EXISTS idx_eia_generation_time_state ON eia_generation_data 
(timestamp_ms, state);

CREATE INDEX IF NOT EXISTS idx_eia_generation_utility_time ON eia_generation_data 
(utility_id, timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_eia_generation_fuel_time ON eia_generation_data 
(fuel_source, timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_eia_generation_plant_time ON eia_generation_data 
(plant_code, timestamp_ms);

-- Extraction run tracking
CREATE INDEX IF NOT EXISTS idx_eia_generation_extraction_run ON eia_generation_data 
(extraction_run_id);

-- Schema creation complete
SELECT 'EIA Generation schema created successfully!' AS status;
