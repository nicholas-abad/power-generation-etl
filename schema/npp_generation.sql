-- NPP Generation Data - PostgreSQL Schema
-- Harmonized schema matching EIA/ENTSOE format
-- Self-contained schema for Indian Nuclear Power Plant generation data
-- Can be executed independently
--
-- Usage:
--   psql power_generation -c "\i schema/npp_generation.sql"

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- NPP TABLES
-- ============================================================================

-- NPP generation data table (harmonized with EIA/ENTSOE)
CREATE TABLE IF NOT EXISTS npp_generation (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata (matches EIA/ENTSOE)
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Facility identifiers
    plant VARCHAR(255) NOT NULL,
    unit VARCHAR(50),
    plant_and_unit VARCHAR(355) NOT NULL,

    -- Time series data
    timestamp_ms BIGINT NOT NULL,
    actual_generation DOUBLE PRECISION NOT NULL,

    -- Data quality constraints
    CONSTRAINT positive_generation_npp CHECK (actual_generation >= 0),
    CONSTRAINT valid_timestamps_npp CHECK (timestamp_ms > 0 AND created_at_ms > 0)
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

-- Time-series queries filtered by plant
CREATE INDEX IF NOT EXISTS idx_npp_generation_time_plant ON npp_generation(timestamp_ms, plant);

-- Plant-specific time-series analysis
CREATE INDEX IF NOT EXISTS idx_npp_generation_plant_time ON npp_generation(plant, timestamp_ms);

-- Track data lineage by extraction run
CREATE INDEX IF NOT EXISTS idx_npp_generation_extraction_run ON npp_generation(extraction_run_id);

-- General time-based queries
CREATE INDEX IF NOT EXISTS idx_npp_generation_timestamp ON npp_generation(timestamp_ms);

-- Schema creation complete
SELECT 'NPP Generation schema created successfully!' AS status;
