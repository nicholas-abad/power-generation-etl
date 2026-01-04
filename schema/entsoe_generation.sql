-- ENTSO-E Power Generation Data - PostgreSQL Schema
-- Self-contained schema for European electricity generation data
-- Can be executed independently
--
-- Usage:
--   psql power_generation -c "\i schema/entsoe_generation.sql"

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- MAIN DATA TABLE
-- ============================================================================

-- Primary time-series data table for generation records
CREATE TABLE IF NOT EXISTS entsoe_generation_data (
    id BIGSERIAL PRIMARY KEY,
    
    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,
    
    -- Location and technology identifiers
    country_code VARCHAR(10) NOT NULL,
    psr_type VARCHAR(50) NOT NULL,        -- Power System Resource type (fuel/technology)
    plant_name TEXT NOT NULL,
    fuel_type VARCHAR(100) NOT NULL,      -- Parsed from plant data
    data_type VARCHAR(50) NOT NULL,       -- Data classification from ENTSO-E
    
    -- Time series data
    timestamp_ms BIGINT NOT NULL,         -- Unix timestamp in milliseconds
    generation_mw DOUBLE PRECISION NOT NULL,
    
    -- Data quality constraints
    CONSTRAINT positive_generation CHECK (generation_mw >= 0),
    CONSTRAINT valid_timestamps CHECK (timestamp_ms > 0 AND created_at_ms > 0)
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

-- Primary time-series analysis index (most critical)
CREATE INDEX IF NOT EXISTS idx_generation_time_country_psr ON entsoe_generation_data 
(timestamp_ms, country_code, psr_type);

-- Plant-specific time-series queries
CREATE INDEX IF NOT EXISTS idx_generation_plant_time ON entsoe_generation_data 
(plant_name, timestamp_ms);

-- Country-level aggregation queries
CREATE INDEX IF NOT EXISTS idx_generation_country_time ON entsoe_generation_data 
(country_code, timestamp_ms);

-- PSR type analysis (fuel mix studies)
CREATE INDEX IF NOT EXISTS idx_generation_psr_time ON entsoe_generation_data 
(psr_type, timestamp_ms);

-- Extraction run tracking and data lineage
CREATE INDEX IF NOT EXISTS idx_generation_extraction_run ON entsoe_generation_data 
(extraction_run_id);

-- Composite index for detailed filtering
CREATE INDEX IF NOT EXISTS idx_generation_country_psr_plant ON entsoe_generation_data 
(country_code, psr_type, plant_name);

-- Fuel type analysis
CREATE INDEX IF NOT EXISTS idx_generation_fuel_time ON entsoe_generation_data 
(fuel_type, timestamp_ms);

-- Schema creation complete
SELECT 'ENTSO-E Power Generation schema created successfully!' AS status;