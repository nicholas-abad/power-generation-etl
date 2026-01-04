-- NPP Generation Data - PostgreSQL Schema
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

-- NPP scraping metadata table
CREATE TABLE IF NOT EXISTS scrape_metadata (
    scrape_id UUID PRIMARY KEY,
    scrape_timestamp TIMESTAMP NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    total_records INTEGER NOT NULL,
    failed_dates_count INTEGER NOT NULL DEFAULT 0,
    failed_dates JSONB,
    success BOOLEAN NOT NULL,
    source_urls JSONB NOT NULL,
    extractor_config JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NPP generation data table
CREATE TABLE IF NOT EXISTS npp_generation (
    id SERIAL PRIMARY KEY,
    scrape_id UUID NOT NULL REFERENCES scrape_metadata(scrape_id),
    date DATE NOT NULL,
    plant VARCHAR(255) NOT NULL,
    unit VARCHAR(50),
    plant_and_unit VARCHAR(355) NOT NULL,
    actual_generation FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

-- Primary analysis indexes
CREATE INDEX IF NOT EXISTS idx_npp_generation_scrape_id ON npp_generation(scrape_id);
CREATE INDEX IF NOT EXISTS idx_npp_generation_date ON npp_generation(date);
CREATE INDEX IF NOT EXISTS idx_npp_generation_plant ON npp_generation(plant);
CREATE INDEX IF NOT EXISTS idx_npp_generation_plant_date ON npp_generation(plant, date);

-- Metadata indexes  
CREATE INDEX IF NOT EXISTS idx_scrape_metadata_timestamp ON scrape_metadata(scrape_timestamp);
CREATE INDEX IF NOT EXISTS idx_scrape_metadata_date_range ON scrape_metadata(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_scrape_metadata_source_urls ON scrape_metadata USING GIN (source_urls);

-- Schema creation complete
SELECT 'NPP Generation schema created successfully!' AS status;
