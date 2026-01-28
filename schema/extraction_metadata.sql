-- Extraction Metadata - PostgreSQL Schema
-- Tracks extraction runs across all data sources
--
-- Usage:
--   psql power_generation -c "\i schema/extraction_metadata.sql"

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- EXTRACTION METADATA TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS extraction_metadata (
    id SERIAL PRIMARY KEY,

    -- Extraction identification
    extraction_run_id UUID NOT NULL UNIQUE,
    source VARCHAR(20) NOT NULL,  -- 'npp', 'eia', 'entsoe'

    -- Extraction timing
    extraction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    extraction_duration_seconds INTEGER,

    -- Data range
    start_date DATE,
    end_date DATE,

    -- Results
    total_records INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT TRUE,

    -- Detailed information (JSONB for flexibility)
    failed_details JSONB,  -- Array of {date, error} objects
    config_snapshot JSONB,  -- Extractor configuration used
    source_urls JSONB,  -- URLs accessed during extraction

    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Find extractions by source
CREATE INDEX IF NOT EXISTS idx_extraction_metadata_source
ON extraction_metadata(source);

-- Find extractions by date range
CREATE INDEX IF NOT EXISTS idx_extraction_metadata_dates
ON extraction_metadata(start_date, end_date);

-- Find recent extractions
CREATE INDEX IF NOT EXISTS idx_extraction_metadata_timestamp
ON extraction_metadata(extraction_timestamp DESC);

-- Find failed extractions
CREATE INDEX IF NOT EXISTS idx_extraction_metadata_success
ON extraction_metadata(success) WHERE success = FALSE;

-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- Summary of extractions by source
CREATE OR REPLACE VIEW extraction_summary AS
SELECT
    source,
    COUNT(*) as total_runs,
    SUM(total_records) as total_records,
    SUM(failed_count) as total_failed,
    COUNT(*) FILTER (WHERE success = TRUE) as successful_runs,
    COUNT(*) FILTER (WHERE success = FALSE) as failed_runs,
    MIN(start_date) as earliest_data,
    MAX(end_date) as latest_data,
    MAX(extraction_timestamp) as last_extraction
FROM extraction_metadata
GROUP BY source
ORDER BY source;

-- Recent extractions
CREATE OR REPLACE VIEW recent_extractions AS
SELECT
    extraction_run_id,
    source,
    extraction_timestamp,
    start_date,
    end_date,
    total_records,
    failed_count,
    success
FROM extraction_metadata
ORDER BY extraction_timestamp DESC
LIMIT 20;

-- Schema creation complete
SELECT 'Extraction Metadata schema created successfully!' AS status;
