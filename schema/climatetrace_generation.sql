-- Climate TRACE Power Generation Data (global, MODELED)
-- Monthly per-plant generation + emissions estimates from the Climate TRACE
-- bulk download packages (satellite + ML estimates, NOT metered readings —
-- unlike every other source table). By default only plant-months whose
-- generation estimate Climate TRACE rates 'high'+ are extracted (~2.5K
-- plants globally, the reported-data-backed subset).
-- One table for ALL countries (country_code column), mirroring how
-- entsoe_generation holds ~30 countries — do not split per country.
-- Data source: https://downloads.climatetrace.org

CREATE TABLE IF NOT EXISTS climatetrace_generation_data (
    id BIGSERIAL PRIMARY KEY,

    -- Extraction metadata
    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    -- Plant identification (climatetrace_id is Climate TRACE's stable
    -- source_id; names can collide across countries)
    climatetrace_id VARCHAR(100) NOT NULL,
    plant_name TEXT NOT NULL,
    country_code VARCHAR(3) NOT NULL,        -- ISO-3166 alpha-3, e.g. 'CHN'
    fuel_type VARCHAR(100),                  -- source_type; mixed like 'gas, coal'

    -- Location (Climate TRACE ships coordinates — unlike other sources,
    -- no plant_crosswalk join is needed for mapping)
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    -- Time series data (calendar-month grain; timestamp = month start UTC)
    timestamp_ms BIGINT NOT NULL,
    generation_mwh DOUBLE PRECISION NOT NULL,
    capacity_mw DOUBLE PRECISION,
    capacity_factor DOUBLE PRECISION,        -- 0..1 ratio
    activity_confidence VARCHAR(20),         -- Climate TRACE's rating of generation_mwh

    -- Emissions (denominated in `gas`, plant-specific factor)
    emissions_tonnes DOUBLE PRECISION,
    emissions_factor DOUBLE PRECISION,       -- t of <gas> per MWh
    gas VARCHAR(30),                         -- e.g. 'co2e_100yr'

    -- Provenance: the package release each value came from. Climate TRACE
    -- REVISES history between releases; the loader upserts (ON CONFLICT DO
    -- UPDATE) so revisions propagate, and ct_version records which release
    -- a row currently reflects.
    ct_version VARCHAR(20),

    -- Data quality constraints
    CONSTRAINT valid_timestamps_climatetrace CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT non_negative_generation_climatetrace CHECK (generation_mwh >= 0)
);

-- Natural key uniqueness — also the upsert conflict target.
CREATE UNIQUE INDEX IF NOT EXISTS uq_climatetrace_natural_key
ON climatetrace_generation_data (climatetrace_id, timestamp_ms);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_climatetrace_country_time ON climatetrace_generation_data (country_code, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_climatetrace_fuel_time ON climatetrace_generation_data (fuel_type, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_climatetrace_timestamp ON climatetrace_generation_data (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_climatetrace_extraction_run ON climatetrace_generation_data (extraction_run_id);
