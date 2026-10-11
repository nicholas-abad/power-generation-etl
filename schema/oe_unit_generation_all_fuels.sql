-- OpenElectricity all-fuel generation, unit grain (staging pilot, tracker issue #32).
-- Daily energy per OE unit (NEM DUID / WEM facility unit) × direction, every
-- fueltech, from `energy-extract openelectricity --all-fuels`.
--
-- Deliberately separate from ingestion.oe_facility_generation_data: that
-- table, mv_oe_plant_monthly and the tracker's coal filter stay untouched. Do
-- not add this table's totals to the facility table's: fossil units appear in
-- both.
--
-- direction: injection (generation), withdrawal (storage charging, pumping;
-- positive amounts), auxiliary (a generating unit's negative values, stored as
-- positive magnitudes), net (a battery's signed aggregate where no components
-- exist), anomaly (a negative value on a withdrawal fueltech, kept signed).
-- basis: 'derived' marks a battery component derived from OE's aggregate
-- (aggregate = discharging - charging).

CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.oe_unit_generation_all_fuels (
    id BIGSERIAL PRIMARY KEY,

    extraction_run_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,

    network_code VARCHAR(10) NOT NULL,
    network_region VARCHAR(20),
    facility_code VARCHAR(100),          -- NULL for series the facility master does not list
    facility_name TEXT,
    unit_code VARCHAR(100) NOT NULL,
    fueltech VARCHAR(50) NOT NULL,
    fueltech_group VARCHAR(30),
    unit_status VARCHAR(20),

    direction VARCHAR(12) NOT NULL,
    basis VARCHAR(10) NOT NULL DEFAULT 'reported',

    timestamp_ms BIGINT NOT NULL,        -- network-local midnight of the day
    energy_mwh DOUBLE PRECISION NOT NULL,
    capacity_registered_mw DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    resolution_minutes INTEGER DEFAULT 1440,

    CONSTRAINT valid_timestamps_oe_all_fuels CHECK (timestamp_ms > 0 AND created_at_ms > 0),
    CONSTRAINT valid_direction_oe_all_fuels CHECK (direction IN ('injection', 'withdrawal', 'auxiliary', 'net', 'anomaly')),
    CONSTRAINT valid_basis_oe_all_fuels CHECK (basis IN ('reported', 'derived')),
    CONSTRAINT non_negative_unless_signed_oe_all_fuels CHECK (energy_mwh >= 0 OR direction IN ('net', 'anomaly')),
    CONSTRAINT finite_energy_oe_all_fuels CHECK (energy_mwh NOT IN ('Infinity'::float8, '-Infinity'::float8, 'NaN'::float8))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_oe_all_fuels_natural_key
ON ingestion.oe_unit_generation_all_fuels (timestamp_ms, network_code, unit_code, direction);

CREATE INDEX IF NOT EXISTS idx_oe_all_fuels_extraction_run
ON ingestion.oe_unit_generation_all_fuels (extraction_run_id);

-- Monthly per unit × direction in NETWORK-LOCAL months (NEM AEST, WEM AWST).
-- Daily rows are stamped at local midnight, i.e. 14:00/16:00 UTC the day before,
-- so UTC month buckets would move each month's first day into the previous one.
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_oe_all_fuels_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)
        AT TIME ZONE CASE network_code WHEN 'WEM' THEN 'Australia/Perth' ELSE 'Australia/Brisbane' END)::date AS month,
    network_code,
    unit_code,
    direction,
    MAX(fueltech) AS fueltech,
    MAX(fueltech_group) AS fueltech_group,
    MAX(facility_code) AS facility_code,
    MAX(facility_name) AS facility_name,
    MAX(network_region) AS network_region,
    SUM(energy_mwh) AS energy_mwh,
    COUNT(*) AS days,
    COUNT(*) FILTER (WHERE basis = 'derived') AS derived_days
FROM ingestion.oe_unit_generation_all_fuels
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_oe_all_fuels_unit_monthly
ON public.mv_oe_all_fuels_unit_monthly (month, network_code, unit_code, direction);
