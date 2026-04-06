-- GCPT (Global Coal Plant Tracker) coal metadata for CO2 emission estimation.
-- Loaded from GCPT CSV via bootstrap_neon_db.py

CREATE TABLE IF NOT EXISTS gcpt_coal_metadata (
    gcpt_unit_id    VARCHAR(50)  NOT NULL,
    eia_unit_id     VARCHAR(50),
    plant_name      VARCHAR(200),
    unit_name       VARCHAR(200),
    coal_type       VARCHAR(50),
    technology      VARCHAR(50),
    capacity_mw     DOUBLE PRECISION,
    country         VARCHAR(100),
    PRIMARY KEY (gcpt_unit_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_gcpt_coal_eia_unit_unique
    ON gcpt_coal_metadata (eia_unit_id)
    WHERE eia_unit_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gcpt_coal_country
    ON gcpt_coal_metadata (country);
