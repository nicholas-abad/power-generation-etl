-- EIA Form 860 generator-level reference data (Technology, Prime Mover, etc.)
-- Loaded from 3_1_Generator_Y2024.xlsx via plant-data's bootstrap_neon_db.py:
-- the "Operable" sheet plus the RETIRED generators of "Retired and Canceled"
-- (status 'RE'), so coal history from units retired before the file's year
-- still joins as coal. Sum nameplate for current capacity with
-- status IS DISTINCT FROM 'RE'.

CREATE TABLE IF NOT EXISTS eia_generator_info (
    plant_code      VARCHAR(20)  NOT NULL,
    generator_id    VARCHAR(20)  NOT NULL,
    technology      VARCHAR(200) NOT NULL,
    prime_mover     VARCHAR(50),
    energy_source_1 VARCHAR(50),
    nameplate_capacity_mw DOUBLE PRECISION,
    status          VARCHAR(10),  -- Form 860 status code: OP, SB, OS, OA, ..., RE (retired)
    PRIMARY KEY (plant_code, generator_id)
);

CREATE INDEX IF NOT EXISTS idx_eia_gen_info_technology
    ON eia_generator_info (technology);
