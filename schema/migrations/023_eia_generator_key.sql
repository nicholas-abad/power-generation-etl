-- Migration 023: one spelling per EIA generator ID (Form 923 and Form 860 agree)
--
-- Coal is identified by joining generation (Form 923, eia_generation_data /
-- mv_eia_unit_monthly) to the generator register (Form 860,
-- eia_generator_info) on (plant_code, generator_id). EIA spells some IDs
-- differently in the two forms, so the join drops their generation: Dry Fork
-- is "1" in 923 but "01" in 860; Rockport reports "MB1"/"MB2" in most months
-- since 2024-03 but "1"/"2" in 860. About 56 TWh of coal since 2019.
--
-- public.eia_generator_key(plant_code, generator_id) is the one rule both
-- sides use: capitals, no spaces, no leading zeros ("01A" → "1A"), letters
-- kept (they are what tells "ST2" coal from "GT2" oil at one plant). Two
-- written exceptions: Rockport's MB1/MB2 are its units 1/2, and plant 56032
-- keeps its IDs because 860 lists "0001" and "001" as two turbines.
--
-- This migration:
--   * creates the function;
--   * keeps EIA's own spelling in eia_generation_data.generator_id_reported;
--   * rewrites stored generator_id (and eia_plant_unit_id) to the key
--     (1,360 rows of 305,635 on staging 2026-10-13; none merge);
--   * adds a BEFORE INSERT trigger so every later load stores the key too.
-- Then refresh mv_eia_unit_monthly. eia_generator_info is keyed the same way
-- by plant-data's loader (bootstrap_neon_db.py), which needs this function.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/023_eia_generator_key.sql
--        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_eia_unit_monthly;
--
-- Rollback: schema/migrations/rollback/023_eia_generator_key.sql.

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE OR REPLACE FUNCTION public.eia_generator_key(plant_code TEXT, generator_id TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN generator_id IS NULL THEN NULL
        -- Rockport (IN): Form 923 alternates MB1/MB2 with 1/2 month by month.
        WHEN plant_code = '6166' AND upper(replace(generator_id, ' ', '')) IN ('MB1', 'MB2')
            THEN right(upper(replace(generator_id, ' ', '')), 1)
        -- Form 860 lists "0001" and "001" at this plant as two turbines.
        WHEN plant_code = '56032' THEN upper(replace(generator_id, ' ', ''))
        ELSE coalesce(nullif(ltrim(upper(replace(generator_id, ' ', '')), '0'), ''), '0')
    END
$$;
COMMENT ON FUNCTION public.eia_generator_key(TEXT, TEXT) IS
    'One spelling per EIA generator ID across Form 923 and Form 860: capitals, no spaces, no leading zeros; Rockport MB1/MB2 = 1/2; plant 56032 unchanged. Migration 023.';

ALTER TABLE ingestion.eia_generation_data
    ADD COLUMN IF NOT EXISTS generator_id_reported VARCHAR(20);
COMMENT ON COLUMN ingestion.eia_generation_data.generator_id_reported IS
    'Generator ID as EIA reported it; generator_id holds eia_generator_key() of it.';

CREATE OR REPLACE FUNCTION ingestion.eia_generation_key_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.generator_id_reported := coalesce(NEW.generator_id_reported, NEW.generator_id);
    NEW.generator_id := public.eia_generator_key(NEW.plant_code, NEW.generator_id_reported);
    NEW.eia_plant_unit_id := NEW.plant_code || '|' || NEW.generator_id;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS eia_generation_key ON ingestion.eia_generation_data;
CREATE TRIGGER eia_generation_key
    BEFORE INSERT ON ingestion.eia_generation_data
    FOR EACH ROW EXECUTE FUNCTION ingestion.eia_generation_key_row();

-- uq_eia_natural_key refuses (and rolls back) if two stored spellings of one
-- generator share a month. Re-running finds every row already keyed.
UPDATE ingestion.eia_generation_data
SET generator_id_reported = generator_id,
    generator_id = public.eia_generator_key(plant_code, generator_id),
    eia_plant_unit_id = plant_code || '|' || public.eia_generator_key(plant_code, generator_id)
WHERE generator_id_reported IS NULL;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('023', 'EIA generator IDs keyed by eia_generator_key() (Form 923 = Form 860 spelling); EIA spelling kept in generator_id_reported; insert trigger')
ON CONFLICT (version) DO NOTHING;

COMMIT;
