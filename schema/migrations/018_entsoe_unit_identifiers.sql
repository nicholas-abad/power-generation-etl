-- Add source generation-unit and production-unit EICs without rewriting
-- legacy observations or changing the existing dashboard view/grants.
--
-- The legacy name/PSR key remains for compatibility with the deployed loader.
-- A second unique index prevents an identified unit/time being stored twice.
-- The identified extractor/preflight rejects multiple EICs sharing a legacy
-- key; a country with such collisions needs an explicit key migration first.
-- Apply before loading identified-unit files. Existing name-only loads remain
-- compatible and must not overwrite known EICs. Stage before production.
--
-- Rollback: remove the new view from the refresher, drop that view and the
-- new index/constraints/columns, and remove ledger 018. Observation values
-- and the legacy name key are unchanged by this migration itself.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL TIME ZONE 'UTC';

ALTER TABLE ingestion.entsoe_generation_data
    ADD COLUMN IF NOT EXISTS unit_eic VARCHAR(16),
    ADD COLUMN IF NOT EXISTS production_unit_eic VARCHAR(16),
    ADD COLUMN IF NOT EXISTS source_unit_name TEXT;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'ingestion.entsoe_generation_data'::regclass AND conname = 'valid_entsoe_unit_eic') THEN
        ALTER TABLE ingestion.entsoe_generation_data ADD CONSTRAINT valid_entsoe_unit_eic
            CHECK (unit_eic IS NULL OR unit_eic ~ '^[A-Z0-9-]{16}$');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'ingestion.entsoe_generation_data'::regclass AND conname = 'valid_entsoe_production_eic') THEN
        ALTER TABLE ingestion.entsoe_generation_data ADD CONSTRAINT valid_entsoe_production_eic
            CHECK (production_unit_eic IS NULL OR production_unit_eic ~ '^[A-Z0-9-]{16}$');
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_entsoe_unit_time
ON ingestion.entsoe_generation_data (country_code, unit_eic, timestamp_ms)
WHERE unit_eic IS NOT NULL;

\ir ../entsoe_unit_monthly.sql
ALTER MATERIALIZED VIEW public.mv_entsoe_unit_monthly OWNER TO etl_writer;
REVOKE ALL ON public.mv_entsoe_unit_monthly FROM PUBLIC, dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('018', 'Retain ENTSO-E unit and production EICs; qualified all-fuel unit monthly view; preserve legacy coal dashboard contract')
ON CONFLICT (version) DO NOTHING;
COMMIT;
