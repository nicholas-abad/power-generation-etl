-- Rollback of migration 018: drop the ENTSO-E unit identity (unit and production
-- EICs, source unit name), its unique index and mv_entsoe_unit_monthly.
--
-- Roll back every ENTSO-E coal rebuild first (rebuild_entsoe_coal.py rollback):
-- rebuilt rows are keyed by EIC, and the rebuild's backup table has these
-- columns, so its rollback needs them. This file refuses while a rebuilt month
-- is still applied. EICs that weekly loads stored since 018 are lost; the
-- observations and the legacy name key stay. Without the columns the loader
-- writes name-only rows again and refresh_views.py skips the unit view.
-- No CASCADE, so an unexpected dependant of the view or columns stops it.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/rollback/018_entsoe_unit_identifiers.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE applied bigint; identified bigint;
BEGIN
    IF to_regclass('ingestion.entsoe_coal_rebuild_months') IS NOT NULL THEN
        SELECT count(*) INTO applied FROM ingestion.entsoe_coal_rebuild_months
        WHERE state <> 'rolled_back';
        IF applied > 0 THEN
            RAISE EXCEPTION '% ENTSO-E rebuild windows are still applied; roll them back first', applied;
        END IF;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'ingestion.entsoe_generation_data'::regclass
               AND attname = 'unit_eic' AND NOT attisdropped) THEN
        EXECUTE 'SELECT count(*) FROM ingestion.entsoe_generation_data WHERE unit_eic IS NOT NULL'
            INTO identified;
        RAISE NOTICE 'dropping the unit EICs of % rows', identified;
    END IF;
END $$;

DROP MATERIALIZED VIEW IF EXISTS public.mv_entsoe_unit_monthly;
DROP INDEX IF EXISTS ingestion.uq_entsoe_unit_time;
ALTER TABLE ingestion.entsoe_generation_data
    DROP CONSTRAINT IF EXISTS valid_entsoe_unit_eic,
    DROP CONSTRAINT IF EXISTS valid_entsoe_production_eic,
    DROP COLUMN IF EXISTS unit_eic,
    DROP COLUMN IF EXISTS production_unit_eic,
    DROP COLUMN IF EXISTS source_unit_name;

DELETE FROM ingestion.schema_migrations WHERE version = '018';

COMMIT;
