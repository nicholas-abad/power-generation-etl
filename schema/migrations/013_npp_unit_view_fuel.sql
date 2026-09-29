-- Migration 013: mv_npp_unit_monthly gains fuel_type
--
-- The GEM tracker frontend (the primary dashboard) filters every NPP read on
-- fuel_type = 'THERMAL'; 012's unit view lacked the column, forcing a join.
-- fuel_type is single-valued per plant, so MAX() is exact. DROP + recreate
-- (adding a column to a matview requires it), grant re-applied.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/013_npp_unit_view_fuel.sql

\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mv_npp_unit_monthly;
CREATE MATERIALIZED VIEW mv_npp_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    COALESCE(unit, '') AS unit,
    MAX(fuel_type) AS fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.npp_generation
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX ux_mv_npp_unit_monthly
ON mv_npp_unit_monthly (month, plant, unit);

GRANT SELECT ON mv_npp_unit_monthly TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('013', 'mv_npp_unit_monthly + fuel_type (GEM tracker frontend filters NPP on THERMAL)');

COMMIT;
DO $$ BEGIN RAISE NOTICE '013 applied'; END $$;
