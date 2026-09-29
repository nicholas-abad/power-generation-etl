-- Migration 014: mv_occto_unit_monthly — Japan's per-unit generation, surfaced
--
-- Twin of 012/013 (India): OCCTO's disclosure is 100% per-unit (13.4M rows,
-- 214 plants -> 335 plant-units, 55 multi-unit — Hekinan alone has 5 coal
-- units); we aggregated to plant grain at display time. Closes the Japan half
-- of tracker issue #7. fuel_type single-valued per plant-unit (verified).
-- Surface check now expects 24 relations. Direct endpoint.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/014_occto_unit_view.sql

\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_occto_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    COALESCE(unit, '') AS unit,
    MAX(fuel_type) AS fuel_type,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.occto_generation_data
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_occto_unit_monthly
ON mv_occto_unit_monthly (month, plant, unit);

GRANT SELECT ON mv_occto_unit_monthly TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('014', 'mv_occto_unit_monthly: Japan per-unit generation (issue #7 Japan half; surface 23 -> 24)');

COMMIT;
DO $$ BEGIN RAISE NOTICE '014 applied'; END $$;
