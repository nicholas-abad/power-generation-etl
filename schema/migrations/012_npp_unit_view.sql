-- Migration 012: mv_npp_unit_monthly — India's per-unit generation, surfaced
--
-- The DGR feed is per-unit for 236 plants (1,204 plant-units); we aggregate to
-- plant grain for the map/KPIs (mv_npp_plant_monthly) and, until now, threw
-- the unit detail away at display time. This view feeds the plant-detail unit
-- table, mirroring how the US pages use mv_eia_unit_monthly. Plant identity
-- (crosswalk/map) stays plant-level on purpose — units are content on the
-- plant page, not new identities.
--
-- unit = '' means the feed row carried no unit (single-unit / whole-plant
-- reporters, 339 plants) — the dashboard excludes those from the unit table.
--
-- Canonical definition lives in schema/materialized_views.sql; the surface
-- check now expects 23 public relations. Use Neon's DIRECT endpoint.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/012_npp_unit_view.sql

\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_npp_unit_monthly AS
SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
    plant,
    COALESCE(unit, '') AS unit,
    SUM(generation_mwh) AS generation_mwh
FROM ingestion.npp_generation
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_npp_unit_monthly
ON mv_npp_unit_monthly (month, plant, unit);

GRANT SELECT ON mv_npp_unit_monthly TO dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('012', 'mv_npp_unit_monthly: India per-unit generation for the plant-detail unit table (surface 22 -> 23)');

COMMIT;
DO $$ BEGIN RAISE NOTICE '012 applied: mv_npp_unit_monthly live'; END $$;
