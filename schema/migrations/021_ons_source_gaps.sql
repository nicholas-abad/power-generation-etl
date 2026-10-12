-- Migration 021: record ONS source gaps instead of loading short months silently.
--
-- ONS publishes some individual plants with null generation (since May 2026 a
-- set of mostly biomass plants; on 22 July 2026 seven large hydro plants). The
-- extractor excludes those rows as missing_generation; this table records each
-- contiguous gap with its approval so plant-months can be shown as incomplete.
-- Measured observations are untouched and nothing is imputed.
--
-- Additive: one table in ingestion and one plain view in public. Like 017's
-- view, both stay ETL-only (no dashboard_ro grant); exposing them belongs with
-- the all-fuel dashboard change. Requires 017. (020 is the ONS plant-hour key
-- from the rename-duplicate fix, PR #117; 019 is Chile.)
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/021_ons_source_gaps.sql
--
-- Rollback: DROP VIEW public.ons_individual_plant_gap_monthly; DROP TABLE
-- ingestion.ons_source_gaps; delete ledger version '021'. Generation data and
-- every materialized view are unaffected.

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

\ir ../ons_source_gaps.sql

ALTER TABLE ingestion.ons_source_gaps OWNER TO etl_writer;
ALTER VIEW public.ons_individual_plant_gap_monthly OWNER TO etl_writer;
REVOKE ALL ON ingestion.ons_source_gaps FROM PUBLIC, dashboard_ro;
REVOKE ALL ON public.ons_individual_plant_gap_monthly FROM PUBLIC, dashboard_ro;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('021', 'ONS source gaps table and monthly gap-hours view; ETL-only; nothing imputed')
ON CONFLICT (version) DO NOTHING;

COMMIT;
