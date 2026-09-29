-- Migration 016: ct_gem_crosswalk — Climate TRACE's own CT → GEM links, readable
--
-- plant-data loads Climate TRACE's published gem_ct_crosswalk (power rows:
-- 26,125 links, 7,335 CT plants → GEM units G… and locations L…, all fuels)
-- with `bootstrap_neon_db.py --ct-gem-only`. plant_crosswalk links a CT plant
-- to ONE GEM location, so the tracker divides whole-station CT output by one
-- location's coal capacity: 68 CT plants read >100% capacity factor (tracker
-- issue #5). Counting every unit at every GEM location a CT plant touches
-- (plant-data's CT_PLANT_UNITS_SQL) leaves 26, none newly over 100%.
-- Load-time join: 91% of the coal view's CT plants have a link (floor: 80%).
-- This migration only makes the table readable and etl_writer-owned (so either
-- role can reload it; no workflow does yet — the table is reloaded by hand when
-- Climate TRACE publishes a new workbook), and registers it in the surface
-- check (24 -> 25 relations).
--
-- Deploy order: after plant-data's --ct-gem-only load (the table must exist).
-- Merge and apply in the same sitting: the weekly surface check expects the
-- table from this commit on.
--
-- Rollback: REVOKE SELECT ON ct_gem_crosswalk FROM dashboard_ro; remove the
-- name from schema/checks/dashboard_ro_surface.sql.
--
-- Usage (direct endpoint):
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/016_ct_gem_crosswalk.sql

\set ON_ERROR_STOP on
BEGIN;

DO $$ BEGIN
  IF to_regclass('public.ct_gem_crosswalk') IS NULL THEN
    RAISE EXCEPTION 'ct_gem_crosswalk missing — run plant-data scripts/bootstrap_neon_db.py --ct-gem-only first';
  END IF;
END $$;

ALTER TABLE public.ct_gem_crosswalk OWNER TO etl_writer;
GRANT SELECT ON public.ct_gem_crosswalk TO dashboard_ro;

-- Post-conditions: the full sheet is there (26,125 links / 7,335 CT plants
-- at load), and it joins the coal view on climatetrace_id for most of the
-- coal view's plants (id shape itself is enforced by the loader). Floors sit
-- a few percent under the load-time counts, so a truncated or wrong-key load
-- fails here instead of silently shrinking capacity for the tracker.
DO $$
DECLARE n_links BIGINT; n_ct BIGINT; n_coal BIGINT; n_matched BIGINT;
BEGIN
  SELECT count(*), count(DISTINCT climatetrace_id) INTO n_links, n_ct FROM ct_gem_crosswalk;
  SELECT count(DISTINCT climatetrace_id) INTO n_coal FROM mv_climatetrace_coal_monthly;
  SELECT count(DISTINCT m.climatetrace_id) INTO n_matched
    FROM mv_climatetrace_coal_monthly m
    WHERE EXISTS (SELECT 1 FROM ct_gem_crosswalk x WHERE x.climatetrace_id = m.climatetrace_id);
  IF n_links < 25000 OR n_ct < 7000 THEN
    RAISE EXCEPTION 'ct_gem_crosswalk looks incomplete: % links, % CT plants (expected ~26,125 / ~7,335)', n_links, n_ct;
  END IF;
  IF n_matched < 0.8 * n_coal THEN
    RAISE EXCEPTION 'only % of % coal-view CT plants have a ct_gem_crosswalk link — key mismatch or wrong sheet?', n_matched, n_coal;
  END IF;
  RAISE NOTICE 'ct_gem_crosswalk OK: % links, % CT plants; % of % coal-view plants linked', n_links, n_ct, n_matched, n_coal;
END $$;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('016', 'ct_gem_crosswalk: Climate TRACE CT -> GEM links readable by dashboard_ro (tracker issue #5; surface 24 -> 25)');

\ir ../checks/dashboard_ro_surface.sql

COMMIT;
DO $$ BEGIN RAISE NOTICE '016 applied'; END $$;
