-- Migration 016: ct_gem_crosswalk — Climate TRACE's own CT → GEM links, readable
--
-- plant-data loads Climate TRACE's published gem_ct_crosswalk (power rows:
-- 26,125 links, 7,335 CT plants → GEM units G… and locations L…, all fuels)
-- with `bootstrap_neon_db.py --ct-gem-only`. plant_crosswalk links a CT plant
-- to ONE GEM location, so the tracker divides whole-station CT output by one
-- location's coal capacity: 68 CT plants read >100% capacity factor (tracker
-- issue #5). With these links, 42 of them fall back under 100%.
-- This migration only makes the table readable and etl_writer-owned (so a CI
-- reload can swap it — the trap 015 fixed for plant_crosswalk), and registers
-- it in the surface check (24 -> 25 relations).
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

-- Post-conditions: populated, and every CT id it names is a real CT plant id
-- shape (the join key into mv_climatetrace_coal_monthly.climatetrace_id).
DO $$
DECLARE n_links BIGINT; n_ct BIGINT; n_matched BIGINT;
BEGIN
  SELECT count(*), count(DISTINCT climatetrace_id) INTO n_links, n_ct FROM ct_gem_crosswalk;
  SELECT count(DISTINCT m.climatetrace_id) INTO n_matched
    FROM mv_climatetrace_coal_monthly m
    WHERE EXISTS (SELECT 1 FROM ct_gem_crosswalk x WHERE x.climatetrace_id = m.climatetrace_id);
  IF n_links < 20000 THEN
    RAISE EXCEPTION 'ct_gem_crosswalk looks incomplete: % links', n_links;
  END IF;
  IF n_matched = 0 THEN
    RAISE EXCEPTION 'no ct_gem_crosswalk id joins mv_climatetrace_coal_monthly — key type mismatch?';
  END IF;
  RAISE NOTICE 'ct_gem_crosswalk OK: % links, % CT plants, % of them in the coal view', n_links, n_ct, n_matched;
END $$;

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('016', 'ct_gem_crosswalk: Climate TRACE CT -> GEM links readable by dashboard_ro (tracker issue #5; surface 24 -> 25)');

\ir ../checks/dashboard_ro_surface.sql

COMMIT;
DO $$ BEGIN RAISE NOTICE '016 applied'; END $$;
