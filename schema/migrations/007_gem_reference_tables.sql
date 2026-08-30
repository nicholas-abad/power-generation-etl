-- Migration 007: grant the GEM reference tables to the dashboard's read-only role
--
-- plant-data's scripts/fetch_gem.py mirrors Global Energy Monitor's power
-- trackers from GEM's Ownership API into four tables:
--   gem_locations              ~14.3K power stations (coal, gas & oil, bioenergy)
--   gem_units                  ~34K units with status, capacity, years, coal type
--   gem_unit_status_snapshots  half-yearly unit status since H2 2014 (coal)
--   gem_external_ids           other systems' IDs — EMPTY until GEM's API exposes them
-- They are created and re-swapped by plant-data (grants preserved across the
-- swap since plant-data #11), so this migration only GRANTs and registers them
-- in the surface check. The dashboard does not read them yet (that is PR D);
-- granting now lets it start when ready without another migration.
--
-- Deploy order: after plant-data's fetch_gem.py has run once (the tables must
-- exist). Merge and apply in the same sitting: the weekly surface check lists
-- the four tables from this commit on.
--
-- Rollback: REVOKE SELECT ON gem_locations, gem_units, gem_unit_status_snapshots,
-- gem_external_ids FROM dashboard_ro; remove the four names from
-- schema/checks/dashboard_ro_surface.sql.
--
-- Use Neon's DIRECT endpoint (host without "-pooler") — see 004.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/007_gem_reference_tables.sql

\set ON_ERROR_STOP on
BEGIN;

DO $$ BEGIN
  IF current_setting('transaction_read_only') = 'on' THEN
    RAISE EXCEPTION 'this session is read-only — connect to the direct (non-pooler) endpoint and re-run';
  END IF;
  IF to_regclass('public.gem_locations') IS NULL OR to_regclass('public.gem_units') IS NULL
     OR to_regclass('public.gem_unit_status_snapshots') IS NULL OR to_regclass('public.gem_external_ids') IS NULL THEN
    RAISE EXCEPTION 'gem_* tables missing — run plant-data/scripts/fetch_gem.py --force-detail first';
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    GRANT SELECT ON gem_locations, gem_units, gem_unit_status_snapshots, gem_external_ids TO dashboard_ro;
  ELSE
    RAISE NOTICE 'dashboard_ro does not exist — nothing granted';
  END IF;
END $$;

-- Post-conditions: the mirror is populated and internally consistent.
DO $$
DECLARE n_loc BIGINT; n_unit BIGINT; n_orphan BIGINT; n_rel INT;
BEGIN
  SELECT count(*) INTO n_loc FROM gem_locations;
  SELECT count(*) INTO n_unit FROM gem_units;
  SELECT count(*) INTO n_orphan FROM gem_units u WHERE NOT EXISTS (SELECT 1 FROM gem_locations l WHERE l.gem_location_id = u.gem_location_id);
  SELECT count(DISTINCT gem_release) INTO n_rel FROM gem_units WHERE tracker = 'GCPT';
  IF n_loc < 14000 OR n_unit < 30000 THEN
    RAISE EXCEPTION 'gem mirror looks incomplete: % locations, % units', n_loc, n_unit;
  END IF;
  IF n_orphan > 0 THEN RAISE EXCEPTION '% gem_units reference no gem_location', n_orphan; END IF;
  IF n_rel <> 1 THEN RAISE EXCEPTION 'gem_units GCPT carries % releases (expected 1)', n_rel; END IF;
  RAISE NOTICE 'gem mirror OK: % locations, % units, one coal release', n_loc, n_unit;
END $$;

\ir ../checks/dashboard_ro_surface.sql

COMMIT;
