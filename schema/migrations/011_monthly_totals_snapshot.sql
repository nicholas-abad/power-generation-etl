-- Migration 011: per-source monthly-totals snapshot for the drift tripwire
--
-- The 2026-09 deep dive found two data-loss classes (Australia's silent
-- halving; EIA's 133 TWh window gap) that a simple week-over-week totals
-- comparison would have caught immediately, from either direction. The new
-- check-totals-drift job compares each source's per-month totals against
-- last week's snapshot and fails on mature-month shifts (see
-- src/check_totals_drift.py), then re-snapshots — so a genuine anomaly fires
-- exactly one loud red run, and a verified-legitimate revision self-blesses
-- the following week.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/011_monthly_totals_snapshot.sql

\set ON_ERROR_STOP on
BEGIN;

CREATE TABLE IF NOT EXISTS ingestion.monthly_totals_snapshot (
    source      text NOT NULL,
    month       date NOT NULL,
    total_mwh   double precision NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, month)
);
COMMENT ON TABLE ingestion.monthly_totals_snapshot IS
    'Last-seen per-source monthly generation totals; the weekly drift check compares against these and re-captures. Not a dashboard surface.';

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('011', 'monthly_totals_snapshot table for the week-over-week drift tripwire (2026-09 deep dive)');

COMMIT;
DO $$ BEGIN RAISE NOTICE '011 applied'; END $$;
