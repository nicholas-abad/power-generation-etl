-- Migration 008: prune cold ingestion indexes, statistics hygiene, migrations ledger
--
-- Follows the 2026-09-01 independent schema audit. Since the dashboard moved to
-- the public materialized views (005/006), the query patterns these indexes
-- served no longer exist: the only readers of ingestion.* are the extractors
-- (natural-key upserts + MAX(timestamp_ms) checkpoints) and the weekly MV
-- refresh (sequential scans). Every index dropped here was re-verified against
-- pg_stat_user_indexes on 2026-09-01: lifetime idx_scan < 200 (most 0-14) OR a
-- pure prefix of the table's uq_*_natural_key. Kept everywhere: primary keys,
-- uq_*_natural_key (the upsert spine), idx_*_extraction_run (lineage triage).
-- Reclaims ~7.5 GB of the ~15 GB index weight on the big tables and removes
-- the matching write amplification from every weekly insert.
--
-- Also:
--   * ingestion.schema_migrations — the applied-migrations ledger this
--     database never had. Every future migration ends by inserting its row.
--   * per-table autovacuum analyze tuning + ANALYZE for the >10M-row tables
--     (ONS planner stats dated from June).
--   * COMMENT ON for objects whose purpose only lived in people's heads.
--   * plant_crosswalk.source_system SET NOT NULL (0 violating rows; the
--     plant-data bootstrap guards re-assert this after each weekly swap).
--
-- Use Neon's DIRECT endpoint (host without "-pooler") — see 004.
-- Rollback: recreate any index from its original DDL in schema/*.sql history;
-- the ledger and comments are inert.
--
-- Usage:
--   psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/migrations/008_prune_cold_indexes.sql

\set ON_ERROR_STOP on
BEGIN;

-- ── applied-migrations ledger ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ingestion.schema_migrations (
    version    text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now(),
    notes      text
);
COMMENT ON TABLE ingestion.schema_migrations IS
    'One row per applied schema/migrations/*.sql. Backfilled 2026-09-01; every migration from 008 on inserts its own row.';

INSERT INTO ingestion.schema_migrations (version, applied_at, notes) VALUES
    ('001a', NULL, 'dedup + natural keys, small tables (applied pre-ledger; date unrecorded)'),
    ('001b', NULL, 'dedup + natural key, ONS (applied pre-ledger; date unrecorded)'),
    ('002',  NULL, 'npp fuel_type (applied pre-ledger; date unrecorded)'),
    ('002b', NULL, 'npp fuel_type backfill (applied pre-ledger; date unrecorded)'),
    ('003',  NULL, 'entsoe mojibake merge (applied pre-ledger; date unrecorded)'),
    ('004',  '2026-08-30', 'dashboard_ro read-only role'),
    ('005',  '2026-08-30', 'mv_eia / mv_oe / mv_climatetrace materialized views'),
    ('006',  '2026-08-31', 'raw tables moved to ingestion schema'),
    ('007',  '2026-08-30', 'GEM reference tables + grants')
ON CONFLICT (version) DO NOTHING;

-- ── ENTSO-E (62.1M rows): 5 indexes, ~3.7 GB ────────────────────────────────
DROP INDEX ingestion.idx_generation_time_country_psr;   -- 955 MB, prefix of uq_entsoe_natural_key
DROP INDEX ingestion.idx_generation_country_time;       -- 802 MB, 1 lifetime scan
DROP INDEX ingestion.idx_generation_fuel_time;          -- 704 MB, 3
DROP INDEX ingestion.idx_generation_psr_time;           -- 670 MB, 14
DROP INDEX ingestion.idx_generation_country_psr_plant;  -- 538 MB, 6

-- ── ONS (11.7M rows): 6 indexes, ~1.5 GB ────────────────────────────────────
DROP INDEX ingestion.idx_ons_gen_plant_time;            -- 861 MB, 2
DROP INDEX ingestion.idx_ons_gen_state_time;            -- 227 MB, 0
DROP INDEX ingestion.idx_ons_gen_fuel_time;             -- 167 MB, 0
DROP INDEX ingestion.idx_ons_gen_subsystem_time;        -- 126 MB, 0
DROP INDEX ingestion.idx_ons_gen_timestamp;             --  79 MB, dup of natural-key lead column
DROP INDEX ingestion.idx_ons_gen_plant_id;              --  76 MB, 0

-- ── OCCTO (12.6M rows): 4 indexes, ~1.1 GB ──────────────────────────────────
DROP INDEX ingestion.idx_occto_gen_plant_time;          -- 806 MB, 58
DROP INDEX ingestion.idx_occto_gen_fuel_time;           -- 112 MB, 79
DROP INDEX ingestion.idx_occto_gen_area_time;           -- 105 MB, 17
DROP INDEX ingestion.idx_occto_gen_plant_code;          --  82 MB, 2

-- ── NPP (2.6M rows): 3 indexes, ~370 MB ─────────────────────────────────────
DROP INDEX ingestion.idx_npp_generation_plant_time;     -- 185 MB, 47
DROP INDEX ingestion.idx_npp_generation_time_plant;     -- 140 MB, prefix of natural key
DROP INDEX ingestion.idx_npp_generation_timestamp;      --  44 MB, dup of natural-key lead column

-- ── Chile: 5 indexes, ~113 MB ───────────────────────────────────────────────
DROP INDEX ingestion.idx_chile_gen_plant_time;          --  52 MB, 2
DROP INDEX ingestion.idx_chile_gen_region_time;         --  23 MB, 0
DROP INDEX ingestion.idx_chile_gen_fuel_time;           --  15 MB, 9
DROP INDEX ingestion.idx_chile_gen_timestamp;           --  14 MB, dup of natural-key lead column
DROP INDEX ingestion.idx_chile_gen_plant_id;            --   9 MB, 2

-- ── OpenElectricity (facility + legacy network table): 6 indexes ────────────
DROP INDEX ingestion.idx_oe_fac_gen_coords;             -- 0 scans
DROP INDEX ingestion.idx_oe_fac_gen_region_time;        -- 1
DROP INDEX ingestion.idx_oe_gen_fueltech_time;          -- 0
DROP INDEX ingestion.idx_oe_gen_fueltech_group_time;    -- 0
DROP INDEX ingestion.idx_oe_gen_region_time;            -- 0
DROP INDEX ingestion.idx_oe_gen_timestamp;              -- dup of natural-key lead column

-- ── EIA: 1 index ─────────────────────────────────────────────────────────────
DROP INDEX ingestion.idx_eia_generation_utility_time;   -- 0 scans

-- ── autovacuum: analyze after each weekly batch on the big tables ───────────
-- Default scale factor (0.10) lets a 62M-row table go months between analyzes;
-- 0.02 re-analyzes after ~1.2M new rows — roughly one weekly ENTSO-E batch.
ALTER TABLE ingestion.entsoe_generation_data SET (autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE ingestion.occto_generation_data  SET (autovacuum_analyze_scale_factor = 0.02);
ALTER TABLE ingestion.ons_generation_data    SET (autovacuum_analyze_scale_factor = 0.02);

-- ── one-off hardening + self-documentation ──────────────────────────────────
ALTER TABLE public.plant_crosswalk ALTER COLUMN source_system SET NOT NULL;

COMMENT ON TABLE public.gem_external_ids IS
    'Placeholder for GEM''s "Other IDs" columns (EIA plant codes etc.) — intentionally empty until the GEM Ownership API exposes them. See plant-data fetch_gem.py.';
COMMENT ON TABLE public.npp_llm_test IS
    'Internal: LLM name-match candidates for NPP (India) plants, loaded by plant-data bootstrap. Not part of the dashboard surface (no dashboard_ro grant).';
COMMENT ON COLUMN ingestion.entsoe_generation_data.data_type IS
    'Legacy overloaded column (document types and fuel names mixed; 44M "Unknown"). Not a generation/consumption flag — consumption rows were purged in the 2026-06 audit and are filtered at the extractor. No consumer reads this.';

INSERT INTO ingestion.schema_migrations (version, notes) VALUES
    ('008', 'pruned 30 cold/redundant ingestion indexes (~7.5 GB), autovacuum analyze tuning, migrations ledger, crosswalk source_system NOT NULL, comments');

COMMIT;

-- ANALYZE cannot batch with DDL in one snapshot-sensitive transaction cleanly;
-- run after commit (ONS stats dated June 2026, others get a fresh baseline).
ANALYZE ingestion.ons_generation_data;
ANALYZE ingestion.entsoe_generation_data;
ANALYZE ingestion.occto_generation_data;

DO $$ BEGIN RAISE NOTICE '008 applied: indexes pruned, ledger created, stats refreshed'; END $$;
