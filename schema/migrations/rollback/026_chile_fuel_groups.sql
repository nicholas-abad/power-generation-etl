-- Rollback of migration 026: drops Chile's fuel-group view and the fuel-group
-- table. The all-fuel table (019) and every coal view are not touched. Tell
-- the tracker first: it reads the view. With the ledger row gone, refreshes
-- skip the view again (refresh_views.MIGRATION_VIEWS), and the dashboard_ro
-- surface check stops expecting it.
--
-- Usage: psql "$DIRECT_DATABASE_URL" -v ON_ERROR_STOP=1 \
--          -f schema/migrations/rollback/026_chile_fuel_groups.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP MATERIALIZED VIEW IF EXISTS public.mv_chile_fuel_group_monthly;
DROP TABLE IF EXISTS ingestion.fuel_groups;
DELETE FROM ingestion.schema_migrations WHERE version = '026';

COMMIT;
