# `src/` — ETL modules

| Module | Role |
|---|---|
| `database_management.py` | **The CLI.** `setup`, `update-schema`, `load-data`, `stats`, `aggregate-export`, `clear-table`. Run it by path (`uv run src/database_management.py …`) — `python -m src.database_management` does **not** work, because the modules import each other flatly. |
| `database.py` | Connection handling, per-source loaders, and the staging-table upsert (`CREATE TEMP TABLE … COPY … INSERT … ON CONFLICT`). Every load goes through here. |
| `validator.py` | Per-source record schemas: required/optional fields, types, ranges, and the natural key used for in-file duplicate detection. Invalid records are skipped (or fail the load with `--strict`). |
| `get_latest_date.py` | Newest `timestamp_ms` in the DB for a source. The weekly cron uses this so each run extracts only what's new. |
| `incremental_extract.py` | Turns that latest date into the extractor's window, honouring any manual override: ENTSO-E in 3-day chunks under a time budget (new days, then a 7-day lookback), OCCTO by month. |
| `refresh_views.py` | Refreshes the materialized views (`mv_*_plant_monthly`, `mv_eia_unit_monthly`, `mv_climatetrace_coal_monthly`, `mv_*_row_counts`, `mv_chile_fuel_group_monthly`) the dashboard reads. Run after any load. Before the fuel-group view it rewrites `ingestion.fuel_groups` from `config/fuel-groups.json` and fails on a source fuel label the file does not know. |
| `check_crosswalk_drift.py` | Reports plants that generate upstream but are missing from `plant_crosswalk` — i.e. the crosswalk needs a rebuild in `data/plant-data`. |
| `backfill_extraction_dates.py` | One-off repair for extraction metadata rows. |

See the [repo README](../README.md) for the pipeline overview and [`docs/INFRASTRUCTURE.md`](../docs/INFRASTRUCTURE.md) for the architecture.
