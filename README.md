# Power Generation ETL with Airflow

Airflow ETL pipelines for ingesting, standardizing, and loading power generation data from multiple sources into a central PostgreSQL database.

---

## Overview

This repository contains the **orchestration layer** for power generation data pipelines.  
It uses **Apache Airflow** to periodically collect data from multiple upstream sources, normalize it into a canonical schema, and load it into a PostgreSQL database for downstream analytics and visualization.

The system is designed to be:

- **Idempotent** — safe to rerun and backfill
- **Source-independent** — each data source updates on its own cadence
- **Database-driven** — PostgreSQL is the source of truth
- **Dashboard-friendly** — consumed by a Streamlit application

---

## Data Sources

All 5 data sources are extracted via the unified [`energy-extractors`](https://github.com/nicholas-abad/energy-extractors) package:

- **EIA** — U.S. Energy Information Administration
- **NPP** — India National Power Portal generation data
- **ENTSOE** — European power generation data
- **ONS** — Brazil ONS (Operador Nacional do Sistema Elétrico) thermal generation data
- **OE** — Australia OpenElectricity (NEM) generation data

This repository **does not** contain scraping logic itself.
It is responsible for **scheduling, coordination, validation, and loading**.

---

## Architecture

High-level flow:
```
+----------------------------------+
|   energy-extractors              |
|  (EIA, ENTSOE, NPP, ONS, OE)    |
|  energy-extract <source> ...     |
+----------------------------------+
        |
        v
+----------------------------------+
|    ETL (This Repo)               |
|  Airflow / CLI                   |
+----------------------------------+
        |
        v
+----------------------------------+
| PostgreSQL (Neon / local)        |
+----------------------------------+
        |
        v
+----------------------------------+
|  Streamlit Dashboard             |
+----------------------------------+
```

## Repository Structure
```
power-generation-etl/
├── .github/workflows/    # CI/CD pipelines
│   └── ci.yml            # GitHub Actions workflow
├── dags/                 # Airflow DAG definitions
│   ├── minimal_etl.py
│   ├── eia_monthly.py
│   ├── npp_monthly.py
│   └── entsoe_monthly.py
├── schema/               # SQL schema definitions
│   ├── eia_generation.sql
│   ├── npp_generation.sql
│   ├── entsoe_generation.sql
│   ├── ons_generation.sql
│   ├── oe_generation.sql
│   ├── oe_facility_generation.sql
│   ├── materialized_views.sql
│   ├── extraction_metadata.sql
│   └── migrations/       # Schema migrations
│       └── 001_add_unique_constraints.sql
├── docs/                 # Documentation
│   └── DATA_UNITS.md     # MW vs MWh documentation
├── src/                  # Core Python modules
│   ├── database.py       # Database operations with validation
│   ├── database_management.py  # CLI tool
│   └── validator.py      # Data validation module
├── tests/                # Unit tests
│   └── test_validator.py # Validation tests
├── logs/                 # Log files (auto-generated)
├── Dockerfile            # Container image definition
├── .dockerignore         # Docker build exclusions
├── README.md
└── pyproject.toml
```
---

## How the Pipelines Work

Each Airflow DAG follows the same high-level pattern:

1. **Compute time window**
   - Derive the data window from Airflow's `logical_date`
2. **Extract**
   - Fetch raw data from the upstream source
3. **Transform**
   - Normalize into a canonical schema
4. **Validate**
   - Validate records against schema, type, and range rules
   - Detect and skip duplicates within each file
   - Generate validation reports
5. **Load**
   - UPSERT only valid records into PostgreSQL (idempotent)
6. **Record metadata**
   - Track successful runs, row counts, and validation statistics

Each source runs independently and can be retried or backfilled safely.

---

## Database

The pipelines load into **PostgreSQL** (currently Neon, with `sslmode=require`).

- **Source-specific tables**: `eia_generation_data`, `npp_generation_data`, `entsoe_generation_data`, `ons_generation_data`, `oe_generation_data`
- **Natural key UNIQUE constraints** on each table prevent duplicate rows across re-runs (e.g. `uq_entsoe_natural_key`, `uq_npp_natural_key`)
- **Staging table upsert**: data is loaded into a temp staging table, then `INSERT ... ON CONFLICT DO NOTHING` merges into the main table — safe for re-runs and partial overlaps
- **Materialized views**: `mv_entsoe_monthly`, `mv_entsoe_plant_monthly`, `mv_ons_monthly`, `mv_ons_plant_monthly`, `mv_npp_monthly`, `mv_npp_plant_monthly` pre-aggregate large tables for dashboard performance
- **Streaming ingestion**: JSONL files are read line-by-line and inserted in configurable batch sizes to keep memory usage low

Downstream consumers (e.g. Streamlit) **read only from the database**.

---

## Running Locally (Quick Start)

### Option 1: Database CLI (Recommended for Testing)

The database CLI provides a simple way to load and manage data without Airflow.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start PostgreSQL
docker run -d \
  --name power-gen-pg \
  -e POSTGRES_DB=power_generation \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15

# 3. Set up database tables
uv run src/database_management.py setup all

# 4. Load data from energy-extractors output (with automatic validation)
uv run src/database_management.py load-data entsoe ../../extractors/energy-extractors/output/*_etl.jsonl
uv run src/database_management.py load-data npp ../../extractors/energy-extractors/output/*_etl.jsonl
uv run src/database_management.py load-data eia ../../extractors/energy-extractors/output/*_etl.jsonl
uv run src/database_management.py load-data ons ../../extractors/energy-extractors/output/*_etl.jsonl
uv run src/database_management.py load-data oe ../../extractors/energy-extractors/output/*_etl.jsonl

# 5. Load data with validation report
uv run src/database_management.py load-data npp ./path/to/npp_data.jsonl \
  --validation-report validation_report.json

# 6. View statistics
uv run src/database_management.py stats
```

**See [DATABASE_CLI.md](./DATABASE_CLI.md) for complete CLI documentation.**

### Option 2: Airflow (For Production Scheduling)

#### 1. Install Airflow

```bash
pip install "apache-airflow==2.9.*"
```

#### 2. Initialize Airflow
```
export AIRFLOW_HOME=~/airflow
airflow db init
airflow standalone
```
Airflow UI will be available at:
http://localhost:8080

#### 3. Configure Database Connection
Create an Airflow connection:
	•	Conn ID: neon_postgres
	•	Conn Type: Postgres
	•	Host: localhost
	•	Login: postgres
	•	Password: postgres
	•	Database: power_generation
	•	Port: 5432

#### 4. Enable a DAG
	•	Place DAG files in $AIRFLOW_HOME/dags
	•	Enable them in the Airflow UI
	•	Observe scheduled runs and task execution

⸻

## Data Source Compatibility

This ETL system supports five data sources with **harmonized schemas** for consistent data loading:

| Data Source | Package Module | Format | Status |
|-------------|----------------|--------|--------|
| **ENTSOE** | `energy_extractors.entsoe` | JSONL | ✅ 100% Compatible |
| **India NPP** | `energy_extractors.npp` | JSONL | ✅ 100% Compatible |
| **EIA USA** | `energy_extractors.eia` | JSONL (`*_etl.jsonl`) | ✅ 100% Compatible |
| **Brazil ONS** | `energy_extractors.ons` | JSONL (`*_etl.jsonl`) | ✅ 100% Compatible |
| **Australia OE** | `energy_extractors.openelectricity` | JSONL (`*_etl.jsonl`) | ✅ 100% Compatible |

### Harmonized Schema

All four data sources use a **consistent metadata format**:

```json
{
  "extraction_run_id": "uuid",     // UUID tracking the extraction run
  "created_at_ms": 1234567890000,  // Milliseconds since epoch (ingestion time)
  "timestamp_ms": 1234567890000,   // Milliseconds since epoch (data timestamp)
  // ... source-specific fields ...
}
```

### Expected File Formats

All sources are extracted via the unified `energy-extractors` package (`energy-extract <source> --output ./output`). Output files are saved to the specified output directory.

**ENTSOE:**
- Files: `entsoe_generation_*_etl.jsonl`
- CLI: `energy-extract entsoe --output ./output --yes`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mw`, `resolution_minutes`, country/plant details
- Note: Uses MW (instantaneous power), typically 15/30/60 minute resolution

**India NPP:**
- Files: `npp_generation_*_etl.jsonl`
- CLI: `energy-extract npp --start-date 2024-01-01 --end-date 2024-12-31 --output ./output`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mwh`, `resolution_minutes`, plant/unit details
- Note: Uses MWh (energy), daily resolution (1440 minutes)

**EIA USA:**
- Files: `eia_generation_*_etl.jsonl`
- CLI: `energy-extract eia --start-year 2019 --end-year 2025 --output ./output`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `net_generation_mwh`, `resolution_minutes`, generator details
- Note: Uses MWh (energy), monthly resolution (null)

**Brazil ONS:**
- Files: `ons_generation_*_etl.jsonl`
- CLI: `energy-extract ons --start-year 2019 --end-year 2025 --output ./output`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mwh`, `resolution_minutes`, plant/fuel/subsystem details
- Note: Uses MWh (energy), hourly resolution (60 minutes)

**Australia OE:**
- Files: `oe_generation_*_etl.jsonl`
- CLI: `energy-extract openelectricity --start-date 2019-01-01 --output ./output`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mwh`, `resolution_minutes`, network/fueltech details
- Note: Uses MWh (energy), daily resolution (1440 minutes)

⸻

## Data Validation

The ETL layer includes comprehensive data validation that runs automatically during data loading:

### Validation Features

- **Schema validation** - Ensures all required fields are present
- **Type validation** - Verifies correct data types (string, int, float)
- **Range validation** - Checks values are within acceptable ranges (e.g., non-negative generation)
- **UUID validation** - Validates extraction_run_id format
- **Timestamp validation** - Ensures timestamps are positive and not in the future
- **Duplicate detection** - Detects and skips duplicate records within each file

### Validation Behavior

By default, invalid records are **skipped** and valid records are inserted. This allows partial data loads to succeed while tracking issues.

```bash
# Load with validation (default behavior - skip invalid records)
uv run src/database_management.py load-data npp data.jsonl

# Save detailed validation report
uv run src/database_management.py load-data npp data.jsonl \
  --validation-report report.json

# Strict mode - fail on any validation errors
uv run src/database_management.py load-data npp data.jsonl --strict
```

### Validation Report

When using `--validation-report`, a JSON file is generated with:
- Total, valid, invalid, and duplicate record counts
- Errors grouped by type
- Sample error details (first 10 errors)

Example report:
```json
{
  "timestamp": "2026-01-20T12:00:00",
  "source_file": "npp_data.jsonl",
  "total_records": 100,
  "valid_records": 95,
  "invalid_records": 3,
  "duplicate_records": 2,
  "errors_by_type": {"extraction_run_id": 1, "plant": 2},
  "sample_errors": [...]
}
```

### Validation Rules by Source

| Source | Duplicate Key | Resolution |
|--------|--------------|------------|
| **NPP** | `(timestamp_ms, plant_and_unit)` | Daily (1440 min) |
| **EIA** | `(timestamp_ms, plant_code, generator_id)` | Monthly (null) |
| **ENTSOE** | `(timestamp_ms, country_code, psr_type, plant_name)` | 15/30/60 min |
| **ONS** | `(timestamp_ms, plant, ons_plant_id)` | Hourly (60 min) |
| **OE** | `(timestamp_ms, fueltech, network_code)` | Daily (1440 min) |

### Data Units

Different sources use different units. See [docs/DATA_UNITS.md](./docs/DATA_UNITS.md) for details:

| Source | Field | Unit | Type |
|--------|-------|------|------|
| **ENTSOE** | `generation_mw` | MW | Instantaneous power |
| **NPP** | `generation_mwh` | MWh | Energy (daily) |
| **EIA** | `net_generation_mwh` | MWh | Energy (monthly) |
| **ONS** | `generation_mwh` | MWh | Energy (hourly) |
| **OE** | `generation_mwh` | MWh | Energy (daily) |

---

## Design Principles
	•	Idempotency first — every task can be safely rerun
	•	Time-aware pipelines — all data tied to explicit windows
	•	Separation of concerns — ingestion, orchestration, storage, and visualization are decoupled
	•	SQL as the integration layer — unification happens in Postgres, not Python

⸻

## Downstream Consumers
	•	Streamlit dashboard (visualization & analysis)
	•	Ad-hoc SQL queries
	•	Future analytics or modeling pipelines

⸻

## Production Features

### Structured Logging

The ETL uses **loguru** for structured logging with:
- Console output with color-coded log levels
- File logging with daily rotation (30-day retention)
- Log files stored in `logs/etl_YYYY-MM-DD.log`

```python
# Log levels used:
# INFO - Normal operations
# WARNING - Skipped records, non-critical issues
# ERROR - Failed operations
# SUCCESS - Completed operations
```

### Retry Logic

Database operations include automatic retry with exponential backoff:
- **3 retry attempts** for transient failures
- **Exponential backoff**: 1-10 seconds between retries
- Retries on: `OperationalError`, `InterfaceError`, `ConnectionError`

### Docker Support

Build and run the ETL in a container:

```bash
# Build the image
docker build -t power-generation-etl .

# Run database setup
docker run --rm \
  -e POSTGRES_HOST=host.docker.internal \
  -e POSTGRES_DB=power_generation \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  power-generation-etl python src/database_management.py setup all

# Load data
docker run --rm \
  -v /path/to/data:/data \
  -e POSTGRES_HOST=host.docker.internal \
  power-generation-etl python src/database_management.py load-data entsoe /data/entsoe.jsonl
```

### CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on push/PR:
- **Lint**: Ruff linter and formatter checks
- **Test**: Pytest with PostgreSQL service container
- **Type Check**: Optional mypy analysis

---

## Future Improvements
	•	Add dbt for SQL modeling and tests
	•	Source-level data quality checks
	•	Alerting on failed or delayed pipelines

