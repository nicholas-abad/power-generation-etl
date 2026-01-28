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

Each data source is maintained in its own repository and exposed as a Python package or container:

- **EIA** — U.S. Energy Information Administration
- **NPP** — Global power plant / generation datasets
- **ENTSOE** — European power generation data

This repository **does not** contain scraping logic itself.  
It is responsible for **scheduling, coordination, validation, and loading**.

---

## Architecture

High-level flow:
```
+-------------+
|   EIA Repo  |
+-------------+
        |
+-------------+
|   NPP Repo  |
+-------------+
        |
+-------------+
| ENTSOE Repo |
+-------------+
        |
        v
+----------------+
|    Airflow     |
|  (This Repo)   |
+----------------+
        |
        v
+----------------+
| PostgreSQL     |
| (Neon / local) |
+----------------+
        |
        v
+----------------+
|  Streamlit     |
|  Dashboard     |
+----------------+
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
│   └── extraction_metadata.sql
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

The pipelines load into **PostgreSQL** (currently Neon).

- Source-specific canonical tables (e.g. `gen_eia`, `gen_npp`, `gen_entsoe`)
- Unified SQL views combine sources for analytics
- Primary keys + UPSERT ensure idempotency

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

# 4. Load data from each source (with automatic validation)
uv run src/database_management.py load-data entsoe ./path/to/entsoe_data.jsonl
uv run src/database_management.py load-data npp ./path/to/npp_data.jsonl
uv run src/database_management.py load-data eia ./path/to/eia_data_etl.jsonl

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

This ETL system supports three data sources with **harmonized schemas** for consistent data loading:

| Data Source | Repository | Format | Status |
|-------------|------------|--------|--------|
| **ENTSOE** | `entsoe-power-generation` | JSONL | ✅ 100% Compatible |
| **India NPP** | `india-generation-npp` | JSONL | ✅ 100% Compatible (Harmonized) |
| **EIA USA** | `eia_usa_generation` | JSONL (`*_etl.jsonl` files) | ✅ 100% Compatible |

### Harmonized Schema

All three data sources use a **consistent metadata format**:

```json
{
  "extraction_run_id": "uuid",     // UUID tracking the extraction run
  "created_at_ms": 1234567890000,  // Milliseconds since epoch (ingestion time)
  "timestamp_ms": 1234567890000,   // Milliseconds since epoch (data timestamp)
  // ... source-specific fields ...
}
```

### Expected File Formats

**ENTSOE:**
- Files: `entsoe_monthly_YYYY_MM_*.jsonl`
- Location: `entsoe-power-generation/data/plant_production/raw_data/`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mw`, `resolution_minutes`, country/plant details
- Note: Uses MW (instantaneous power), typically 15/30/60 minute resolution

**India NPP:**
- Files: `npp_*.jsonl`
- Location: `india-generation-npp/output/`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `generation_mwh`, `resolution_minutes`, plant/unit details
- Note: Uses MWh (energy), daily resolution (1440 minutes)

**EIA USA:**
- Files: `eia_generator_data_*_etl.jsonl`
- Location: `eia_usa_generation/output/`
- Schema: `extraction_run_id`, `created_at_ms`, `timestamp_ms`, `net_generation_mwh`, `resolution_minutes`, generator details
- Note: Uses MWh (energy), monthly resolution (null)

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

### Data Units

Different sources use different units. See [docs/DATA_UNITS.md](./docs/DATA_UNITS.md) for details:

| Source | Field | Unit | Type |
|--------|-------|------|------|
| **ENTSOE** | `generation_mw` | MW | Instantaneous power |
| **NPP** | `generation_mwh` | MWh | Energy (daily) |
| **EIA** | `net_generation_mwh` | MWh | Energy (monthly) |

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
	•	Materialized views for heavy aggregations
	•	Source-level data quality checks
	•	Alerting on failed or delayed pipelines

---

## Project Tickets

1. **Design SQL schema for EIA data**  
   Define tables and fields for US electricity generation data. This ensures data is organized for efficient storage and querying.

2. **Design SQL schema for NPP (India) data**  
   Create a schema for India's power generation dataset. Necessary for integrating and analyzing Indian data alongside other sources.

3. **Design SQL schema for ENTSOE (Europe) data**  
   Structure tables for European electricity data. Enables consistent storage and cross-region analysis.

4. **Set up local PostgreSQL database and create tables**  
   Install and configure a local database for development. Allows you to test ETL scripts and schema before cloud deployment.

5. **Refactor EIA ETL notebook into Python module**  
   Convert notebook code to reusable Python scripts. Improves maintainability and enables automation.

6. **Refactor NPP ETL notebook into Python module**  
   Modularize India ETL code for consistency and easier updates.

7. **Refactor ENTSOE ETL notebook into Python module**  
   Standardize ETL process for European data, facilitating integration.

8. **Write data loading scripts for each dataset**  
   Automate loading cleaned data into the database. Ensures repeatable and error-free ingestion.

9. **Test data ingestion into local database**  
   Validate that data loads correctly and schema supports queries. Prevents issues before cloud migration.

10. **Document schema and ETL process in README**  
    Provide clear instructions and schema details. Helps collaborators understand and use the project.

11. **Set up cloud PostgreSQL database (Neon or similar)**  
    Prepare a cloud database for production use. Enables remote access and dashboard integration.

12. **Update ETL scripts to load data into cloud database**  
    Modify scripts to use cloud connection strings. Ensures seamless transition from local to cloud.

13. **Create sample queries for dashboard integration**  
    Develop example SQL queries for dashboard use. Demonstrates how to access and visualize data.

14. **Set up automated ETL workflow (GitHub Actions or cron)**  
    Schedule regular data updates. Keeps database and dashboard current with minimal manual effort.

15. **Add error handling and logging to ETL scripts**  
    Implement robust error checks and logs. Improves reliability and simplifies debugging.