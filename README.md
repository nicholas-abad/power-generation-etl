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
├── dags/                 # Airflow DAG definitions
│   ├── minimal_etl.py
│   ├── eia_monthly.py
│   ├── npp_monthly.py
│   └── entsoe_monthly.py
├── scripts/              # Helper scripts (optional)
├── sql/                  # SQL helpers / schema (optional)
├── README.md
└── pyproject.toml
```
---

## How the Pipelines Work

Each Airflow DAG follows the same high-level pattern:

1. **Compute time window**
   - Derive the data window from Airflow’s `logical_date`
2. **Extract**
   - Fetch raw data from the upstream source
3. **Transform**
   - Normalize into a canonical schema
4. **Load**
   - UPSERT into PostgreSQL (idempotent)
5. **Record metadata**
   - Track successful runs and row counts

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

# 4. Load data from each source
uv run src/database_management.py load-data entsoe ./path/to/entsoe_data.jsonl
uv run src/database_management.py load-data npp ./path/to/npp_data.jsonl --metadata ./path/to/metadata.json
uv run src/database_management.py load-data eia ./path/to/eia_data_etl.jsonl

# 5. View statistics
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

This ETL system supports three data sources:

| Data Source | Repository | Format | Status |
|-------------|------------|--------|--------|
| **ENTSOE** | `entsoe-power-generation` | JSONL | ✅ 100% Compatible |
| **India NPP** | `india-generation-npp` | JSONL + JSON metadata | ✅ Compatible (with PR #3) |
| **EIA USA** | `eia_usa_generation` | JSONL (`*_etl.jsonl` files) | ✅ Compatible (with PR #5) |

### Expected File Formats

**ENTSOE:**
- Files: `entsoe_monthly_YYYY_MM_*.jsonl`
- Location: `entsoe-power-generation/data/plant_production/raw_data/`
- Includes: `extraction_run_id`, `created_at_ms` (already compatible)

**India NPP:**
- Files: `npp_generation_*.jsonl` + `scrape_metadata_*.json`
- Location: `india-generation-npp/data/`
- Requires: Separate metadata JSON file (generated by extractor)

**EIA USA:**
- Files: `eia_generator_data_*_etl.jsonl` (use ETL-compatible format)
- Location: `eia_usa_generation/output/`
- Note: Use `*_etl.jsonl` files, not legacy `*.jsonl` files

⸻

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