# Database Management CLI Guide

This guide covers using the database management CLI for loading power generation data from multiple sources.

## Overview

The `database_management.py` CLI provides a unified interface for:
- Setting up database tables
- Loading data from JSONL files
- Viewing database statistics
- Updating schemas

## Quick Start

```bash
# 1. Set up database (creates all tables)
python src/database_management.py setup all

# 2. Load data from each source
python src/database_management.py load-data entsoe ./data/entsoe_data.jsonl
python src/database_management.py load-data npp ./data/npp_data.jsonl --metadata ./data/metadata.json
python src/database_management.py load-data eia ./data/eia_data_etl.jsonl

# 3. View statistics
python src/database_management.py stats
```

## Configuration

The CLI uses environment variables for database connection:

```bash
# Required environment variables (or create .env file)
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=power_generation
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password
```

Or use `.env` file:
```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=power_generation
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

## Commands

### 1. Setup Database

Create database tables for one or all data sources.

```bash
# Create all tables
python src/database_management.py setup all

# Create specific table
python src/database_management.py setup npp
python src/database_management.py setup entsoe
python src/database_management.py setup eia
```

**Tables created:**
- `npp`: Creates `scrape_metadata` and `npp_generation` tables
- `entsoe`: Creates `entsoe_generation_data` table
- `eia`: Creates `eia_generation_data` table

### 2. Load Data

Load JSONL data into the database.

#### ENTSOE Data

```bash
python src/database_management.py load-data entsoe ./data/entsoe_monthly_2020_01.jsonl
```

**Expected format:**
```json
{
  "extraction_run_id": "batch-20260101-164658",
  "created_at_ms": 1767282419683,
  "country_code": "10YDE-VE-------2",
  "psr_type": "B04",
  "plant_name": "Lichterfelde GUD",
  "fuel_type": "Unknown",
  "data_type": "Actual",
  "timestamp_ms": 1577836800000,
  "generation_mw": 200.75
}
```

**Notes:**
- ENTSOE data already includes `extraction_run_id` and `created_at_ms`
- Data is loaded as-is without modification
- Use files from: `entsoe-power-generation/data/plant_production/raw_data/*.jsonl`

#### India NPP Data

```bash
python src/database_management.py load-data npp \
  ./data/npp_generation_20250101_20250107.jsonl \
  --metadata ./data/scrape_metadata_<uuid>.json
```

**Expected data format:**
```json
{
  "date": 1546473600,
  "plant": "I.P.CCPP",
  "unit": null,
  "actual_generation": 0.98,
  "plant_and_unit": "I.P.CCPP"
}
```

**Expected metadata format:**
```json
{
  "scrape_id": "7b7bb7a0-52d7-4241-9fe2-45e852175ade",
  "scrape_timestamp": "2025-12-29T12:21:17.790036",
  "start_date": "2025-11-23",
  "end_date": "2025-11-29",
  "total_records": 7944,
  "failed_dates_count": 0,
  "failed_dates": [],
  "success": true,
  "source_urls": { ... },
  "extractor_config": { ... }
}
```

**Notes:**
- Metadata file is optional (UUID will be generated if not provided)
- Metadata is loaded into separate `scrape_metadata` table
- Data is loaded into `npp_generation` table with linked `scrape_id`
- Use files from: `india-generation-npp/data/*.jsonl` and `india-generation-npp/data/scrape_metadata_*.json`

#### EIA USA Data

```bash
python src/database_management.py load-data eia ./data/eia_generator_data_2022_2022_etl.jsonl
```

**Expected ETL-compatible format:**
```json
{
  "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
  "created_at_ms": 1704067200000,
  "utility_id": "195",
  "plant_code": "3",
  "generator_id": "A2ST",
  "state": "AL",
  "fuel_source": "NG",
  "prime_mover": "CA",
  "energy_source": "NG",
  "timestamp_ms": 1640995200000,
  "net_generation_mwh": 129296.0
}
```

**Legacy format support:**
The CLI also supports legacy format (without `extraction_run_id` and `created_at_ms`). These fields will be auto-generated.

**Notes:**
- Use ETL-compatible files: `eia_usa_generation/output/*_etl.jsonl`
- ETL-compatible files include all required metadata fields
- Legacy files will have metadata added automatically

### 3. View Statistics

Display record counts for all tables.

```bash
python src/database_management.py stats
```

**Example output:**
```
📊 Database Statistics:
Total records across all tables: 1,234,567
  entsoe_generation_data: 1,000,000 records
  npp_generation: 200,000 records
  eia_generation_data: 34,567 records
  scrape_metadata: 10 records
```

### 4. Update Schema

Update existing table schemas (e.g., after schema changes).

```bash
# Update ENTSOE schema
python src/database_management.py update-schema entsoe

# Update all schemas
python src/database_management.py update-schema all
```

## Data Source File Locations

### ENTSOE
```
entsoe-power-generation/
└── data/
    └── plant_production/
        └── raw_data/
            ├── entsoe_monthly_2020_01_batch-TIMESTAMP.jsonl
            ├── entsoe_monthly_2020_02_batch-TIMESTAMP.jsonl
            └── entsoe_combined_2020_2025_full_extraction.jsonl
```

### India NPP
```
india-generation-npp/
├── data/
│   ├── npp_generation_YYYYMMDD_YYYYMMDD_timestamp.jsonl
│   └── scrape_metadata_<uuid>.json
└── output/
    └── scrape_metadata_<uuid>.json
```

### EIA USA
```
eia_usa_generation/
└── output/
    ├── eia_generator_data_YYYY_YYYY_timestamp.jsonl  (legacy)
    └── eia_generator_data_YYYY_YYYY_timestamp_etl.jsonl  (ETL-compatible)
```

## Batch Loading Examples

### Load multiple ENTSOE files

```bash
for file in entsoe-power-generation/data/plant_production/raw_data/entsoe_monthly_*.jsonl; do
  python src/database_management.py load-data entsoe "$file"
done
```

### Load NPP data with metadata

```bash
# Find matching metadata file
DATA_FILE="./india-generation-npp/data/npp_generation_20190101_20251228.jsonl"
METADATA_FILE="./india-generation-npp/data/scrape_metadata_<uuid>.json"

python src/database_management.py load-data npp "$DATA_FILE" --metadata "$METADATA_FILE"
```

### Load EIA ETL-compatible files

```bash
for file in eia_usa_generation/output/*_etl.jsonl; do
  python src/database_management.py load-data eia "$file"
done
```

## Troubleshooting

### Connection Failed

```
❌ Database connection failed
```

**Solutions:**
1. Check environment variables are set correctly
2. Ensure PostgreSQL is running: `docker ps` or `pg_isready`
3. Test connection: `psql -h localhost -U postgres -d power_generation`
4. Verify credentials in `.env` file

### File Not Found

```
❌ File not found: ./data/file.jsonl
```

**Solutions:**
1. Check file path is correct (use absolute paths if unsure)
2. Verify file exists: `ls -l ./data/file.jsonl`
3. Check file permissions

### Schema Mismatch

```
❌ Failed to insert data: column "xyz" does not exist
```

**Solutions:**
1. Run `setup` command to ensure tables exist: `python src/database_management.py setup all`
2. Check JSONL file format matches expected schema
3. For EIA data, ensure using `*_etl.jsonl` files (not legacy format)

### Duplicate Records

The database uses `ON CONFLICT DO NOTHING` for idempotent loading. Duplicate records are silently skipped.

## Advanced Usage

### Using Python API Directly

```python
from src.database import PowerGenerationDatabase

# Create database instance
db = PowerGenerationDatabase()

# Test connection
if db.test_connection():
    # Load ENTSOE data
    db.insert_entsoe_jsonl_data("./data/entsoe_data.jsonl")

    # Load NPP data with metadata
    db.insert_npp_jsonl_data(
        "./data/npp_data.jsonl",
        metadata_file_path="./data/metadata.json"
    )

    # Load EIA data
    db.insert_eia_jsonl_data("./data/eia_data_etl.jsonl")

    # Get statistics
    counts = db.get_all_record_counts()
    print(counts)

db.close()
```

### Docker Compose Setup

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: power_generation
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

Start database:
```bash
docker-compose up -d
python src/database_management.py setup all
```

## Integration with Data Sources

This CLI is designed to work with the following repositories:

1. **entsoe-power-generation** - ENTSOE data is 100% compatible out of the box
2. **india-generation-npp** - Use JSON metadata files (PR #3)
3. **eia_usa_generation** - Use ETL-compatible `*_etl.jsonl` files (PR #5)

## Next Steps

After loading data:

1. **Verify data:** `python src/database_management.py stats`
2. **Query data:** Use SQL client or pandas
3. **Build dashboards:** Connect Streamlit or other viz tools
4. **Set up Airflow:** Automate data loading with DAGs
