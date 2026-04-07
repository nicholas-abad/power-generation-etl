#!/usr/bin/env python3
"""
Unified Power Generation Database Module
Handles ingestion for NPP, ENTSO-E, EIA, ONS, and OE generation data into PostgreSQL.
"""

import json
import os
import re
import sys
import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import OperationalError, InterfaceError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from validator import DataValidator, ValidationReport, save_report

# ENTSO-E PSR type codes to human-readable fuel type names.
# Used to fix fuel_type during JSONL ingestion (records extracted before the
# extractor fix may have fuel_type="Unknown" due to column-name parsing bugs).
PSR_TO_FUEL_TYPE = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
}

# Suffixes that leak into plant names from column flattening
_FUEL_TYPE_SUFFIXES = sorted(PSR_TO_FUEL_TYPE.values(), key=len, reverse=True)
_DATA_TYPE_SUFFIXES = ["Actual Aggregated", "Actual Consumption"]


# Retry configuration for database operations
DB_RETRY_ATTEMPTS = 3
DB_WAIT_MULTIPLIER = 1  # seconds
DB_WAIT_MIN = 1  # seconds
DB_WAIT_MAX = 10  # seconds


def db_retry_decorator():
    """Create a retry decorator for database operations with exponential backoff."""
    return retry(
        stop=stop_after_attempt(DB_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=DB_WAIT_MULTIPLIER, min=DB_WAIT_MIN, max=DB_WAIT_MAX
        ),
        retry=retry_if_exception_type(
            (OperationalError, InterfaceError, ConnectionError)
        ),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )


try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# Configure loguru for structured logging
# Remove default handler and add custom format
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)
# Add file logging with rotation
logger.add(
    "logs/etl_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
)


_VALID_SQL_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

# All tables this ETL is allowed to operate on.
_KNOWN_TABLES = frozenset({
    "npp_generation",
    "entsoe_generation_data",
    "eia_generation_data",
    "ons_generation_data",
    "oe_generation_data",
    "oe_facility_generation_data",
    "occto_generation_data",
    "extraction_metadata",
    "plant_crosswalk",
    "eia_generator_info",
    "gcpt_coal_metadata",
})


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier (lowercase, alphanumeric + underscores).

    Raises ValueError if the name contains unexpected characters.
    Returns the name unchanged if valid.
    """
    if not _VALID_SQL_IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _validate_table(name: str) -> str:
    """Validate that a table name is in the known whitelist.

    Raises ValueError if unknown.
    """
    if name not in _KNOWN_TABLES:
        raise ValueError(f"Unknown table: {name!r}")
    return name


class PowerGenerationDatabase:
    """Unified database handler for all power generation data sources."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        username: str = None,
        password: str = None,
    ):
        """
        Initialize database connection.

        Args:
            host: Database host (defaults to env var POSTGRES_HOST)
            port: Database port (defaults to env var POSTGRES_PORT or 5432)
            database: Database name (defaults to env var POSTGRES_DB)
            username: Database username (defaults to env var POSTGRES_USER)
            password: Database password (defaults to env var POSTGRES_PASSWORD)
        """
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "power_generation")
        self.username = username or os.environ["POSTGRES_USER"]
        self.password = password or os.environ["POSTGRES_PASSWORD"]

        self.sslmode = os.getenv("POSTGRES_SSLMODE", "")
        query_params = {"sslmode": self.sslmode} if self.sslmode else {}
        self.connection_url = URL.create(
            drivername="postgresql",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=query_params,
        )

        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine, creating it if necessary."""
        if self._engine is None:
            self._engine = create_engine(self.connection_url)
        return self._engine

    @db_retry_decorator()
    def _execute_with_retry(self, query_func):
        """Execute a database operation with retry logic.

        Args:
            query_func: A callable that performs the database operation.

        Returns:
            The result of query_func.

        Raises:
            The original exception after retries are exhausted.
        """
        return query_func()

    def _upsert_via_staging(
        self,
        df: pd.DataFrame,
        target_table: str,
        conflict_columns: list,
        conflict_expr: str = None,
    ) -> int:
        """Insert rows via a staging table, skipping duplicates on conflict.

        Uses CREATE TEMP TABLE + COPY + INSERT ... ON CONFLICT DO NOTHING
        for efficient bulk upsert.

        Args:
            df: DataFrame of rows to insert.
            target_table: Destination table name.
            conflict_columns: List of column names forming the natural key
                (used when the conflict target is plain columns).
            conflict_expr: Raw SQL expression for the ON CONFLICT target when
                the unique index uses expressions (e.g. COALESCE). When set,
                this is used instead of conflict_columns in the ON CONFLICT clause.

        Returns:
            Number of rows actually inserted (excluding duplicates).
        """
        _validate_table(target_table)
        staging = f"_staging_{target_table}"
        columns = list(df.columns)
        for col in columns:
            _validate_identifier(col)
        col_list = ", ".join(columns)

        if conflict_expr:
            conflict_target = f"({conflict_expr})"
        else:
            for col in conflict_columns:
                _validate_identifier(col)
            conflict_target = f"({', '.join(conflict_columns)})"

        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()

            # 1. Create temp staging table (dropped at end of transaction)
            cursor.execute(
                f"CREATE TEMP TABLE {staging} "
                f"(LIKE {target_table} INCLUDING DEFAULTS) "
                f"ON COMMIT DROP"
            )

            # 2. COPY data into staging table
            output = StringIO()
            df.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
            output.seek(0)
            cursor.copy_from(output, staging, columns=columns, sep="\t", null="\\N")

            # 3. INSERT from staging into target, skipping duplicates
            cursor.execute(
                f"INSERT INTO {target_table} ({col_list}) "
                f"SELECT {col_list} FROM {staging} "
                f"ON CONFLICT {conflict_target} DO NOTHING"
            )
            inserted = cursor.rowcount

            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def test_connection(self) -> bool:
        """Test database connection."""
        try:

            def _test():
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True

            self._execute_with_retry(_test)
            logger.info("Database connection successful", database=self.database)
            return True
        except Exception as e:
            logger.error("Database connection failed", error=str(e))
            return False

    def create_database_if_not_exists(self) -> bool:
        """Create the database if it doesn't exist."""
        try:
            # Connect to default postgres database to create target database
            default_conn_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/postgres"
            if self.sslmode:
                default_conn_string += f"?sslmode={self.sslmode}"
            default_engine = create_engine(default_conn_string)

            with default_engine.connect() as conn:
                # Check if database exists
                result = conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                    {"db_name": self.database},
                )

                if result.fetchone() is None:
                    # Database doesn't exist, create it
                    conn.commit()
                    conn.execute(text(f'CREATE DATABASE "{self.database}"'))
                    conn.commit()
                    logger.info("Created database", database=self.database)
                else:
                    logger.info("Database already exists", database=self.database)

            default_engine.dispose()
            return True

        except Exception as e:
            logger.error("Failed to create database", error=str(e))
            return False

    def _execute_schema_file(self, schema_filename: str) -> bool:
        """Execute a schema SQL file."""
        schema_path = Path(__file__).parent.parent / "schema" / schema_filename

        try:
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            with self.engine.connect() as conn:
                conn.execute(text(schema_sql))
                conn.commit()
                logger.info("Schema executed successfully", schema=schema_filename)

            return True

        except FileNotFoundError:
            logger.error("Schema file not found", path=str(schema_path))
            return False
        except Exception as e:
            logger.error(
                "Failed to execute schema", schema=schema_filename, error=str(e)
            )
            return False

    def create_npp_table(self) -> bool:
        """Create NPP generation table."""
        return self._execute_schema_file("npp_generation.sql")

    def create_entsoe_table(self) -> bool:
        """Create ENTSO-E generation table."""
        return self._execute_schema_file("entsoe_generation.sql")

    def create_eia_table(self) -> bool:
        """Create EIA generation table."""
        return self._execute_schema_file("eia_generation.sql")

    def create_ons_table(self) -> bool:
        """Create ONS Brazil generation table."""
        return self._execute_schema_file("ons_generation.sql")

    def create_oe_table(self) -> bool:
        """Create OpenElectricity Australia generation table."""
        return self._execute_schema_file("oe_generation.sql")

    def create_occto_table(self) -> bool:
        """Create OCCTO Japan generation table."""
        return self._execute_schema_file("occto_generation.sql")

    def create_oe_facility_table(self) -> bool:
        """Create OpenElectricity Australia facility generation table."""
        return self._execute_schema_file("oe_facility_generation.sql")

    def create_extraction_metadata_table(self) -> bool:
        """Create extraction metadata table."""
        return self._execute_schema_file("extraction_metadata.sql")

    def create_all_tables(self) -> bool:
        """Create all generation tables including metadata."""
        success = True
        for table_type in ["npp", "entsoe", "eia", "ons", "oe", "oe_facility", "occto", "extraction_metadata"]:
            try:
                method = getattr(self, f"create_{table_type}_table")
                if not method():
                    success = False
                    logger.error("Failed to create table", table_type=table_type)
            except Exception as e:
                logger.error(
                    "Error creating table", table_type=table_type, error=str(e)
                )
                success = False

        if success:
            logger.info("All tables created successfully")
        return success

    def insert_npp_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert NPP data from JSONL file with validation.

        Harmonized format matching EIA/ENTSOE schema:
        - extraction_run_id (UUID)
        - created_at_ms (BIGINT milliseconds)
        - timestamp_ms (BIGINT milliseconds)

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No NPP data found in JSONL file")
                return True, None

            # Generate extraction metadata (matches EIA/ENTSOE pattern)
            if extraction_run_id is None:
                extraction_run_id = str(uuid.uuid4())
            created_at_ms = int(datetime.now().timestamp() * 1000)

            # Transform data to match harmonized schema
            for record in data:
                # Support new harmonized format (preferred)
                if "extraction_run_id" in record and "timestamp_ms" in record:
                    # Already harmonized, just validate created_at_ms
                    if "created_at_ms" not in record:
                        record["created_at_ms"] = created_at_ms
                else:
                    # Legacy format: convert on the fly
                    record["extraction_run_id"] = extraction_run_id
                    record["created_at_ms"] = created_at_ms

                    # Convert date (Unix seconds) to timestamp_ms (milliseconds)
                    if "date" in record:
                        timestamp_seconds = record.pop("date")  # Remove old field
                        record["timestamp_ms"] = int(timestamp_seconds * 1000)

                    # Remove old scrape_id field if present
                    record.pop("scrape_id", None)

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "npp", jsonl_file_path
            )

            # Log validation summary
            logger.info(
                "Validation complete",
                valid=report.valid_count,
                total=report.total_count,
            )
            if report.invalid_count > 0:
                logger.warning("Skipped invalid records", count=report.invalid_count)
            if report.duplicate_count > 0:
                logger.warning(
                    "Skipped duplicate records", count=report.duplicate_count
                )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records via upsert (skips duplicates)
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _upsert_npp():
                    return self._upsert_via_staging(
                        df, "npp_generation", ["timestamp_ms", "plant_and_unit"]
                    )

                inserted = self._execute_with_retry(_upsert_npp)
                skipped = len(valid_records) - inserted
                logger.success(
                    f"NPP upsert: {inserted} inserted, {skipped} duplicates skipped"
                )

                # Record extraction metadata
                run_id = valid_records[0].get("extraction_run_id", extraction_run_id)
                self.insert_extraction_metadata(
                    extraction_run_id=run_id,
                    source="npp",
                    extraction_timestamp=datetime.now(),
                    total_records=inserted,
                    failed_count=report.invalid_count,
                    success=True,
                )
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert NPP data", error=str(e))
            return False, None

    def insert_entsoe_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
        batch_size: int = 500000,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert ENTSO-E data from JSONL file with validation.

        Uses streaming/batching for large files to avoid memory issues.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Set up extraction metadata
            if extraction_run_id is None:
                extraction_run_id = str(uuid.uuid4())
            created_at_ms = int(datetime.now().timestamp() * 1000)

            expected_columns = [
                "extraction_run_id",
                "created_at_ms",
                "country_code",
                "psr_type",
                "plant_name",
                "fuel_type",
                "data_type",
                "timestamp_ms",
                "generation_mw",
                "resolution_minutes",
            ]

            # Count total lines first for progress reporting
            logger.info("Counting total records...")
            with open(jsonl_file_path, "r") as f:
                total_lines = sum(1 for line in f if line.strip())
            logger.info(f"Total records to process: {total_lines:,}")

            # Initialize aggregate validation report
            total_valid = 0
            total_invalid = 0
            total_duplicate = 0
            total_inserted = 0
            batch_num = 0
            first_run_id = None

            validator = DataValidator()

            # Process file in batches
            with open(jsonl_file_path, "r") as f:
                batch = []
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue

                    record = json.loads(line)

                    # Add extraction metadata if not present
                    if "extraction_run_id" not in record:
                        record["extraction_run_id"] = extraction_run_id
                    if "created_at_ms" not in record:
                        record["created_at_ms"] = created_at_ms

                    # Capture first run_id for metadata
                    if first_run_id is None:
                        first_run_id = record.get("extraction_run_id", extraction_run_id)

                    # Convert datetime strings to Unix timestamps in milliseconds
                    if "timestamp_ms" in record:
                        ts = record["timestamp_ms"]
                        if isinstance(ts, str):
                            try:
                                dt = pd.to_datetime(ts, errors="coerce")
                                if pd.isnull(dt):
                                    record["timestamp_ms"] = None
                                else:
                                    record["timestamp_ms"] = int(dt.timestamp() * 1000)
                            except Exception:
                                record["timestamp_ms"] = None
                        elif ts is not None:
                            record["timestamp_ms"] = int(ts)

                    # Fix fuel_type: derive from psr_type (always correct)
                    psr = record.get("psr_type", "")
                    if psr in PSR_TO_FUEL_TYPE:
                        record["fuel_type"] = PSR_TO_FUEL_TYPE[psr]

                    # Clean plant_name: strip leaked fuel-type and data-type suffixes
                    plant_name = record.get("plant_name", "")
                    for suffix in _DATA_TYPE_SUFFIXES:
                        if plant_name.endswith("_" + suffix):
                            plant_name = plant_name[: -(len(suffix) + 1)]
                            break
                    for suffix in _FUEL_TYPE_SUFFIXES:
                        if plant_name.endswith("_" + suffix):
                            plant_name = plant_name[: -(len(suffix) + 1)]
                            break
                    record["plant_name"] = plant_name

                    batch.append(record)

                    # Process batch when full
                    if len(batch) >= batch_size:
                        batch_num += 1
                        inserted, valid, invalid, dup = self._insert_entsoe_batch(
                            batch, expected_columns, validator, batch_num, line_num, total_lines
                        )
                        total_inserted += inserted
                        total_valid += valid
                        total_invalid += invalid
                        total_duplicate += dup
                        batch = []

                # Process remaining records
                if batch:
                    batch_num += 1
                    inserted, valid, invalid, dup = self._insert_entsoe_batch(
                        batch, expected_columns, validator, batch_num, line_num, total_lines
                    )
                    total_inserted += inserted
                    total_valid += valid
                    total_invalid += invalid
                    total_duplicate += dup

            # Create aggregate validation report
            report = ValidationReport(
                source_type="entsoe",
                file_path=jsonl_file_path,
                total_count=total_valid + total_invalid + total_duplicate,
                valid_count=total_valid,
                invalid_count=total_invalid,
                duplicate_count=total_duplicate,
            )

            # Log final summary
            logger.success(
                f"ENTSO-E load complete: {total_inserted:,} records inserted, "
                f"{total_invalid:,} invalid, {total_duplicate:,} duplicates skipped"
            )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Record extraction metadata
            self.insert_extraction_metadata(
                extraction_run_id=first_run_id or extraction_run_id,
                source="entsoe",
                extraction_timestamp=datetime.now(),
                total_records=total_valid,
                failed_count=total_invalid,
                success=True,
            )
            return True, report

        except Exception as e:
            logger.error(
                "Failed to insert ENTSO-E data", file=jsonl_file_path, error=str(e)
            )
            return False, None

    def _insert_entsoe_batch(
        self,
        batch: list,
        expected_columns: list,
        validator: "DataValidator",
        batch_num: int,
        current_line: int,
        total_lines: int,
    ) -> Tuple[int, int, int, int]:
        """Insert a batch of ENTSO-E records.

        Returns:
            Tuple of (inserted_count, valid_count, invalid_count, duplicate_count)
        """
        # Validate batch
        valid_records, report = validator.validate_file(
            batch, "entsoe", f"batch_{batch_num}"
        )

        if not valid_records:
            logger.warning(f"Batch {batch_num}: No valid records")
            return 0, report.valid_count, report.invalid_count, report.duplicate_count

        # Convert to DataFrame
        df = pd.DataFrame(valid_records)
        df = df[expected_columns]

        # Insert via staging table upsert (handles existing UNIQUE constraint)
        def _upsert():
            return self._upsert_via_staging(
                df,
                "entsoe_generation_data",
                ["timestamp_ms", "country_code", "psr_type", "plant_name"],
            )

        inserted = self._execute_with_retry(_upsert)

        skipped = len(df) - inserted
        pct = (current_line / total_lines) * 100
        logger.info(
            f"Batch {batch_num}: {inserted:,} inserted, {skipped:,} duplicates skipped "
            f"({current_line:,}/{total_lines:,} = {pct:.1f}%)"
        )

        return inserted, report.valid_count, report.invalid_count, report.duplicate_count

    def aggregate_entsoe_to_monthly(
        self, output_dir: str, granularity: str = "plant"
    ) -> Tuple[bool, int]:
        """Aggregate ENTSOE hourly data to monthly and export to CSV files.

        Args:
            output_dir: Directory to save CSV files
            granularity: Level of aggregation - 'plant', 'country-fuel', or 'country'

        Returns:
            Tuple of (success, total_rows_exported)
        """
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Build aggregation query based on granularity
            if granularity == "plant":
                group_cols = "month, country_code, psr_type, plant_name"
                select_cols = """
                    TO_CHAR(date_trunc('month', to_timestamp(timestamp_ms / 1000)), 'YYYY-MM-01') AS month,
                    country_code,
                    psr_type,
                    plant_name,
                    SUM(generation_mw) AS total_generation_mwh,
                    COUNT(*) AS hours_of_data,
                    AVG(generation_mw) AS avg_generation_mw,
                    MAX(generation_mw) AS peak_generation_mw
                """
            elif granularity == "country-fuel":
                group_cols = "month, country_code, psr_type"
                select_cols = """
                    TO_CHAR(date_trunc('month', to_timestamp(timestamp_ms / 1000)), 'YYYY-MM-01') AS month,
                    country_code,
                    psr_type,
                    SUM(generation_mw) AS total_generation_mwh,
                    COUNT(*) AS hours_of_data,
                    AVG(generation_mw) AS avg_generation_mw,
                    MAX(generation_mw) AS peak_generation_mw
                """
            else:  # country
                group_cols = "month, country_code"
                select_cols = """
                    TO_CHAR(date_trunc('month', to_timestamp(timestamp_ms / 1000)), 'YYYY-MM-01') AS month,
                    country_code,
                    SUM(generation_mw) AS total_generation_mwh,
                    COUNT(*) AS hours_of_data,
                    AVG(generation_mw) AS avg_generation_mw,
                    MAX(generation_mw) AS peak_generation_mw
                """

            # Get distinct years in the data
            with self.engine.connect() as conn:
                years_result = conn.execute(
                    text("""
                        SELECT DISTINCT EXTRACT(YEAR FROM to_timestamp(timestamp_ms / 1000))::INTEGER AS year
                        FROM entsoe_generation_data
                        ORDER BY year
                    """)
                )
                years = [row[0] for row in years_result]

            if not years:
                logger.warning("No data found in entsoe_generation_data")
                return True, 0

            logger.info(f"Found data for years: {years}")
            total_rows = 0

            # Export each year to a separate CSV
            for year in years:
                logger.info(f"Aggregating year {year}...")

                query = f"""
                    SELECT {select_cols}
                    FROM entsoe_generation_data
                    WHERE EXTRACT(YEAR FROM to_timestamp(timestamp_ms / 1000)) = :year
                    GROUP BY {group_cols}
                    ORDER BY {group_cols}
                """

                df = pd.read_sql(text(query), self.engine, params={"year": year})

                if len(df) > 0:
                    csv_path = output_path / f"entsoe_monthly_{year}.csv"
                    df.to_csv(csv_path, index=False)
                    total_rows += len(df)
                    logger.success(f"Exported {len(df):,} rows to {csv_path}")

            logger.success(f"Total exported: {total_rows:,} rows across {len(years)} files")
            return True, total_rows

        except Exception as e:
            logger.error(f"Failed to aggregate ENTSOE data: {e}")
            return False, 0

    def clear_entsoe_data(self) -> Tuple[bool, int]:
        """Clear all data from the ENTSOE table.

        Returns:
            Tuple of (success, rows_deleted)
        """
        try:
            with self.engine.connect() as conn:
                # Get count before deletion
                count_result = conn.execute(
                    text("SELECT COUNT(*) FROM entsoe_generation_data")
                )
                row_count = count_result.scalar()

                # Delete all rows
                conn.execute(text("TRUNCATE TABLE entsoe_generation_data"))
                conn.commit()

                logger.success(f"Cleared {row_count:,} rows from entsoe_generation_data")
                return True, row_count

        except Exception as e:
            logger.error(f"Failed to clear ENTSOE data: {e}")
            return False, 0

    def insert_eia_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert EIA data from JSONL file with validation.

        Supports both legacy format (without metadata) and ETL-compatible format
        (with extraction_run_id and created_at_ms already included).

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No EIA data found in JSONL file")
                return True, None

            # Check if metadata fields already exist in the data
            has_extraction_run_id = "extraction_run_id" in data[0]
            has_created_at_ms = "created_at_ms" in data[0]

            # Add extraction metadata only if not already present
            if not has_extraction_run_id or not has_created_at_ms:
                if extraction_run_id is None:
                    extraction_run_id = str(uuid.uuid4())
                created_at_ms = int(datetime.now().timestamp() * 1000)

            for record in data:
                if not has_extraction_run_id:
                    record["extraction_run_id"] = extraction_run_id
                if not has_created_at_ms:
                    record["created_at_ms"] = created_at_ms
                # Convert IDs to strings for SQL VARCHAR columns
                if "utility_id" in record and not isinstance(record["utility_id"], str):
                    record["utility_id"] = str(record["utility_id"])
                if "plant_code" in record and not isinstance(record["plant_code"], str):
                    record["plant_code"] = str(record["plant_code"])
                if "generator_id" in record and not isinstance(
                    record["generator_id"], str
                ):
                    record["generator_id"] = str(record["generator_id"])
                # Add optional fields if missing
                if "fuel_source" not in record:
                    record["fuel_source"] = None
                if "energy_source" not in record:
                    record["energy_source"] = None

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "eia", jsonl_file_path
            )

            # Log validation summary
            logger.info(
                "Validation complete",
                valid=report.valid_count,
                total=report.total_count,
            )
            if report.invalid_count > 0:
                logger.warning("Skipped invalid records", count=report.invalid_count)
            if report.duplicate_count > 0:
                logger.warning(
                    "Skipped duplicate records", count=report.duplicate_count
                )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records via upsert (skips duplicates)
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _upsert_eia():
                    return self._upsert_via_staging(
                        df,
                        "eia_generation_data",
                        ["timestamp_ms", "plant_code", "generator_id"],
                    )

                inserted = self._execute_with_retry(_upsert_eia)
                skipped = len(valid_records) - inserted
                logger.success(
                    f"EIA upsert: {inserted} inserted, {skipped} duplicates skipped"
                )

                # Record extraction metadata
                run_id = valid_records[0].get("extraction_run_id", extraction_run_id)
                self.insert_extraction_metadata(
                    extraction_run_id=run_id,
                    source="eia",
                    extraction_timestamp=datetime.now(),
                    total_records=inserted,
                    failed_count=report.invalid_count,
                    success=True,
                )
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert EIA data", error=str(e))
            return False, None

    def insert_ons_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
        chunk_lines: int = 50_000,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert ONS Brazil data from JSONL file with validation.

        Processes the file in streaming chunks to handle large files (11M+ rows)
        without loading everything into memory.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            if extraction_run_id is None:
                extraction_run_id = str(uuid.uuid4())
            created_at_ms = int(datetime.now().timestamp() * 1000)

            validator = DataValidator()
            total_inserted = 0
            total_valid = 0
            total_invalid = 0
            total_duplicates = 0
            total_records = 0
            chunk_num = 0

            with open(jsonl_file_path, "r") as f:
                while True:
                    # Read a chunk of lines
                    chunk = []
                    for _ in range(chunk_lines):
                        line = f.readline()
                        if not line:
                            break
                        line = line.strip()
                        if line:
                            chunk.append(json.loads(line))

                    if not chunk:
                        break

                    chunk_num += 1

                    # Add extraction metadata
                    has_extraction_run_id = "extraction_run_id" in chunk[0]
                    has_created_at_ms = "created_at_ms" in chunk[0]

                    for record in chunk:
                        if not has_extraction_run_id:
                            record["extraction_run_id"] = extraction_run_id
                        if not has_created_at_ms:
                            record["created_at_ms"] = created_at_ms

                    # Validate chunk
                    valid_records, report = validator.validate_file(
                        chunk, "ons", jsonl_file_path
                    )

                    total_records += report.total_count
                    total_valid += report.valid_count
                    total_invalid += report.invalid_count
                    total_duplicates += report.duplicate_count

                    # Insert valid records via upsert (skips duplicates)
                    if valid_records:
                        df = pd.DataFrame(valid_records)

                        def _upsert_chunk():
                            return self._upsert_via_staging(
                                df,
                                "ons_generation_data",
                                ["timestamp_ms", "plant", "ons_plant_id"],
                                conflict_expr="timestamp_ms, plant, COALESCE(ons_plant_id, '')",
                            )

                        inserted = self._execute_with_retry(_upsert_chunk)
                        total_inserted += inserted

                    logger.info(
                        f"Chunk {chunk_num} done",
                        chunk_valid=report.valid_count,
                        total_inserted=total_inserted,
                    )

                    # Free memory
                    del chunk, valid_records

            # Log final summary
            logger.info(
                "Validation complete",
                valid=total_valid,
                invalid=total_invalid,
                duplicates=total_duplicates,
                total=total_records,
            )
            if total_invalid > 0:
                logger.warning("Skipped invalid records", count=total_invalid)
            if total_duplicates > 0:
                logger.warning("Skipped duplicate records", count=total_duplicates)

            if total_inserted > 0:
                self.insert_extraction_metadata(
                    extraction_run_id=extraction_run_id,
                    source="ons",
                    extraction_timestamp=datetime.now(),
                    total_records=total_valid,
                    failed_count=total_invalid,
                    success=True,
                )
                logger.success("Inserted ONS records", count=total_inserted)
            else:
                logger.warning("No valid records to insert")

            # Build an aggregate report for the return value
            aggregate_report = ValidationReport(
                source_file=jsonl_file_path,
                total_count=total_records,
                valid_count=total_valid,
                invalid_count=total_invalid,
                duplicate_count=total_duplicates,
            )

            if validation_report_path:
                save_report(aggregate_report, validation_report_path)

            return True, aggregate_report

        except Exception as e:
            logger.error("Failed to insert ONS data", error=str(e))
            return False, None

    def insert_occto_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert OCCTO Japan data from JSONL file with validation.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No OCCTO data found in JSONL file")
                return True, None

            # Add extraction metadata if not already present
            has_extraction_run_id = "extraction_run_id" in data[0]
            has_created_at_ms = "created_at_ms" in data[0]

            if not has_extraction_run_id or not has_created_at_ms:
                if extraction_run_id is None:
                    extraction_run_id = str(uuid.uuid4())
                created_at_ms = int(datetime.now().timestamp() * 1000)

            for record in data:
                if not has_extraction_run_id:
                    record["extraction_run_id"] = extraction_run_id
                if not has_created_at_ms:
                    record["created_at_ms"] = created_at_ms

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "occto", jsonl_file_path
            )

            # Log validation summary
            logger.info(
                "Validation complete",
                valid=report.valid_count,
                total=report.total_count,
            )
            if report.invalid_count > 0:
                logger.warning("Skipped invalid records", count=report.invalid_count)
            if report.duplicate_count > 0:
                logger.warning(
                    "Skipped duplicate records", count=report.duplicate_count
                )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records via upsert (skips duplicates)
            if valid_records:
                df = pd.DataFrame(valid_records)

                # Keep only columns that exist in the DB table
                db_columns = [
                    "extraction_run_id", "created_at_ms", "plant", "unit",
                    "plant_code", "fuel_code", "fuel_type", "area_code",
                    "area_name", "timestamp_ms", "generation_mwh",
                    "resolution_minutes",
                ]
                df = df[[c for c in db_columns if c in df.columns]]

                def _upsert_occto():
                    return self._upsert_via_staging(
                        df,
                        "occto_generation_data",
                        conflict_columns=["timestamp_ms", "plant"],
                        conflict_expr="timestamp_ms, plant, COALESCE(unit, '')",
                    )

                inserted = self._execute_with_retry(_upsert_occto)
                skipped = len(valid_records) - inserted
                logger.success(
                    f"OCCTO upsert: {inserted} inserted, {skipped} duplicates skipped"
                )

                # Record extraction metadata
                run_id = valid_records[0].get("extraction_run_id", extraction_run_id)
                self.insert_extraction_metadata(
                    extraction_run_id=run_id,
                    source="occto",
                    extraction_timestamp=datetime.now(),
                    total_records=inserted,
                    failed_count=report.invalid_count,
                    success=True,
                )
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert OCCTO data", error=str(e))
            return False, None

    def insert_oe_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert OpenElectricity Australia data from JSONL file with validation.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No OE data found in JSONL file")
                return True, None

            # Add extraction metadata if not already present
            has_extraction_run_id = "extraction_run_id" in data[0]
            has_created_at_ms = "created_at_ms" in data[0]

            if not has_extraction_run_id or not has_created_at_ms:
                if extraction_run_id is None:
                    extraction_run_id = str(uuid.uuid4())
                created_at_ms = int(datetime.now().timestamp() * 1000)

            for record in data:
                if not has_extraction_run_id:
                    record["extraction_run_id"] = extraction_run_id
                if not has_created_at_ms:
                    record["created_at_ms"] = created_at_ms

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "oe", jsonl_file_path
            )

            # Log validation summary
            logger.info(
                "Validation complete",
                valid=report.valid_count,
                total=report.total_count,
            )
            if report.invalid_count > 0:
                logger.warning("Skipped invalid records", count=report.invalid_count)
            if report.duplicate_count > 0:
                logger.warning(
                    "Skipped duplicate records", count=report.duplicate_count
                )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records via upsert (skips duplicates)
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _upsert_oe():
                    return self._upsert_via_staging(
                        df,
                        "oe_generation_data",
                        ["timestamp_ms", "fueltech", "network_code"],
                    )

                inserted = self._execute_with_retry(_upsert_oe)
                skipped = len(valid_records) - inserted
                logger.success(
                    f"OE upsert: {inserted} inserted, {skipped} duplicates skipped"
                )

                # Record extraction metadata
                run_id = valid_records[0].get("extraction_run_id", extraction_run_id)
                self.insert_extraction_metadata(
                    extraction_run_id=run_id,
                    source="oe",
                    extraction_timestamp=datetime.now(),
                    total_records=inserted,
                    failed_count=report.invalid_count,
                    success=True,
                )
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert OE data", error=str(e))
            return False, None

    def insert_oe_facility_jsonl_data(
        self,
        jsonl_file_path: str,
        extraction_run_id: str = None,
        validation_report_path: str = None,
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert OpenElectricity Australia facility data from JSONL file with validation.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No OE facility data found in JSONL file")
                return True, None

            # Add extraction metadata if not already present
            has_extraction_run_id = "extraction_run_id" in data[0]
            has_created_at_ms = "created_at_ms" in data[0]

            if not has_extraction_run_id or not has_created_at_ms:
                if extraction_run_id is None:
                    extraction_run_id = str(uuid.uuid4())
                created_at_ms = int(datetime.now().timestamp() * 1000)

            for record in data:
                if not has_extraction_run_id:
                    record["extraction_run_id"] = extraction_run_id
                if not has_created_at_ms:
                    record["created_at_ms"] = created_at_ms

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "oe_facility", jsonl_file_path
            )

            # Log validation summary
            logger.info(
                "Validation complete",
                valid=report.valid_count,
                total=report.total_count,
            )
            if report.invalid_count > 0:
                logger.warning("Skipped invalid records", count=report.invalid_count)
            if report.duplicate_count > 0:
                logger.warning(
                    "Skipped duplicate records", count=report.duplicate_count
                )

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records via upsert (skips duplicates)
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _upsert_oe_facility():
                    return self._upsert_via_staging(
                        df,
                        "oe_facility_generation_data",
                        ["timestamp_ms", "facility_code", "fueltech"],
                    )

                inserted = self._execute_with_retry(_upsert_oe_facility)
                skipped = len(valid_records) - inserted
                logger.success(
                    f"OE facility upsert: {inserted} inserted, {skipped} duplicates skipped"
                )

                # Record extraction metadata
                run_id = valid_records[0].get("extraction_run_id", extraction_run_id)
                self.insert_extraction_metadata(
                    extraction_run_id=run_id,
                    source="oe_facility",
                    extraction_timestamp=datetime.now(),
                    total_records=inserted,
                    failed_count=report.invalid_count,
                    success=True,
                )
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert OE facility data", error=str(e))
            return False, None

    def get_record_count(self, table_name: str) -> int:
        """Get total number of records in a specific table."""
        _validate_table(table_name)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                logger.info("Record count", table=table_name, count=count)
                return count
        except Exception as e:
            logger.error("Failed to get record count", table=table_name, error=str(e))
            return 0

    def get_all_record_counts(self) -> Dict[str, int]:
        """Get record counts for all main tables."""
        tables = [
            "npp_generation", "entsoe_generation_data", "eia_generation_data",
            "ons_generation_data", "oe_generation_data",
            "oe_facility_generation_data", "occto_generation_data",
        ]
        counts = {}

        for table in tables:
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    counts[table] = result.scalar()
            except Exception:
                counts[table] = 0

        return counts

    def insert_extraction_metadata(
        self,
        extraction_run_id: str,
        source: str,
        extraction_timestamp: datetime,
        start_date: str = None,
        end_date: str = None,
        total_records: int = 0,
        failed_count: int = 0,
        success: bool = True,
        failed_details: list = None,
        config_snapshot: dict = None,
        source_urls: dict = None,
        extraction_duration_seconds: int = None,
    ) -> bool:
        """Insert extraction metadata into the database.

        Args:
            extraction_run_id: UUID of the extraction run
            source: Data source ('npp', 'eia', 'entsoe', 'ons', 'oe')
            extraction_timestamp: When the extraction was performed
            start_date: Start of data range (YYYY-MM-DD)
            end_date: End of data range (YYYY-MM-DD)
            total_records: Number of records extracted
            failed_count: Number of failed extractions
            success: Whether extraction was successful overall
            failed_details: List of failed date/error details
            config_snapshot: Extractor configuration used
            source_urls: URLs accessed during extraction
            extraction_duration_seconds: How long extraction took

        Returns:
            True if insertion successful, False otherwise
        """
        try:
            insert_sql = text("""
                INSERT INTO extraction_metadata (
                    extraction_run_id, source, extraction_timestamp,
                    start_date, end_date, total_records, failed_count, success,
                    failed_details, config_snapshot, source_urls,
                    extraction_duration_seconds
                ) VALUES (
                    :extraction_run_id, :source, :extraction_timestamp,
                    :start_date, :end_date, :total_records, :failed_count, :success,
                    :failed_details, :config_snapshot, :source_urls,
                    :extraction_duration_seconds
                )
                ON CONFLICT (extraction_run_id) DO UPDATE SET
                    total_records = EXCLUDED.total_records,
                    failed_count = EXCLUDED.failed_count,
                    success = EXCLUDED.success,
                    failed_details = EXCLUDED.failed_details
            """)

            with self.engine.connect() as conn:
                conn.execute(
                    insert_sql,
                    {
                        "extraction_run_id": extraction_run_id,
                        "source": source,
                        "extraction_timestamp": extraction_timestamp,
                        "start_date": start_date,
                        "end_date": end_date,
                        "total_records": total_records,
                        "failed_count": failed_count,
                        "success": success,
                        "failed_details": json.dumps(failed_details) if failed_details else None,
                        "config_snapshot": json.dumps(config_snapshot) if config_snapshot else None,
                        "source_urls": json.dumps(source_urls) if source_urls else None,
                        "extraction_duration_seconds": extraction_duration_seconds,
                    },
                )
                conn.commit()

            logger.info(
                "Extraction metadata inserted",
                extraction_run_id=extraction_run_id,
                source=source,
                total_records=total_records,
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to insert extraction metadata",
                extraction_run_id=extraction_run_id,
                error=str(e),
            )
            return False

    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


def create_power_generation_database() -> PowerGenerationDatabase:
    """Create database instance using environment variables."""
    return PowerGenerationDatabase()


if __name__ == "__main__":
    # Example usage for NPP data ingestion
    db = create_power_generation_database()

    # Test connection and setup
    if not db.test_connection():
        db.create_database_if_not_exists()

    # Create NPP table
    db.create_npp_table()

    # Example file paths
    jsonl_file = "/Users/nicholasabad/Desktop/workspace/consulting-christine/india-generation-npp/output/test/npp_test_extraction_2025-12-29_12-21-26.jsonl"
    metadata_file = "/Users/nicholasabad/Desktop/workspace/consulting-christine/india-generation-npp/output/scrape_metadata_7b7bb7a0-52d7-4241-9fe2-45e852175ade.json"

    # Insert data
    db.insert_npp_jsonl_data(jsonl_file, metadata_file)

    db.close()
