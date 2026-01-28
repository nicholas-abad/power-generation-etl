#!/usr/bin/env python3
"""
Unified Power Generation Database Module
Handles ingestion for NPP, ENTSO-E, and EIA generation data into PostgreSQL.
"""

import json
import os
import sys
import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, InterfaceError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from validator import DataValidator, ValidationReport, save_report


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
        self.username = username or os.getenv("POSTGRES_USER", "postgres")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "")

        self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine, creating it if necessary."""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
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

    def create_extraction_metadata_table(self) -> bool:
        """Create extraction metadata table."""
        return self._execute_schema_file("extraction_metadata.sql")

    def create_all_tables(self) -> bool:
        """Create all generation tables including metadata."""
        success = True
        for table_type in ["npp", "entsoe", "eia", "extraction_metadata"]:
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

            # Insert only valid records with retry logic
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _insert_npp():
                    df.to_sql(
                        "npp_generation",
                        self.engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )

                self._execute_with_retry(_insert_npp)
                logger.success("Inserted NPP records", count=len(valid_records))
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
    ) -> Tuple[bool, Optional[ValidationReport]]:
        """Insert ENTSO-E data from JSONL file with validation.

        Returns:
            Tuple of (success, validation_report)
        """
        try:
            # Read JSONL data as list of dicts for validation
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f if line.strip()]

            if not data:
                logger.warning("No ENTSO-E data found", file=jsonl_file_path)
                return True, None

            # Add extraction run ID and current timestamp only if not already present
            if extraction_run_id is None:
                extraction_run_id = str(uuid.uuid4())
            created_at_ms = int(datetime.now().timestamp() * 1000)

            for record in data:
                if "extraction_run_id" not in record:
                    record["extraction_run_id"] = extraction_run_id
                if "created_at_ms" not in record:
                    record["created_at_ms"] = created_at_ms

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

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "entsoe", jsonl_file_path
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

            if not valid_records:
                logger.warning("No valid records to insert")
                return True, report

            # Convert to DataFrame for insertion
            df = pd.DataFrame(valid_records)

            # Ensure column order matches table schema
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

            # Reorder columns to match expected schema
            df = df[expected_columns]

            # Use PostgreSQL COPY for fast bulk insert with retry logic
            def _insert_entsoe():
                conn = self.engine.raw_connection()
                try:
                    cursor = conn.cursor()
                    # Create a StringIO object from the DataFrame
                    output = StringIO()
                    df.to_csv(output, sep="\t", header=False, index=False)
                    output.seek(0)

                    # Use COPY to insert data efficiently
                    cursor.copy_from(
                        output,
                        "entsoe_generation_data",
                        columns=expected_columns,
                        sep="\t",
                    )

                    conn.commit()
                finally:
                    conn.close()

            self._execute_with_retry(_insert_entsoe)
            logger.success("Inserted ENTSO-E records", count=len(df))
            return True, report

        except Exception as e:
            logger.error(
                "Failed to insert ENTSO-E data", file=jsonl_file_path, error=str(e)
            )
            return False, None

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

            # Insert only valid records with retry logic
            if valid_records:
                df = pd.DataFrame(valid_records)

                def _insert_eia():
                    df.to_sql(
                        "eia_generation_data",
                        self.engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )

                self._execute_with_retry(_insert_eia)
                logger.success("Inserted EIA records", count=len(valid_records))
            else:
                logger.warning("No valid records to insert")

            return True, report

        except Exception as e:
            logger.error("Failed to insert EIA data", error=str(e))
            return False, None

    def get_record_count(self, table_name: str) -> int:
        """Get total number of records in a specific table."""
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
        tables = ["npp_generation", "entsoe_generation_data", "eia_generation_data"]
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
            source: Data source ('npp', 'eia', 'entsoe')
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
