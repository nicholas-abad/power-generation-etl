#!/usr/bin/env python3
"""
Unified Power Generation Database Module
Handles ingestion for NPP, ENTSO-E, and EIA generation data into PostgreSQL.
"""

import json
import os
import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from validator import DataValidator, ValidationReport, save_report

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


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

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✅ Database connection successful: {self.database}")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
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
                    print(f"✅ Created database: {self.database}")
                else:
                    print(f"ℹ️ Database already exists: {self.database}")

            default_engine.dispose()
            return True

        except Exception as e:
            print(f"❌ Failed to create database: {e}")
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
                print(f"✅ Schema executed successfully: {schema_filename}")

            return True

        except FileNotFoundError:
            print(f"❌ Schema file not found: {schema_path}")
            return False
        except Exception as e:
            print(f"❌ Failed to execute schema {schema_filename}: {e}")
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

    def create_all_tables(self) -> bool:
        """Create all generation tables."""
        success = True
        for table_type in ["npp", "entsoe", "eia"]:
            try:
                method = getattr(self, f"create_{table_type}_table")
                if not method():
                    success = False
                    print(f"❌ Failed to create {table_type} table")
            except Exception as e:
                print(f"❌ Error creating {table_type} table: {e}")
                success = False

        if success:
            print("✅ All tables created successfully")
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
                print("⚠️ No NPP data found in JSONL file")
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
            print(
                f"📋 Validation: {report.valid_count}/{report.total_count} records valid"
            )
            if report.invalid_count > 0:
                print(f"⚠️ Skipped {report.invalid_count} invalid records")
            if report.duplicate_count > 0:
                print(f"⚠️ Skipped {report.duplicate_count} duplicate records")

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records
            if valid_records:
                df = pd.DataFrame(valid_records)
                df.to_sql(
                    "npp_generation",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                print(f"✅ Inserted {len(valid_records)} NPP records")
            else:
                print("⚠️ No valid records to insert")

            return True, report

        except Exception as e:
            print(f"❌ Failed to insert NPP data: {e}")
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
                print(f"⚠️ No ENTSO-E data found in {jsonl_file_path}")
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
            print(
                f"📋 Validation: {report.valid_count}/{report.total_count} records valid"
            )
            if report.invalid_count > 0:
                print(f"⚠️ Skipped {report.invalid_count} invalid records")
            if report.duplicate_count > 0:
                print(f"⚠️ Skipped {report.duplicate_count} duplicate records")

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            if not valid_records:
                print("⚠️ No valid records to insert")
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
            ]

            # Reorder columns to match expected schema
            df = df[expected_columns]

            # Use PostgreSQL COPY for fast bulk insert
            conn = self.engine.raw_connection()
            try:
                cursor = conn.cursor()
                # Create a StringIO object from the DataFrame
                output = StringIO()
                df.to_csv(output, sep="\t", header=False, index=False)
                output.seek(0)

                # Use COPY to insert data efficiently
                cursor.copy_from(
                    output, "entsoe_generation_data", columns=expected_columns, sep="\t"
                )

                conn.commit()
                print(f"✅ Inserted {len(df)} ENTSO-E records")
            finally:
                conn.close()
            return True, report

        except Exception as e:
            print(f"❌ Failed to insert ENTSO-E data from {jsonl_file_path}: {e}")
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
                print("⚠️ No EIA data found in JSONL file")
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

            # Validate data
            validator = DataValidator()
            valid_records, report = validator.validate_file(
                data, "eia", jsonl_file_path
            )

            # Log validation summary
            print(
                f"📋 Validation: {report.valid_count}/{report.total_count} records valid"
            )
            if report.invalid_count > 0:
                print(f"⚠️ Skipped {report.invalid_count} invalid records")
            if report.duplicate_count > 0:
                print(f"⚠️ Skipped {report.duplicate_count} duplicate records")

            # Save validation report if requested
            if validation_report_path:
                save_report(report, validation_report_path)

            # Insert only valid records
            if valid_records:
                df = pd.DataFrame(valid_records)
                df.to_sql(
                    "eia_generation_data",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                print(f"✅ Inserted {len(valid_records)} EIA records")
            else:
                print("⚠️ No valid records to insert")

            return True, report

        except Exception as e:
            print(f"❌ Failed to insert EIA data: {e}")
            return False, None

    def get_record_count(self, table_name: str) -> int:
        """Get total number of records in a specific table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                print(f"📊 Total records in {table_name}: {count:,}")
                return count
        except Exception as e:
            print(f"❌ Failed to get record count for {table_name}: {e}")
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
