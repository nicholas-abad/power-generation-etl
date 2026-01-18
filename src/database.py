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
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

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
        self, jsonl_file_path: str, extraction_run_id: str = None
    ) -> bool:
        """Insert NPP data from JSONL file.

        Harmonized format matching EIA/ENTSOE schema:
        - extraction_run_id (UUID)
        - created_at_ms (BIGINT milliseconds)
        - timestamp_ms (BIGINT milliseconds)
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f]

            if not data:
                print("⚠️ No NPP data found in JSONL file")
                return True

            # Generate extraction metadata (matches EIA/ENTSOE pattern)
            if extraction_run_id is None:
                extraction_run_id = str(uuid.uuid4())
            created_at_ms = int(datetime.now().timestamp() * 1000)

            # Transform data to match harmonized schema
            for record in data:
                # Add extraction metadata
                record["extraction_run_id"] = extraction_run_id
                record["created_at_ms"] = created_at_ms

                # Convert date (Unix seconds) to timestamp_ms (milliseconds)
                if "date" in record:
                    timestamp_seconds = record.pop("date")  # Remove old field
                    record["timestamp_ms"] = int(timestamp_seconds * 1000)

                # Remove old scrape_id field if present
                record.pop("scrape_id", None)

            # Bulk insert using pandas (matches EIA pattern)
            df = pd.DataFrame(data)
            df.to_sql(
                "npp_generation",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            print(f"✅ Inserted {len(data)} NPP records")
            return True

        except Exception as e:
            print(f"❌ Failed to insert NPP data: {e}")
            return False

    def insert_entsoe_jsonl_data(
        self, jsonl_file_path: str, extraction_run_id: str = None
    ) -> bool:
        """Insert ENTSO-E data from JSONL file."""
        try:
            # Read JSONL file into DataFrame
            df = pd.read_json(jsonl_file_path, lines=True)

            if df.empty:
                print(f"⚠️ No ENTSO-E data found in {jsonl_file_path}")
                return True

            # Add extraction run ID and current timestamp only if not already present
            if "extraction_run_id" not in df.columns:
                if extraction_run_id is None:
                    extraction_run_id = str(uuid.uuid4())
                df["extraction_run_id"] = extraction_run_id

            if "created_at_ms" not in df.columns:
                df["created_at_ms"] = int(datetime.now().timestamp() * 1000)

            # Convert datetime strings to Unix timestamps in milliseconds
            if "timestamp_ms" in df.columns:
                # Handle both string datetime and existing timestamp formats
                def convert_to_timestamp_ms(ts):
                    if isinstance(ts, pd.Timestamp):
                        # Convert pandas Timestamp directly to milliseconds
                        return int(ts.timestamp() * 1000)
                    elif isinstance(ts, str):
                        try:
                            # Parse datetime string and convert to milliseconds
                            dt = pd.to_datetime(ts, errors="coerce")
                            if pd.isnull(dt):
                                raise ValueError(f"Invalid datetime format: {ts}")
                            return int(dt.timestamp() * 1000)
                        except Exception as e:
                            print(f"⚠️ Skipping invalid timestamp: {ts} ({e})")
                            return None  # Return None for invalid timestamps
                    elif pd.notnull(ts):
                        return int(ts)  # Assume it's already a valid timestamp
                    else:
                        return None  # Handle NaN or None values

                df["timestamp_ms"] = df["timestamp_ms"].apply(convert_to_timestamp_ms)

                # Drop rows with invalid timestamps
                df = df.dropna(subset=["timestamp_ms"]).reset_index(drop=True)

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
            return True

        except Exception as e:
            print(f"❌ Failed to insert ENTSO-E data from {jsonl_file_path}: {e}")
            return False

    def insert_eia_jsonl_data(
        self, jsonl_file_path: str, extraction_run_id: str = None
    ) -> bool:
        """Insert EIA data from JSONL file.

        Supports both legacy format (without metadata) and ETL-compatible format
        (with extraction_run_id and created_at_ms already included).
        """
        try:
            # Read JSONL data
            with open(jsonl_file_path, "r") as f:
                data = [json.loads(line) for line in f]

            if not data:
                print("⚠️ No EIA data found in JSONL file")
                return True

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

            # Bulk insert using pandas
            df = pd.DataFrame(data)
            df.to_sql(
                "eia_generation_data",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            print(f"✅ Inserted {len(data)} EIA records")
            return True

        except Exception as e:
            print(f"❌ Failed to insert EIA data: {e}")
            return False

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
