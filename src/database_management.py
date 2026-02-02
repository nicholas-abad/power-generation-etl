#!/usr/bin/env python3
"""
Database Management CLI for Power Generation ETL
Unified management interface for NPP, ENTSO-E, EIA, ONS, and OE data.
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from database import create_power_generation_database
from sqlalchemy import text


# Configure loguru for CLI output
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
)


def setup_database(table_type: str = "all"):
    """Initialize database and create tables."""
    logger.info(f"Setting up database tables: {table_type}")

    db = create_power_generation_database()

    # Create database if it doesn't exist
    if not db.test_connection():
        logger.info("Creating database...")
        if not db.create_database_if_not_exists():
            logger.error("Failed to create database")
            return False

        # Test connection again
        if not db.test_connection():
            logger.error("Database connection failed after creation")
            return False

    # Create tables based on type
    success = False
    if table_type == "all":
        success = db.create_all_tables()
    elif table_type == "npp":
        success = db.create_npp_table()
    elif table_type == "entsoe":
        success = db.create_entsoe_table()
    elif table_type == "eia":
        success = db.create_eia_table()
    elif table_type == "ons":
        success = db.create_ons_table()
    elif table_type == "oe":
        success = db.create_oe_table()
    elif table_type == "oe_facility":
        success = db.create_oe_facility_table()
    else:
        logger.error(f"Unknown table type: {table_type}")
        return False

    db.close()
    return success


def update_schema(table_type: str = "entsoe"):
    """Update existing table schemas."""
    logger.info(f"Updating schema for: {table_type}")

    db = create_power_generation_database()

    if not db.test_connection():
        logger.error("Database connection failed")
        return False

    try:
        with db.engine.connect() as conn:
            if table_type == "entsoe" or table_type == "all":
                # Update ENTSO-E table to handle longer country codes
                conn.execute(
                    text(
                        "ALTER TABLE entsoe_generation_data ALTER COLUMN country_code TYPE VARCHAR(32)"
                    )
                )
                conn.commit()
                logger.success(
                    "Updated entsoe_generation_data.country_code to VARCHAR(32)"
                )

            if table_type == "all":
                # Add other schema updates here as needed
                pass

        db.close()
        return True

    except Exception as e:
        logger.error(f"Failed to update schema: {e}")
        db.close()
        return False


def load_data(
    data_source: str,
    jsonl_file: str,
    validation_report: str = None,
    strict: bool = False,
):
    """Load data from JSONL file into database with validation.

    Args:
        data_source: Type of data source (npp, entsoe, eia)
        jsonl_file: Path to JSONL file
        validation_report: Optional path to save validation report
        strict: If True, fail on any validation errors
    """
    logger.info(f"Loading {data_source} data from {jsonl_file}")

    if not Path(jsonl_file).exists():
        logger.error(f"File not found: {jsonl_file}")
        return False

    db = create_power_generation_database()

    if not db.test_connection():
        logger.error("Database connection failed")
        return False

    success = False
    report = None

    if data_source == "npp":
        success, report = db.insert_npp_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    elif data_source == "entsoe":
        success, report = db.insert_entsoe_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    elif data_source == "eia":
        success, report = db.insert_eia_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    elif data_source == "ons":
        success, report = db.insert_ons_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    elif data_source == "oe":
        success, report = db.insert_oe_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    elif data_source == "oe_facility":
        success, report = db.insert_oe_facility_jsonl_data(
            jsonl_file, validation_report_path=validation_report
        )
    else:
        logger.error(f"Unknown data source: {data_source}")
        db.close()
        return False

    # Check strict mode
    if strict and report and (report.invalid_count > 0 or report.duplicate_count > 0):
        logger.error("Strict mode: failing due to validation errors")
        db.close()
        return False

    if success:
        logger.success("Data loaded successfully!")

    db.close()
    return success


def show_database_stats():
    """Show database statistics."""
    logger.info("Database Statistics:")

    db = create_power_generation_database()

    if not db.test_connection():
        logger.error("Database connection failed")
        return False

    counts = db.get_all_record_counts()
    total_records = sum(counts.values())

    logger.info(f"Total records across all tables: {total_records:,}")
    for table, count in counts.items():
        logger.info(f"  {table}: {count:,} records")

    db.close()
    return True


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(
        description="Power Generation Database Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python database_management.py setup all
  python database_management.py setup npp
  python database_management.py load-data npp ./data/npp_data.jsonl
  python database_management.py load-data entsoe ./data/entsoe_data.jsonl
  python database_management.py load-data eia ./data/eia_data.jsonl --strict
  python database_management.py load-data ons ./data/ons_data_etl.jsonl
  python database_management.py load-data oe ./data/oe_data_etl.jsonl
  python database_management.py load-data oe_facility ./data/oe_facility_data_etl.jsonl
  python database_management.py load-data npp ./data/npp_data.jsonl --validation-report report.json
  python database_management.py stats
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup command
    setup_parser = subparsers.add_parser(
        "setup", help="Initialize database and create tables"
    )
    setup_parser.add_argument(
        "table_type",
        choices=["all", "npp", "entsoe", "eia", "ons", "oe", "oe_facility"],
        default="all",
        nargs="?",
        help="Type of tables to create (default: all)",
    )

    # Update schema command
    update_parser = subparsers.add_parser(
        "update-schema", help="Update existing table schemas"
    )
    update_parser.add_argument(
        "table_type",
        choices=["all", "npp", "entsoe", "eia", "ons", "oe", "oe_facility"],
        default="entsoe",
        nargs="?",
        help="Schema to update (default: entsoe)",
    )

    # Load data command
    load_parser = subparsers.add_parser(
        "load-data", help="Load JSONL data into database with validation"
    )
    load_parser.add_argument(
        "data_source", choices=["npp", "entsoe", "eia", "ons", "oe", "oe_facility"], help="Type of data source"
    )
    load_parser.add_argument("jsonl_file", help="Path to JSONL file")
    load_parser.add_argument(
        "--validation-report",
        "-r",
        help="Path to save validation report JSON",
        default=None,
    )
    load_parser.add_argument(
        "--strict",
        "-s",
        action="store_true",
        help="Fail on any validation errors (default: skip invalid records)",
    )

    # Stats command
    subparsers.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute commands
    success = False

    if args.command == "setup":
        success = setup_database(args.table_type)
    elif args.command == "update-schema":
        success = update_schema(args.table_type)
    elif args.command == "load-data":
        success = load_data(
            args.data_source,
            args.jsonl_file,
            validation_report=args.validation_report,
            strict=args.strict,
        )
    elif args.command == "stats":
        success = show_database_stats()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
