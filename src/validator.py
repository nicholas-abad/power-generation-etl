#!/usr/bin/env python3
"""
Data Validation Module for Power Generation ETL
Validates incoming JSONL data before database insertion.
"""

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger


# UUID regex pattern for validation
UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Valid US state codes (50 states + DC + territories)
US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
}

# Current timestamp in milliseconds (for future date validation)
# Allow 1 day buffer for timezone differences
MAX_FUTURE_BUFFER_MS = 24 * 60 * 60 * 1000


@dataclass
class ValidationResult:
    """Result of validating a single record."""

    valid: bool
    errors: List[str] = field(default_factory=list)
    record: Optional[Dict[str, Any]] = None


@dataclass
class ValidationReport:
    """Summary report of validation results for a file."""

    source_file: str
    total_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    sample_errors: List[Dict[str, Any]] = field(default_factory=list)

    def add_error(self, error_type: str, record_index: int, details: str):
        """Add an error to the report."""
        self.errors_by_type[error_type] = self.errors_by_type.get(error_type, 0) + 1
        if len(self.sample_errors) < 10:
            self.sample_errors.append(
                {
                    "record_index": record_index,
                    "error_type": error_type,
                    "details": details,
                }
            )


# Schema definitions for each data source
NPP_SCHEMA = {
    "required_fields": {
        "extraction_run_id": {"type": "str", "validation": "uuid"},
        "created_at_ms": {"type": "int", "validation": "positive_timestamp"},
        "timestamp_ms": {"type": "int", "validation": "positive_timestamp"},
        "plant": {"type": "str", "validation": "non_empty"},
        "plant_and_unit": {"type": "str", "validation": "non_empty"},
        "generation_mwh": {"type": "float", "validation": "non_negative"},
    },
    "optional_fields": {
        "unit": {"type": "str_or_null_or_number", "validation": None},
        "resolution_minutes": {"type": "int_or_null", "validation": None},
    },
    "duplicate_key": ("timestamp_ms", "plant_and_unit"),
}

EIA_SCHEMA = {
    "required_fields": {
        "extraction_run_id": {"type": "str", "validation": "uuid"},
        "created_at_ms": {"type": "int", "validation": "positive_timestamp"},
        "timestamp_ms": {"type": "int", "validation": "positive_timestamp"},
        "utility_id": {"type": "int_or_str", "validation": None},
        "plant_code": {"type": "int_or_str", "validation": None},
        "generator_id": {"type": "int_or_str", "validation": None},
        "state": {"type": "str", "validation": "state_code"},
        "prime_mover": {"type": "str", "validation": "non_empty"},
        "net_generation_mwh": {"type": "float", "validation": "non_negative"},
    },
    "optional_fields": {
        "fuel_source": {"type": "str_or_null", "validation": None},
        "energy_source": {"type": "str_or_null", "validation": None},
        "resolution_minutes": {"type": "int_or_null", "validation": None},
        "in_gcpt_crosswalk": {"type": "bool_or_null", "validation": None},
        "eia_plant_unit_id": {"type": "str_or_null", "validation": None},
    },
    "duplicate_key": ("timestamp_ms", "plant_code", "generator_id"),
}

ENTSOE_SCHEMA = {
    "required_fields": {
        "extraction_run_id": {"type": "str", "validation": "uuid"},
        "created_at_ms": {"type": "int", "validation": "positive_timestamp"},
        "timestamp_ms": {"type": "int", "validation": "positive_timestamp"},
        "country_code": {"type": "str", "validation": "non_empty"},
        "psr_type": {"type": "str", "validation": "non_empty"},
        "plant_name": {"type": "str", "validation": "non_empty"},
        "fuel_type": {"type": "str", "validation": "non_empty"},
        "data_type": {"type": "str", "validation": "non_empty"},
        "generation_mw": {"type": "float", "validation": "non_negative"},
        "resolution_minutes": {"type": "int", "validation": "positive"},
    },
    "optional_fields": {},
    "duplicate_key": ("timestamp_ms", "country_code", "psr_type", "plant_name"),
}


class DataValidator:
    """Validates power generation data records."""

    def __init__(self):
        self.schemas = {
            "npp": NPP_SCHEMA,
            "eia": EIA_SCHEMA,
            "entsoe": ENTSOE_SCHEMA,
        }

    def _is_valid_uuid(self, value: str) -> bool:
        """Check if value is a valid UUID."""
        if not isinstance(value, str):
            return False
        return bool(UUID_PATTERN.match(value))

    def _is_positive_timestamp(self, value: Any) -> bool:
        """Check if value is a positive timestamp (not in the future)."""
        if not isinstance(value, (int, float)):
            return False
        if value <= 0:
            return False
        # Check not too far in the future
        current_ms = int(time.time() * 1000)
        if value > current_ms + MAX_FUTURE_BUFFER_MS:
            return False
        return True

    def _is_non_empty_string(self, value: Any) -> bool:
        """Check if value is a non-empty string."""
        return isinstance(value, str) and len(value.strip()) > 0

    def _is_state_code(self, value: Any) -> bool:
        """Check if value is a valid US state code."""
        return isinstance(value, str) and value.upper() in US_STATE_CODES

    def _is_non_negative(self, value: Any) -> bool:
        """Check if value is a non-negative number."""
        if not isinstance(value, (int, float)):
            return False
        return value >= 0

    def _check_type(self, value: Any, expected_type: str) -> Tuple[bool, str]:
        """Check if value matches expected type."""
        if expected_type == "str":
            if not isinstance(value, str):
                return False, f"expected string, got {type(value).__name__}"
        elif expected_type == "int":
            if not isinstance(value, int) or isinstance(value, bool):
                return False, f"expected int, got {type(value).__name__}"
        elif expected_type == "float":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return False, f"expected float, got {type(value).__name__}"
        elif expected_type == "str_or_null":
            if value is not None and not isinstance(value, str):
                return False, f"expected string or null, got {type(value).__name__}"
        elif expected_type == "int_or_str":
            if not isinstance(value, (int, str)) or isinstance(value, bool):
                return False, f"expected int or string, got {type(value).__name__}"
        elif expected_type == "str_or_null_or_number":
            if value is not None and not isinstance(value, (str, int, float)):
                return (
                    False,
                    f"expected string, number, or null, got {type(value).__name__}",
                )
        elif expected_type == "int_or_null":
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                return False, f"expected int or null, got {type(value).__name__}"
        elif expected_type == "bool_or_null":
            if value is not None and not isinstance(value, bool):
                return False, f"expected bool or null, got {type(value).__name__}"
        return True, ""

    def _check_validation(self, value: Any, validation: str) -> Tuple[bool, str]:
        """Check if value passes validation rule."""
        if validation == "uuid":
            if not self._is_valid_uuid(value):
                return False, "invalid UUID format"
        elif validation == "positive_timestamp":
            if not self._is_positive_timestamp(value):
                return False, "invalid timestamp (must be positive and not in future)"
        elif validation == "non_empty":
            if not self._is_non_empty_string(value):
                return False, "must be non-empty string"
        elif validation == "state_code":
            if not self._is_state_code(value):
                return False, "must be 2-character state code"
        elif validation == "non_negative":
            if not self._is_non_negative(value):
                return False, "must be non-negative number"
        elif validation == "positive":
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
                return False, "must be a positive number"
        return True, ""

    def _validate_record(
        self, record: Dict[str, Any], schema: Dict[str, Any]
    ) -> ValidationResult:
        """Validate a single record against a schema."""
        errors = []

        # Check required fields
        for field_name, field_spec in schema["required_fields"].items():
            if field_name not in record:
                errors.append(f"missing required field: {field_name}")
                continue

            value = record[field_name]

            # Type check
            type_ok, type_error = self._check_type(value, field_spec["type"])
            if not type_ok:
                errors.append(f"{field_name}: {type_error}")
                continue

            # Validation check
            if field_spec["validation"]:
                valid_ok, valid_error = self._check_validation(
                    value, field_spec["validation"]
                )
                if not valid_ok:
                    errors.append(f"{field_name}: {valid_error}")

        # Check optional fields if present
        for field_name, field_spec in schema.get("optional_fields", {}).items():
            if field_name in record:
                value = record[field_name]
                type_ok, type_error = self._check_type(value, field_spec["type"])
                if not type_ok:
                    errors.append(f"{field_name}: {type_error}")

        return ValidationResult(valid=len(errors) == 0, errors=errors, record=record)

    def validate_npp_record(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a single NPP record."""
        return self._validate_record(record, NPP_SCHEMA)

    def validate_eia_record(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a single EIA record."""
        return self._validate_record(record, EIA_SCHEMA)

    def validate_entsoe_record(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a single ENTSOE record."""
        return self._validate_record(record, ENTSOE_SCHEMA)

    def _get_duplicate_key(
        self, record: Dict[str, Any], key_fields: Tuple[str, ...]
    ) -> Optional[Tuple]:
        """Extract duplicate detection key from record."""
        try:
            return tuple(record.get(field) for field in key_fields)
        except Exception:
            return None

    def validate_file(
        self, records: List[Dict[str, Any]], source_type: str, source_file: str = ""
    ) -> Tuple[List[Dict[str, Any]], ValidationReport]:
        """
        Validate all records from a file.

        Args:
            records: List of record dictionaries
            source_type: One of 'npp', 'eia', 'entsoe'
            source_file: Path to source file (for reporting)

        Returns:
            Tuple of (valid_records, validation_report)
        """
        if source_type not in self.schemas:
            raise ValueError(f"Unknown source type: {source_type}")

        schema = self.schemas[source_type]
        report = ValidationReport(source_file=source_file, total_count=len(records))

        valid_records = []
        seen_keys: Set[Tuple] = set()
        duplicate_key_fields = schema["duplicate_key"]

        # Get the appropriate validation method
        validate_method = getattr(self, f"validate_{source_type}_record")

        for idx, record in enumerate(records):
            # Validate the record
            result = validate_method(record)

            if not result.valid:
                report.invalid_count += 1
                for error in result.errors:
                    error_type = error.split(":")[0] if ":" in error else error
                    report.add_error(error_type, idx, error)
                continue

            # Check for duplicates
            dup_key = self._get_duplicate_key(record, duplicate_key_fields)
            if dup_key and dup_key in seen_keys:
                report.duplicate_count += 1
                report.add_error(
                    "duplicate",
                    idx,
                    f"duplicate key: {duplicate_key_fields} = {dup_key}",
                )
                continue

            if dup_key:
                seen_keys.add(dup_key)

            valid_records.append(record)
            report.valid_count += 1

        return valid_records, report


def save_report(report: ValidationReport, output_path: str) -> None:
    """
    Save validation report as JSON.

    Args:
        report: ValidationReport to save
        output_path: Path to output JSON file
    """
    report_dict = {
        "timestamp": datetime.now().isoformat(),
        "source_file": report.source_file,
        "total_records": report.total_count,
        "valid_records": report.valid_count,
        "invalid_records": report.invalid_count,
        "duplicate_records": report.duplicate_count,
        "errors_by_type": report.errors_by_type,
        "sample_errors": report.sample_errors,
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report_dict, f, indent=2)

    logger.info(f"Validation report saved to: {output_path}")


def load_and_validate_jsonl(
    jsonl_file_path: str, source_type: str
) -> Tuple[List[Dict[str, Any]], ValidationReport]:
    """
    Load and validate a JSONL file.

    Args:
        jsonl_file_path: Path to JSONL file
        source_type: One of 'npp', 'eia', 'entsoe'

    Returns:
        Tuple of (valid_records, validation_report)
    """
    with open(jsonl_file_path, "r") as f:
        records = [json.loads(line) for line in f if line.strip()]

    validator = DataValidator()
    return validator.validate_file(records, source_type, jsonl_file_path)
