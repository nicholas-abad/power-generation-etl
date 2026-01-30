#!/usr/bin/env python3
"""
Unit tests for the data validation module.
"""

import json
import sys
import tempfile
import time
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from validator import (
    DataValidator,
    ValidationReport,
    ValidationResult,
    load_and_validate_jsonl,
    save_report,
)


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_valid_result(self):
        result = ValidationResult(valid=True, errors=[], record={"test": 1})
        assert result.valid is True
        assert result.errors == []
        assert result.record == {"test": 1}

    def test_invalid_result(self):
        result = ValidationResult(valid=False, errors=["error1", "error2"])
        assert result.valid is False
        assert len(result.errors) == 2


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_add_error(self):
        report = ValidationReport(source_file="test.jsonl")
        report.add_error("missing_field", 0, "missing required field: plant")
        report.add_error("missing_field", 1, "missing required field: plant")
        report.add_error("type_error", 2, "expected int")

        assert report.errors_by_type["missing_field"] == 2
        assert report.errors_by_type["type_error"] == 1
        assert len(report.sample_errors) == 3

    def test_sample_errors_limit(self):
        """Test that sample_errors is limited to 10 entries."""
        report = ValidationReport(source_file="test.jsonl")
        for i in range(15):
            report.add_error("error", i, f"error {i}")

        assert len(report.sample_errors) == 10


class TestDataValidatorNPP:
    """Tests for NPP data validation."""

    def setup_method(self):
        self.validator = DataValidator()
        self.current_time_ms = int(time.time() * 1000)
        self.valid_record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": self.current_time_ms,
            "timestamp_ms": self.current_time_ms - 1000,
            "plant": "Kudankulam",
            "plant_and_unit": "Kudankulam Unit 1",
            "generation_mwh": 1000.5,
            "unit": "Unit 1",
        }

    def test_valid_record(self):
        result = self.validator.validate_npp_record(self.valid_record)
        assert result.valid is True
        assert result.errors == []

    def test_missing_required_field(self):
        record = self.valid_record.copy()
        del record["plant"]
        result = self.validator.validate_npp_record(record)
        assert result.valid is False
        assert any("missing required field: plant" in e for e in result.errors)

    def test_invalid_uuid(self):
        record = self.valid_record.copy()
        record["extraction_run_id"] = "not-a-uuid"
        result = self.validator.validate_npp_record(record)
        assert result.valid is False
        assert any("invalid UUID format" in e for e in result.errors)

    def test_negative_generation(self):
        record = self.valid_record.copy()
        record["generation_mwh"] = -100.0
        result = self.validator.validate_npp_record(record)
        assert result.valid is False
        assert any("non-negative" in e for e in result.errors)

    def test_empty_plant_name(self):
        record = self.valid_record.copy()
        record["plant"] = ""
        result = self.validator.validate_npp_record(record)
        assert result.valid is False
        assert any("non-empty" in e for e in result.errors)

    def test_future_timestamp(self):
        record = self.valid_record.copy()
        # Set timestamp to 2 days in the future
        record["timestamp_ms"] = self.current_time_ms + (48 * 60 * 60 * 1000)
        result = self.validator.validate_npp_record(record)
        assert result.valid is False
        assert any("timestamp" in e for e in result.errors)

    def test_optional_unit_null(self):
        record = self.valid_record.copy()
        record["unit"] = None
        result = self.validator.validate_npp_record(record)
        assert result.valid is True

    def test_optional_unit_missing(self):
        record = self.valid_record.copy()
        del record["unit"]
        result = self.validator.validate_npp_record(record)
        assert result.valid is True


class TestDataValidatorEIA:
    """Tests for EIA data validation."""

    def setup_method(self):
        self.validator = DataValidator()
        self.current_time_ms = int(time.time() * 1000)
        self.valid_record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": self.current_time_ms,
            "timestamp_ms": self.current_time_ms - 1000,
            "utility_id": "12345",
            "plant_code": "PLANT001",
            "generator_id": "GEN01",
            "state": "CA",
            "fuel_source": "Natural Gas",
            "prime_mover": "CT",
            "energy_source": "NG",
            "net_generation_mwh": 5000.0,
        }

    def test_valid_record(self):
        result = self.validator.validate_eia_record(self.valid_record)
        assert result.valid is True
        assert result.errors == []

    def test_invalid_state_code(self):
        record = self.valid_record.copy()
        record["state"] = "California"
        result = self.validator.validate_eia_record(record)
        assert result.valid is False
        assert any("state code" in e for e in result.errors)

    def test_missing_generator_id(self):
        record = self.valid_record.copy()
        del record["generator_id"]
        result = self.validator.validate_eia_record(record)
        assert result.valid is False
        assert any("generator_id" in e for e in result.errors)


class TestDataValidatorENTSOE:
    """Tests for ENTSOE data validation."""

    def setup_method(self):
        self.validator = DataValidator()
        self.current_time_ms = int(time.time() * 1000)
        self.valid_record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": self.current_time_ms,
            "timestamp_ms": self.current_time_ms - 1000,
            "country_code": "DE",
            "psr_type": "B01",
            "plant_name": "Solar Farm 1",
            "fuel_type": "Solar",
            "data_type": "Actual",
            "generation_mw": 500.0,
            "resolution_minutes": 60,
        }

    def test_valid_record(self):
        result = self.validator.validate_entsoe_record(self.valid_record)
        assert result.valid is True
        assert result.errors == []

    def test_empty_country_code(self):
        record = self.valid_record.copy()
        record["country_code"] = ""
        result = self.validator.validate_entsoe_record(record)
        assert result.valid is False
        assert any("non-empty" in e for e in result.errors)

    def test_negative_generation(self):
        record = self.valid_record.copy()
        record["generation_mw"] = -50.0
        result = self.validator.validate_entsoe_record(record)
        assert result.valid is False

    def test_zero_resolution_minutes(self):
        """Test that resolution_minutes=0 fails positive validation."""
        record = self.valid_record.copy()
        record["resolution_minutes"] = 0
        result = self.validator.validate_entsoe_record(record)
        assert result.valid is False
        assert any("positive" in e for e in result.errors)

    def test_negative_resolution_minutes(self):
        """Test that negative resolution_minutes fails positive validation."""
        record = self.valid_record.copy()
        record["resolution_minutes"] = -15
        result = self.validator.validate_entsoe_record(record)
        assert result.valid is False


class TestFileValidation:
    """Tests for file-level validation with duplicate detection."""

    def setup_method(self):
        self.validator = DataValidator()
        self.current_time_ms = int(time.time() * 1000)

    def test_duplicate_detection_npp(self):
        """Test that duplicates are detected based on (timestamp_ms, plant_and_unit)."""
        records = [
            {
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 1000000,
                "plant": "Plant A",
                "plant_and_unit": "Plant A Unit 1",
                "generation_mwh": 100.0,
            },
            {
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440001",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 1000000,  # Same timestamp
                "plant": "Plant A",
                "plant_and_unit": "Plant A Unit 1",  # Same plant_and_unit
                "generation_mwh": 200.0,
            },
            {
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440002",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 1000000,
                "plant": "Plant A",
                "plant_and_unit": "Plant A Unit 2",  # Different unit
                "generation_mwh": 300.0,
            },
        ]

        valid_records, report = self.validator.validate_file(records, "npp")

        assert len(valid_records) == 2
        assert report.total_count == 3
        assert report.valid_count == 2
        assert report.duplicate_count == 1

    def test_mixed_valid_invalid(self):
        """Test file with mix of valid, invalid, and duplicate records."""
        records = [
            {
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 1000000,
                "plant": "Plant A",
                "plant_and_unit": "Plant A Unit 1",
                "generation_mwh": 100.0,
            },
            {
                # Invalid: missing plant
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440001",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 2000000,
                "plant_and_unit": "Plant B Unit 1",
                "generation_mwh": 200.0,
            },
            {
                # Invalid: negative generation
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440002",
                "created_at_ms": self.current_time_ms,
                "timestamp_ms": 3000000,
                "plant": "Plant C",
                "plant_and_unit": "Plant C Unit 1",
                "generation_mwh": -50.0,
            },
        ]

        valid_records, report = self.validator.validate_file(records, "npp")

        assert len(valid_records) == 1
        assert report.total_count == 3
        assert report.valid_count == 1
        assert report.invalid_count == 2
        assert report.duplicate_count == 0

    def test_empty_file(self):
        """Test validation of empty file."""
        valid_records, report = self.validator.validate_file([], "npp")

        assert len(valid_records) == 0
        assert report.total_count == 0
        assert report.valid_count == 0

    def test_unknown_source_type(self):
        """Test that unknown source type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown source type"):
            self.validator.validate_file([], "unknown")


class TestSaveReport:
    """Tests for report saving functionality."""

    def test_save_report(self):
        """Test that report is saved correctly as JSON."""
        report = ValidationReport(
            source_file="test.jsonl",
            total_count=100,
            valid_count=90,
            invalid_count=8,
            duplicate_count=2,
        )
        report.add_error("missing_field", 0, "missing required field: plant")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            save_report(report, output_path)

            with open(output_path) as f:
                saved = json.load(f)

            assert saved["source_file"] == "test.jsonl"
            assert saved["total_records"] == 100
            assert saved["valid_records"] == 90
            assert saved["invalid_records"] == 8
            assert saved["duplicate_records"] == 2
            assert "timestamp" in saved
            assert len(saved["sample_errors"]) == 1
        finally:
            Path(output_path).unlink(missing_ok=True)


class TestLoadAndValidateJSONL:
    """Tests for JSONL file loading and validation."""

    def test_load_and_validate(self):
        """Test loading and validating a JSONL file."""
        current_time_ms = int(time.time() * 1000)
        records = [
            {
                "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
                "created_at_ms": current_time_ms,
                "timestamp_ms": current_time_ms - 1000,
                "plant": "Plant A",
                "plant_and_unit": "Plant A Unit 1",
                "generation_mwh": 100.0,
            }
        ]

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
            jsonl_path = f.name

        try:
            valid_records, report = load_and_validate_jsonl(jsonl_path, "npp")

            assert len(valid_records) == 1
            assert report.valid_count == 1
            assert report.invalid_count == 0
        finally:
            Path(jsonl_path).unlink(missing_ok=True)


class TestTypeValidation:
    """Tests for type validation edge cases."""

    def setup_method(self):
        self.validator = DataValidator()

    def test_boolean_not_int(self):
        """Test that boolean is not accepted as int."""
        current_time_ms = int(time.time() * 1000)
        record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": True,  # Boolean instead of int
            "timestamp_ms": current_time_ms,
            "plant": "Plant A",
            "plant_and_unit": "Plant A Unit 1",
            "generation_mwh": 100.0,
        }
        result = self.validator.validate_npp_record(record)
        assert result.valid is False

    def test_string_not_accepted_as_float(self):
        """Test that string is not accepted as float."""
        current_time_ms = int(time.time() * 1000)
        record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": current_time_ms,
            "timestamp_ms": current_time_ms,
            "plant": "Plant A",
            "plant_and_unit": "Plant A Unit 1",
            "generation_mwh": "100.0",  # String instead of float
        }
        result = self.validator.validate_npp_record(record)
        assert result.valid is False

    def test_int_accepted_as_float(self):
        """Test that int is accepted where float is expected."""
        current_time_ms = int(time.time() * 1000)
        record = {
            "extraction_run_id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at_ms": current_time_ms,
            "timestamp_ms": current_time_ms,
            "plant": "Plant A",
            "plant_and_unit": "Plant A Unit 1",
            "generation_mwh": 100,  # Int instead of float
        }
        result = self.validator.validate_npp_record(record)
        assert result.valid is True
