"""Tests for the EnergyDataConverter module."""

import json
import os
import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from pipeline.data_processing.energy_data_converter import EnergyDataConverter
from pipeline.tests.data_processing.conftest import load_fixture

class TestEnergyDataConverter:
    """Tests for the EnergyDataConverter class."""
    
    @pytest.fixture
    def converter(self):
        """Create a converter with a temporary output directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield EnergyDataConverter(output_dir=temp_dir)
    
    @pytest.fixture
    def gas_consumption_data(self):
        """Load the gas consumption test data."""
        return load_fixture("gas_consumption_test.json")
    
    @pytest.fixture
    def gas_consumption_file_path(self):
        """Get the path to the gas consumption test data file."""
        fixtures_dir = Path(__file__).parent / "fixtures"
        return fixtures_dir / "gas_consumption_test.json"
    
    def test_init_creates_output_dir(self):
        """Test that the constructor creates the output directory."""
        output_dir = Path(tempfile.mkdtemp()) / "test_output"
        assert not output_dir.exists()
        
        converter = EnergyDataConverter(output_dir=str(output_dir))
        assert output_dir.exists()
    
    def test_convert_to_jsonl_from_dict(self, converter, gas_consumption_data):
        """Test converting a dictionary to JSONL."""
        output_file = Path(converter.output_dir) / "test_output.jsonl"
        
        result = converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file
        )
        
        # Check that the result is the output file path
        assert result == str(output_file)
        
        # Check that the file exists
        assert output_file.exists()
        
        # Check file contents
        lines = output_file.read_text().strip().split('\n')
        assert len(lines) == 8  # Should have 8 readings
        
        # Check the first line
        first_record = json.loads(lines[0])
        assert first_record["resource_id"] == "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4"
        assert first_record["resource_name"] == "gas consumption"
        assert first_record["timestamp"] == 1748217600
        assert "timestamp_iso" in first_record
        assert first_record["value"] == 0
        
        # Check a non-zero reading
        third_record = json.loads(lines[2])
        assert third_record["timestamp"] == 1748232000
        assert third_record["value"] == 4.08431053
    
    def test_convert_to_jsonl_from_file(self, converter, gas_consumption_file_path):
        """Test converting a file to JSONL."""
        output_file = Path(converter.output_dir) / "test_output_from_file.jsonl"
        
        result = converter.convert_to_jsonl(
            gas_consumption_file_path, 
            output_file=output_file
        )
        
        # Check that the file exists
        assert Path(result).exists()
        
        # Check file contents
        with open(result, 'r') as f:
            lines = f.read().strip().split('\n')
        
        assert len(lines) == 8  # Should have 8 readings
        
        # Parse one of the lines and check the contents
        record = json.loads(lines[4])
        assert record["resource_id"] == "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4"
        assert record["timestamp"] == 1748246400
        assert record["value"] == 0.1346476
    
    def test_convert_to_jsonl_without_output_file(self, converter, gas_consumption_data):
        """Test converting without specifying an output file."""
        result = converter.convert_to_jsonl(gas_consumption_data)
        
        # Check that the file exists
        assert Path(result).exists()
        
        # Check that the filename contains the resource name
        assert "gas_consumption" in Path(result).stem
        
        # Check file contents
        with open(result, 'r') as f:
            lines = f.read().strip().split('\n')
        
        assert len(lines) == 8  # Should have 8 readings
    
    def test_timestamp_conversion(self, converter, gas_consumption_data):
        """Test that timestamps are correctly converted to ISO format."""
        output_file = Path(converter.output_dir) / "timestamp_test.jsonl"
        
        converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file
        )
        
        # Check file contents
        with open(output_file, 'r') as f:
            lines = f.read().strip().split('\n')
        
        # Check the ISO timestamp format
        first_record = json.loads(lines[0])
        timestamp = first_record["timestamp"]
        iso_timestamp = first_record["timestamp_iso"]
        
        # Convert the timestamp manually and compare
        expected_iso = datetime.fromtimestamp(timestamp).isoformat()
        assert iso_timestamp == expected_iso
    
    def test_metadata_preservation(self, converter, gas_consumption_data):
        """Test that all metadata is preserved in the JSONL output."""
        output_file = Path(converter.output_dir) / "metadata_test.jsonl"
        
        converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file
        )
        
        # Check file contents
        with open(output_file, 'r') as f:
            first_record = json.loads(f.readline())
        
        # Check all metadata fields
        assert first_record["resource_id"] == gas_consumption_data["resource_id"]
        assert first_record["resource_name"] == gas_consumption_data["resource_name"]
        assert first_record["units"] == gas_consumption_data["resource_unit"]
        assert first_record["classifier"] == gas_consumption_data["resource_classifier"]
        assert first_record["from_date"] == gas_consumption_data["start_date"]
        assert first_record["to_date"] == gas_consumption_data["end_date"]
        assert first_record["period"] == gas_consumption_data["period"]
    
    def test_convert_batch_to_jsonl(self, converter, gas_consumption_file_path, tmp_path):
        """Test converting multiple files in a batch."""
        # Create a second test file
        second_file = tmp_path / "second_test.json"
        with open(gas_consumption_file_path, 'r') as src, open(second_file, 'w') as dst:
            data = json.load(src)
            data["resource_name"] = "electricity consumption"
            json.dump(data, dst)
        
        # Run batch conversion
        results = converter.convert_batch_to_jsonl([
            str(gas_consumption_file_path),
            str(second_file)
        ])
        
        # Check that we got two results
        assert len(results) == 2
        
        # Check that both files exist
        for result in results:
            assert Path(result).exists()
        
        # Check contents of second file
        with open(results[1], 'r') as f:
            first_record = json.loads(f.readline())
        
        assert first_record["resource_name"] == "electricity consumption"
    
    def test_different_input_formats(self, converter, tmp_path):
        """Test that the converter can handle different input formats."""
        # Create a test file with a different format
        different_format = {
            "resourceId": "different-id-123",
            "name": "different format",
            "resourceTypeId": "test-type",
            "query": {
                "from": "2025-01-01T00:00:00",
                "to": "2025-01-02T00:00:00",
                "period": "P1D"
            },
            "data": [
                [1735689600, 10.5],
                [1735776000, 12.3]
            ]
        }
        
        test_file = tmp_path / "different_format.json"
        with open(test_file, 'w') as f:
            json.dump(different_format, f)
        
        output_file = Path(converter.output_dir) / "different_format.jsonl"
        
        result = converter.convert_to_jsonl(
            test_file, 
            output_file=output_file
        )
        
        # Check file contents
        with open(result, 'r') as f:
            lines = f.read().strip().split('\n')
        
        assert len(lines) == 2
        
        # Check the content
        record = json.loads(lines[0])
        assert record["resource_id"] == "different-id-123"
        assert record["resource_name"] == "different format"
        assert record["resource_type"] == "test-type"
        assert record["from_date"] == "2025-01-01T00:00:00"
        assert record["value"] == 10.5
    
    def test_handle_missing_file(self, converter):
        """Test handling of a non-existent input file."""
        with pytest.raises(FileNotFoundError):
            converter.convert_to_jsonl("non_existent_file.json")

    def test_handle_invalid_json(self, converter, tmp_path):
        """Test handling of invalid JSON."""
        # Create an invalid JSON file
        invalid_file = tmp_path / "invalid.json"
        with open(invalid_file, 'w') as f:
            f.write("{invalid json")
        
        with pytest.raises(json.JSONDecodeError):
            converter.convert_to_jsonl(invalid_file)

    def test_handle_empty_data(self, converter, tmp_path):
        """Test handling of empty data array."""
        # Create a file with empty data
        empty_data = {
            "resource_id": "test-id",
            "resource_name": "test resource",
            "readings": []
        }
        
        empty_file = tmp_path / "empty.json"
        with open(empty_file, 'w') as f:
            json.dump(empty_data, f)
        
        output_file = Path(converter.output_dir) / "empty_output.jsonl"
        
        result = converter.convert_to_jsonl(
            empty_file, 
            output_file=output_file
        )
        
        # File should exist but be empty (or contain only newlines)
        assert Path(result).exists()
        with open(result, 'r') as f:
            content = f.read().strip()
        
        assert content == ""