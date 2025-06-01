import json
import os
import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from pipeline.data_processing.jsonl_converter import EnergyDataConverter
from pipeline.tests.data_processing.conftest import load_fixture

class TestEnergyDataConverter:
    
    @pytest.fixture
    def converter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield EnergyDataConverter(output_dir=temp_dir)
    
    @pytest.fixture
    def gas_consumption_data(self):
        return load_fixture("gas_consumption_test.json")
    
    @pytest.fixture
    def gas_consumption_file_path(self):
        fixtures_dir = Path(__file__).parent / "fixtures"
        return fixtures_dir / "gas_consumption_test.json"
    
    @pytest.fixture
    def alternate_format_data(self):
        return {
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
    
    def test_constructor_creates_output_directory(self):
        nonexistent_dir_path = Path(tempfile.mkdtemp()) / "test_output"
        assert not nonexistent_dir_path.exists()
        
        converter = EnergyDataConverter(output_dir=str(nonexistent_dir_path))
        assert nonexistent_dir_path.exists()
    
    def test_convert_dictionary_to_jsonl(self, converter, gas_consumption_data):
        output_file_path = Path(converter.output_dir) / "test_output.jsonl"
        
        result_path = converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file_path
        )
        
        assert result_path == str(output_file_path)
        assert output_file_path.exists()
        
        jsonl_lines = output_file_path.read_text().strip().split('\n')
        assert len(jsonl_lines) == 8
        
        first_reading = json.loads(jsonl_lines[0])
        assert first_reading["resource_id"] == "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4"
        assert first_reading["resource_name"] == "gas consumption"
        assert first_reading["timestamp"] == 1748217600
        assert "timestamp_iso" in first_reading
        assert first_reading["value"] == 0
        
        non_zero_reading = json.loads(jsonl_lines[2])
        assert non_zero_reading["timestamp"] == 1748232000
        assert non_zero_reading["value"] == 4.08431053
    
    def test_convert_file_to_jsonl(self, converter, gas_consumption_file_path):
        output_file_path = Path(converter.output_dir) / "test_output_from_file.jsonl"
        
        result_path = converter.convert_to_jsonl(
            gas_consumption_file_path, 
            output_file=output_file_path
        )
        
        assert Path(result_path).exists()
        
        with open(result_path, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        assert len(jsonl_lines) == 8
        
        mid_reading = json.loads(jsonl_lines[4])
        assert mid_reading["resource_id"] == "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4"
        assert mid_reading["timestamp"] == 1748246400
        assert mid_reading["value"] == 0.1346476
    
    def test_auto_generated_output_filename(self, converter, gas_consumption_data):
        result_path = converter.convert_to_jsonl(gas_consumption_data)
        
        output_file = Path(result_path)
        assert output_file.exists()
        assert "gas_consumption" in output_file.stem
        
        with open(result_path, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        assert len(jsonl_lines) == 8
    
    def test_unix_timestamp_conversion_to_iso_format(self, converter, gas_consumption_data):
        output_file_path = Path(converter.output_dir) / "timestamp_test.jsonl"
        
        converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file_path
        )
        
        with open(output_file_path, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        first_reading = json.loads(jsonl_lines[0])
        unix_timestamp = first_reading["timestamp"]
        iso_formatted_timestamp = first_reading["timestamp_iso"]
        
        expected_iso_format = datetime.fromtimestamp(unix_timestamp).isoformat()
        assert iso_formatted_timestamp == expected_iso_format
    
    def test_preserves_all_metadata_fields(self, converter, gas_consumption_data):
        output_file_path = Path(converter.output_dir) / "metadata_test.jsonl"
        
        converter.convert_to_jsonl(
            gas_consumption_data, 
            output_file=output_file_path
        )
        
        with open(output_file_path, 'r') as f:
            first_reading = json.loads(f.readline())
        
        assert first_reading["resource_id"] == gas_consumption_data["resource_id"]
        assert first_reading["resource_name"] == gas_consumption_data["resource_name"]
        assert first_reading["units"] == gas_consumption_data["resource_unit"]
        assert first_reading["classifier"] == gas_consumption_data["resource_classifier"]
        assert first_reading["from_date"] == gas_consumption_data["start_date"]
        assert first_reading["to_date"] == gas_consumption_data["end_date"]
        assert first_reading["period"] == gas_consumption_data["period"]
    
    def test_batch_conversion_of_multiple_files(self, converter, gas_consumption_file_path, tmp_path):
        electricity_file_path = tmp_path / "electricity_test.json"
        with open(gas_consumption_file_path, 'r') as src, open(electricity_file_path, 'w') as dst:
            gas_data = json.load(src)
            gas_data["resource_name"] = "electricity consumption"
            json.dump(gas_data, dst)
        
        output_file_paths = converter.convert_batch_to_jsonl([
            str(gas_consumption_file_path),
            str(electricity_file_path)
        ])
        
        assert len(output_file_paths) == 2
        
        for file_path in output_file_paths:
            assert Path(file_path).exists()
        
        with open(output_file_paths[1], 'r') as f:
            electricity_reading = json.loads(f.readline())
        
        assert electricity_reading["resource_name"] == "electricity consumption"
    
    def test_handles_alternate_api_formats(self, converter, tmp_path, alternate_format_data):
        alternate_format_file = tmp_path / "alternate_format.json"
        with open(alternate_format_file, 'w') as f:
            json.dump(alternate_format_data, f)
        
        output_file_path = Path(converter.output_dir) / "alternate_format.jsonl"
        
        result_path = converter.convert_to_jsonl(
            alternate_format_file, 
            output_file=output_file_path
        )
        
        with open(result_path, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        assert len(jsonl_lines) == 2
        
        reading = json.loads(jsonl_lines[0])
        assert reading["resource_id"] == "different-id-123"
        assert reading["resource_name"] == "different format"
        assert reading["resource_type"] == "test-type"
        assert reading["from_date"] == "2025-01-01T00:00:00"
        assert reading["value"] == 10.5
    
    def test_raises_error_for_nonexistent_file(self, converter):
        with pytest.raises(FileNotFoundError):
            converter.convert_to_jsonl("non_existent_file.json")

    def test_raises_error_for_invalid_json_file(self, converter, tmp_path):
        malformed_json_file = tmp_path / "invalid.json"
        with open(malformed_json_file, 'w') as f:
            f.write("{invalid json")
        
        with pytest.raises(json.JSONDecodeError):
            converter.convert_to_jsonl(malformed_json_file)

    def test_handles_empty_data_arrays(self, converter, tmp_path):
        empty_data_file = tmp_path / "empty.json"
        with open(empty_data_file, 'w') as f:
            json.dump({
                "resource_id": "test-id",
                "resource_name": "test resource",
                "readings": []
            }, f)
        
        output_file_path = Path(converter.output_dir) / "empty_output.jsonl"
        
        result_path = converter.convert_to_jsonl(
            empty_data_file, 
            output_file=output_file_path
        )
        
        assert Path(result_path).exists()
        with open(result_path, 'r') as f:
            content = f.read().strip()
        
        assert content == ""