import json
import os
import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from pipeline.data_processing.jsonl_converter import EnergyDataConverter

def load_fixture(filename):
    fixture_path = Path(__file__).parent / 'fixtures' / filename
    with open(fixture_path, 'r') as f:
        return json.load(f)

class TestEnergyDataConverter:
    
    @pytest.fixture
    def converter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield EnergyDataConverter(output_dir=temp_dir)
    
    @pytest.fixture
    def gas_consumption_data(self):
        return load_fixture("gas_consumption_test.json")
    
    @pytest.fixture
    def gas_cost_data(self):
        consumption_data = load_fixture("gas_consumption_test.json")
        cost_data = consumption_data.copy()
        cost_data["resource_name"] = "gas cost"
        cost_data["resource_classifier"] = "gas.consumption.cost"
        cost_data["resource_unit"] = "pence"
        
        # Update the readings to have the same timestamps but different values
        cost_readings = []
        for reading in cost_data["readings"]:
            cost_readings.append([reading[0], reading[1] * 0.15])  # Multiply by price
        cost_data["readings"] = cost_readings
        
        return cost_data
    
    @pytest.fixture
    def electricity_consumption_data(self):
        return {
            "resource_id": "04678775-6c72-43c9-8378-c9914756384a",
            "resource_name": "electricity consumption",
            "resource_unit": "kWh",
            "resource_classifier": "electricity.consumption",
            "start_date": "2025-02-01T00:00:00",
            "end_date": "2025-02-28T00:00:00",
            "period": "PT30M",
            "timezone_offset": 0,
            "readings": [
                [1738368000, 0.047],
                [1738369800, 0.059],
                [1738371600, 0.039]
            ]
        }

    @pytest.fixture
    def electricity_cost_data(self):
        return {
            "resource_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
            "resource_name": "electricity cost",
            "resource_unit": "pence",
            "resource_classifier": "electricity.consumption.cost",
            "start_date": "2025-02-01T00:00:00",
            "end_date": "2025-02-28T00:00:00",
            "period": "PT30M",
            "timezone_offset": 0,
            "readings": [
                [1738368000, 0.78773],
                [1738369800, 0.44709],
                [1738371600, 0.5123]
            ]
        }
    
    @pytest.fixture
    def gas_consumption_file_path(self, tmp_path, gas_consumption_data):
        file_path = tmp_path / "gas_consumption_test.json"
        with open(file_path, 'w') as f:
            json.dump(gas_consumption_data, f)
        return file_path
    
    @pytest.fixture
    def gas_cost_file_path(self, tmp_path, gas_cost_data):
        file_path = tmp_path / "gas_cost_test.json"
        with open(file_path, 'w') as f:
            json.dump(gas_cost_data, f)
        return file_path
    
    @pytest.fixture
    def electricity_consumption_file_path(self, tmp_path, electricity_consumption_data):
        file_path = tmp_path / "electricity_consumption_test.json"
        with open(file_path, 'w') as f:
            json.dump(electricity_consumption_data, f)
        return file_path
    
    @pytest.fixture
    def electricity_cost_file_path(self, tmp_path, electricity_cost_data):
        file_path = tmp_path / "electricity_cost_test.json"
        with open(file_path, 'w') as f:
            json.dump(electricity_cost_data, f)
        return file_path
    
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
        assert len(jsonl_lines) == len(gas_consumption_data["readings"])
        
        first_reading = json.loads(jsonl_lines[0])
        assert first_reading["resource_id"] == "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4"
        assert first_reading["resource_name"] == "gas consumption"
        assert first_reading["timestamp"] == 1748217600
        assert "timestamp_iso" in first_reading
        assert first_reading["value"] == 0
    
    def test_convert_file_to_jsonl(self, converter, gas_consumption_file_path):
        output_file_path = Path(converter.output_dir) / "test_output_from_file.jsonl"
        
        result_path = converter.convert_to_jsonl(
            gas_consumption_file_path, 
            output_file=output_file_path
        )
        
        assert Path(result_path).exists()
        
        with open(result_path, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        assert len(jsonl_lines) > 0
    
    def test_extract_resource_type(self, converter):
        assert converter.extract_resource_type("electricity consumption") == "electricity"
        assert converter.extract_resource_type("gas cost") == "gas"
        assert converter.extract_resource_type("water usage") == "water"
        assert converter.extract_resource_type("unknown resource") == "energy"
        assert converter.extract_resource_type("") == "energy"
    
    def test_combine_consumption_and_cost(self, converter, gas_consumption_file_path, gas_cost_file_path):
        combined_readings, metadata = converter.merge_consumption_and_cost_data(
            gas_consumption_file_path,
            gas_cost_file_path
        )
        
        assert isinstance(combined_readings, dict)
        assert isinstance(metadata, dict)
        
        assert "resource_type" in metadata
        assert metadata["resource_type"] == "gas"
        
        assert "consumption_id" in metadata
        assert "cost_id" in metadata
        assert "consumption_unit" in metadata
        assert "cost_unit" in metadata
        
        assert len(combined_readings) > 0
        
        for timestamp, reading in combined_readings.items():
            assert "timestamp" in reading
            assert "timestamp_iso" in reading
            assert "consumption_value" in reading
            assert "cost_value" in reading
    
    def test_combine_resource_files(self, converter, gas_consumption_file_path, gas_cost_file_path):
        output_file_path = Path(converter.output_dir) / "combined_test.jsonl"
        
        result_path = converter.combine_consumption_and_cost(
            gas_consumption_file_path,
            gas_cost_file_path,
            output_file=output_file_path
        )
        
        assert result_path == str(output_file_path)
        assert output_file_path.exists()
        
        jsonl_lines = output_file_path.read_text().strip().split('\n')
        assert len(jsonl_lines) > 0
        
        first_reading = json.loads(jsonl_lines[0])
        assert "resource_type" in first_reading
        assert first_reading["resource_type"] == "gas"
        
        assert "consumption_id" in first_reading
        assert "cost_id" in first_reading
        
        assert "timestamp" in first_reading
        assert "timestamp_iso" in first_reading
        assert "consumption_value" in first_reading
        assert "cost_value" in first_reading
    
    def test_find_matching_resource_files(self, converter, tmp_path, electricity_consumption_data, electricity_cost_data):
        test_data_dir = tmp_path / "test_matching"
        test_data_dir.mkdir()
        
        # Create files with filenames matching the expected format in the implementation
        consumption_file = test_data_dir / "electricity_consumption_20250101_to_20250131.json"
        cost_file = test_data_dir / "electricity_cost_20250101_to_20250131.json"
        
        gas_consumption_file = test_data_dir / "gas_consumption_20250101_to_20250131.json"
        gas_cost_file = test_data_dir / "gas_cost_20250101_to_20250131.json"
        
        # Also add a file without a match to ensure it's not included
        unmatched_file = test_data_dir / "water_consumption_20250101_to_20250131.json"
        
        # Create test files
        with open(consumption_file, 'w') as f:
            json.dump(electricity_consumption_data, f)
        
        with open(cost_file, 'w') as f:
            json.dump(electricity_cost_data, f)
        
        with open(gas_consumption_file, 'w') as f:
            gas_data = electricity_consumption_data.copy()
            gas_data["resource_name"] = "gas consumption"
            json.dump(gas_data, f)
        
        with open(gas_cost_file, 'w') as f:
            gas_cost_data = electricity_cost_data.copy()
            gas_cost_data["resource_name"] = "gas cost"
            json.dump(gas_cost_data, f)
        
        with open(unmatched_file, 'w') as f:
            water_data = electricity_consumption_data.copy()
            water_data["resource_name"] = "water consumption"
            json.dump(water_data, f)
        
        matched_pairs = converter.find_matching_resource_files(test_data_dir)
        
        print(f"Found matched pairs: {matched_pairs}")
        assert len(matched_pairs) == 2
        
        found_electricity = False
        found_gas = False
        
        for consumption_path, cost_path in matched_pairs:
            consumption_path = str(consumption_path)
            cost_path = str(cost_path)
            
            if "electricity" in consumption_path.lower():
                found_electricity = True
                assert "consumption" in consumption_path.lower()
                assert "cost" in cost_path.lower()
            
            if "gas" in consumption_path.lower():
                found_gas = True
                assert "consumption" in consumption_path.lower()
                assert "cost" in cost_path.lower()
        
        assert found_electricity, "No electricity consumption/cost pair found"
        assert found_gas, "No gas consumption/cost pair found"
    
    def test_combine_batch_resources(self, converter, tmp_path, electricity_consumption_data, electricity_cost_data):
        test_data_dir = tmp_path / "test_batch"
        test_data_dir.mkdir()
        
        # Create test files with matching naming patterns
        elec_consumption_file = test_data_dir / "electricity_consumption_20250101_to_20250131.json"
        elec_cost_file = test_data_dir / "electricity_cost_20250101_to_20250131.json"
        
        gas_consumption_file = test_data_dir / "gas_consumption_20250101_to_20250131.json"
        gas_cost_file = test_data_dir / "gas_cost_20250101_to_20250131.json"
        
        # Also add an unmatched file to test it's properly ignored
        unmatched_file = test_data_dir / "water_consumption_20250101_to_20250131.json"
        
        with open(elec_consumption_file, 'w') as f:
            modified_data = electricity_consumption_data.copy()
            modified_data["resource_name"] = "electricity consumption"
            json.dump(modified_data, f)
        
        with open(elec_cost_file, 'w') as f:
            modified_data = electricity_cost_data.copy()
            modified_data["resource_name"] = "electricity cost"
            json.dump(modified_data, f)
        
        with open(gas_consumption_file, 'w') as f:
            gas_data = electricity_consumption_data.copy()
            gas_data["resource_name"] = "gas consumption"
            json.dump(gas_data, f)
        
        with open(gas_cost_file, 'w') as f:
            gas_cost_data = electricity_cost_data.copy()
            gas_cost_data["resource_name"] = "gas cost"
            json.dump(gas_cost_data, f)
        
        with open(unmatched_file, 'w') as f:
            water_data = electricity_consumption_data.copy()
            water_data["resource_name"] = "water consumption"
            json.dump(water_data, f)
        
        # Debug prints to help diagnose issues
        print(f"\nFiles in test directory:")
        for file in test_data_dir.glob("*.json"):
            print(f" - {file.name}")
        
        # Run batch combination using the correct method name
        output_files = converter.batch_combine_resource_files(test_data_dir)
        
        print(f"\nOutput files: {output_files}")
        assert len(output_files) == 2, f"Expected 2 output files, got {len(output_files)}"
        
        for output_file in output_files:
            assert Path(output_file).exists(), f"Output file {output_file} does not exist"
            
            with open(output_file, 'r') as f:
                first_line = f.readline().strip()
                print(f"Content of {output_file}: {first_line[:100]}...")
                data = json.loads(first_line)
                
                assert "resource_type" in data, f"Missing resource_type in {output_file}"
                assert data["resource_type"] in ["electricity", "gas"], f"Invalid resource_type: {data['resource_type']}"
                assert "consumption_value" in data, f"Missing consumption_value in {output_file}"
                assert "cost_value" in data, f"Missing cost_value in {output_file}"

    def test_batch_conversion_of_multiple_files(self, converter, gas_consumption_file_path, electricity_consumption_file_path):
        output_file_paths = converter.batch_convert_to_jsonl([
            str(gas_consumption_file_path),
            str(electricity_consumption_file_path)
        ])
        
        assert len(output_file_paths) == 2
        
        for file_path in output_file_paths:
            assert Path(file_path).exists()
        
        with open(output_file_paths[1], 'r') as f:
            electricity_reading = json.loads(f.readline())
        
        assert electricity_reading["resource_name"] == "electricity consumption"

    def test_combine_all_resources(self, converter, tmp_path, electricity_consumption_data, electricity_cost_data):
        test_data_dir = tmp_path / "test_all_resources"
        test_data_dir.mkdir()
        
        # Create test files for electricity
        elec_consumption_file = test_data_dir / "electricity_consumption_20250101_to_20250131.json"
        elec_cost_file = test_data_dir / "electricity_cost_20250101_to_20250131.json"
        
        # Create test files for gas
        gas_consumption_file = test_data_dir / "gas_consumption_20250101_to_20250131.json"
        gas_cost_file = test_data_dir / "gas_cost_20250101_to_20250131.json"
        
        # Create electricity files
        with open(elec_consumption_file, 'w') as f:
            json.dump(electricity_consumption_data, f)
        
        with open(elec_cost_file, 'w') as f:
            json.dump(electricity_cost_data, f)
        
        # Create gas files (based on electricity but with different values)
        with open(gas_consumption_file, 'w') as f:
            gas_data = electricity_consumption_data.copy()
            gas_data["resource_name"] = "gas consumption"
            gas_data["resource_id"] = "20cb0793-1adb-4d7f-92f4-fa30ddbf1f35"
            gas_data["resource_classifier"] = "gas.consumption"
            
            # Modify readings to have different values
            gas_readings = []
            for reading in gas_data["readings"]:
                gas_readings.append([reading[0], reading[1] * 0.5])  # Different values
            gas_data["readings"] = gas_readings
            
            json.dump(gas_data, f)
        
        with open(gas_cost_file, 'w') as f:
            gas_cost_data = electricity_cost_data.copy()
            gas_cost_data["resource_name"] = "gas cost"
            gas_cost_data["resource_id"] = "e5345576-d775-44cf-bf00-a97b665e6702"
            gas_cost_data["resource_classifier"] = "gas.consumption.cost"
            
            # Modify readings to have different values
            gas_cost_readings = []
            for reading in gas_cost_data["readings"]:
                gas_cost_readings.append([reading[0], reading[1] * 0.2])  # Different values
            gas_cost_data["readings"] = gas_cost_readings
            
            json.dump(gas_cost_data, f)
        
        # Run the correct method name
        output_file = converter.combine_all_resources_into_single_file(test_data_dir)
        
        # Verify the output
        assert output_file is not None
        assert Path(output_file).exists()
        
        with open(output_file, 'r') as f:
            jsonl_lines = f.read().strip().split('\n')
        
        assert len(jsonl_lines) > 0
        
        # Check the content of the first line
        first_entry = json.loads(jsonl_lines[0])
        
        # Should have electricity and gas data for the same timestamp
        assert "electricity_consumption" in first_entry
        assert "electricity_cost" in first_entry
        assert "gas_consumption" in first_entry
        assert "gas_cost" in first_entry
        
        # Should have metadata for both resources
        assert "electricity_consumption_id" in first_entry
        assert "electricity_cost_id" in first_entry
        assert "gas_consumption_id" in first_entry
        assert "gas_cost_id" in first_entry
        
        # Should have timestamp information
        assert "timestamp" in first_entry
        assert "timestamp_iso" in first_entry
        
        # Values should match what we expect
        assert first_entry["electricity_consumption_id"] == "04678775-6c72-43c9-8378-c9914756384a"
        assert first_entry["gas_consumption_id"] == "20cb0793-1adb-4d7f-92f4-fa30ddbf1f35"