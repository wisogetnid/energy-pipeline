import json
import os
import pytest
import tempfile
from pathlib import Path

from pipeline.data_processing.jsonl_converter import EnergyDataConverter
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

def load_fixture(filename):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def gas_consumption_data():
    return load_fixture("gas_consumption_test.json")

@pytest.fixture
def gas_consumption_file_path():
    fixtures_dir = Path(__file__).parent / "fixtures"
    return fixtures_dir / "gas_consumption_test.json"

@pytest.fixture
def alternate_format_data():
    return {
        "resourceId": "different-id-123",
        "name": "electricity consumption",
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

@pytest.fixture
def converter(tmp_path):
    output_dir = tmp_path / "parquet_test_output"
    return JsonlToParquetConverter(output_dir=str(output_dir))

@pytest.fixture
def energy_data_converter(tmp_path):
    output_dir = tmp_path / "jsonl_test_output"
    return EnergyDataConverter(output_dir=str(output_dir))

@pytest.fixture
def sample_jsonl_file(energy_data_converter, gas_consumption_data):
    output_file_path = Path(energy_data_converter.output_dir) / "test_data.jsonl"
    jsonl_path = energy_data_converter.convert_to_jsonl(
        gas_consumption_data, 
        output_file=output_file_path
    )
    return jsonl_path

@pytest.fixture
def alternate_jsonl_file(energy_data_converter, alternate_format_data):
    output_file_path = Path(energy_data_converter.output_dir) / "electricity_data.jsonl"
    jsonl_path = energy_data_converter.convert_to_jsonl(
        alternate_format_data, 
        output_file=output_file_path
    )
    return jsonl_path

@pytest.fixture
def empty_jsonl_file(tmp_path):
    empty_file = tmp_path / "empty.jsonl"
    empty_file.touch()
    return str(empty_file)

@pytest.fixture
def malformed_jsonl_file(tmp_path):
    malformed_file = tmp_path / "malformed.jsonl"
    with open(malformed_file, 'w') as f:
        f.write('{"valid": "json"}\n')
        f.write('{"invalid json\n')
    return str(malformed_file)

@pytest.fixture
def large_jsonl_file(sample_jsonl_file, tmp_path):
    large_jsonl_file = tmp_path / "large_test.jsonl"
    
    with open(sample_jsonl_file, 'r') as src, open(large_jsonl_file, 'w') as dst:
        content = src.read()
        for _ in range(10):
            dst.write(content)
    
    return large_jsonl_file