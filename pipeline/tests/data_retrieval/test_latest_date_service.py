import pytest
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from pipeline.data_retrieval.latest_date_service import get_latest_available_date

@pytest.fixture
def temp_data_dir():
    temp_test_directory = tempfile.mkdtemp()
    yield temp_test_directory
    shutil.rmtree(temp_test_directory)

def test_get_latest_available_date_empty(temp_data_dir):
    default_sync_date = datetime(2023, 1, 1)
    result = get_latest_available_date(temp_data_dir, ["Electricity"], default_date=default_sync_date)
    assert result == default_sync_date

def test_get_latest_available_date_single_resource(temp_data_dir):
    Path(temp_data_dir, "electricity_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_20250201_to_20250228.json").touch()
    
    result = get_latest_available_date(temp_data_dir, ["Electricity"])
    assert result == datetime(2025, 2, 1)

def test_get_latest_available_date_multiple_resources_sync(temp_data_dir):
    Path(temp_data_dir, "electricity_consumption_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_consumption_20250201_to_20250228.json").touch()
    
    Path(temp_data_dir, "gas_consumption_20250101_to_20250131.json").touch()
    
    Path(temp_data_dir, "electricity_cost_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_cost_20250201_to_20250228.json").touch()
    
    resources_to_sync = ["Electricity consumption", "Gas consumption", "Electricity cost"]
    
    result = get_latest_available_date(temp_data_dir, resources_to_sync)
    
    assert result == datetime(2025, 1, 1)

def test_get_latest_available_date_no_matching_files(temp_data_dir):
    Path(temp_data_dir, "random_file.json").touch()
    Path(temp_data_dir, "other_resource_20250101_to_20250131.json").touch()
    
    default_date = datetime(2024, 6, 1)
    result = get_latest_available_date(temp_data_dir, ["Electricity"], default_date=default_date)
    assert result == default_date

def test_get_latest_available_date_case_insensitivity(temp_data_dir):
    Path(temp_data_dir, "electricity_consumption_20250101_to_20250131.json").touch()
    
    result = get_latest_available_date(temp_data_dir, ["ELECTRICITY CONSUMPTION"])
    assert result == datetime(2025, 1, 1)
