import pytest
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from pipeline.data_retrieval.latest_date_service import get_latest_available_date

@pytest.fixture
def temp_data_dir():
    # Create a temporary directory for test data
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up after tests
    shutil.rmtree(temp_dir)

def test_get_latest_available_date_empty(temp_data_dir):
    default_date = datetime(2023, 1, 1)
    result = get_latest_available_date(temp_data_dir, ["Electricity"], default_date=default_date)
    assert result == default_date

def test_get_latest_available_date_single_resource(temp_data_dir):
    # Create some mock files
    Path(temp_data_dir, "electricity_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_20250201_to_20250228.json").touch()
    
    result = get_latest_available_date(temp_data_dir, ["Electricity"])
    # Should return the start date of the latest file to use as new sync point (pivot)
    assert result == datetime(2025, 2, 1)

def test_get_latest_available_date_multiple_resources_sync(temp_data_dir):
    # Electricity up to Feb
    Path(temp_data_dir, "electricity_consumption_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_consumption_20250201_to_20250228.json").touch()
    
    # Gas only up to Jan
    Path(temp_data_dir, "gas_consumption_20250101_to_20250131.json").touch()
    
    # Cost up to Feb
    Path(temp_data_dir, "electricity_cost_20250101_to_20250131.json").touch()
    Path(temp_data_dir, "electricity_cost_20250201_to_20250228.json").touch()
    
    resources = ["Electricity consumption", "Gas consumption", "Electricity cost"]
    
    result = get_latest_available_date(temp_data_dir, resources)
    
    # Latest dates are:
    # Elec cons: 2025-02-28 (start 2025-02-01)
    # Gas cons: 2025-01-31 (start 2025-01-01)
    # Elec cost: 2025-02-28 (start 2025-02-01)
    # Global sync point (min of latest ends) is 2025-01-31.
    # The start date of the file ending on 2025-01-31 is 2025-01-01.
    
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
