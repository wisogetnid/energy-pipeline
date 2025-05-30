import json
import pytest
from pathlib import Path

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