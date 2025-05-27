import json
import pytest
from pathlib import Path

def load_fixture(filename):
    """Load a JSON fixture file."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def gas_consumption_data():
    """Load the gas consumption test data."""
    return load_fixture("gas_consumption_test.json")

@pytest.fixture
def gas_consumption_file_path():
    """Get the path to the gas consumption test data file."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    return fixtures_dir / "gas_consumption_test.json"