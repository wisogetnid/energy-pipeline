import pytest
import json
from pathlib import Path

# This allows all test modules to find the right fixtures
@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent

def load_fixture(filename):
    """Load a JSON fixture file."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)

# Any other shared fixtures that are used across modules