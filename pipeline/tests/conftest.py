import pytest
import json
from pathlib import Path

@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).parent.parent

def load_fixture(filename):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)