import json
import os
from pathlib import Path
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

def load_fixture(filename):
    """Load a JSON fixture file."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def mock_client():
    """Return a mock GlowmarktClient."""
    return Mock(spec=GlowmarktClient)

@pytest.fixture
def get_patch():
    """Patch requests.get with a successful response matching the JSON fixture."""
    response_data = load_fixture("resource_readings_response.json")
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.content = json.dumps(response_data).encode('utf-8')
    
    return patch("requests.get", return_value=mock_response)

@pytest.fixture
def mock_readings_response():
    """Create a mock response for readings."""
    response_data = load_fixture("resource_readings_response.json")
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.content = json.dumps(response_data).encode('utf-8')
    
    return mock_response

@pytest.fixture
def mock_empty_readings_response():
    """Create a mock response with empty readings."""
    response_data = load_fixture("empty_resource_readings_response.json")
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.content = json.dumps(response_data).encode('utf-8')
    
    return mock_response

@pytest.fixture
def mock_token():
    """Return a mock token."""
    return "test-token-123456"

@pytest.fixture
def mock_auth_response():
    """Create a mock response for authentication."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"token": "test-token-123456"}
    return mock_response

@pytest.fixture
def sample_resource_id():
    """Return a sample resource ID."""
    return "resource-123"

@pytest.fixture
def sample_date_range():
    """Return a sample date range as strings."""
    return {"start_date": "2023-01-01T00:00:00", "end_date": "2023-01-07T23:59:59"}

@pytest.fixture
def sample_datetime_range():
    """Return a sample date range as datetime objects."""
    from datetime import datetime
    return {
        "start_date": datetime(2023, 1, 1, 0, 0, 0),
        "end_date": datetime(2023, 1, 7, 23, 59, 59)
    }

@pytest.fixture
def auth_patch():
    """Patch requests.post with a successful auth response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"token": "test-token-123456"}
    return patch("requests.post", return_value=mock_response)

@pytest.fixture
def mock_virtual_entities_response():
    """Create a mock response for virtual entities."""
    response_data = load_fixture("virtual_entities_response.json")
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.content = json.dumps(response_data).encode('utf-8')
    
    return mock_response

@pytest.fixture
def mock_ve_resources_response():
    """Create a mock response for virtual entity resources."""
    response_data = load_fixture("virtual_entity_resources_response.json")
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.content = json.dumps(response_data).encode('utf-8')
    
    return mock_response