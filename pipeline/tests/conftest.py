import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def mock_token():
    """Return a mock authentication token."""
    return "test-token-12345"

@pytest.fixture
def mock_auth_response(mock_token):
    """Return a mock response for authentication."""
    response = Mock()
    response.json.return_value = {"token": mock_token}
    response.status_code = 200
    return response

@pytest.fixture
def mock_readings_response():
    """Return a mock response for readings data."""
    response = Mock()
    response.json.return_value = {"readings": [[1620000000000, 5.5], [1620001800000, 6.2]]}
    response.status_code = 200
    return response

@pytest.fixture
def mock_resources_response():
    """Return a mock response for resources data."""
    response = Mock()
    response.json.return_value = {"resources": [{"id": "resource1"}, {"id": "resource2"}]}
    response.status_code = 200
    return response

@pytest.fixture
def mock_empty_readings_response():
    """Return a mock response with empty readings."""
    response = Mock()
    response.json.return_value = {"readings": []}
    response.status_code = 200
    return response

@pytest.fixture
def sample_resource_id():
    """Return a sample resource ID."""
    return "test-resource-123"

@pytest.fixture
def sample_date_range():
    """Return a sample date range as strings."""
    return {
        "start_date": "2023-01-01T00:00:00",
        "end_date": "2023-01-07T23:59:59"
    }

@pytest.fixture
def sample_datetime_range():
    """Return a sample date range as datetime objects."""
    return {
        "start_date": datetime(2023, 1, 1),
        "end_date": datetime(2023, 1, 7, 23, 59, 59)
    }

@pytest.fixture
def auth_patch(mock_auth_response):
    """Return a context manager that patches requests.post for auth."""
    return patch("requests.post", return_value=mock_auth_response)

@pytest.fixture
def get_patch(mock_readings_response):
    """Return a context manager that patches requests.get."""
    return patch("requests.get", return_value=mock_readings_response)