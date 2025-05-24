import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def get_patch():
    """Patch requests.get with a successful response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [[1620000000000, 5.5], [1620001800000, 6.2]]}
    # Setup content attribute for response.content checks
    mock_response.content = b'{"data": [[1620000000000, 5.5], [1620001800000, 6.2]]}'
    return patch("requests.get", return_value=mock_response)

@pytest.fixture
def mock_readings_response():
    """Create a mock response for readings."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [[1620000000000, 5.5], [1620001800000, 6.2]]}
    # Setup content attribute for response.content checks
    mock_response.content = b'{"data": [[1620000000000, 5.5], [1620001800000, 6.2]]}'
    return mock_response

@pytest.fixture
def mock_empty_readings_response():
    """Create a mock response with empty readings."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    # Setup content attribute for response.content checks
    mock_response.content = b'{"data": []}'
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
    mock_response = Mock()
    mock_response.status_code = 200
    
    # Create a realistic mock return value instead of a mock object
    mock_data = [
        {
            "name": "Smart Home 1",
            "veId": "dc9069a7-7695-43fd-8f27-16b1c94213da",
            "resources": [
                {
                    "resourceId": "73f70bcd-3743-4009-a2c4-e98cc959c030",
                    "resourceTypeId": "electricity"
                },
                {
                    "resourceId": "4b6ea57c-8f0e-4f9f-8d89-3b7395f1ba3c",
                    "resourceTypeId": "gas"
                }
            ]
        }
    ]
    mock_response.json.return_value = mock_data
    return mock_response