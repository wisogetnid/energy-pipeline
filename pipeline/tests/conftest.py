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

@pytest.fixture
def mock_virtual_entities_response():
    """Return a mock response for virtual entities data."""
    response = Mock()
    response.json.return_value = [
        {
            "name": "Smart Home 1",
            "veId": "dc9069a7-7695-43fd-8f27-16b1c94213da",
            "veTypeId": "cc90b599-2705-4b13-98d4-3306f81169cf",
            "ownerId": "f78a3812-d4fc-4b00-99c5-20fd581721a6",
            "applicationId": "b0f1b774-a586-4f72-9edd-27ead8aa7a8d",
            "updatedAt": "2018-10-26T17:10:02.670Z",
            "createdAt": "2018-10-26T17:10:02.670Z",
            "resources": [
                {
                    "resourceId": "73f70bcd-3743-4009-a2c4-e98cc959c030",
                    "resourceTypeId": "ea02304a-2820-4ea0-8399-f1d1b430c3a0"
                },
                {
                    "resourceId": "b120977-aeb6-4b56-a0d3-d4a9b485848a",
                    "resourceTypeId": "b4158501-a678-484a-837a-874194d3bd48"
                }
            ]
        }
    ]
    response.status_code = 200
    return response