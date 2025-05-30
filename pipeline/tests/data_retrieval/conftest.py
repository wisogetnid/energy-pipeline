import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime

from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

def load_fixture(filename):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / filename
    
    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def mock_client():
    return Mock(spec=GlowmarktClient)

@pytest.fixture
def mock_token():
    return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbkhhc2giOiJlOTRlYzE2MzgyMzE0YzdjMDdlZDliZmEwZGFiNDhhZTNhOTA0NDhlYjNjZTU0MzI4YWEwOTMwNTEzMjI4ZjY2ZjAwMWNiODRiYTIyZDczMjliZmZlMDlmZjlhZDFkZiIsImlhdCI6MTUzNjEzNDkxMCwiZXhwIjoxNTM2NzM5NzEwfQ.D1lTvyfo5ap69tT6MK9jceEFNLp-xmMAz6WGohIuUR4"

@pytest.fixture
def sample_resource_id():
    return "73f70bcd-3743-4009-a2c4-e98cc959c030"

@pytest.fixture
def sample_date_range():
    return {
        "start_date": "2023-01-01T00:00:00",
        "end_date": "2023-01-07T23:59:59"
    }

@pytest.fixture
def sample_datetime_range():
    return {
        "start_date": datetime(2023, 1, 1),
        "end_date": datetime(2023, 1, 7, 23, 59, 59)
    }

def create_mock_http_response(fixture_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = fixture_data
    mock_response.content = json.dumps(fixture_data).encode('utf-8')
    return mock_response

@pytest.fixture
def mock_readings_response():
    response_data = load_fixture("resource_readings_response.json")
    return create_mock_http_response(response_data)

@pytest.fixture
def mock_empty_readings_response():
    response_data = load_fixture("empty_resource_readings_response.json")
    return create_mock_http_response(response_data)

@pytest.fixture
def mock_auth_response():
    response_data = load_fixture("authentication_response.json")
    return create_mock_http_response(response_data)

@pytest.fixture
def auth_patch():
    response_data = load_fixture("authentication_response.json")
    mock_response = create_mock_http_response(response_data)
    return patch("requests.post", return_value=mock_response)

@pytest.fixture
def get_patch():
    response_data = load_fixture("resource_readings_response.json")
    mock_response = create_mock_http_response(response_data)
    return patch("requests.get", return_value=mock_response)

@pytest.fixture
def mock_virtual_entities_response():
    response_data = load_fixture("virtual_entities_response.json")
    return create_mock_http_response(response_data)

@pytest.fixture
def mock_ve_resources_response():
    response_data = load_fixture("virtual_entity_resources_response.json")
    return create_mock_http_response(response_data)