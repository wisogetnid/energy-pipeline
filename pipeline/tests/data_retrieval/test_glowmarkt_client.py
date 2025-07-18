import pytest
import requests
from unittest.mock import patch, Mock
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

@pytest.fixture
def client(mock_token):
    return GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")

class TestGlowmarktClientAuth:
    
    def test_authenticate_returns_token(self, mock_auth_response, auth_patch):
        client = GlowmarktClient(username="test", password="password", application_id="test-app-id")
        
        with auth_patch as mock_post:
            token = client.authenticate()
            
            mock_post.assert_called_once()
            assert "username" in mock_post.call_args[1]["json"]
            
            expected_token = mock_auth_response.json.return_value["token"]
            assert token == expected_token
            assert client.token == expected_token

    def test_get_readings_auto_authenticates(self, mock_auth_response, sample_resource_id, mock_readings_response):
        client = GlowmarktClient(username="test", password="password", application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        with patch("requests.post", return_value=mock_auth_response) as mock_post, \
             patch("requests.get", return_value=mock_readings_response) as mock_get:

            client.get_readings(sample_resource_id)

            mock_post.assert_called_once()
            mock_get.assert_called_once()

            expected_token = mock_auth_response.json.return_value["token"]
            assert mock_get.call_args[1]["headers"]["token"] == expected_token

class TestGlowmarktClientGetReadings:

    def test_get_readings_uses_correct_headers(self, client, mock_token, sample_resource_id, get_patch):
        with get_patch as mock_get:
            client.get_readings(sample_resource_id)
            
            mock_get.assert_called_once()
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"

    def test_get_readings_returns_valid_data(self, client, sample_resource_id, mock_readings_response, get_patch):
        with get_patch:
            result = client.get_readings(sample_resource_id)
            
            expected_data = mock_readings_response.json.return_value
            assert result == expected_data
            assert result["status"] == "OK"

    def test_get_readings_with_date_range(self, client, sample_resource_id, sample_date_range, mock_empty_readings_response):
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_date_range["start_date"], 
                end_date=sample_date_range["end_date"]
            )
            
            params = mock_get.call_args[1]["params"]
            assert params["from"] == sample_date_range["start_date"]
            assert params["to"] == sample_date_range["end_date"]
    
    def test_get_readings_handles_datetime_objects(self, client, sample_resource_id, sample_datetime_range, mock_empty_readings_response):
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_datetime_range["start_date"], 
                end_date=sample_datetime_range["end_date"]
            )
            
            params = mock_get.call_args[1]["params"]
            assert params["from"] == "2023-01-01T00:00:00"
            assert params["to"] == "2023-01-07T23:59:59"

    def test_get_readings_with_all_parameters(self, client, sample_resource_id, sample_date_range, mock_readings_response):
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(
                sample_resource_id, 
                start_date=sample_date_range["start_date"],
                end_date=sample_date_range["end_date"],
                period="P1D",
                function="sum",
                offset=-60
            )
            
            request_params = mock_get.call_args[1]["params"]
            assert request_params["period"] == "P1D"
            assert request_params["function"] == "sum"
            assert request_params["offset"] == -60
            
            assert result["query"]["period"] == "P1D"

class TestGlowmarktClientEntityAndResource:

    def test_get_virtual_entities(self, client, mock_token, mock_virtual_entities_response):
        with patch("requests.get", return_value=mock_virtual_entities_response) as mock_get:
            entities = client.get_virtual_entities()
            
            mock_get.assert_called_once()
            assert "/virtualentity" in mock_get.call_args[0][0]
            
            assert isinstance(entities, list)
            assert entities[0]["veId"] == "dc9069a7-7695-43fd-8f27-16b1c94213da"
    
    def test_get_virtual_entity_resources(self, client, mock_ve_resources_response):
        virtual_entity_id = "dc9069a7-7695-43fd-8f27-16b1c94213da"
        
        with patch("requests.get", return_value=mock_ve_resources_response) as mock_get:
            entity_details = client.get_virtual_entity_resources(virtual_entity_id)
            
            mock_get.assert_called_once()
            assert f"/virtualentity/{virtual_entity_id}/resources" in mock_get.call_args[0][0]
            
            assert entity_details["veId"] == virtual_entity_id
            assert len(entity_details["resources"]) == 4

class TestGlowmarktClientErrorHandling:

    def test_connection_error(self, client):
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with pytest.raises(ConnectionError):
                client.get_virtual_entities()

    def test_invalid_json_response(self, client):
        invalid_json_response = Mock()
        invalid_json_response.status_code = 200
        invalid_json_response.content = b""
        invalid_json_response.json.side_effect = ValueError("Invalid JSON")

        with patch("requests.get", return_value=invalid_json_response):
            with pytest.raises(ValueError):
                client.get_virtual_entities()

    def test_http_error(self, client):
        forbidden_response = Mock()
        forbidden_response.status_code = 403
        forbidden_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Forbidden")

        with patch("requests.get", return_value=forbidden_response):
            with pytest.raises(Exception):
                client.get_virtual_entities()
