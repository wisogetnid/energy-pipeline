import pytest
import requests
from unittest.mock import patch, Mock
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestGlowmarktClient:
    
    def test_authenticate_returns_token(self, mock_token, auth_patch):
        # Create client and mock the post request
        client = GlowmarktClient(username="test", password="password", application_id="test-app-id")
        with auth_patch as mock_post:
            token = client.authenticate()
            
            # Assert the request was made correctly
            mock_post.assert_called_once()
            assert "auth" in mock_post.call_args[0][0]
            assert mock_post.call_args[1]["json"] == {"username": "test", "password": "password"}
            assert mock_post.call_args[1]["headers"]["applicationId"] == "test-app-id"
            assert mock_post.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            # Assert token was returned and stored
            assert token == mock_token
            assert client.token == mock_token
    
    def test_get_readings_with_existing_token(self, mock_token, sample_resource_id, mock_readings_response, get_patch):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        # Mock the request
        with get_patch as mock_get:
            result = client.get_readings(sample_resource_id)
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert sample_resource_id in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "test-app-id"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            # Assert data was returned
            assert result == mock_readings_response.json.return_value
    
    def test_get_readings_auto_authenticates(self, mock_token, sample_resource_id, 
                                            mock_auth_response, mock_readings_response):
        # Create client without token
        client = GlowmarktClient(username="test", password="password", application_id="test-app-id")
        
        # Mock requests
        with patch("requests.post", return_value=mock_auth_response) as mock_post:
            with patch("requests.get", return_value=mock_readings_response) as mock_get:
                result = client.get_readings(sample_resource_id)
                
                # Assert authentication was called
                mock_post.assert_called_once()
                
                # Assert readings request was made with token
                mock_get.assert_called_once()
                assert sample_resource_id in mock_get.call_args[0][0]
                assert mock_get.call_args[1]["headers"]["token"] == mock_token
                assert mock_get.call_args[1]["headers"]["applicationId"] == "test-app-id"
                
                # Assert data was returned
                assert result == mock_readings_response.json.return_value
    
    def test_get_readings_with_date_range(self, mock_token, sample_resource_id, 
                                         sample_date_range, mock_empty_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token)
        
        # Mock the request
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_date_range["start_date"], 
                end_date=sample_date_range["end_date"]
            )
            
            # Assert the request parameters
            params = mock_get.call_args[1]["params"]
            assert params["from"] == sample_date_range["start_date"]
            assert params["to"] == sample_date_range["end_date"]
    
    def test_get_readings_handles_datetime_objects(self, mock_token, sample_resource_id, 
                                                  sample_datetime_range, mock_empty_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token)
        
        # Mock the request
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_datetime_range["start_date"], 
                end_date=sample_datetime_range["end_date"]
            )
            
            # Assert the request parameters
            params = mock_get.call_args[1]["params"]
            assert params["from"] == "2023-01-01T00:00:00"
            assert params["to"] == "2023-01-07T23:59:59"
    
    def test_get_resources(self, mock_token, mock_resources_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        # Mock the request
        with patch("requests.get", return_value=mock_resources_response) as mock_get:
            result = client.get_resources()
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert "/resource" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "test-app-id"
            
            # Assert data was returned
            assert result == mock_resources_response.json.return_value
    
    def test_error_handling(self, mock_token):
        # Setup test
        client = GlowmarktClient(token=mock_token)
        
        # Test connection error
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with pytest.raises(ConnectionError):
                client.get_virtual_entities()  # Using virtual_entities instead of resources
        
        # Test JSON decode error
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(ValueError):
                client.get_virtual_entities()  # Using virtual_entities instead of resources
        
        # Test HTTP error
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Forbidden")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception):
                client.get_virtual_entities()  # Using virtual_entities instead of resources
    
    def test_get_readings_with_offset(self, mock_token, sample_resource_id, mock_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        # Define the offset (BST example from specification)
        offset = -60
        
        # Mock the request
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(sample_resource_id, offset=offset)
            
            # Assert the request was made with the correct offset parameter
            assert mock_get.call_args[1]["params"]["offset"] == offset
    
    def test_get_readings_with_all_parameters(self, mock_token, sample_resource_id, sample_date_range, mock_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        # Define all parameters
        start_date = sample_date_range["start_date"]
        end_date = sample_date_range["end_date"]
        period = "P1D"
        function = "sum"
        offset = -60
        
        # Mock the request
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(
                sample_resource_id, 
                start_date=start_date,
                end_date=end_date,
                period=period,
                function=function,
                offset=offset
            )
            
            # Assert all parameters were passed correctly
            params = mock_get.call_args[1]["params"]
            assert params["from"] == start_date
            assert params["to"] == end_date
            assert params["period"] == period
            assert params["function"] == function
            assert params["offset"] == offset
    
    def test_get_virtual_entities(self, mock_token, mock_virtual_entities_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        # Mock the request
        with patch("requests.get", return_value=mock_virtual_entities_response) as mock_get:
            result = client.get_virtual_entities()
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert "/virtualentity" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "test-app-id"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            # Assert data was returned
            assert result == mock_virtual_entities_response.json.return_value
            assert isinstance(result, list)
            assert len(result) > 0
            
            # Verify structure of the first entity
            entity = result[0]
            assert entity["name"] == "Smart Home 1"
            assert entity["veId"] == "dc9069a7-7695-43fd-8f27-16b1c94213da"
            assert "resources" in entity
            
            # Verify resources within the entity
            resources = entity["resources"]
            assert isinstance(resources, list)
            assert len(resources) == 2
            assert resources[0]["resourceId"] == "73f70bcd-3743-4009-a2c4-e98cc959c030"
