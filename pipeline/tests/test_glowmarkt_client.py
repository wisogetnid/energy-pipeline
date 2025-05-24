import pytest
import requests
from unittest.mock import patch, Mock
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestGlowmarktClient:
    
    def test_authenticate_returns_token(self, mock_token, auth_patch):
        # Create client and mock the post request
        client = GlowmarktClient(username="test", password="password")
        with auth_patch as mock_post:
            token = client.authenticate()
            
            # Assert the request was made correctly
            mock_post.assert_called_once()
            assert "auth" in mock_post.call_args[0][0]
            assert mock_post.call_args[1]["json"] == {"username": "test", "password": "password"}
            
            # Assert token was returned and stored
            assert token == mock_token
            assert client.token == mock_token
    
    def test_get_readings_with_existing_token(self, mock_token, sample_resource_id, mock_readings_response, get_patch):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token)
        
        # Mock the request
        with get_patch as mock_get:
            result = client.get_readings(sample_resource_id)
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert sample_resource_id in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"] == {"token": mock_token}
            
            # Assert data was returned
            assert result == mock_readings_response.json.return_value
    
    def test_get_readings_auto_authenticates(self, mock_token, sample_resource_id, 
                                            mock_auth_response, mock_readings_response):
        # Create client without token
        client = GlowmarktClient(username="test", password="password")
        
        # Mock requests
        with patch("requests.post", return_value=mock_auth_response) as mock_post:
            with patch("requests.get", return_value=mock_readings_response) as mock_get:
                result = client.get_readings(sample_resource_id)
                
                # Assert authentication was called
                mock_post.assert_called_once()
                
                # Assert readings request was made with token
                mock_get.assert_called_once()
                assert sample_resource_id in mock_get.call_args[0][0]
                assert mock_get.call_args[1]["headers"] == {"token": mock_token}
                
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
        client = GlowmarktClient(token=mock_token)
        
        # Mock the request
        with patch("requests.get", return_value=mock_resources_response) as mock_get:
            result = client.get_resources()
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert "/resource" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"] == {"token": mock_token}
            
            # Assert data was returned
            assert result == mock_resources_response.json.return_value
    
    def test_error_handling(self, mock_token):
        # Setup test
        client = GlowmarktClient(token=mock_token)
        
        # Test connection error
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with pytest.raises(ConnectionError):
                client.get_resources()
        
        # Test JSON decode error
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(ValueError):
                client.get_resources()
        
        # Test HTTP error
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Forbidden")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception):
                client.get_resources()
