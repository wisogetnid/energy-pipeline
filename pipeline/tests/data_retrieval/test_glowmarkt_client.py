import pytest
import requests
from unittest.mock import patch, Mock
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestGlowmarktClient:
    
    def test_authenticate_returns_token(self, mock_auth_response, auth_patch):
        # Create client
        client = GlowmarktClient(username="test", password="password", application_id="test-app-id")
        
        # Mock the post request
        with auth_patch as mock_post:
            # Call authenticate
            token = client.authenticate()
            
            # Assert post was called with correct data
            mock_post.assert_called_once()
            assert "username" in mock_post.call_args[1]["json"]
            assert mock_post.call_args[1]["json"]["username"] == "test"
            assert mock_post.call_args[1]["json"]["password"] == "password"
            
            # Assert token was set in client and returned
            expected_token = mock_auth_response.json.return_value["token"]
            assert token == expected_token
            assert client.token == expected_token
    
    def test_get_readings_with_existing_token(self, mock_token, sample_resource_id, mock_readings_response, get_patch):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        # Mock the request
        with get_patch as mock_get:
            result = client.get_readings(sample_resource_id)
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert sample_resource_id in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            # Assert data was returned and matches the fixture
            expected_data = mock_readings_response.json.return_value
            assert result == expected_data
            assert result["status"] == "OK"
            assert "electricity consumption" in result["name"]
            assert isinstance(result["data"], list)
            assert len(result["data"]) == 2
            assert result["data"][0][1] == 48.79  # Check the value from the fixture

    def test_get_readings_auto_authenticates(self, mock_auth_response, sample_resource_id, mock_readings_response):
        # Create client without token
        client = GlowmarktClient(username="test", password="password", application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        # Mock requests
        with patch("requests.post", return_value=mock_auth_response) as mock_post:
            with patch("requests.get", return_value=mock_readings_response) as mock_get:
                result = client.get_readings(sample_resource_id)
                
                # Assert authentication was called
                mock_post.assert_called_once()
                
                # Assert readings request was made with token from authentication response
                mock_get.assert_called_once()
                assert sample_resource_id in mock_get.call_args[0][0]
                
                # Get the expected token from the authentication response
                expected_token = mock_auth_response.json.return_value["token"]
                assert mock_get.call_args[1]["headers"]["token"] == expected_token
                
                # Assert data was returned
                assert result == mock_readings_response.json.return_value
                assert result["status"] == "OK"
                assert "data" in result
                assert isinstance(result["data"], list)

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
            
            # Assert the request was called
            mock_get.assert_called_once()
            
            # Make sure call_args exists and has params
            assert mock_get.call_args is not None
            assert len(mock_get.call_args) >= 2
            assert mock_get.call_args[1] is not None
            assert "params" in mock_get.call_args[1]
            
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
    
    def test_error_handling(self, mock_token):
        # Setup test
        client = GlowmarktClient(token=mock_token)
        
        # Test connection error
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with pytest.raises(ConnectionError):
                client.get_virtual_entities()
        
        # Test JSON decode error
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b""  # Add this line
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(ValueError):
                client.get_virtual_entities()
        
        # Test HTTP error
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.content = b""  # Add this line
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Forbidden")
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception):
                client.get_virtual_entities()
    
    def test_get_readings_with_offset(self, mock_token, sample_resource_id, mock_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        # Define the offset (BST example from specification)
        offset = -60
        
        # Mock the request
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(sample_resource_id, offset=offset)
            
            # Assert the request was made with the correct offset parameter
            assert mock_get.call_args[1]["params"]["offset"] == offset
            
            # Verify result matches the fixture data
            assert result == mock_readings_response.json.return_value
            assert result["status"] == "OK"
            assert "resourceId" in result
            assert "query" in result

    def test_get_readings_with_all_parameters(self, mock_token, sample_resource_id, sample_date_range, mock_readings_response):
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
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
            
            # Verify result matches the fixture data
            assert result == mock_readings_response.json.return_value
            assert result["query"]["from"] == "2018-04-10T00:00:00"
            assert result["query"]["to"] == "2018-04-11T23:59:59"
            assert result["query"]["period"] == "P1D"
    
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
            assert result  # Checks if non-empty
            
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
    
    def test_get_virtual_entity_resources(self, mock_token, mock_ve_resources_response):
        """Test getting detailed resource information for a virtual entity."""
        # Create client with existing token
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        # Sample VE ID from the fixture
        ve_id = "dc9069a7-7695-43fd-8f27-16b1c94213da"
        
        # Mock the request
        with patch("requests.get", return_value=mock_ve_resources_response) as mock_get:
            result = client.get_virtual_entity_resources(ve_id)
            
            # Assert the request was made correctly
            mock_get.assert_called_once()
            assert f"/virtualentity/{ve_id}/resources" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            # Assert data was returned
            response_data = mock_ve_resources_response.json.return_value
            assert result == response_data
            assert result["name"] == "Smart Home 1"
            assert result["veId"] == ve_id
            
            # Check resources array
            resources = result["resources"]
            assert isinstance(resources, list)
            assert len(resources) == 4
            
            # Verify specific resource details
            electricity = resources[0]
            assert electricity["name"] == "electricity"
            assert electricity["classifier"] == "electricity.consumption"
            assert electricity["baseUnit"] == "kWh"
            
            gas = resources[2]
            assert gas["name"] == "gas"
            assert gas["classifier"] == "gas.consumption"
            assert gas["baseUnit"] == "kWh"
            
            # Verify cost resources
            electricity_cost = resources[1]
            assert electricity_cost["name"] == "electricity cost"
            assert electricity_cost["classifier"] == "electricity.consumption.cost"
            assert electricity_cost["baseUnit"] == "pence"
            
            gas_cost = resources[3]
            assert gas_cost["name"] == "gas cost"
            assert gas_cost["classifier"] == "gas.consumption.cost"
            assert gas_cost["baseUnit"] == "pence"
