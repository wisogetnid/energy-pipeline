import pytest
import requests
from unittest.mock import patch, Mock
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestGlowmarktClient:
    
    def test_authenticate_returns_token(self, mock_auth_response, auth_patch):
        client = GlowmarktClient(username="test", password="password", application_id="test-app-id")
        
        with auth_patch as mock_post:
            token = client.authenticate()
            
            mock_post.assert_called_once()
            assert "username" in mock_post.call_args[1]["json"]
            assert mock_post.call_args[1]["json"]["username"] == "test"
            assert mock_post.call_args[1]["json"]["password"] == "password"
            
            expected_token = mock_auth_response.json.return_value["token"]
            assert token == expected_token
            assert client.token == expected_token
    
    def test_get_readings_with_existing_token(self, mock_token, sample_resource_id, mock_readings_response, get_patch):
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        with get_patch as mock_get:
            result = client.get_readings(sample_resource_id)
            
            mock_get.assert_called_once()
            assert sample_resource_id in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            expected_data = mock_readings_response.json.return_value
            assert result == expected_data
            assert result["status"] == "OK"
            assert "electricity consumption" in result["name"]
            assert isinstance(result["data"], list)
            assert len(result["data"]) == 2
            assert result["data"][0][1] == 48.79

    def test_get_readings_auto_authenticates(self, mock_auth_response, sample_resource_id, mock_readings_response):
        client = GlowmarktClient(username="test", password="password", application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        with patch("requests.post", return_value=mock_auth_response) as mock_post:
            with patch("requests.get", return_value=mock_readings_response) as mock_get:
                result = client.get_readings(sample_resource_id)
                
                mock_post.assert_called_once()
                
                mock_get.assert_called_once()
                assert sample_resource_id in mock_get.call_args[0][0]
                
                expected_token = mock_auth_response.json.return_value["token"]
                assert mock_get.call_args[1]["headers"]["token"] == expected_token
                
                assert result == mock_readings_response.json.return_value
                assert result["status"] == "OK"
                assert "data" in result
                assert isinstance(result["data"], list)

    def test_get_readings_with_date_range(self, mock_token, sample_resource_id, 
                                     sample_date_range, mock_empty_readings_response):
        client = GlowmarktClient(token=mock_token)
        
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_date_range["start_date"], 
                end_date=sample_date_range["end_date"]
            )
            
            assert mock_get.call_args is not None
            assert len(mock_get.call_args) >= 2
            assert mock_get.call_args[1] is not None
            assert "params" in mock_get.call_args[1]
            
            params = mock_get.call_args[1]["params"]
            assert params["from"] == sample_date_range["start_date"]
            assert params["to"] == sample_date_range["end_date"]
    
    def test_get_readings_handles_datetime_objects(self, mock_token, sample_resource_id, 
                                                  sample_datetime_range, mock_empty_readings_response):
        client = GlowmarktClient(token=mock_token)
        
        with patch("requests.get", return_value=mock_empty_readings_response) as mock_get:
            client.get_readings(
                sample_resource_id, 
                start_date=sample_datetime_range["start_date"], 
                end_date=sample_datetime_range["end_date"]
            )
            
            params = mock_get.call_args[1]["params"]
            assert params["from"] == "2023-01-01T00:00:00"
            assert params["to"] == "2023-01-07T23:59:59"
    
    def test_error_handling(self, mock_token):
        client = GlowmarktClient(token=mock_token)
        
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with pytest.raises(ConnectionError):
                client.get_virtual_entities()
        
        invalid_json_response = Mock()
        invalid_json_response.status_code = 200
        invalid_json_response.content = b""
        invalid_json_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch("requests.get", return_value=invalid_json_response):
            with pytest.raises(ValueError):
                client.get_virtual_entities()
        
        forbidden_response = Mock()
        forbidden_response.status_code = 403
        forbidden_response.content = b""
        forbidden_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Forbidden")
        
        with patch("requests.get", return_value=forbidden_response):
            with pytest.raises(Exception):
                client.get_virtual_entities()
    
    def test_get_readings_with_offset(self, mock_token, sample_resource_id, mock_readings_response):
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        timezone_offset = -60
        
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(sample_resource_id, offset=timezone_offset)
            
            assert mock_get.call_args[1]["params"]["offset"] == timezone_offset
            
            assert result == mock_readings_response.json.return_value
            assert result["status"] == "OK"
            assert "resourceId" in result
            assert "query" in result

    def test_get_readings_with_all_parameters(self, mock_token, sample_resource_id, sample_date_range, mock_readings_response):
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        start_date = sample_date_range["start_date"]
        end_date = sample_date_range["end_date"]
        period = "P1D"
        function = "sum"
        timezone_offset = -60
        
        with patch("requests.get", return_value=mock_readings_response) as mock_get:
            result = client.get_readings(
                sample_resource_id, 
                start_date=start_date,
                end_date=end_date,
                period=period,
                function=function,
                offset=timezone_offset
            )
            
            request_params = mock_get.call_args[1]["params"]
            assert request_params["from"] == start_date
            assert request_params["to"] == end_date
            assert request_params["period"] == period
            assert request_params["function"] == function
            assert request_params["offset"] == timezone_offset
            
            assert result == mock_readings_response.json.return_value
            assert result["query"]["from"] == "2018-04-10T00:00:00"
            assert result["query"]["to"] == "2018-04-11T23:59:59"
            assert result["query"]["period"] == "P1D"
    
    def test_get_virtual_entities(self, mock_token, mock_virtual_entities_response):
        client = GlowmarktClient(token=mock_token, application_id="test-app-id")
        
        with patch("requests.get", return_value=mock_virtual_entities_response) as mock_get:
            entities = client.get_virtual_entities()
            
            mock_get.assert_called_once()
            assert "/virtualentity" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "test-app-id"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            assert entities == mock_virtual_entities_response.json.return_value
            assert isinstance(entities, list)
            assert entities
            
            first_entity = entities[0]
            assert first_entity["name"] == "Smart Home 1"
            assert first_entity["veId"] == "dc9069a7-7695-43fd-8f27-16b1c94213da"
            assert "resources" in first_entity
            
            entity_resources = first_entity["resources"]
            assert isinstance(entity_resources, list)
            assert len(entity_resources) == 2
            assert entity_resources[0]["resourceId"] == "73f70bcd-3743-4009-a2c4-e98cc959c030"
    
    def test_get_virtual_entity_resources(self, mock_token, mock_ve_resources_response):
        client = GlowmarktClient(token=mock_token, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d")
        
        virtual_entity_id = "dc9069a7-7695-43fd-8f27-16b1c94213da"
        
        with patch("requests.get", return_value=mock_ve_resources_response) as mock_get:
            entity_details = client.get_virtual_entity_resources(virtual_entity_id)
            
            mock_get.assert_called_once()
            assert f"/virtualentity/{virtual_entity_id}/resources" in mock_get.call_args[0][0]
            assert mock_get.call_args[1]["headers"]["token"] == mock_token
            assert mock_get.call_args[1]["headers"]["applicationId"] == "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
            assert mock_get.call_args[1]["headers"]["Content-Type"] == "application/json"
            
            response_data = mock_ve_resources_response.json.return_value
            assert entity_details == response_data
            assert entity_details["name"] == "Smart Home 1"
            assert entity_details["veId"] == virtual_entity_id
            
            resources_list = entity_details["resources"]
            assert isinstance(resources_list, list)
            assert len(resources_list) == 4
            
            electricity_resource = resources_list[0]
            assert electricity_resource["name"] == "electricity"
            assert electricity_resource["classifier"] == "electricity.consumption"
            assert electricity_resource["baseUnit"] == "kWh"
            
            gas_resource = resources_list[2]
            assert gas_resource["name"] == "gas"
            assert gas_resource["classifier"] == "gas.consumption"
            assert gas_resource["baseUnit"] == "kWh"
            
            electricity_cost_resource = resources_list[1]
            assert electricity_cost_resource["name"] == "electricity cost"
            assert electricity_cost_resource["classifier"] == "electricity.consumption.cost"
            assert electricity_cost_resource["baseUnit"] == "pence"
            
            gas_cost_resource = resources_list[3]
            assert gas_cost_resource["name"] == "gas cost"
            assert gas_cost_resource["classifier"] == "gas.consumption.cost"
            assert gas_cost_resource["baseUnit"] == "pence"
