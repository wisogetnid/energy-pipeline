import pytest
from unittest.mock import patch, Mock
from pipeline.data_retrieval.retrieve import retrieve_data  

def test_retrieve_data_returns_json():
    # Test successful API call with valid JSON response
    mock_json = {"status": "OK", "data": [1, 2, 3]}
    mock_response = Mock()
    mock_response.json.return_value = mock_json
    mock_response.status_code = 200

    with patch("requests.get", return_value=mock_response) as mock_get:
        url = "https://example.com/api"
        params = None
        result = retrieve_data(url)
        mock_get.assert_called_once_with(url, params=params)
        assert result == mock_json

def test_retrieve_data_with_params():
    # Test API call with query parameters
    mock_json = {"status": "OK", "data": [4, 5, 6]}
    mock_response = Mock()
    mock_response.json.return_value = mock_json
    mock_response.status_code = 200

    with patch("requests.get", return_value=mock_response) as mock_get:
        url = "https://example.com/api"
        params = {"from": "2025-03-27", "to": "2025-03-31"}
        result = retrieve_data(url, params=params)
        mock_get.assert_called_once_with(url, params=params)
        assert result == mock_json

def test_retrieve_data_handles_error():
    # Test handling of HTTP error response
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = Exception("404 Not Found")

    with patch("requests.get", return_value=mock_response) as mock_get:
        url = "https://example.com/api"
        with pytest.raises(Exception):
            retrieve_data(url)

def test_retrieve_data_handles_connection_error():
    # Test handling of connection error
    with patch("requests.get", side_effect=ConnectionError("Failed to connect")) as mock_get:
        url = "https://example.com/api"
        with pytest.raises(ConnectionError):
            retrieve_data(url)

def test_retrieve_data_handles_json_decode_error():
    # Test handling of invalid JSON response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")

    with patch("requests.get", return_value=mock_response) as mock_get:
        url = "https://example.com/api"
        with pytest.raises(ValueError):
            retrieve_data(url)