import requests

def retrieve_data(url, params=None):
    """
    Retrieve JSON data from an API endpoint.
    
    Args:
        url (str): The API endpoint URL
        params (dict, optional): Query parameters to include in the request
        
    Returns:
        dict: The JSON response from the API
        
    Raises:
        ConnectionError: If there's a network error
        ValueError: If the response is not valid JSON
        Exception: If the HTTP status code indicates an error
    """
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises exception for 4XX/5XX responses
        return response.json()
    except requests.exceptions.ConnectionError as e:
        # Re-raise connection errors (e.g., network issues)
        raise ConnectionError(f"Failed to connect to {url}: {str(e)}")
    except requests.exceptions.JSONDecodeError as e:
        # Handle invalid JSON responses
        raise ValueError(f"Invalid JSON response from {url}: {str(e)}")
    except requests.exceptions.RequestException as e:
        # Handle other request errors (including HTTP error status codes)
        raise Exception(f"Error retrieving data from {url}: {str(e)}")