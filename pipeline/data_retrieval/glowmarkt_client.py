import requests
from datetime import datetime, timedelta

class GlowmarktClient:
    """Client for interacting with the Glowmarkt API."""
    
    def __init__(self, base_url="https://api.glowmarkt.com/api/v0-1", username=None, password=None, token=None):
        """Initialize the client with credentials.
        
        Args:
            base_url (str): Base URL for the Glowmarkt API
            username (str, optional): Glowmarkt username
            password (str, optional): Glowmarkt password
            token (str, optional): Existing API token if already authenticated
        """
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = token
        
    def authenticate(self):
        """Authenticate and get an API token.
        
        Returns:
            str: The authentication token
            
        Raises:
            Exception: If authentication fails
        """
        auth_url = f"{self.base_url}/auth"
        payload = {
            "username": self.username,
            "password": self.password
        }
        response = requests.post(auth_url, json=payload)
        response.raise_for_status()
        data = response.json()
        self.token = data.get("token")
        return self.token
        
    def get_readings(self, resource_id, start_date=None, end_date=None, period="PT30M", function="sum"):
        """Get readings for a specific resource.
        
        Args:
            resource_id (str): The resource ID to fetch data for
            start_date (str or datetime, optional): Start date for readings
            end_date (str or datetime, optional): End date for readings
            period (str, optional): Data granularity (e.g., "PT30M" for 30 minutes)
            function (str, optional): Aggregation function to apply
            
        Returns:
            dict: The JSON response containing readings data
            
        Raises:
            ValueError: If no token is available and no credentials were provided
            ConnectionError: If there's a network error
            Exception: If the API returns an error
        """
        if not self.token and not (self.username and self.password):
            raise ValueError("No authentication token or credentials provided")
            
        if not self.token:
            self.authenticate()
            
        # Default to last 7 days if no dates provided
        if not start_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
        # Format dates if they're datetime objects
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            
        url = f"{self.base_url}/resource/{resource_id}/readings"
        params = {
            "from": start_date,
            "to": end_date,
            "period": period,
            "function": function
        }
        headers = {"token": self.token}
        
        return self._make_request(url, params=params, headers=headers)
    
    def get_resources(self):
        """Get all resources available to the authenticated user.
        
        Returns:
            dict: The JSON response containing resources
            
        Raises:
            ValueError: If no token is available and no credentials were provided
        """
        if not self.token and not (self.username and self.password):
            raise ValueError("No authentication token or credentials provided")
            
        if not self.token:
            self.authenticate()
            
        url = f"{self.base_url}/resource"
        headers = {"token": self.token}
        
        return self._make_request(url, headers=headers)
    
    def _make_request(self, url, method="get", params=None, headers=None, json=None):
        """Make an HTTP request to the Glowmarkt API.
        
        Args:
            url (str): The API endpoint URL
            method (str, optional): HTTP method (get, post, etc.)
            params (dict, optional): Query parameters
            headers (dict, optional): HTTP headers
            json (dict, optional): JSON payload for POST requests
            
        Returns:
            dict: The JSON response from the API
            
        Raises:
            ConnectionError: If there's a network error
            ValueError: If the response is not valid JSON
            Exception: If the HTTP status code indicates an error
        """
        try:
            request_method = getattr(requests, method.lower())
            response = request_method(url, params=params, headers=headers, json=json)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to {url}: {str(e)}")
        except requests.exceptions.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response from {url}: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error retrieving data from {url}: {str(e)}")