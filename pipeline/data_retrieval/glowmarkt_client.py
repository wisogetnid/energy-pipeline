import json
import requests
from datetime import datetime, timedelta

class GlowmarktClient:
    """Client for interacting with the Glowmarkt API."""
    
    def __init__(self, base_url="https://api.glowmarkt.com/api/v0-1", username=None, password=None, token=None, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d"):
        """Initialize the client with credentials.
        
        Args:
            base_url (str): Base URL for the Glowmarkt API
            username (str, optional): Glowmarkt username
            password (str, optional): Glowmarkt password
            token (str, optional): Existing API token if already authenticated
            application_id (str, optional): Application ID for authentication
        """
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = token
        self.application_id = application_id
        
    def authenticate(self):
        """Authenticate and get an API token.
        
        Returns:
            str: The authentication token
            
        Raises:
            Exception: If authentication fails
        """
        auth_url = f"{self.base_url}/auth"
        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application_id
        }
        payload = {
            "username": self.username,
            "password": self.password
        }
        response = requests.post(auth_url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        self.token = data.get("token")
        return self.token
    
    def get_virtual_entities(self):
        """Get all virtual entities available to the authenticated user.
        
        Virtual entities represent logical groupings like homes or businesses,
        each containing multiple resources (meters, devices, etc.).
        
        Returns:
            list: A list of virtual entities with their associated resources
            
        Raises:
            ValueError: If no token is available and no credentials were provided
            ConnectionError: If there's a network error
            Exception: If the API returns an error
        """
        if not self.token and not (self.username and self.password):
            raise ValueError("No authentication token or credentials provided")
            
        if not self.token:
            self.authenticate()
            
        url = f"{self.base_url}/virtualentity"
        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application_id,
            "token": self.token,
        }
        
        return self._make_request(url, headers=headers)
        
    def get_readings(self, resource_id, start_date=None, end_date=None, period="PT30M", function="sum", offset=None):
        """Get readings for a specific resource."""
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
        
        # Use proper URL and params approach instead of building URL manually
        url = f"{self.base_url}/resource/{resource_id}/readings"
        params = {
            "from": start_date,
            "to": end_date,
            "period": period,
            "function": function
        }
        
        # Add offset parameter if provided
        if offset is not None:
            params["offset"] = offset
        
        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application_id,
            "token": self.token,
        }
        
        return self._make_request(url, params=params, headers=headers)
    
    def get_virtual_entity_resources(self, ve_id):
        """Get detailed resource information for a specific virtual entity.
        
        This endpoint returns the full definition of all resources that belong to a 
        virtual entity, including detailed information like resource type, classifier,
        description, and base unit.
        
        Args:
            ve_id (str): The ID of the virtual entity
            
        Returns:
            dict: Virtual entity details with full resource definitions
            
        Raises:
            ValueError: If no token is available and no credentials were provided
            ConnectionError: If there's a network error
            Exception: If the API returns an error
        """
        if not self.token and not (self.username and self.password):
            raise ValueError("No authentication token or credentials provided")
            
        if not self.token:
            self.authenticate()
            
        url = f"{self.base_url}/virtualentity/{ve_id}/resources"
        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application_id,
            "token": self.token,
        }
        
        return self._make_request(url, headers=headers)
    
    def _make_request(self, url, method="get", params=None, headers=None, json_data=None):
        """Make an HTTP request to the Glowmarkt API."""
        try:
            request_method = getattr(requests, method.lower())
            response = request_method(url, params=params, headers=headers, json=json_data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to {url}: {str(e)}")
        except requests.exceptions.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response from {url}: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error retrieving data from {url}: {str(e)}")