import json
import requests
from datetime import datetime, timedelta

class GlowmarktClient:
    
    def __init__(self, base_url="https://api.glowmarkt.com/api/v0-1", username=None, password=None, token=None, application_id="b0f1b774-a586-4f72-9edd-27ead8aa7a8d"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = token
        self.application_id = application_id
        
    def authenticate(self):
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
        if not self.token and not (self.username and self.password):
            raise ValueError("No authentication token or credentials provided")
        
        if not self.token:
            self.authenticate()
        
        if not start_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
        
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
        
        if offset is not None:
            params["offset"] = offset
        
        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application_id,
            "token": self.token,
        }
        
        return self._make_request(url, params=params, headers=headers)
    
    def get_virtual_entity_resources(self, ve_id):
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