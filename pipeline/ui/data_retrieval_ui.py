#!/usr/bin/env python

import os
import json
from datetime import datetime, timedelta
from dateutil import parser

from pipeline.ui.base_ui import BaseUI
from pipeline.data_retrieval import GlowmarktClient, get_historical_readings

class DataRetrievalUI(BaseUI):
    
    def __init__(self, client=None):
        super().__init__()
        self.client = client
        self.selected_entity = None
        self.selected_resource_id = None
        self.selected_resource_name = None
        self.selected_resource_unit = None
        self.selected_resource_classifier = None
        self.start_date = None
        self.end_date = None
        self.period = "PT30M"
        self.offset = 0
        self.timezone_name = "UTC"
        self.date_range = ""
        self.batch_days = 10
    
    def setup_client(self, username=None, password=None, token=None):
        self.print_header("Glowmarkt API Authentication")
        
        self.client = GlowmarktClient(username=username, password=password, token=token)
        
        if not token and username and password:
            try:
                print("Authenticating with Glowmarkt API...")
                token = self.client.authenticate()
                print(f"Successfully authenticated. Token: {token[:10]}...")
                return True
            except Exception as e:
                print(f"Authentication failed: {str(e)}")
                return False
        return True
    
    def select_entity(self):
        if not self.client:
            print("Error: Client not initialized")
            return False
        
        try:
            self.print_header("Virtual Entity Selection")
            print("Fetching your virtual entities...")
            
            entities = self.client.get_virtual_entities()
            
            if not entities:
                print("No virtual entities found for your account.")
                return False
            
            self.selected_entity = entities[0]
            entity_name = self.selected_entity.get("name", "Unknown Entity")
            entity_id = self.selected_entity.get("veId", "Unknown ID")
            
            print(f"\nSelected virtual entity: {entity_name} (ID: {entity_id})")
            return True
            
        except Exception as e:
            print(f"Error fetching virtual entities: {str(e)}")
            return False
    
    def select_resource(self):
        try:
            self.print_header("Resource Selection")
            
            ve_id = self.selected_entity.get("veId")
            print(f"Fetching resources for entity {self.selected_entity.get('name')}...")
            
            entity_details = self.client.get_virtual_entity_resources(ve_id)
            resources = entity_details.get("resources", [])
            
            if not resources:
                print("No resources found for this entity.")
                return False
            
            return self._display_and_select_resource(resources)
            
        except Exception as e:
            print(f"Error fetching resources: {str(e)}")
            return False
    
    def _display_and_select_resource(self, resources):
        print("\nAvailable resources:")
        
        valid_resources = []
        for resource in resources:
            name = resource.get("name", "Unknown")
            classifier = resource.get("classifier", "Unknown")
            unit = resource.get("baseUnit", "Unknown")
            
            if "consumption" in classifier:
                valid_resources.append(resource)
        
        if not valid_resources:
            print("No consumption resources found.")
            return False
        
        for i, resource in enumerate(valid_resources, 1):
            name = resource.get("name", "Unknown")
            classifier = resource.get("classifier", "Unknown")
            unit = resource.get("baseUnit", "Unknown")
            
            print(f"{i}. {name} ({classifier}) [{unit}]")
        
        choice = self.get_int_input("\nSelect a resource: ", 1, len(valid_resources))
        selected = valid_resources[choice - 1]
        
        self.selected_resource_id = selected.get("resourceId")
        self.selected_resource_name = selected.get("name")
        self.selected_resource_unit = selected.get("baseUnit")
        self.selected_resource_classifier = selected.get("classifier")
        
        print(f"\nSelected: {self.selected_resource_name} ({self.selected_resource_classifier})")
        return True
    
    def select_time_range(self):
        self.print_header("Time Range Selection")
        
        print("Choose a date range:")
        print("1. Current month")
        print("2. Previous month")
        print("3. Two months ago")
        print("4. Three months ago")
        print("5. Custom range")
        
        choice = self.get_int_input("\nSelect a range: ", 1, 5)
        
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        if choice == 1:
            self.start_date = datetime(current_year, current_month, 1)
            if current_month == 12:
                self.end_date = datetime(current_year + 1, 1, 1) - timedelta(seconds=1)
            else:
                self.end_date = datetime(current_year, current_month + 1, 1) - timedelta(seconds=1)
            self.date_range = f"current month ({self.start_date.strftime('%B %Y')})"
        
        elif choice == 2:
            prev_month = current_month - 1
            prev_year = current_year
            if prev_month == 0:
                prev_month = 12
                prev_year -= 1
            
            self.start_date = datetime(prev_year, prev_month, 1)
            self.end_date = datetime(current_year, current_month, 1) - timedelta(seconds=1)
            self.date_range = f"previous month ({self.start_date.strftime('%B %Y')})"
        
        elif choice == 3:
            prev_month = current_month - 2
            prev_year = current_year
            while prev_month <= 0:
                prev_month += 12
                prev_year -= 1
            
            next_month = prev_month + 1
            next_year = prev_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            
            self.start_date = datetime(prev_year, prev_month, 1)
            self.end_date = datetime(next_year, next_month, 1) - timedelta(seconds=1)
            self.date_range = f"two months ago ({self.start_date.strftime('%B %Y')})"
        
        elif choice == 4:
            prev_month = current_month - 3
            prev_year = current_year
            while prev_month <= 0:
                prev_month += 12
                prev_year -= 1
            
            next_month = prev_month + 1
            next_year = prev_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            
            self.start_date = datetime(prev_year, prev_month, 1)
            self.end_date = datetime(next_year, next_month, 1) - timedelta(seconds=1)
            self.date_range = f"three months ago ({self.start_date.strftime('%B %Y')})"
        
        elif choice == 5:
            try:
                start_input = input("\nEnter start date (YYYY-MM-DD): ")
                self.start_date = parser.parse(start_input)
                
                end_input = input("Enter end date (YYYY-MM-DD): ")
                self.end_date = parser.parse(end_input)
                
                if self.start_date > self.end_date:
                    print("Error: Start date must be before end date")
                    return False
                
                self.date_range = f"custom range: {self.start_date.date()} to {self.end_date.date()}"
            except Exception as e:
                print(f"Error parsing dates: {str(e)}")
                return False
        
        print(f"\nSelected date range: {self.date_range}")
        return True
    
    def retrieve_data(self):
        self.print_header("Retrieving Data")
        
        try:
            print(f"Fetching data for {self.selected_resource_name} over {self.date_range}...")
            print(f"Using PT30M granularity and UTC timezone...")
            print(f"This may take a while for large date ranges...")
            
            readings = get_historical_readings(
                self.client,
                self.selected_resource_id,
                self.start_date,
                self.end_date,
                period=self.period,
                offset=self.offset,
                batch_days=self.batch_days
            )
            
            return readings
        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None
    
    def display_readings(self, readings):
        if not readings:
            print("No readings found")
            return
        
        print(f"\nRetrieved {len(readings)} readings")
        
        readings_count = min(5, len(readings))
        if readings_count > 0:
            print("\nSample readings (first few):")
            for i in range(readings_count):
                reading = readings[i]
                timestamp = reading[0]
                value = reading[1]
                
                try:
                    timestamp_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                    dt = datetime.fromtimestamp(timestamp_seconds)
                    date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    date_str = str(timestamp)
                
                print(f"{date_str}: {value} {self.selected_resource_unit}")
    
    def save_data(self, readings):
        self.print_header("Save Data")
        
        if not readings:
            print("No data to save")
            return None
        
        try:
            data_dir = os.path.join("data", "glowmarkt_api_raw")
            os.makedirs(data_dir, exist_ok=True)
            
            resource_name_safe = self.selected_resource_name.lower().replace(" ", "_")
            start_date_str = self.start_date.strftime("%Y%m%d") if isinstance(self.start_date, datetime) else "unknown"
            end_date_str = self.end_date.strftime("%Y%m%d") if isinstance(self.end_date, datetime) else "unknown"
            filename = f"{resource_name_safe}_{start_date_str}_to_{end_date_str}.json"
            filepath = os.path.join(data_dir, filename)
            
            data = {
                "resource_id": self.selected_resource_id,
                "resource_name": self.selected_resource_name,
                "resource_unit": self.selected_resource_unit,
                "resource_classifier": self.selected_resource_classifier,
                "start_date": self.start_date.isoformat() if isinstance(self.start_date, datetime) else self.start_date,
                "end_date": self.end_date.isoformat() if isinstance(self.end_date, datetime) else self.end_date,
                "period": self.period,
                "timezone_offset": self.offset,
                "readings": readings
            }
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"\nData saved to: {filepath}")
            print(f"Total readings: {len(readings)}")
            
            return filepath
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            return None
    
    def run(self):
        if not self.client:
            if not self.setup_client():
                return
        
        print("\nNote: First virtual entity will be automatically selected.")
        if not self.select_entity():
            return
        
        if not self.select_resource():
            return
            
        if not self.select_time_range():
            return
        
        readings = self.retrieve_data()
        if readings:
            self.display_readings(readings)
            json_filepath = self.save_data(readings)
            
            if json_filepath:
                return json_filepath
        else:
            print("Failed to retrieve readings. Please try again.")
        
        return None