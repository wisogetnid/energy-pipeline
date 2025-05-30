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
            
            return self._select_resource_basic(resources)
            
        except Exception as e:
            print(f"Error fetching resources: {str(e)}")
            return False
    
    def _select_resource_basic(self, resources):
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
        print("1. Last 24 hours")
        print("2. Last 7 days")
        print("3. Last 30 days")
        print("4. Last 90 days")
        print("5. Last year")
        print("6. Custom range")
        
        choice = self.get_int_input("\nSelect a range: ", 1, 6)
        
        now = datetime.now()
        
        if choice == 1:
            self.end_date = now
            self.start_date = now - timedelta(days=1)
            self.date_range = "last 24 hours"
        elif choice == 2:
            self.end_date = now
            self.start_date = now - timedelta(days=7)
            self.date_range = "last 7 days"
        elif choice == 3:
            self.end_date = now
            self.start_date = now - timedelta(days=30)
            self.date_range = "last 30 days"
        elif choice == 4:
            self.end_date = now
            self.start_date = now - timedelta(days=90)
            self.date_range = "last 90 days"
        elif choice == 5:
            self.end_date = now
            self.start_date = now - timedelta(days=365)
            self.date_range = "last year"
        elif choice == 6:
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
    
    def select_granularity(self):
        self.print_header("Data Granularity")
        
        print("Choose data granularity:")
        print("1. 30 minutes (PT30M)")
        print("2. 1 hour (PT1H)")
        print("3. 1 day (P1D)")
        print("4. 1 week (P1W)")
        print("5. 1 month (P1M)")
        
        choice = self.get_int_input("\nSelect a granularity: ", 1, 5)
        
        if choice == 1:
            self.period = "PT30M"
        elif choice == 2:
            self.period = "PT1H"
        elif choice == 3:
            self.period = "P1D"
        elif choice == 4:
            self.period = "P1W"
        elif choice == 5:
            self.period = "P1M"
        
        print(f"\nSelected granularity: {self.period}")
        return True
    
    def select_timezone(self):
        self.print_header("Timezone Selection")
        
        print("Choose timezone offset:")
        print("1. UTC (+0)")
        print("2. UK/Ireland (+0/+1)")
        print("3. Central Europe (+1/+2)")
        print("4. US Eastern (-5/-4)")
        print("5. US Pacific (-8/-7)")
        print("6. Custom offset")
        
        choice = self.get_int_input("\nSelect a timezone: ", 1, 6)
        
        if choice == 1:
            self.offset = 0
            self.timezone_name = "UTC"
        elif choice == 2:
            self.offset = 0  # or 60 during DST
            self.timezone_name = "UK/Ireland"
        elif choice == 3:
            self.offset = 60  # or 120 during DST
            self.timezone_name = "Central Europe"
        elif choice == 4:
            self.offset = -300  # or -240 during DST
            self.timezone_name = "US Eastern"
        elif choice == 5:
            self.offset = -480  # or -420 during DST
            self.timezone_name = "US Pacific"
        elif choice == 6:
            try:
                custom_offset = int(input("\nEnter offset in minutes (e.g., 60 for +1 hour, -300 for -5 hours): "))
                self.offset = custom_offset
                self.timezone_name = f"Custom ({custom_offset} minutes)"
            except ValueError:
                print("Invalid offset. Using UTC.")
                self.offset = 0
                self.timezone_name = "UTC"
        
        print(f"\nSelected timezone: {self.timezone_name} (offset: {self.offset} minutes)")
        return True
    
    def confirm_settings(self):
        self.print_header("Confirm Settings")
        
        print(f"Resource: {self.selected_resource_name} ({self.selected_resource_classifier})")
        print(f"Date Range: {self.date_range}")
        print(f"Granularity: {self.period}")
        print(f"Timezone: {self.timezone_name} (offset: {self.offset} minutes)")
        print(f"Batch Size: {self.batch_days} days")
        
        return self.get_yes_no_input("\nProceed with these settings?")
    
    def retrieve_data(self):
        self.print_header("Retrieving Data")
        
        try:
            print(f"Fetching data for {self.selected_resource_name} over {self.date_range}...")
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
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            resource_name_safe = self.selected_resource_name.lower().replace(" ", "_")
            filename = f"{resource_name_safe}_{timestamp}.json"
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
        
        while True:
            if not self.select_resource():
                break
            
            if not self.select_time_range():
                break
            
            if not self.select_granularity():
                break
            
            if not self.select_timezone():
                break
            
            if not self.confirm_settings():
                print("Operation cancelled")
                break
            
            readings = self.retrieve_data()
            if readings:
                self.display_readings(readings)
                json_filepath = self.save_data(readings)
                
                if json_filepath:
                    return json_filepath
                
                another = self.get_yes_no_input("\nWould you like to retrieve data for another resource?")
                if not another:
                    break
            else:
                print("Failed to retrieve readings. Please try again.")
                break
        
        return None