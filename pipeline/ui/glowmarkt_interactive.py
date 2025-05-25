#!/usr/bin/env python
"""Interactive UI for Glowmarkt API client."""

import sys
import os
import json
from datetime import datetime, timedelta
from dateutil import parser

from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient
from pipeline.data_retrieval.batch_retrieval import get_historical_readings


class GlowmarktUI:
    """Interactive UI for interacting with the Glowmarkt API."""

    def __init__(self):
        """Initialize the UI component."""
        self.client = None
        self.selected_entity = None
        self.selected_resource_id = None
        self.start_date = None
        self.end_date = None
        self.period = "PT30M"
        self.offset = 0
        self.timezone_name = "UTC"
        self.date_range = ""
        self.batch_days = 10

    def setup_client(self, username=None, password=None, token=None):
        """Set up the Glowmarkt client with authentication."""
        print("====================================")
        print("   Interactive Glowmarkt Client    ")
        print("====================================\n")
        
        # Create client
        self.client = GlowmarktClient(username=username, password=password, token=token)
        
        # If we don't have a token but have credentials, authenticate to get one
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
        """Automatically select the first virtual entity."""
        try:
            print("\nFetching virtual entities...")
            virtual_entities = self.client.get_virtual_entities()
            
            if not virtual_entities:
                print("No virtual entities found.")
                return False
            
            # Display available entities (informational only)
            print("\nAvailable Virtual Entities:")
            for i, entity in enumerate(virtual_entities, 1):
                print(f"{i}. {entity.get('name', 'Unknown Entity')} (ID: {entity.get('veId')})")
            
            # Automatically select the first entity
            self.selected_entity = virtual_entities[0]
            
            print(f"\nAutomatically selected first entity: {self.selected_entity.get('name')}")
            return True
        except Exception as e:
            print(f"Error selecting entity: {str(e)}")
            return False

    def select_resource(self):
        """Let user select a resource from the selected entity with essential information."""
        if not self.selected_entity:
            print("No entity selected.")
            return False
        
        try:
            # Get basic resources first
            basic_resources = self.selected_entity.get('resources', [])
            if not basic_resources:
                print("No resources found for this entity.")
                return False
            
            # Get detailed resource information
            ve_id = self.selected_entity.get('veId')
            print(f"\nFetching detailed resource information for {self.selected_entity.get('name')}...")
            
            detailed_entity = self.client.get_virtual_entity_resources(ve_id)
            detailed_resources = detailed_entity.get('resources', [])
            
            if not detailed_resources:
                print("No detailed resources found. Using basic resource information.")
                # Fall back to basic resource selection
                return self._select_resource_basic(basic_resources)
            
            print("\nAvailable Resources:")
            for i, resource in enumerate(detailed_resources, 1):
                name = resource.get('name', 'Unknown')
                classifier = resource.get('classifier', 'Unknown')
                base_unit = resource.get('baseUnit', 'Unknown')
                
                print(f"{i}. {name.title()} ({classifier})")
                print(f"   Unit: {base_unit}")
                print("")
            
            # Let user choose a resource
            while True:
                try:
                    choice = int(input("\nSelect a resource number: "))
                    if 1 <= choice <= len(detailed_resources):
                        selected_resource = detailed_resources[choice-1]
                        self.selected_resource_id = selected_resource.get('resourceId')
                        self.selected_resource_name = selected_resource.get('name', 'Unknown')
                        self.selected_resource_unit = selected_resource.get('baseUnit', 'Unknown')
                        self.selected_resource_classifier = selected_resource.get('classifier', 'Unknown')
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(detailed_resources)}")
                except ValueError:
                    print("Please enter a valid number")
            
            print(f"\nSelected: {self.selected_resource_name} ({self.selected_resource_classifier})")
            print(f"Unit: {self.selected_resource_unit}")
            return True
        
        except Exception as e:
            print(f"Error fetching detailed resources: {str(e)}")
            print("Falling back to basic resource selection...")
            # Fall back to basic resource selection
            return self._select_resource_basic(basic_resources)

    def _select_resource_basic(self, resources):
        """Basic resource selection with minimal information."""
        if not resources:
            print("No resources found for this entity.")
            return False
        
        print("\nAvailable Resources (Basic Information):")
        for i, resource in enumerate(resources, 1):
            resource_type = resource.get('resourceTypeId', 'Unknown')
            print(f"{i}. Type: {resource_type}")
    
        # Let user choose a resource
        while True:
            try:
                choice = int(input("\nSelect a resource number: "))
                if 1 <= choice <= len(resources):
                    selected_resource = resources[choice-1]
                    self.selected_resource_id = selected_resource.get('resourceId')
                    self.selected_resource_name = resource_type
                    self.selected_resource_unit = "Unknown"
                    self.selected_resource_classifier = "Unknown"
                    break
                else:
                    print(f"Please enter a number between 1 and {len(resources)}")
            except ValueError:
                print("Please enter a valid number")
    
        print(f"\nSelected: {self.selected_resource_name}")
        return True

    def select_time_range(self):
        """Let user select a time range."""
        print("\nTime Range Options:")
        print("1. Last day")
        print("2. Last week")
        print("3. Last month")
        print("4. Last year")
        print("5. Custom range")
        
        while True:
            try:
                choice = int(input("\nSelect a time range: "))
                if 1 <= choice <= 5:
                    break
                else:
                    print("Please enter a number between 1 and 5")
            except ValueError:
                print("Please enter a valid number")
        
        # Set date range based on user choice
        # Set end_date to midnight of the current day
        self.end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if choice == 1:
            # Last day
            self.start_date = self.end_date - timedelta(hours=24)
            self.date_range = "last day"
        elif choice == 2:
            # Last week
            self.start_date = self.end_date - timedelta(days=7)
            self.date_range = "last 7 days"
        elif choice == 3:
            # Last month
            self.start_date = self.end_date - timedelta(days=30)
            self.date_range = "last 30 days"
        elif choice == 4:
            # Last year
            self.start_date = self.end_date - timedelta(days=365)
            self.date_range = "last 365 days"
        else:
            # Custom range
            while True:
                try:
                    start_input = input("\nEnter start date (YYYY-MM-DD): ")
                    self.start_date = parser.parse(start_input)
                    
                    end_input = input("Enter end date (YYYY-MM-DD or press Enter for current date): ")
                    if end_input.strip():
                        self.end_date = parser.parse(end_input)
                    
                    if self.start_date > self.end_date:
                        print("Start date must be before end date")
                        continue
                    
                    self.date_range = f"{self.start_date.date()} to {self.end_date.date()}"
                    break
                except Exception as e:
                    print(f"Invalid date format: {e}")
        
        return True

    def select_granularity(self):
        """Let user select data granularity."""
        print("\nData Granularity Options:")
        print("1. 30 minutes (PT30M)")
        print("2. 1 hour (PT1H)")
        print("3. 1 day (P1D)")
        print("4. 1 week (P1W)")
        print("5. 1 month (P1M)")
        
        while True:
            try:
                choice = int(input("\nSelect data granularity: "))
                if 1 <= choice <= 5:
                    break
                else:
                    print("Please enter a number between 1 and 5")
            except ValueError:
                print("Please enter a valid number")
        
        # Set period based on user choice
        if choice == 1:
            self.period = "PT30M"
        elif choice == 2:
            self.period = "PT1H"
        elif choice == 3:
            self.period = "P1D"
        elif choice == 4:
            self.period = "P1W"
        else:
            self.period = "P1M"
        
        return True

    def select_timezone(self):
        """Let user select timezone offset."""
        print("\nTimezone Options:")
        print("1. UTC (+0)")
        print("2. BST (UTC+1)")
        print("3. EST (UTC-5)")
        print("4. Custom offset")
        
        while True:
            try:
                choice = int(input("\nSelect timezone: "))
                if 1 <= choice <= 4:
                    break
                else:
                    print("Please enter a number between 1 and 4")
            except ValueError:
                print("Please enter a valid number")
        
        # Set offset based on user choice
        if choice == 1:
            self.offset = 0
            self.timezone_name = "UTC"
        elif choice == 2:
            self.offset = -60  # BST
            self.timezone_name = "BST (UTC+1)"
        elif choice == 3:
            self.offset = 300  # EST
            self.timezone_name = "EST (UTC-5)"
        else:
            while True:
                try:
                    hours = float(input("\nEnter timezone offset in hours from UTC (e.g., 1 for UTC+1, -5 for UTC-5): "))
                    self.offset = int(-hours * 60)  # Convert to minutes and invert
                    self.timezone_name = f"UTC{'+' if hours >= 0 else ''}{hours}"
                    break
                except ValueError:
                    print("Please enter a valid number")
        
        return True

    def confirm_settings(self):
        """Show summary and confirm settings."""
        print("\n=== Data Retrieval Summary ===")
        resource_name = getattr(self, 'selected_resource_name', 'Unknown')
        resource_classifier = getattr(self, 'selected_resource_classifier', 'Unknown')
        resource_unit = getattr(self, 'selected_resource_unit', '')
        
        print(f"Resource: {resource_name} ({resource_classifier})")
        print(f"Unit: {resource_unit}")
        print(f"Time Range: {self.date_range}")
        print(f"From: {self.start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"To: {self.end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Granularity: {self.period}")
        print(f"Timezone: {self.timezone_name}")
        
        confirm = input("\nRetrieve data with these settings? (y/n): ")
        return confirm.lower() == 'y'

    def retrieve_data(self):
        """Retrieve data with the selected settings."""
        if not all([self.client, self.selected_resource_id, self.start_date, self.end_date]):
            print("Missing required settings. Please complete all selections.")
            return None
        
        print("\nRetrieving data in batches...")
        
        # Calculate total days to estimate batches
        total_days = (self.end_date - self.start_date).days
        estimated_batches = (total_days // self.batch_days) + 1
        print(f"Date range spans {total_days} days, will use approximately {estimated_batches} batches")
        
        try:
            start_time = datetime.now()
            all_readings = get_historical_readings(
                self.client,
                self.selected_resource_id,
                self.start_date,
                self.end_date,
                period=self.period,
                offset=self.offset,
                batch_days=self.batch_days
            )
            end_time = datetime.now()
            retrieval_time = (end_time - start_time).total_seconds()
            
            print(f"\nRetrieved {len(all_readings)} readings in {retrieval_time:.2f} seconds")
            return all_readings
        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None

    def display_readings(self, readings):
        """Display readings data."""
        if not readings:
            print("No readings were retrieved for the specified period")
            return
        
        # Get resource details for better display
        resource_name = getattr(self, 'selected_resource_name', 'Unknown')
        resource_unit = getattr(self, 'selected_resource_unit', '')
        resource_classifier = getattr(self, 'selected_resource_classifier', 'Unknown')
        
        print(f"\nShowing {resource_name} readings ({resource_classifier})")
        print(f"Unit: {resource_unit}")
        
        print("\nFirst 5 readings:")
        for reading in readings[:5]:
            timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
            value = reading[1]
            print(f"  {timestamp}: {value} {resource_unit}")
        
        print("\nLast 5 readings:")
        for reading in readings[-5:]:
            timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
            value = reading[1]
            print(f"  {timestamp}: {value} {resource_unit}")

    def save_data(self, readings):
        """Automatically save data to a file."""
        if not readings:
            return None
        
        # Create filename based on classifier and date range
        resource_classifier = getattr(self, 'selected_resource_classifier', 'unknown')
        # Format classifier for filename (remove dots, replace with underscores)
        safe_classifier = resource_classifier.replace('.', '_').lower()
        
        # Ensure data/json directory exists
        data_dir = os.path.join("data", "json")
        os.makedirs(data_dir, exist_ok=True)
        
        # Create full path with filename
        filename = f"{safe_classifier}_{self.start_date.strftime('%Y%m%d')}_to_{self.end_date.strftime('%Y%m%d')}.json"
        filepath = os.path.join(data_dir, filename)
        
        # Format data for saving
        output_data = {
            "resource_id": self.selected_resource_id,
            "resource_name": getattr(self, 'selected_resource_name', 'Unknown'),
            "resource_classifier": getattr(self, 'selected_resource_classifier', 'Unknown'),
            "resource_unit": getattr(self, 'selected_resource_unit', 'Unknown'),
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "period": self.period,
            "timezone_offset": self.offset,
            "timezone_name": self.timezone_name,
            "readings": readings
        }
        
        with open(filepath, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nData automatically saved to {filepath}")
        return filepath

    def run(self):
        """Run the interactive client workflow."""
        try:
            # Steps in sequence
            print("\nNote: First virtual entity will be automatically selected.")
            if not self.select_entity():
                return
            
            while True:  # Loop to allow multiple resource retrievals
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
                    filepath = self.save_data(readings)
                    
                    # Ask if user wants to retrieve data for another resource
                    another = input("\nWould you like to retrieve data for another resource? (y/n): ")
                    if another.lower() != 'y':
                        break
                else:
                    print("Failed to retrieve readings. Please try again.")
                    break
                    
            print("\nThank you for using the Interactive Glowmarkt Client!")
        except Exception as e:
            print(f"Error in interactive client: {str(e)}")


def start_interactive_client(username=None, password=None, token=None):
    """Start the interactive client with credentials."""
    ui = GlowmarktUI()
    if ui.setup_client(username, password, token):
        ui.run()