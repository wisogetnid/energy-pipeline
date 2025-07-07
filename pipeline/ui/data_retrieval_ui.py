#!/usr/bin/env python

import os
import json
from datetime import datetime, timedelta
from dateutil import parser
from pathlib import Path

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
        self.retrieved_filepaths = []
    
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
        
        print(f"{len(valid_resources) + 1}. Fetch ALL resources")
        
        choice = self.get_int_input("\nSelect a resource: ", 1, len(valid_resources) + 1)
        
        if choice <= len(valid_resources):
            selected = valid_resources[choice - 1]
            
            self.selected_resource_id = selected.get("resourceId")
            self.selected_resource_name = selected.get("name")
            self.selected_resource_unit = selected.get("baseUnit")
            self.selected_resource_classifier = selected.get("classifier")
            
            print(f"\nSelected: {self.selected_resource_name} ({self.selected_resource_classifier})")
            return True
        else:
            return self._fetch_all_resources(valid_resources)
    
    def _fetch_all_resources(self, resources):
        self.print_header("Fetching All Resources")
        print(f"Selected {len(resources)} resources to download")
        
        if not self.select_time_range():
            return False
        
        self.retrieved_filepaths = []
        failed_resources = []
        
        for i, resource in enumerate(resources, 1):
            resource_name = resource.get("name", "Unknown")
            resource_classifier = resource.get("classifier", "Unknown")
            
            print(f"\nProcessing resource {i}/{len(resources)}: {resource_name}")
            
            self.selected_resource_id = resource.get("resourceId")
            self.selected_resource_name = resource_name
            self.selected_resource_unit = resource.get("baseUnit", "Unknown")
            self.selected_resource_classifier = resource_classifier
            
            readings = self.retrieve_data()
            if readings:
                self.display_readings(readings)
                json_filepath = self.save_data(readings)
                
                if json_filepath:
                    self.retrieved_filepaths.append(json_filepath)
            else:
                failed_resources.append(resource_name)
                print(f"Failed to retrieve readings for {resource_name}. Continuing with next resource.")
        
        if failed_resources:
            print("\nFailed to retrieve data for these resources:")
            for resource_name in failed_resources:
                print(f"- {resource_name}")
        
        if self.retrieved_filepaths:
            print("\nSuccessfully retrieved data for these resources:")
            for filepath in self.retrieved_filepaths:
                print(f"- {Path(filepath).name}")
            return self.retrieved_filepaths
        else:
            print("\nFailed to retrieve any resources.")
            return False
    
    def select_time_range(self, preset=None):
        self.print_header("Time Range Selection")
        
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        if preset:
            # Update preset mapping since we're changing the options
            choice = {"select_month": 1, "custom": 2}.get(preset, 1)
        else:
            print("Choose a date range:")
            print("1. Select month and year")
            print("2. Custom range (enter specific dates)")
            
            choice = self.get_int_input("\nSelect a range: ", 1, 2)
        
        if choice == 1:
            # This is now the "Select month and year" option (previously option 6)
            try:
                # Display month selection
                print("\nSelect month:")
                month_names = [
                    "January", "February", "March", "April", 
                    "May", "June", "July", "August",
                    "September", "October", "November", "December"
                ]
                
                for i, month_name in enumerate(month_names, 1):
                    print(f"{i}. {month_name}")
                
                month = self.get_int_input("\nEnter month (1-12): ", 1, 12)
                
                # Year selection with current year as default
                print(f"\nEnter year (default: {current_year}):")
                year_input = input(f"Year [{current_year}]: ")
                
                if not year_input.strip():
                    year = current_year
                else:
                    try:
                        year = int(year_input)
                        if year < 2000 or year > 2100:
                            print("Year should be between 2000 and 2100, using current year instead.")
                            year = current_year
                    except ValueError:
                        print("Invalid year format, using current year instead.")
                        year = current_year
                
                # Set start and end dates for the selected month and year
                self.start_date = datetime(year, month, 1)
                
                if month == 12:
                    self.end_date = datetime(year + 1, 1, 1) - timedelta(seconds=1)
                else:
                    self.end_date = datetime(year, month + 1, 1) - timedelta(seconds=1)
                
                self.date_range = f"{month_names[month-1]} {year}"
                
            except Exception as e:
                print(f"Error setting month and year: {str(e)}")
                return False
        
        elif choice == 2:
            # This is now the "Custom range" option (previously option 5)
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
    
    def retrieve_data(self, skip_if_exists=True):
        self.print_header("Retrieving Data")
        
        try:
            # Check if data already exists
            if skip_if_exists:
                data_dir = os.path.join("data", "glowmarkt_api_raw")
                resource_name_safe = self.selected_resource_name.lower().replace(" ", "_")
                start_date_str = self.start_date.strftime("%Y%m%d") if isinstance(self.start_date, datetime) else "unknown"
                end_date_str = self.end_date.strftime("%Y%m%d") if isinstance(self.end_date, datetime) else "unknown"
                filename = f"{resource_name_safe}_{start_date_str}_to_{end_date_str}.json"
                filepath = os.path.join(data_dir, filename)
                
                if os.path.exists(filepath):
                    print(f"Data for {self.selected_resource_name} already exists at {filepath}")
                    print("Loading existing data instead of retrieving again...")
                    
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                        if "readings" in data and data["readings"]:
                            return data["readings"]
            
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
        
        resource_selection_result = self.select_resource()
        if not resource_selection_result:
            return
        
        if isinstance(resource_selection_result, list):
            return resource_selection_result
            
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
    
    def fetch_and_combine_resources(self):
        if not self.client:
            print("Error: Client not initialized")
            return False
        
        if not self.selected_entity:
            if not self.select_entity():
                return False
        
        if not self.select_time_range():
            return False
        
        ve_id = self.selected_entity.get("veId")
        print(f"\nFetching resources for entity {self.selected_entity.get('name')}...")
        
        try:
            entity_details = self.client.get_virtual_entity_resources(ve_id)
            resources = entity_details.get("resources", [])
            
            if not resources:
                print("No resources found for this entity.")
                return False
            
            valid_resources = []
            for resource in resources:
                name = resource.get("name", "Unknown")
                classifier = resource.get("classifier", "Unknown")
                
                if "consumption" in classifier:
                    valid_resources.append(resource)
            
            if not valid_resources:
                print("No consumption resources found.")
                return False
            
            print(f"\nFound {len(valid_resources)} consumption resources.")
            
            retrieved_files = []
            failed_resources = []
            skipped_resources = []
            
            # Create a temporary directory specifically for this run's files
            import tempfile
            temp_dir = Path(tempfile.mkdtemp(prefix="energy_data_"))
            print(f"\nCreating temporary directory for data processing: {temp_dir}")
            
            # Current date range for naming files
            start_date_str = self.start_date.strftime("%Y%m%d") if isinstance(self.start_date, datetime) else "unknown"
            end_date_str = self.end_date.strftime("%Y%m%d") if isinstance(self.end_date, datetime) else "unknown"
            
            for i, resource in enumerate(valid_resources, 1):
                resource_name = resource.get("name", "Unknown")
                resource_classifier = resource.get("classifier", "Unknown")
                
                print(f"\nProcessing resource {i}/{len(valid_resources)}: {resource_name}")
                
                self.selected_resource_id = resource.get("resourceId")
                self.selected_resource_name = resource_name
                self.selected_resource_unit = resource.get("baseUnit", "Unknown")
                self.selected_resource_classifier = resource_classifier
                
                # First save to the permanent location
                readings = self.retrieve_data()
                if readings:
                    permanent_filepath = self.save_data(readings)
                    
                    if permanent_filepath:
                        # Now copy to our temporary directory for processing just this run's files
                        resource_name_safe = resource_name.lower().replace(" ", "_")
                        temp_filename = f"{resource_name_safe}_{start_date_str}_to_{end_date_str}.json"
                        temp_filepath = temp_dir / temp_filename
                        
                        # Copy the data to the temp directory
                        with open(permanent_filepath, 'r') as src_file, open(temp_filepath, 'w') as dst_file:
                            dst_file.write(src_file.read())
                        
                        retrieved_files.append(str(temp_filepath))
                else:
                    failed_resources.append(resource_name)
                    print(f"Failed to retrieve readings for {resource_name}. Continuing with next resource.")
            
            if skipped_resources:
                print("\nSkipped retrieving data for these resources (already downloaded):")
                for resource_name in skipped_resources:
                    print(f"- {resource_name}")
            
            if failed_resources:
                print("\nFailed to retrieve data for these resources:")
                for resource_name in failed_resources:
                    print(f"- {resource_name}")
            
            if retrieved_files:
                print("\nSuccessfully retrieved data for resources:")
                for filepath in retrieved_files:
                    print(f"- {Path(filepath).name}")
                
                print("\nCombining just this run's resources into a single file...")
                from pipeline.data_processing.jsonl_converter import EnergyDataConverter
                
                # Use our temporary directory that only contains files from this run
                output_dir = Path("data/processed")
                
                converter = EnergyDataConverter(output_dir=output_dir)
                combined_filepath = converter.combine_all_resources_into_single_file(temp_dir)
                
                if combined_filepath:
                    print(f"\nAll resources successfully combined into a single file: {combined_filepath}")
                    
                    print("\nConverting combined file to Parquet format...")
                    from pipeline.data_processing.parquet_converter import JsonlToParquetConverter
                    
                    parquet_dir = Path("data/parquet")
                    parquet_converter = JsonlToParquetConverter(output_dir=str(parquet_dir))
                    parquet_filepath = parquet_converter.convert_jsonl_to_parquet_file(combined_filepath)
                    
                    if parquet_filepath:
                        print(f"\nSuccessfully converted to Parquet format: {parquet_filepath}")
                        
                        # Get the original paths, not the temp ones, for returning
                        original_paths = []
                        for temp_path in retrieved_files:
                            filename = Path(temp_path).name
                            original_path = os.path.join("data", "glowmarkt_api_raw", filename)
                            original_paths.append(original_path)
                        
                        # Delete the temporary directory
                        import shutil
                        shutil.rmtree(temp_dir)
                        print(f"\nTemporary directory removed: {temp_dir}")
                        
                        return [parquet_filepath, combined_filepath, *original_paths]
                    else:
                        # Delete the temporary directory
                        import shutil
                        shutil.rmtree(temp_dir)
                        print(f"\nTemporary directory removed: {temp_dir}")
                        
                        return [combined_filepath, *original_paths]
                else:
                    # Delete the temporary directory
                    import shutil
                    shutil.rmtree(temp_dir)
                    print(f"\nTemporary directory removed: {temp_dir}")
                    
                    print("\nFailed to combine resources. Individual files are still available.")
                    return retrieved_files
            else:
                # Delete the temporary directory
                import shutil
                shutil.rmtree(temp_dir)
                print(f"\nTemporary directory removed: {temp_dir}")
                
                print("\nNo files were successfully retrieved.")
                return False
        
        except Exception as e:
            print(f"Error fetching or combining resources: {str(e)}")
            return False