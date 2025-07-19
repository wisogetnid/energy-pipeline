#!/usr/bin/env python

import os
import json
from datetime import datetime, timedelta
from dateutil import parser
from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_retrieval import GlowmarktClient, get_historical_readings
from pipeline.data_retrieval.n3rgy_csv_client import N3rgyCSVClient

class DataRetrievalUI(BaseUI):
    
    def __init__(self, client=None):
        super().__init__()
        self.client = client
        self.client_type = None  # 'glowmarkt' or 'n3rgy'
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
    
    def select_data_source(self):
        self.print_header("Data Source Selection")
        
        print("Choose your data source:")
        print("1. Glowmarkt API (online)")
        print("2. N3rgy CSV files (local)")
        
        choice = self.get_int_input("\nSelect a source: ", 1, 2)
        
        if choice == 1:
            self.client_type = 'glowmarkt'
            return self.setup_glowmarkt_client()
        else:
            self.client_type = 'n3rgy'
            return self.setup_n3rgy_client()
    
    def setup_glowmarkt_client(self, username=None, password=None, token=None):
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
    
    def setup_n3rgy_client(self, source_dir=None, output_dir=None):
        self.print_header("N3rgy CSV File Setup")
        
        # Ask for source directory if not provided
        if not source_dir:
            default_source = "./data/n3rgy_raw"
            source_input = input(f"Enter path to CSV files directory [{default_source}]: ")
            source_dir = source_input if source_input.strip() else default_source
        
        # Ask for output directory if not provided
        if not output_dir:
            default_output = "./data/processed"
            output_input = input(f"Enter path to save processed files [{default_output}]: ")
            output_dir = output_input if output_input.strip() else default_output
        
        # Create the client
        try:
            self.client = N3rgyCSVClient(source_dir=source_dir, output_dir=output_dir)
            
            # Check if source directory exists and has CSV files
            source_path = Path(source_dir)
            if not source_path.exists():
                print(f"Warning: Source directory {source_dir} does not exist.")
                create_dir = self.get_yes_no_input("Create the directory now?")
                if create_dir:
                    source_path.mkdir(parents=True, exist_ok=True)
                    print(f"Directory created: {source_dir}")
                else:
                    return False
            
            csv_files = list(source_path.glob("*.csv"))
            if not csv_files:
                print(f"Warning: No CSV files found in {source_dir}")
                print("Please add CSV files to this directory before proceeding.")
                return self.get_yes_no_input("Continue anyway?")
            
            print(f"Found {len(csv_files)} CSV files in {source_dir}")
            return True
            
        except Exception as e:
            print(f"Error setting up N3rgy CSV client: {str(e)}")
            return False
    
    def select_entity(self):
        if not self.client:
            print("Error: Client not initialized")
            return False
        
        # Only applicable for Glowmarkt client
        if self.client_type != 'glowmarkt':
            return True
        
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
        if self.client_type == 'glowmarkt':
            return self._select_glowmarkt_resource()
        else:
            return self._select_n3rgy_resource()
    
    def _select_glowmarkt_resource(self):
        try:
            self.print_header("Resource Selection (Glowmarkt)")
            
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
    
    def _select_n3rgy_resource(self):
        self.print_header("Resource Selection (N3rgy CSV)")
        
        # Process all files to generate JSONs
        json_files = self.client.process_all_files(extract_cost=True, combine_to_jsonl=False)
        
        if not json_files:
            print("No resources found or processing failed.")
            return False
        
        # Group by resource type
        resources = []
        resource_ids = set()
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    resource_id = data.get('resource_id')
                    
                    # Skip if we've already added this resource
                    if resource_id in resource_ids:
                        continue
                    
                    resource_ids.add(resource_id)
                    resources.append({
                        "resourceId": resource_id,
                        "name": data.get('resource_name', 'Unknown'),
                        "classifier": data.get('resource_classifier', 'Unknown'),
                        "baseUnit": data.get('resource_unit', 'Unknown')
                    })
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
        
        if not resources:
            print("No valid resources found in processed files.")
            return False
        
        return self._display_and_select_resource(resources)
    
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
                data_dir = self._get_data_directory()
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
            
            if self.client_type == 'glowmarkt':
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
            else:  # n3rgy
                resource_data = self.client.get_resource_data(
                    self.selected_resource_id,
                    start_date=self.start_date,
                    end_date=self.end_date
                )
                
                if resource_data and "readings" in resource_data:
                    readings = resource_data["readings"]
                else:
                    print(f"No data found for {self.selected_resource_name} in the selected date range.")
                    readings = []
            
            return readings
        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None
    
    def _get_data_directory(self):
        if self.client_type == 'glowmarkt':
            data_dir = os.path.join("data", "glowmarkt_api_raw")
        else:  # n3rgy
            data_dir = os.path.join("data", "n3rgy_processed")
        
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    
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
            data_dir = self._get_data_directory()
            
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
        # First select the data source
        if not self.client:
            if not self.select_data_source():
                return
        
        # For Glowmarkt, we need to select an entity
        if self.client_type == 'glowmarkt':
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
        # First select the data source if not already set
        if not self.client:
            if not self.select_data_source():
                return False
        
        # For Glowmarkt, we need to select an entity
        if self.client_type == 'glowmarkt':
            if not self.selected_entity:
                if not self.select_entity():
                    return False
            
            # Continue with Glowmarkt flow
            return self._fetch_and_combine_glowmarkt_resources()
        else:
            # For N3rgy, we can directly process all files
            return self._fetch_and_combine_n3rgy_resources()
    
    def _fetch_and_combine_glowmarkt_resources(self):
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
            
            return self._process_combined_files(retrieved_files, failed_resources, skipped_resources, temp_dir)
            
        except Exception as e:
            print(f"Error fetching or combining resources: {str(e)}")
            return False
    
    def _fetch_and_combine_n3rgy_resources(self):
        if not self.select_time_range():
            return False
        
        try:
            # Process all files and create a combined JSONL
            print("\nProcessing all N3rgy CSV files...")
            
            # Create a temp directory for this run's files
            import tempfile
            temp_dir = Path(tempfile.mkdtemp(prefix="energy_data_"))
            
            # First process all files to JSON
            json_files = self.client.process_all_files(extract_cost=True, combine_to_jsonl=False)
            
            if not json_files:
                print("No CSV files found or processing failed.")
                return False
            
            # Copy files to temp directory
            retrieved_files = []
            for json_file in json_files:
                temp_filename = Path(json_file).name
                temp_filepath = temp_dir / temp_filename
                
                # Copy the data to the temp directory
                with open(json_file, 'r') as src_file, open(temp_filepath, 'w') as dst_file:
                    dst_file.write(src_file.read())
                
                retrieved_files.append(str(temp_filepath))
            
            return self._process_combined_files(retrieved_files, [], [], temp_dir)
            
        except Exception as e:
            print(f"Error processing N3rgy files: {str(e)}")
            return False
    
    def _process_combined_files(self, retrieved_files, failed_resources, skipped_resources, temp_dir):
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
                    data_dir = self._get_data_directory()
                    for temp_path in retrieved_files:
                        filename = Path(temp_path).name
                        original_path = os.path.join(data_dir, filename)
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