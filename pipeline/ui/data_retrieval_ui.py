import os
import json
from datetime import datetime, timedelta
from dateutil import parser
from pathlib import Path
import tempfile
import shutil

from pipeline.ui.base_ui import BaseUI
from pipeline.data_retrieval import GlowmarktClient, get_historical_readings
from pipeline.data_retrieval.n3rgy_csv_client import N3rgyCSVClient
from pipeline.data_retrieval.latest_date_service import get_latest_available_date

class DataRetrievalUI(BaseUI):
    
    def __init__(self, client=None):
        super().__init__()
        self.client = client
        self.client_type = None
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
        self.is_latest_fetch = False
    
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
        
        if self.client_type == 'glowmarkt' and not (username or token):
            from pipeline.utils.credentials import get_credentials
            env_username, env_password, env_token = get_credentials()
            username = username or env_username
            password = password or env_password
            token = token or env_token
            print("Using credentials from environment variables.")
        
        self.client = GlowmarktClient(username=username, password=password, token=token)
        
        if self.client_type == 'glowmarkt' and not token and username and password:
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
        
        if not source_dir:
            default_source = "./data/n3rgy_raw/csv"
            source_input = input(f"Enter path to CSV files directory [{default_source}]: ")
            source_dir = source_input if source_input.strip() else default_source
        
        if not output_dir:
            default_output = "./data/n3rgy_raw"
            output_input = input(f"Enter path to save processed files [{default_output}]: ")
            output_dir = output_input if output_input.strip() else default_output
        
        try:
            self.client = N3rgyCSVClient(source_dir=source_dir, output_dir=output_dir)
            
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
    
    def ensure_client_initialized(self):
        """Ensure that the client is initialized and valid."""
        if not self.client:
            raise ValueError("Client is not initialized. Please set up the client first.")

        if self.client_type == 'glowmarkt' and not isinstance(self.client, GlowmarktClient):
            raise TypeError("Client is not a GlowmarktClient instance.")

        if self.client_type == 'n3rgy' and not isinstance(self.client, N3rgyCSVClient):
            raise TypeError("Client is not an N3rgyCSVClient instance.")

    def select_entity(self):
        if not self.client:
            raise ValueError("Client is not initialized")

        if self.client_type != 'glowmarkt':
            return True

        if not isinstance(self.client, GlowmarktClient):
            raise TypeError("Client is not a GlowmarktClient instance.")

        try:
            self.print_header("Virtual Entity Selection")
            print("Fetching your virtual entities...")

            entities = self.client.get_virtual_entities()

            if not entities:
                raise ValueError("No virtual entities found for your account.")

            self.selected_entity = entities[0]
            entity_name = self.selected_entity.get("name", "Unknown Entity")
            entity_id = self.selected_entity.get("veId", "Unknown ID")

            print(f"\nSelected virtual entity: {entity_name} (ID: {entity_id})")
            return True

        except Exception as e:
            print(f"Error fetching virtual entities: {str(e)}")
            return False

    def _select_glowmarkt_resource(self):
        """Automatically fetch and select all resources for Glowmarkt."""
        self.ensure_client_initialized()

        if not isinstance(self.client, GlowmarktClient):
            raise TypeError("Client is not a GlowmarktClient instance.")

        try:
            self.print_header("Resource Selection (Glowmarkt)")

            if not self.selected_entity:
                raise ValueError("No entity selected. Please select an entity first.")

            ve_id = self.selected_entity.get("veId", None)
            if not ve_id:
                raise ValueError("Selected entity does not have a valid ID.")

            print(f"Fetching resources for entity {self.selected_entity.get('name', 'Unknown')}...")

            entity_details = self.client.get_virtual_entity_resources(ve_id)
            resources = entity_details.get("resources", [])

            if not resources:
                print("No resources found for this entity.")
                return []

            print(f"Automatically selecting all {len(resources)} resources.")
            return resources

        except AttributeError as e:
            print(f"Error: {str(e)}. Ensure the client and entity are properly initialized.")
            return []

    def _select_n3rgy_resource(self):
        """Automatically fetch all resources for N3rgy."""
        self.ensure_client_initialized()

        if not isinstance(self.client, N3rgyCSVClient):
            raise TypeError("Client is not an N3rgyCSVClient instance.")

        try:
            self.print_header("Resource Selection (N3rgy CSV)")
            print("Processing all N3rgy CSV files...")
            
            json_files = self.client.process_all_files(extract_cost=True, combine_to_jsonl=False)

            if not json_files:
                print("No resources found or processing failed.")
                return []

            print(f"Automatically selecting all {len(json_files)} resources.")
            return json_files
        except Exception as e:
            print(f"Error processing N3rgy files: {str(e)}")
            return []

    def select_resource(self):
        """Automatically retrieve all resources based on the client type."""
        if self.client_type == 'glowmarkt':
            resources = self._select_glowmarkt_resource()
        elif self.client_type == 'n3rgy':
            resources = self._select_n3rgy_resource()
        else:
            raise ValueError("Unsupported client type.")

        if resources:
            print(f"Automatically selected {len(resources)} resources.")
            return resources
        return []
    def select_time_range(self, preset=None, resources=None):
        self.print_header("Time Range Selection")
        
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        if preset:
            choice = {"select_month": 1, "custom": 2, "latest": 3}.get(preset, 1)
        else:
            print("Choose a date range:")
            print("1. Select month and year")
            print("2. Custom range (enter specific dates)")
            print("3. Get latest data (automatic)")
            
            choice = self.get_int_input("\nSelect a range: ", 1, 3)
        
        if choice == 1:
            self.is_latest_fetch = False
            try:
                print("\nSelect month:")
                month_names = [
                    "January", "February", "March", "April", 
                    "May", "June", "July", "August",
                    "September", "October", "November", "December"
                ]
                
                for i, month_name in enumerate(month_names, 1):
                    print(f"{i}. {month_name}")
                
                month = self.get_int_input("\nEnter month (1-12): ", 1, 12)
                
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
            self.is_latest_fetch = False
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
        elif choice == 3:
            try:
                if not resources:
                    print("Error: Resources must be selected before determining latest date.")
                    return False
                
                print("Detecting latest available data...")
                data_dir = self._get_data_directory()
                resource_names = [r.get("name") for r in resources if r.get("name")]
                
                self.start_date = get_latest_available_date(data_dir, resource_names)
                self.end_date = now
                self.is_latest_fetch = True
                
                self.date_range = f"latest data: {self.start_date.date()} to {self.end_date.date()}"
                print(f"Detected latest sync point. Starting from: {self.start_date.date()}")
            except Exception as e:
                print(f"Error determining latest date: {str(e)}")
                return False
    
        print(f"\nSelected date range: {self.date_range}")
        return True
    
    def retrieve_data(self, skip_if_exists=True):
        self.print_header("Retrieving Data")

        try:
            if not self.selected_resource_name:
                raise ValueError("No resource selected. Please select a resource first.")

            if skip_if_exists:
                data_dir = self._get_data_directory()
                resource_name_safe = (self.selected_resource_name or "unknown").lower().replace(" ", "_")
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
                if isinstance(self.client, GlowmarktClient):
                    if not self.selected_resource_id:
                        raise ValueError("No resource ID selected for Glowmarkt.")

                    if not self.start_date or not self.end_date:
                        raise ValueError("Start date or end date is not set.")

                    readings = get_historical_readings(
                        self.client,
                        self.selected_resource_id,
                        self.start_date,
                        self.end_date,
                        period=self.period,
                        offset=self.offset,
                        batch_days=self.batch_days
                    )
                else:
                    raise TypeError("Client is not a GlowmarktClient instance.")
            elif self.client_type == 'n3rgy':
                if isinstance(self.client, N3rgyCSVClient):
                    if not self.selected_resource_id:
                        raise ValueError("No resource ID selected for N3rgy.")

                    if not self.start_date or not self.end_date:
                        raise ValueError("Start date or end date is not set.")

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
                else:
                    raise TypeError("Client is not an N3rgyCSVClient instance.")
            else:
                raise ValueError("Unsupported client type.")

            return readings
        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None
    
    def _get_data_directory(self):
        """Ensure the data directory exists and return its path."""
        if self.client_type == 'glowmarkt':
            data_dir = os.path.join("data", "glowmarkt_api_raw")
        else:
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
            
            # Handle potential None values with safe defaults
            resource_name = self.selected_resource_name or "unknown_resource"
            resource_name_safe = resource_name.lower().replace(" ", "_")
            
            start_date_str = self.start_date.strftime("%Y%m%d") if isinstance(self.start_date, datetime) else "unknown"
            end_date_str = self.end_date.strftime("%Y%m%d") if isinstance(self.end_date, datetime) else "unknown"
            filename = f"{resource_name_safe}_{start_date_str}_to_{end_date_str}.json"
            filepath = os.path.join(data_dir, filename)
            
            data = {
                "resource_id": self.selected_resource_id or "unknown",
                "resource_name": resource_name,
                "resource_unit": self.selected_resource_unit or "unknown",
                "resource_classifier": self.selected_resource_classifier or "unknown",
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
        """Main method to run the data retrieval process."""
        if not self.client:
            if not self.select_data_source():
                return

        if self.client_type == 'glowmarkt':
            print("\nNote: First virtual entity will be automatically selected.")
            if not self.select_entity():
                return
        
        # Select all resources
        resources = self.select_resource()
        if not resources:
            print("No resources available or selection failed.")
            return
        
        # Select time range for all resources
        if not self.select_time_range(resources=resources):
            return
        
        # Process and download each resource
        downloaded_filepaths = self._download_all_resources(resources)
        
        if downloaded_filepaths:
            print(f"\nSuccessfully downloaded {len(downloaded_filepaths)} resources.")
            return downloaded_filepaths
        else:
            print("Failed to download any resources. Please try again.")
            return None
    
    def _download_all_resources(self, resources):
        """Download data for all resources."""
        self.print_header("Downloading Resources")
        
        downloaded_filepaths = []
        failed_resources = []
        
        # Store original start/end dates if we're doing a multi-month fetch
        original_start = self.start_date
        original_end = self.end_date
        
        for i, resource in enumerate(resources, 1):
            try:
                print(f"\nProcessing resource {i}/{len(resources)}...")
                
                # Set resource properties based on client type
                if self.client_type == 'glowmarkt':
                    self.selected_resource_id = resource.get("resourceId")
                    self.selected_resource_name = resource.get("name", "Unknown")
                    self.selected_resource_unit = resource.get("baseUnit", "Unknown")
                    self.selected_resource_classifier = resource.get("classifier", "Unknown")
                elif self.client_type == 'n3rgy':
                    # For N3rgy, the resource is a filepath
                    self.selected_resource_id = resource
                    resource_filename = Path(resource).stem
                    self.selected_resource_name = resource_filename
                    self.selected_resource_unit = "Unknown"
                    self.selected_resource_classifier = "Unknown"
                
                print(f"Retrieving data for: {self.selected_resource_name}")
                
                if self.is_latest_fetch:
                    # Multi-month fetch split into monthly chunks
                    month_ranges = list(self._get_month_ranges(original_start, original_end))
                    print(f"Splitting fetch into {len(month_ranges)} monthly chunks...")
                    
                    for month_start, month_end in month_ranges:
                        self.start_date = month_start
                        self.end_date = month_end
                        self.date_range = f"{month_start.date()} to {month_end.date()}"
                        
                        print(f"\nFetching month: {self.date_range}")
                        # Always overwrite for latest fetch
                        readings = self.retrieve_data(skip_if_exists=False)
                        if readings:
                            filepath = self.save_data(readings)
                            if filepath:
                                downloaded_filepaths.append(filepath)
                        else:
                            print(f"Failed to retrieve data for {self.selected_resource_name} in range {self.date_range}")
                            # Stop immediately on failure as per plan
                            raise RuntimeError(f"Fetch failed for {self.selected_resource_name} at {self.date_range}")
                else:
                    # Regular single-range fetch
                    readings = self.retrieve_data()
                    if readings:
                        self.display_readings(readings)
                        filepath = self.save_data(readings)
                        if filepath:
                            downloaded_filepaths.append(filepath)
                            print(f"Successfully downloaded data for {self.selected_resource_name}")
                        else:
                            failed_resources.append(self.selected_resource_name)
                            print(f"Failed to save data for {self.selected_resource_name}")
                    else:
                        failed_resources.append(self.selected_resource_name)
                        print(f"Failed to retrieve data for {self.selected_resource_name}")
            
            except Exception as e:
                failed_resources.append(self.selected_resource_name or f"Resource {i}")
                print(f"Error processing resource: {str(e)}")
                if self.is_latest_fetch:
                    # Stop processing other resources if latest fetch failed
                    break
        
        # Restore original dates
        self.start_date = original_start
        self.end_date = original_end
        
        if failed_resources:
            print("\nFailed to download these resources:")
            for name in failed_resources:
                print(f"- {name}")
        
        return downloaded_filepaths
    
    def fetch_and_combine_resources(self):
        self.print_header("Combine Resources")
        

        data_dir = Path("data")
        

        valid_dirs = []
        for subdir in data_dir.iterdir():
            if subdir.is_dir() and list(subdir.glob("*.json")):
                valid_dirs.append(subdir)
        
        if not valid_dirs:
            print("No directories with JSON files found in 'data' folder.")
            return False
        
        print("\nAvailable data sources:")
        for i, directory in enumerate(valid_dirs, 1):
            json_count = len(list(directory.glob("*.json")))
            print(f"{i}. {directory.name} ({json_count} JSON files)")
        
        if self.client:
            print(f"{len(valid_dirs) + 1}. Use current {self.client_type} client to fetch new data")
        
        max_option = len(valid_dirs) + (1 if self.client else 0)
        choice = self.get_int_input("\nSelect data source: ", 1, max_option)
        

        if self.client and choice == len(valid_dirs) + 1:
            if self.client_type == 'glowmarkt':
                if not self.selected_entity:
                    if not self.select_entity():
                        return False
                return self._fetch_and_combine_glowmarkt_resources()
            else:
                return self._fetch_and_combine_n3rgy_resources()
        

        selected_dir = valid_dirs[choice - 1]
        return self._process_existing_json_files(selected_dir)

    def _process_existing_json_files(self, directory):
        self.print_header(f"Processing Files from {directory.name}")
        

        temp_dir = Path(tempfile.mkdtemp(prefix="energy_data_"))
        print(f"\nCreating temporary directory for data processing: {temp_dir}")
        

        json_files = list(directory.glob("*.json"))
        print(f"\nFound {len(json_files)} JSON files in {directory}")
        

        resource_types = set()
        for json_file in json_files:
            filename = json_file.name.lower()
            if "electricity" in filename:
                resource_types.add("electricity")
            elif "gas" in filename:
                resource_types.add("gas")
            elif "water" in filename:
                resource_types.add("water")
        
        print(f"Resource types found: {', '.join(resource_types)}")
        

        retrieved_files = []
        for json_file in json_files:
            temp_filepath = temp_dir / json_file.name
            

            with open(json_file, 'r') as src_file, open(temp_filepath, 'w') as dst_file:
                dst_file.write(src_file.read())
            
            retrieved_files.append(str(temp_filepath))
        

        try:
            from_date = "unknown"
            to_date = "unknown"
            for json_file in json_files:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    from_date_value = data.get("start_date", data.get("query", {}).get("from", None))
                    to_date_value = data.get("end_date", data.get("query", {}).get("to", None))
                    if from_date_value and from_date == "unknown":
                        from_date = from_date_value
                    if to_date_value and to_date == "unknown":
                        to_date = to_date_value
            
            if isinstance(from_date, str) and 'T' in from_date:
                from_date = from_date.split('T')[0]
            if isinstance(to_date, str) and 'T' in to_date:
                to_date = to_date.split('T')[0]
                
            print(f"Date range found: {from_date} to {to_date}")
        except Exception as e:
            print(f"Could not determine date range: {e}")
        
        return self._process_combined_files(retrieved_files, [], [], temp_dir)
    
    def _fetch_and_combine_glowmarkt_resources(self):
        """Fetch and combine all resources from Glowmarkt."""
        self.ensure_client_initialized()
        
        if not self.selected_entity:
            if not self.select_entity():
                return False

        if not isinstance(self.client, GlowmarktClient):
            print("Error: Client is not a GlowmarktClient instance.")
            return False
            
        ve_id = self.selected_entity.get("veId", None)
        if not ve_id:
            print("Error: Selected entity does not have a valid ID.")
            return False
            
        entity_name = self.selected_entity.get("name", "Unknown")
        print(f"\nFetching resources for entity {entity_name}...")
            
        entity_details = self.client.get_virtual_entity_resources(ve_id)
        resources = entity_details.get("resources", [])
        
        if not resources:
            print("No resources found for this entity.")
            return False

        if not self.select_time_range(resources=resources):
            return False
        
        try:
            # Download all resources using the _download_all_resources method
            downloaded_filepaths = self._download_all_resources(resources)
            
            if downloaded_filepaths:
                print(f"\nSuccessfully downloaded {len(downloaded_filepaths)} resources.")
                return downloaded_filepaths
            else:
                print("Failed to download any resources.")
                return False
                
        except Exception as e:
            print(f"Error fetching or combining resources: {str(e)}")
            return False
    
    def _fetch_and_combine_n3rgy_resources(self):
        """Fetch and combine all resources from N3rgy CSV files."""
        self.ensure_client_initialized()
        
        if not isinstance(self.client, N3rgyCSVClient):
            print("Error: Client is not an N3rgyCSVClient instance.")
            return False
            
        if not self.select_time_range():
            return False
        
        try:
            print("\nProcessing all N3rgy CSV files...")
            
            import tempfile
            temp_dir = Path(tempfile.mkdtemp(prefix="energy_data_"))
            
            json_files = self.client.process_all_files(extract_cost=True, combine_to_jsonl=False)
            
            if not json_files:
                print("No CSV files found or processing failed.")
                import shutil
                shutil.rmtree(temp_dir)
                return False
            
            retrieved_files = []
            for json_file in json_files:
                temp_filename = Path(json_file).name
                temp_filepath = temp_dir / temp_filename
                
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
            
            print("\nHow would you like to combine these resources?")
            print("1. Create a single combined file for all data")
            print("2. Create separate combined files for each month")
            
            choice = self.get_int_input("\nSelect an option: ", 1, 2)
            split_by_month = (choice == 2)
            
            print("\nCombining resources into a single file...")
            from pipeline.data_processing.jsonl_converter import EnergyDataConverter
            

            output_dir = Path("data/processed")
            
            converter = EnergyDataConverter(output_dir=output_dir)
            combined_filepath = converter.combine_all_resources_into_single_file(
                temp_dir, 
                split_by_month=split_by_month
            )
            
            if combined_filepath:
                if isinstance(combined_filepath, list):
                    print(f"\nAll resources successfully combined into {len(combined_filepath)} monthly files:")
                    for filepath in combined_filepath:
                        print(f"- {filepath}")
                    

                    print("\nWould you like to convert these files to Parquet format?")
                    convert_to_parquet = self.get_yes_no_input("Convert to Parquet? (y/n): ")
                    
                    if convert_to_parquet:
                        from pipeline.data_processing.parquet_converter import JsonlToParquetConverter
                        
                        parquet_dir = Path("data/parquet")
                        parquet_converter = JsonlToParquetConverter(output_dir=str(parquet_dir))
                        
                        parquet_filepaths = []
                        for jsonl_file in combined_filepath:
                            print(f"\nConverting {Path(jsonl_file).name} to Parquet...")
                            parquet_filepath = parquet_converter.convert_jsonl_to_parquet_file(jsonl_file)
                            if parquet_filepath:
                                parquet_filepaths.append(parquet_filepath)
                        
                        if parquet_filepaths:
                            print(f"\nSuccessfully converted {len(parquet_filepaths)} files to Parquet format:")
                            for filepath in parquet_filepaths:
                                print(f"- {filepath}")
                    

                    original_paths = []
                    data_dir = self._get_data_directory()
                    for temp_path in retrieved_files:
                        filename = Path(temp_path).name
                        original_path = os.path.join(data_dir, filename)
                        original_paths.append(original_path)
                    

                    import shutil
                    shutil.rmtree(temp_dir)
                    print(f"\nTemporary directory removed: {temp_dir}")
                    
                    return [*combined_filepath, *original_paths]
                else:
                    print(f"\nAll resources successfully combined into a single file: {combined_filepath}")
                    
                    print("\nConverting combined file to Parquet format...")
                    from pipeline.data_processing.parquet_converter import JsonlToParquetConverter
                    
                    parquet_dir = Path("data/parquet")
                    parquet_converter = JsonlToParquetConverter(output_dir=str(parquet_dir))
                    parquet_filepath = parquet_converter.convert_jsonl_to_parquet_file(combined_filepath)
                    
                    if parquet_filepath:
                        print(f"\nSuccessfully converted to Parquet format: {parquet_filepath}")
                        

                        original_paths = []
                        data_dir = self._get_data_directory()
                        for temp_path in retrieved_files:
                            filename = Path(temp_path).name
                            original_path = os.path.join(data_dir, filename)
                            original_paths.append(original_path)
                        

                        import shutil
                        shutil.rmtree(temp_dir)
                        print(f"\nTemporary directory removed: {temp_dir}")
                        
                        return [parquet_filepath, combined_filepath, *original_paths]
                    else:

                        import shutil
                        shutil.rmtree(temp_dir)
                        print(f"\nTemporary directory removed: {temp_dir}")
                        
                        return [combined_filepath]
            else:

                import shutil
                shutil.rmtree(temp_dir)
                print(f"\nTemporary directory removed: {temp_dir}")
                
                print("\nFailed to combine resources. Individual files are still available.")
                return retrieved_files
        else:

            import shutil
            shutil.rmtree(temp_dir)
            print(f"\nTemporary directory removed: {temp_dir}")
            
            print("\nNo files were successfully retrieved.")
            return False
    
    def _get_month_ranges(self, start_date, end_date):
        """Yield (month_start, month_end) tuples for the range [start_date, end_date]."""
        current_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while current_start <= end_date:
            next_month = current_start.month + 1
            next_year = current_start.year
            if next_month > 12:
                next_month = 1
                next_year += 1
            
            month_end = datetime(next_year, next_month, 1) - timedelta(seconds=1)
            
            # Clip to the actual start and end dates
            actual_start = max(current_start, start_date)
            actual_end = min(month_end, end_date)
            
            if actual_start <= actual_end:
                yield actual_start, actual_end
                
            current_start = datetime(next_year, next_month, 1)