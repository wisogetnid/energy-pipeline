import os
import json
from datetime import datetime, timedelta
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
            self.client_type = "glowmarkt"
            return self.setup_glowmarkt_client()
        else:
            self.client_type = "n3rgy"
            return self.setup_n3rgy_client()

    def setup_glowmarkt_client(self, username=None, password=None, token=None):
        self.print_header("Glowmarkt API Authentication")

        if self.client_type == "glowmarkt" and not (username or token):
            from pipeline.utils.credentials import get_credentials

            env_username, env_password, env_token = get_credentials()
            username = username or env_username
            password = password or env_password
            token = token or env_token
            print("Using credentials from environment variables.")

        self.client = GlowmarktClient(username=username, password=password, token=token)

        if self.client_type == "glowmarkt" and not token and username and password:
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
            source_input = input(
                f"Enter path to CSV files directory [{default_source}]: "
            )
            source_dir = source_input if source_input.strip() else default_source

        if not output_dir:
            default_output = "./data/n3rgy_raw"
            output_input = input(
                f"Enter path to save processed files [{default_output}]: "
            )
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
        if not self.client:
            raise ValueError(
                "Client is not initialized. Please set up the client first."
            )

        if self.client_type == "glowmarkt" and not isinstance(
            self.client, GlowmarktClient
        ):
            raise TypeError("Client is not a GlowmarktClient instance.")

        if self.client_type == "n3rgy" and not isinstance(self.client, N3rgyCSVClient):
            raise TypeError("Client is not an N3rgyCSVClient instance.")

    def select_entity(self):
        if not self.client:
            raise ValueError("Client is not initialized")

        if self.client_type != "glowmarkt":
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

            print(
                f"Fetching resources for entity {self.selected_entity.get('name', 'Unknown')}..."
            )

            entity_details = self.client.get_virtual_entity_resources(ve_id)
            resources = entity_details.get("resources", [])

            if not resources:
                print("No resources found for this entity.")
                return []

            print(f"Automatically selecting all {len(resources)} resources.")
            return resources

        except AttributeError as e:
            print(
                f"Error: {str(e)}. Ensure the client and entity are properly initialized."
            )
            return []

    def _select_n3rgy_resource(self):
        self.ensure_client_initialized()

        if not isinstance(self.client, N3rgyCSVClient):
            raise TypeError("Client is not an N3rgyCSVClient instance.")

        try:
            self.print_header("Resource Selection (N3rgy CSV)")
            print("Processing all N3rgy CSV files...")

            json_files = self.client.process_all_files(
                extract_cost=True, combine_to_jsonl=False
            )

            if not json_files:
                print("No resources found or processing failed.")
                return []

            print(f"Automatically selecting all {len(json_files)} resources.")
            return json_files
        except Exception as e:
            print(f"Error processing N3rgy files: {str(e)}")
            return []

    def select_resource(self):
        if self.client_type == "glowmarkt":
            resources = self._select_glowmarkt_resource()
        elif self.client_type == "n3rgy":
            resources = self._select_n3rgy_resource()
        else:
            raise ValueError("Unsupported client type.")

        if resources:
            print(f"Automatically selected {len(resources)} resources.")
            return resources
        return []

    def select_time_range(self, resources=None):
        self.print_header("Time Range Selection")

        now = datetime.now()

        if self.client_type == "n3rgy":
            self.is_latest_fetch = False
            self.date_range = "all available data from processed files"
            return True

        try:
            if not resources:
                print(
                    "Error: Resources must be selected before determining latest date."
                )
                return False

            print("Detecting latest available data...")
            data_dir = self._get_data_directory()
            resource_names = [r.get("name") for r in resources if r.get("name")]

            self.start_date = get_latest_available_date(data_dir, resource_names)
            self.end_date = now
            self.is_latest_fetch = True

            self.date_range = f"{self.start_date.date()} to {self.end_date.date()}"
            print(
                f"Detected latest sync point. Starting from: {self.start_date.date()}"
            )
            print(f"\nSelected date range: {self.date_range}")
            return True
        except Exception as e:
            print(f"Error determining latest date: {str(e)}")
            return False

    def retrieve_data(self, skip_if_exists=True):
        self.print_header("Retrieving Data")

        try:
            if not self.selected_resource_name:
                raise ValueError(
                    "No resource selected. Please select a resource first."
                )

            if skip_if_exists:
                data_dir = self._get_data_directory()
                resource_name_safe = (
                    (self.selected_resource_name or "unknown").lower().replace(" ", "_")
                )
                start_date_str = (
                    self.start_date.strftime("%Y%m%d")
                    if isinstance(self.start_date, datetime)
                    else "unknown"
                )
                end_date_str = (
                    self.end_date.strftime("%Y%m%d")
                    if isinstance(self.end_date, datetime)
                    else "unknown"
                )
                filename = (
                    f"{resource_name_safe}_{start_date_str}_to_{end_date_str}.json"
                )
                filepath = os.path.join(data_dir, filename)

                if os.path.exists(filepath):
                    print(
                        f"Data for {self.selected_resource_name} already exists at {filepath}"
                    )
                    print("Loading existing data instead of retrieving again...")

                    with open(filepath, "r") as f:
                        data = json.load(f)
                        if "readings" in data and data["readings"]:
                            return data["readings"]

            print(
                f"Fetching data for {self.selected_resource_name} over {self.date_range}..."
            )

            if self.client_type == "glowmarkt":
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
                        batch_days=self.batch_days,
                    )
                else:
                    raise TypeError("Client is not a GlowmarktClient instance.")
            elif self.client_type == "n3rgy":
                if isinstance(self.client, N3rgyCSVClient):
                    if not self.selected_resource_id:
                        raise ValueError("No resource ID selected for N3rgy.")

                    if not self.start_date or not self.end_date:
                        raise ValueError("Start date or end date is not set.")

                    resource_data = self.client.get_resource_data(
                        self.selected_resource_id,
                        start_date=self.start_date,
                        end_date=self.end_date,
                    )

                    if resource_data and "readings" in resource_data:
                        readings = resource_data["readings"]
                    else:
                        print(
                            f"No data found for {self.selected_resource_name} in the selected date range."
                        )
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
        if self.client_type == "glowmarkt":
            data_dir = os.path.join("data", "glowmarkt_api_raw")
        elif self.client_type == "n3rgy_api":
            data_dir = os.path.join("data", "n3rgy_raw")
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
                    timestamp_seconds = (
                        timestamp / 1000 if timestamp > 9999999999 else timestamp
                    )
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

            safe_resource_name = (
                (self.selected_resource_name or "unknown_resource")
                .lower()
                .replace(" ", "_")
            )

            start_date_str = (
                self.start_date.strftime("%Y%m%d")
                if isinstance(self.start_date, datetime)
                else "unknown"
            )
            end_date_str = (
                self.end_date.strftime("%Y%m%d")
                if isinstance(self.end_date, datetime)
                else "unknown"
            )
            filename = f"{safe_resource_name}_{start_date_str}_to_{end_date_str}.json"
            filepath = os.path.join(data_dir, filename)

            data = {
                "resource_id": self.selected_resource_id or "unknown",
                "resource_name": self.selected_resource_name or "unknown_resource",
                "resource_unit": self.selected_resource_unit or "unknown",
                "resource_classifier": self.selected_resource_classifier or "unknown",
                "start_date": (
                    self.start_date.isoformat()
                    if isinstance(self.start_date, datetime)
                    else self.start_date
                ),
                "end_date": (
                    self.end_date.isoformat()
                    if isinstance(self.end_date, datetime)
                    else self.end_date
                ),
                "period": self.period,
                "timezone_offset": self.offset,
                "readings": readings,
            }

            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)

            print(f"\nData saved to: {filepath}")
            print(f"Total readings: {len(readings)}")

            return filepath
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            return None

    def run(self):
        if not self.client:
            if not self.select_data_source():
                return

        if self.client_type == "glowmarkt":
            print("\nNote: First virtual entity will be automatically selected.")
            if not self.select_entity():
                return

        resources = self.select_resource()
        if not resources:
            print("No resources available or selection failed.")
            return

        if not self.select_time_range(resources=resources):
            return

        downloaded_filepaths = self._download_all_resources(resources)

        if downloaded_filepaths:
            print(f"\nSuccessfully downloaded {len(downloaded_filepaths)} resources.")
            return downloaded_filepaths
        else:
            print("Failed to download any resources. Please try again.")
            return None

    def _download_all_resources(self, resources):
        self.print_header("Downloading Resources")

        downloaded_filepaths = []
        failed_resources = []

        original_fetch_start = self.start_date
        original_fetch_end = self.end_date

        for i, resource in enumerate(resources, 1):
            try:
                print(f"\nProcessing resource {i}/{len(resources)}...")

                if self.client_type == "glowmarkt":
                    self.selected_resource_id = resource.get("resourceId")
                    self.selected_resource_name = resource.get("name", "Unknown")
                    self.selected_resource_unit = resource.get("baseUnit", "Unknown")
                    self.selected_resource_classifier = resource.get(
                        "classifier", "Unknown"
                    )
                elif self.client_type == "n3rgy":
                    self.selected_resource_id = resource
                    resource_filename = Path(resource).stem
                    self.selected_resource_name = resource_filename
                    self.selected_resource_unit = "Unknown"
                    self.selected_resource_classifier = "Unknown"

                print(f"Retrieving data for: {self.selected_resource_name}")

                if self.is_latest_fetch:
                    monthly_chunks = list(
                        self._get_month_ranges(original_fetch_start, original_fetch_end)
                    )
                    print(
                        f"Splitting fetch into {len(monthly_chunks)} monthly chunks..."
                    )

                    for month_start, month_end in monthly_chunks:
                        self.start_date = month_start
                        self.end_date = month_end
                        self.date_range = f"{month_start.date()} to {month_end.date()}"

                        print(f"\nFetching month: {self.date_range}")
                        readings = self.retrieve_data(skip_if_exists=False)
                        if readings:
                            filepath = self.save_data(readings)
                            if filepath:
                                downloaded_filepaths.append(filepath)
                        else:
                            print(
                                f"No data retrieved for {self.selected_resource_name} in range {self.date_range}"
                            )
                else:
                    readings = self.retrieve_data()
                    if readings:
                        self.display_readings(readings)
                        filepath = self.save_data(readings)
                        if filepath:
                            downloaded_filepaths.append(filepath)
                            print(
                                f"Successfully downloaded data for {self.selected_resource_name}"
                            )
                        else:
                            failed_resources.append(self.selected_resource_name)
                            print(
                                f"Failed to save data for {self.selected_resource_name}"
                            )
                    else:
                        failed_resources.append(self.selected_resource_name)
                        print(
                            f"Failed to retrieve data for {self.selected_resource_name}"
                        )

            except Exception as e:
                failed_resources.append(self.selected_resource_name or f"Resource {i}")
                print(f"Error processing resource: {str(e)}")

        self.start_date = original_fetch_start
        self.end_date = original_fetch_end

        if failed_resources:
            print("\nFailed to download these resources:")
            for name in failed_resources:
                print(f"- {name}")

        return downloaded_filepaths

    def fetch_and_combine_resources(self):
        self.print_header("Combine Resources")

        data_directory = Path("data")

        directories_with_json_data = []
        for subdir in data_directory.iterdir():
            if subdir.is_dir() and list(subdir.glob("*.json")):
                directories_with_json_data.append(subdir)

        if not directories_with_json_data:
            print("No directories with JSON files found in 'data' folder.")
            return False

        print("\nAvailable data sources:")
        for i, directory in enumerate(directories_with_json_data, 1):
            json_file_count = len(list(directory.glob("*.json")))
            print(f"{i}. {directory.name} ({json_file_count} JSON files)")

        if self.client:
            print(
                f"{len(directories_with_json_data) + 1}. Use current {self.client_type} client to fetch new data"
            )

        max_source_option = len(directories_with_json_data) + (1 if self.client else 0)
        source_choice = self.get_int_input(
            "\nSelect data source: ", 1, max_source_option
        )

        if self.client and source_choice == len(directories_with_json_data) + 1:
            if self.client_type == "glowmarkt":
                if not self.selected_entity:
                    if not self.select_entity():
                        return False
                return self._fetch_and_combine_glowmarkt_resources()
            else:
                return self._fetch_and_combine_n3rgy_resources()

        selected_directory = directories_with_json_data[source_choice - 1]
        return self._process_existing_json_files(selected_directory)

    def _process_existing_json_files(self, directory):
        self.print_header(f"Processing Files from {directory.name}")

        temporary_processing_directory = Path(tempfile.mkdtemp(prefix="energy_data_"))
        print(
            f"\nCreating temporary directory for data processing: {temporary_processing_directory}"
        )

        json_files_to_process = list(directory.glob("*.json"))
        print(f"\nFound {len(json_files_to_process)} JSON files in {directory}")

        detected_resource_types = set()
        for json_file in json_files_to_process:
            filename = json_file.name.lower()
            if "electricity" in filename:
                detected_resource_types.add("electricity")
            elif "gas" in filename:
                detected_resource_types.add("gas")
            elif "water" in filename:
                detected_resource_types.add("water")

        print(f"Resource types found: {', '.join(detected_resource_types)}")

        prepared_files = []
        for json_file in json_files_to_process:
            temporary_filepath = temporary_processing_directory / json_file.name

            with open(json_file, "r") as source_reader, open(
                temporary_filepath, "w"
            ) as temp_writer:
                temp_writer.write(source_reader.read())

            prepared_files.append(str(temporary_filepath))

        try:
            detected_from_date = "unknown"
            detected_to_date = "unknown"
            for json_file in json_files_to_process:
                with open(json_file, "r") as f:
                    data = json.load(f)
                    from_date_value = data.get(
                        "start_date", data.get("query", {}).get("from", None)
                    )
                    to_date_value = data.get(
                        "end_date", data.get("query", {}).get("to", None)
                    )
                    if from_date_value and detected_from_date == "unknown":
                        detected_from_date = from_date_value
                    if to_date_value and detected_to_date == "unknown":
                        detected_to_date = to_date_value

            if isinstance(detected_from_date, str) and "T" in detected_from_date:
                detected_from_date = detected_from_date.split("T")[0]
            if isinstance(detected_to_date, str) and "T" in detected_to_date:
                detected_to_date = detected_to_date.split("T")[0]

            print(f"Date range found: {detected_from_date} to {detected_to_date}")
        except Exception as e:
            print(f"Could not determine date range: {e}")

        return self._process_combined_files(
            prepared_files, [], [], temporary_processing_directory
        )

    def _fetch_and_combine_glowmarkt_resources(self):
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
            downloaded_filepaths = self._download_all_resources(resources)

            if downloaded_filepaths:
                print(
                    f"\nSuccessfully downloaded {len(downloaded_filepaths)} resources."
                )
                return downloaded_filepaths
            else:
                print("Failed to download any resources.")
                return False

        except Exception as e:
            print(f"Error fetching or combining resources: {str(e)}")
            return False

    def _fetch_and_combine_n3rgy_resources(self):
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

            json_files = self.client.process_all_files(
                extract_cost=True, combine_to_jsonl=False
            )

            if not json_files:
                print("No CSV files found or processing failed.")
                import shutil

                shutil.rmtree(temp_dir)
                return False

            retrieved_files = []
            for json_file in json_files:
                temp_filename = Path(json_file).name
                temp_filepath = temp_dir / temp_filename

                with open(json_file, "r") as src_file, open(
                    temp_filepath, "w"
                ) as dst_file:
                    dst_file.write(src_file.read())

                retrieved_files.append(str(temp_filepath))

            return self._process_combined_files(retrieved_files, [], [], temp_dir)

        except Exception as e:
            print(f"Error processing N3rgy files: {str(e)}")
            return False

    def _process_combined_files(
        self, retrieved_files, failed_resources, skipped_resources, temp_dir
    ):
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

            print("\nCombining resources into monthly JSONL files...")
            from pipeline.data_processing.jsonl_converter import EnergyDataConverter

            output_dir = Path("data/processed")

            converter = EnergyDataConverter(output_dir=output_dir)
            combined_filepath = converter.combine_all_resources(temp_dir)

            if combined_filepath:
                print(
                    f"\nAll resources successfully combined into {len(combined_filepath)} monthly files:"
                )
                for filepath in combined_filepath:
                    print(f"- {filepath}")

                convert_to_parquet = self.get_yes_no_input(
                    "Convert to Parquet? (y/n): "
                )

                parquet_filepaths = []
                if convert_to_parquet:
                    from pipeline.data_processing.parquet_converter import (
                        JsonlToParquetConverter,
                    )

                    parquet_dir = Path("data/parquet")
                    parquet_converter = JsonlToParquetConverter(
                        output_dir=str(parquet_dir)
                    )

                    for jsonl_file in combined_filepath:
                        print(f"\nConverting {Path(jsonl_file).name} to Parquet...")
                        parquet_filepath = (
                            parquet_converter.convert_jsonl_to_parquet_file(jsonl_file)
                        )
                        if parquet_filepath:
                            parquet_filepaths.append(parquet_filepath)

                    if parquet_filepaths:
                        print(
                            f"\nSuccessfully converted {len(parquet_filepaths)} files to Parquet format:"
                        )
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

                return [*combined_filepath, *parquet_filepaths, *original_paths]
            else:
                import shutil

                shutil.rmtree(temp_dir)
                print(f"\nTemporary directory removed: {temp_dir}")

                print(
                    "\nFailed to combine resources. Individual files are still available."
                )
                return retrieved_files
        else:
            import shutil

            shutil.rmtree(temp_dir)
            print(f"\nTemporary directory removed: {temp_dir}")

            print("\nNo files were successfully retrieved.")
            return False

    def _get_month_ranges(self, start_date, end_date):
        current_start = start_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        while current_start <= end_date:
            next_month = current_start.month + 1
            next_year = current_start.year
            if next_month > 12:
                next_month = 1
                next_year += 1

            month_end = datetime(next_year, next_month, 1) - timedelta(seconds=1)

            actual_start = max(current_start, start_date)
            actual_end = min(month_end, end_date)

            if actual_start <= actual_end:
                yield actual_start, actual_end

            current_start = datetime(next_year, next_month, 1)
