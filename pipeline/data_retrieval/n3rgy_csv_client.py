#!/usr/bin/env python
import csv
import json
import datetime
import argparse
import re
import os
from pathlib import Path


class N3rgyCSVClient:
    """
    Client for processing N3rgy CSV files and converting them to JSON/JSONL format.
    Instead of fetching data from an API, this client processes local CSV files.
    """
    
    def __init__(self, source_dir=None, output_dir=None):
        """
        Initialize the N3rgy CSV client.
        
        Args:
            source_dir (str): Directory containing CSV files to process
            output_dir (str): Directory to save output JSON/JSONL files
        """
        self.source_dir = Path(source_dir) if source_dir else Path("./raw-csv")
        self.output_dir = Path(output_dir) if output_dir else Path("./processed")
        
        # Ensure output directory exists
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Energy resource metadata
        self.energy_resource_metadata = {
            'electricity': {
                'consumption': {
                    'resource_id': 'n3rgy-electricity',
                    'resource_name': 'electricity consumption',
                    'resource_unit': 'kWh',
                    'resource_classifier': 'electricity.consumption'
                },
                'cost': {
                    'resource_id': 'n3rgy-electricity-cost',
                    'resource_name': 'electricity cost',
                    'resource_unit': 'pence',
                    'resource_classifier': 'electricity.consumption.cost'
                }
            },
            'gas': {
                'consumption': {
                    'resource_id': 'n3rgy-gas',
                    'resource_name': 'gas consumption',
                    'resource_unit': 'kWh',
                    'resource_classifier': 'gas.consumption'
                },
                'cost': {
                    'resource_id': 'n3rgy-gas-cost',
                    'resource_name': 'gas cost',
                    'resource_unit': 'pence',
                    'resource_classifier': 'gas.consumption.cost'
                }
            }
        }
    
    def process_all_files(self, extract_cost=True, combine_to_jsonl=False):
        """
        Process all CSV files in the source directory.
        
        Args:
            extract_cost (bool): Whether to extract cost data
            combine_to_jsonl (bool): Whether to combine all data into a single JSONL file
            
        Returns:
            list: Paths to generated JSON files
        """
        all_json_files = []
        
        # Find all CSV files in the source folder
        csv_files = list(self.source_dir.glob("*.csv"))
        if not csv_files:
            print(f"No CSV files found in {self.source_dir}")
            return []
        
        print(f"Found {len(csv_files)} CSV files to process")
        
        # Process each CSV file
        for csv_file in csv_files:
            # Extract energy type and date range from filename
            energy_type = self._extract_energy_type_from_filename(csv_file.stem)
            if not energy_type:
                print(f"Warning: Could not determine energy type for {csv_file.name}, skipping...")
                continue
            
            date_range = self._extract_date_range_from_filename(csv_file.stem)
            
            # Create output JSON path
            output_json = self.output_dir / f"{energy_type}_consumption_{date_range}.json"
            
            # Process the CSV file
            try:
                consumption_path, cost_path = self.transform_csv_to_json(
                    csv_file, 
                    energy_type, 
                    output_json,
                    extract_cost_data=extract_cost
                )
                
                all_json_files.append(consumption_path)
                if cost_path:
                    all_json_files.append(cost_path)
                    
            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
        
        # Combine into JSONL if requested
        if combine_to_jsonl and all_json_files:
            self.create_jsonl_from_json_files(all_json_files)
        
        return all_json_files
    
    def transform_csv_to_json(self, source_csv_path, energy_type, destination_json_path=None, extract_cost_data=True):
        """
        Transform a CSV energy consumption file to standardized JSON format.
        
        Args:
            source_csv_path (str or Path): Path to the source CSV file
            energy_type (str): Energy type identifier ('electricity' or 'gas')
            destination_json_path (str or Path, optional): Where to save output JSON file
            extract_cost_data (bool): Whether to extract cost data
            
        Returns:
            tuple: Paths to created JSON files (consumption_file_path, cost_file_path)
        """
        if energy_type not in self.energy_resource_metadata:
            valid_types = ', '.join(self.energy_resource_metadata.keys())
            raise ValueError(f"Energy type must be one of: {valid_types}")
        
        consumption_data_points = []
        cost_data_points = []
        earliest_timestamp = None
        latest_timestamp = None
        
        with open(source_csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            csv_header = next(csv_reader)  # Skip header row
            
            for row_data in csv_reader:
                if len(row_data) < 2:
                    continue
                    
                timestamp_string = row_data[0]
                consumption_value_string = row_data[1] if len(row_data) > 1 else None
                cost_value_string = row_data[2] if len(row_data) > 2 else None
                
                if not consumption_value_string or consumption_value_string == 'energyConsumption (kWh)':
                    continue
                    
                try:
                    timestamp_datetime = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M')
                    unix_timestamp = int(timestamp_datetime.timestamp())
                    
                    if earliest_timestamp is None or timestamp_datetime < earliest_timestamp:
                        earliest_timestamp = timestamp_datetime
                    if latest_timestamp is None or timestamp_datetime > latest_timestamp:
                        latest_timestamp = timestamp_datetime
                    
                    parsed_consumption_value = float(consumption_value_string) if consumption_value_string else 0
                    consumption_data_points.append([unix_timestamp, parsed_consumption_value])
                    
                    if extract_cost_data and cost_value_string and cost_value_string != "current £/day" and cost_value_string.strip():
                        parsed_cost_value = float(cost_value_string) * 100  # Convert £/day to pence
                        cost_data_points.append([unix_timestamp, parsed_cost_value])
                    
                except (ValueError, TypeError) as error:
                    print(f"Warning: Could not parse row: {row_data}. Error: {error}")
        
        # Create consumption JSON
        consumption_json = {
            "resource_id": self.energy_resource_metadata[energy_type]["consumption"]["resource_id"],
            "resource_name": self.energy_resource_metadata[energy_type]["consumption"]["resource_name"],
            "resource_unit": self.energy_resource_metadata[energy_type]["consumption"]["resource_unit"],
            "resource_classifier": self.energy_resource_metadata[energy_type]["consumption"]["resource_classifier"],
            "start_date": earliest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if earliest_timestamp else "",
            "end_date": latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if latest_timestamp else "",
            "period": "PT30M",
            "timezone_offset": 0,
            "readings": consumption_data_points
        }
        
        # Determine output path if not provided
        if destination_json_path is None:
            source_path = Path(source_csv_path)
            date_range_string = ""
            if earliest_timestamp and latest_timestamp:
                start_date_string = earliest_timestamp.strftime("%Y%m%d")
                end_date_string = latest_timestamp.strftime("%Y%m%d")
                date_range_string = f"{start_date_string}_to_{end_date_string}"
            else:
                date_range_string = self._extract_date_range_from_filename(source_path.stem)
            consumption_json_path = self.output_dir / f"{energy_type}_consumption_{date_range_string}.json"
        else:
            consumption_json_path = destination_json_path
        
        # Save consumption JSON
        with open(consumption_json_path, 'w') as output_file:
            json.dump(consumption_json, output_file, indent=2)
        
        print(f"Created consumption JSON from {source_csv_path} at {consumption_json_path}")
        print(f"Processed {len(consumption_data_points)} readings from {earliest_timestamp} to {latest_timestamp}")
        
        # Process cost data if available
        cost_json_path = None
        if extract_cost_data and cost_data_points:
            cost_json = {
                "resource_id": self.energy_resource_metadata[energy_type]["cost"]["resource_id"],
                "resource_name": self.energy_resource_metadata[energy_type]["cost"]["resource_name"],
                "resource_unit": self.energy_resource_metadata[energy_type]["cost"]["resource_unit"],
                "resource_classifier": self.energy_resource_metadata[energy_type]["cost"]["resource_classifier"],
                "start_date": earliest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if earliest_timestamp else "",
                "end_date": latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if latest_timestamp else "",
                "period": "PT30M",
                "timezone_offset": 0,
                "readings": cost_data_points
            }
            
            cost_json_path = str(consumption_json_path).replace("_consumption_", "_cost_")
            
            with open(cost_json_path, 'w') as output_file:
                json.dump(cost_json, output_file, indent=2)
            
            print(f"Created cost JSON from {source_csv_path} at {cost_json_path}")
            print(f"Processed {len(cost_data_points)} cost readings from {earliest_timestamp} to {latest_timestamp}")
        
        return consumption_json_path, cost_json_path
    
    def create_jsonl_from_json_files(self, source_json_paths, destination_jsonl_path=None):
        """
        Create a combined JSONL file from multiple JSON files.
        
        Args:
            source_json_paths (list): List of JSON file paths to combine
            destination_jsonl_path (str or Path, optional): Where to save the output JSONL file
            
        Returns:
            Path: Path to the created JSONL file
        """
        # Determine overall date range from filenames
        date_components = []
        for file_path in source_json_paths:
            filename = Path(file_path).stem
            date_range = self._extract_date_range_from_filename(filename)
            parts = date_range.split('_to_')
            if len(parts) == 2:
                date_components.extend(parts)
        
        earliest_date = min(date_components) if date_components else "unknown"
        latest_date = max(date_components) if date_components else "unknown"
        
        # Set default output path if not provided
        if destination_jsonl_path is None:
            destination_jsonl_path = self.output_dir / f"all_resources_{earliest_date}_to_{latest_date}.jsonl"
        
        # Merge the JSON files
        jsonl_path = self._merge_json_files_into_jsonl(source_json_paths, destination_jsonl_path)
        return jsonl_path
    
    def _merge_json_files_into_jsonl(self, source_json_paths, destination_jsonl_path):
        """
        Merge multiple energy JSON files into a single JSONL file with standardized format.
        
        Args:
            source_json_paths (list): List of JSON file paths to merge
            destination_jsonl_path (str or Path): Where to save the output JSONL file
            
        Returns:
            Path: Path to the created JSONL file
        """
        energy_resource_data = {}
        
        # Load all JSON files
        for json_file_path in source_json_paths:
            if json_file_path is None:
                continue
                
            with open(json_file_path, 'r') as json_file:
                resource_data = json.load(json_file)
                
                resource_classifier = resource_data['resource_classifier']
                classifier_parts = resource_classifier.split('.')
                energy_type = classifier_parts[0]  # 'electricity' or 'gas'
                data_category = 'cost' if 'cost' in resource_classifier else 'consumption'
                
                if energy_type not in energy_resource_data:
                    energy_resource_data[energy_type] = {}
                
                energy_resource_data[energy_type][data_category] = resource_data
        
        # Extract all timestamps
        all_timestamps = []
        for resource_type_data in energy_resource_data.values():
            for category_data in resource_type_data.values():
                if category_data['start_date']:
                    all_timestamps.append(datetime.datetime.strptime(category_data['start_date'], "%Y-%m-%dT%H:%M:%S"))
                if category_data['end_date']:
                    all_timestamps.append(datetime.datetime.strptime(category_data['end_date'], "%Y-%m-%dT%H:%M:%S"))
        
        earliest_timestamp = min(all_timestamps) if all_timestamps else None
        latest_timestamp = max(all_timestamps) if all_timestamps else None
        
        # Collect readings by timestamp
        readings_by_timestamp = {}
        
        for energy_type, resource_type_data in energy_resource_data.items():
            for data_category, category_data in resource_type_data.items():
                for timestamp, value in category_data['readings']:
                    if timestamp not in readings_by_timestamp:
                        readings_by_timestamp[timestamp] = {
                            'timestamp': timestamp,
                            'timestamp_iso': datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
                        }
                    
                    field_name = f"{energy_type}_{data_category}"
                    readings_by_timestamp[timestamp][field_name] = value
        
        # Create common metadata
        common_metadata = {
            'period': 'PT30M',
            'from_date': earliest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if earliest_timestamp else "",
            'to_date': latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if latest_timestamp else ""
        }
        
        # Add resource metadata
        for energy_type, resource_type_data in energy_resource_data.items():
            for data_category, category_data in resource_type_data.items():
                field_prefix = f"{energy_type}_{data_category}"
                common_metadata[f'{field_prefix}_id'] = category_data['resource_id']
                common_metadata[f'{field_prefix}_name'] = category_data['resource_name']
                common_metadata[f'{field_prefix}_unit'] = category_data['resource_unit']
                common_metadata[f'{field_prefix}_classifier'] = category_data['resource_classifier']
        
        # Add missing resource metadata for completeness
        for energy_type in ['electricity', 'gas']:
            for data_category in ['consumption', 'cost']:
                field_prefix = f"{energy_type}_{data_category}"
                
                if f'{field_prefix}_id' not in common_metadata:
                    common_metadata[f'{field_prefix}_id'] = f"unknown-{energy_type}-{data_category}"
                    common_metadata[f'{field_prefix}_name'] = f"{energy_type} {data_category}"
                    common_metadata[f'{field_prefix}_unit'] = 'kWh' if data_category == 'consumption' else 'pence'
                    common_metadata[f'{field_prefix}_classifier'] = f"{energy_type}.{data_category}" if data_category == 'consumption' else f"{energy_type}.consumption.cost"
        
        # Write JSONL file
        with open(destination_jsonl_path, 'w') as output_file:
            for timestamp in sorted(readings_by_timestamp.keys()):
                reading_data = readings_by_timestamp[timestamp]
                
                # Ensure all fields exist, even with zero values
                for energy_type in ['electricity', 'gas']:
                    for data_category in ['consumption', 'cost']:
                        field_name = f"{energy_type}_{data_category}"
                        if field_name not in reading_data:
                            reading_data[field_name] = 0
                
                complete_reading = {**common_metadata, **reading_data}
                output_file.write(json.dumps(complete_reading) + '\n')
        
        print(f"Created combined JSONL file at: {destination_jsonl_path}")
        print(f"Contains {len(readings_by_timestamp)} readings from {earliest_timestamp} to {latest_timestamp}")
        
        return destination_jsonl_path
    
    def _extract_energy_type_from_filename(self, filename):
        """Extract the energy type (electricity or gas) from a filename."""
        filename = filename.lower()
        if 'electricity' in filename:
            return 'electricity'
        elif 'gas' in filename:
            return 'gas'
        return None
    
    def _extract_date_range_from_filename(self, filename):
        """Extract date range from a filename in format YYYYMMDD_to_YYYYMMDD."""
        # Try to find pattern like 20240501_to_20240531
        date_pattern = r'(\d{8})_to_(\d{8})'
        match = re.search(date_pattern, filename)
        if match:
            return f"{match.group(1)}_to_{match.group(2)}"
        
        # Try to find pattern like 202405 (YYYYMM) and convert to full date range
        month_pattern = r'(\d{4})(\d{2})'
        match = re.search(month_pattern, filename)
        if match:
            year = match.group(1)
            month = match.group(2)
            import calendar
            last_day = calendar.monthrange(int(year), int(month))[1]
            return f"{year}{month}01_to_{year}{month}{last_day:02d}"
        
        # Try to find pattern like 2024-05 (YYYY-MM) and convert to full date range
        month_pattern_dash = r'(\d{4})-(\d{2})'
        match = re.search(month_pattern_dash, filename)
        if match:
            year = match.group(1)
            month = match.group(2)
            import calendar
            last_day = calendar.monthrange(int(year), int(month))[1]
            return f"{year}{month}01_to_{year}{month}{last_day:02d}"
        
        return "unknown_date_range"
    
    def get_resource_data(self, resource_id, start_date=None, end_date=None):
        """
        Get resource data from processed JSON files, similar to GlowmarktClient's get_readings.
        This allows for a consistent interface between both clients.
        
        Args:
            resource_id (str): Resource ID to look for
            start_date (str or datetime): Start date filter (optional)
            end_date (str or datetime): End date filter (optional)
            
        Returns:
            dict: Resource data
        """
        # Convert dates to strings if they're datetime objects
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, datetime.datetime):
            end_date = end_date.strftime("%Y-%m-%d")
            
        # Look for matching JSON files in output directory
        json_files = list(self.output_dir.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    
                    # Check if this file contains the requested resource
                    if data.get('resource_id') == resource_id:
                        # Filter by date range if specified
                        if start_date or end_date:
                            filtered_readings = []
                            for timestamp, value in data.get('readings', []):
                                reading_date = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                                
                                if start_date and reading_date < start_date:
                                    continue
                                if end_date and reading_date > end_date:
                                    continue
                                    
                                filtered_readings.append([timestamp, value])
                                
                            # Create a copy with filtered readings
                            filtered_data = data.copy()
                            filtered_data['readings'] = filtered_readings
                            return filtered_data
                        
                        # No filtering needed
                        return data
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                
        # No matching resource found
        return None


def main():
    """Command-line interface for the N3rgy CSV client."""
    arg_parser = argparse.ArgumentParser(description='Convert N3rgy CSV files to JSON and JSONL formats')
    arg_parser.add_argument('--source-dir', default='./raw-csv', help='Directory containing CSV files to convert')
    arg_parser.add_argument('--output-dir', default='./processed', help='Directory to save output files')
    arg_parser.add_argument('--no-cost', action='store_true', help='Skip extracting cost data')
    arg_parser.add_argument('--combine', action='store_true', help='Combine all data into a single JSONL file')
    
    cmd_args = arg_parser.parse_args()
    
    client = N3rgyCSVClient(cmd_args.source_dir, cmd_args.output_dir)
    client.process_all_files(
        extract_cost=not cmd_args.no_cost,
        combine_to_jsonl=cmd_args.combine
    )


if __name__ == "__main__":
    main()