
import csv
import json
import datetime
import argparse
import re
import os
from pathlib import Path


class N3rgyCSVClient:
    def __init__(self, source_dir=None, output_dir=None):
        self.source_dir = Path(source_dir) if source_dir else Path("./csv")
        self.output_dir = Path(output_dir) if output_dir else Path("./processed")
        self.output_dir.mkdir(exist_ok=True, parents=True)
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
        all_json_files = []
        csv_files_to_process = list(self.source_dir.glob("*.csv"))
        if not csv_files_to_process:
            print(f"No CSV files found in {self.source_dir}")
            return []
        
        print(f"Found {len(csv_files_to_process)} CSV files to process")
        
        for csv_file in csv_files_to_process:
            energy_type = self._extract_energy_type_from_filename(csv_file.stem)
            if not energy_type:
                print(f"Warning: Could not determine energy type for {csv_file.name}, skipping...")
                continue
            
            date_range_suffix = self._extract_date_range_from_filename(csv_file.stem)
            output_json_path = self.output_dir / f"{energy_type}_consumption_{date_range_suffix}.json"
            
            try:
                consumption_path, cost_path = self.transform_csv_to_json(
                    csv_file, 
                    energy_type, 
                    output_json_path,
                    extract_cost_data=extract_cost
                )
                
                all_json_files.append(consumption_path)
                if cost_path:
                    all_json_files.append(cost_path)
                    
            except Exception as e:
                print(f"Error processing {csv_file}: {e}")
        
        if combine_to_jsonl and all_json_files:
            self.create_jsonl_from_json_files(all_json_files)
        
        return all_json_files
    
    def transform_csv_to_json(self, source_csv_path, energy_type, destination_json_path=None, extract_cost_data=True):
        if energy_type not in self.energy_resource_metadata:
            valid_energy_types = ', '.join(self.energy_resource_metadata.keys())
            raise ValueError(f"Energy type must be one of: {valid_energy_types}")
        
        consumption_readings = []
        cost_readings = []
        earliest_reading_timestamp = None
        latest_reading_timestamp = None
        
        with open(source_csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header_row = next(csv_reader)
            
            for row in csv_reader:
                if len(row) < 2:
                    continue
                    
                timestamp_string = row[0]
                consumption_value_string = row[1] if len(row) > 1 else None
                cost_value_string = row[2] if len(row) > 2 else None
                
                if not consumption_value_string or consumption_value_string == 'energyConsumption (kWh)':
                    continue
                    
                try:
                    reading_datetime = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M')
                    unix_timestamp = int(reading_datetime.timestamp())
                    
                    if earliest_reading_timestamp is None or reading_datetime < earliest_reading_timestamp:
                        earliest_reading_timestamp = reading_datetime
                    if latest_reading_timestamp is None or reading_datetime > latest_reading_timestamp:
                        latest_reading_timestamp = reading_datetime
                    
                    parsed_consumption_value = float(consumption_value_string) if consumption_value_string else 0
                    consumption_readings.append([unix_timestamp, parsed_consumption_value])
                    
                    if extract_cost_data and cost_value_string and cost_value_string != "current £/day" and cost_value_string.strip():
                        parsed_cost_value_in_pence = float(cost_value_string) * 100
                        cost_readings.append([unix_timestamp, parsed_cost_value_in_pence])
                    
                except (ValueError, TypeError) as parse_error:
                    print(f"Warning: Could not parse row: {row}. Error: {parse_error}")
        
        consumption_data_structure = {
            "resource_id": self.energy_resource_metadata[energy_type]["consumption"]["resource_id"],
            "resource_name": self.energy_resource_metadata[energy_type]["consumption"]["resource_name"],
            "resource_unit": self.energy_resource_metadata[energy_type]["consumption"]["resource_unit"],
            "resource_classifier": self.energy_resource_metadata[energy_type]["consumption"]["resource_classifier"],
            "start_date": earliest_reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if earliest_reading_timestamp else "",
            "end_date": latest_reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if latest_reading_timestamp else "",
            "period": "PT30M",
            "timezone_offset": 0,
            "readings": consumption_readings
        }
        
        if destination_json_path is None:
            source_file_path = Path(source_csv_path)
            if earliest_reading_timestamp and latest_reading_timestamp:
                start_date_str = earliest_reading_timestamp.strftime("%Y%m%d")
                end_date_str = latest_reading_timestamp.strftime("%Y%m%d")
                date_range_label = f"{start_date_str}_to_{end_date_str}"
            else:
                date_range_label = self._extract_date_range_from_filename(source_file_path.stem)
            consumption_json_output_path = self.output_dir / f"{energy_type}_consumption_{date_range_label}.json"
        else:
            consumption_json_output_path = destination_json_path
        
        with open(consumption_json_output_path, 'w') as output_file:
            json.dump(consumption_data_structure, output_file, indent=2)
        
        print(f"Created consumption JSON from {source_csv_path} at {consumption_json_output_path}")
        print(f"Processed {len(consumption_readings)} readings from {earliest_reading_timestamp} to {latest_reading_timestamp}")
        
        cost_json_output_path = None
        if extract_cost_data and cost_readings:
            cost_data_structure = {
                "resource_id": self.energy_resource_metadata[energy_type]["cost"]["resource_id"],
                "resource_name": self.energy_resource_metadata[energy_type]["cost"]["resource_name"],
                "resource_unit": self.energy_resource_metadata[energy_type]["cost"]["resource_unit"],
                "resource_classifier": self.energy_resource_metadata[energy_type]["cost"]["resource_classifier"],
                "start_date": earliest_reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if earliest_reading_timestamp else "",
                "end_date": latest_reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if latest_reading_timestamp else "",
                "period": "PT30M",
                "timezone_offset": 0,
                "readings": cost_readings
            }
            
            cost_json_output_path = str(consumption_json_output_path).replace("_consumption_", "_cost_")
            
            with open(cost_json_output_path, 'w') as output_file:
                json.dump(cost_data_structure, output_file, indent=2)
            
            print(f"Created cost JSON from {source_csv_path} at {cost_json_output_path}")
            print(f"Processed {len(cost_readings)} cost readings from {earliest_reading_timestamp} to {latest_reading_timestamp}")
        
        return consumption_json_output_path, cost_json_output_path
    
    def create_jsonl_from_json_files(self, source_json_paths, destination_jsonl_path=None):
        date_components = []
        for file_path in source_json_paths:
            filename = Path(file_path).stem
            date_range_label = self._extract_date_range_from_filename(filename)
            parts = date_range_label.split('_to_')
            if len(parts) == 2:
                date_components.extend(parts)
        
        earliest_processed_date = min(date_components) if date_components else "unknown"
        latest_processed_date = max(date_components) if date_components else "unknown"
        
        if destination_jsonl_path is None:
            destination_jsonl_path = self.output_dir / f"all_resources_{earliest_processed_date}_to_{latest_processed_date}.jsonl"
        
        jsonl_output_path = self._merge_json_files_into_jsonl(source_json_paths, destination_jsonl_path)
        return jsonl_output_path
    
    def _merge_json_files_into_jsonl(self, source_json_paths, destination_jsonl_path):
        combined_energy_data = {}
        
        for json_file_path in source_json_paths:
            if json_file_path is None:
                continue
                
            with open(json_file_path, 'r') as json_file:
                resource_data = json.load(json_file)
                
                classifier = resource_data['resource_classifier']
                classifier_segments = classifier.split('.')
                energy_type = classifier_segments[0]
                data_category = 'cost' if 'cost' in classifier else 'consumption'
                
                if energy_type not in combined_energy_data:
                    combined_energy_data[energy_type] = {}
                
                combined_energy_data[energy_type][data_category] = resource_data
        
        all_encountered_timestamps = []
        for resource_type_data in combined_energy_data.values():
            for category_data in resource_type_data.values():
                if category_data['start_date']:
                    all_encountered_timestamps.append(datetime.datetime.strptime(category_data['start_date'], "%Y-%m-%dT%H:%M:%S"))
                if category_data['end_date']:
                    all_encountered_timestamps.append(datetime.datetime.strptime(category_data['end_date'], "%Y-%m-%dT%H:%M:%S"))
        
        overall_earliest_timestamp = min(all_encountered_timestamps) if all_encountered_timestamps else None
        overall_latest_timestamp = max(all_encountered_timestamps) if all_encountered_timestamps else None
        
        merged_readings_by_timestamp = {}
        
        for energy_type, resource_type_data in combined_energy_data.items():
            for data_category, category_data in resource_type_data.items():
                for timestamp, value in category_data['readings']:
                    if timestamp not in merged_readings_by_timestamp:
                        merged_readings_by_timestamp[timestamp] = {
                            'timestamp': timestamp,
                            'timestamp_iso': datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
                        }
                    
                    data_field_name = f"{energy_type}_{data_category}"
                    merged_readings_by_timestamp[timestamp][data_field_name] = value
        
        consolidated_metadata = {
            'period': 'PT30M',
            'from_date': overall_earliest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if overall_earliest_timestamp else "",
            'to_date': overall_latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S") if overall_latest_timestamp else ""
        }
        
        for energy_type, resource_type_data in combined_energy_data.items():
            for data_category, category_data in resource_type_data.items():
                field_prefix = f"{energy_type}_{data_category}"
                consolidated_metadata[f'{field_prefix}_id'] = category_data['resource_id']
                consolidated_metadata[f'{field_prefix}_name'] = category_data['resource_name']
                consolidated_metadata[f'{field_prefix}_unit'] = category_data['resource_unit']
                consolidated_metadata[f'{field_prefix}_classifier'] = category_data['resource_classifier']
        
        for energy_type in ['electricity', 'gas']:
            for data_category in ['consumption', 'cost']:
                field_prefix = f"{energy_type}_{data_category}"
                
                if f'{field_prefix}_id' not in consolidated_metadata:
                    consolidated_metadata[f'{field_prefix}_id'] = f"unknown-{energy_type}-{data_category}"
                    consolidated_metadata[f'{field_prefix}_name'] = f"{energy_type} {data_category}"
                    consolidated_metadata[f'{field_prefix}_unit'] = 'kWh' if data_category == 'consumption' else 'pence'
                    consolidated_metadata[f'{field_prefix}_classifier'] = f"{energy_type}.{data_category}" if data_category == 'consumption' else f"{energy_type}.consumption.cost"
        
        with open(destination_jsonl_path, 'w') as output_file:
            for timestamp in sorted(merged_readings_by_timestamp.keys()):
                reading_record = merged_readings_by_timestamp[timestamp]
                
                for energy_type in ['electricity', 'gas']:
                    for data_category in ['consumption', 'cost']:
                        field_name = f"{energy_type}_{data_category}"
                        if field_name not in reading_record:
                            reading_record[field_name] = 0
                
                complete_record = {**consolidated_metadata, **reading_record}
                output_file.write(json.dumps(complete_record) + '\n')
        
        print(f"Created combined JSONL file at: {destination_jsonl_path}")
        print(f"Contains {len(merged_readings_by_timestamp)} readings from {overall_earliest_timestamp} to {overall_latest_timestamp}")
        
        return destination_jsonl_path
    
    def _extract_energy_type_from_filename(self, filename):
        lowercase_filename = filename.lower()
        if 'electricity' in lowercase_filename:
            return 'electricity'
        elif 'gas' in lowercase_filename:
            return 'gas'
        return None
    
    def _extract_date_range_from_filename(self, filename):
        iso_date_range_pattern = r'(\d{8})_to_(\d{8})'
        iso_match = re.search(iso_date_range_pattern, filename)
        if iso_match:
            return f"{iso_match.group(1)}_to_{iso_match.group(2)}"
        
        compact_month_pattern = r'(\d{4})(\d{2})'
        compact_match = re.search(compact_month_pattern, filename)
        if compact_match:
            year = compact_match.group(1)
            month = compact_match.group(2)
            import calendar
            last_day_of_month = calendar.monthrange(int(year), int(month))[1]
            return f"{year}{month}01_to_{year}{month}{last_day_of_month:02d}"
        
        dashed_month_pattern = r'(\d{4})-(\d{2})'
        dashed_match = re.search(dashed_month_pattern, filename)
        if dashed_match:
            year = dashed_match.group(1)
            month = dashed_match.group(2)
            import calendar
            last_day_of_month = calendar.monthrange(int(year), int(month))[1]
            return f"{year}{month}01_to_{year}{month}{last_day_of_month:02d}"
        
        return "unknown_date_range"
    
    def get_resource_data(self, resource_id, start_date=None, end_date=None):
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, datetime.datetime):
            end_date = end_date.strftime("%Y-%m-%d")
            
        available_json_files = list(self.output_dir.glob("*.json"))
        
        for json_file in available_json_files:
            try:
                with open(json_file, 'r') as f:
                    file_content = json.load(f)
                    
                    if file_content.get('resource_id') == resource_id:
                        if start_date or end_date:
                            filtered_readings_list = []
                            for timestamp, value in file_content.get('readings', []):
                                reading_date_str = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                                
                                if start_date and reading_date_str < start_date:
                                    continue
                                if end_date and reading_date_str > end_date:
                                    continue
                                    
                                filtered_readings_list.append([timestamp, value])
                                
                            filtered_data_result = file_content.copy()
                            filtered_data_result['readings'] = filtered_readings_list
                            return filtered_data_result
                        
                        return file_content
            except Exception as read_error:
                print(f"Error reading {json_file}: {read_error}")
                
        return None


def main():
    command_line_argument_parser = argparse.ArgumentParser(description='Convert N3rgy CSV files to JSON and JSONL formats')
    command_line_argument_parser.add_argument('--source-dir', default='./raw-csv', help='Directory containing CSV files to convert')
    command_line_argument_parser.add_argument('--output-dir', default='./processed', help='Directory to save output files')
    command_line_argument_parser.add_argument('--no-cost', action='store_true', help='Skip extracting cost data')
    command_line_argument_parser.add_argument('--combine', action='store_true', help='Combine all data into a single JSONL file')
    
    parsed_arguments = command_line_argument_parser.parse_args()
    
    n3rgy_client = N3rgyCSVClient(parsed_arguments.source_dir, parsed_arguments.output_dir)
    n3rgy_client.process_all_files(
        extract_cost=not parsed_arguments.no_cost,
        combine_to_jsonl=parsed_arguments.combine
    )


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()