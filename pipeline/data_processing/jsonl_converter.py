#!/usr/bin/env python
import json
import logging
import re
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnergyDataConverter:
    
    def __init__(self, output_dir: Union[str, Path] = "data/processed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_json_from_file(self, file_path: Union[str, Path]) -> Dict:
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def convert_to_jsonl(self, input_file: Union[str, Path, Dict], output_file: Optional[Union[str, Path]] = None) -> str:
        if isinstance(input_file, dict):
            data = input_file
            if output_file is None:
                raise ValueError("output_file must be provided when input_file is a dictionary")
            output_file = Path(output_file)
        else:
            input_path = Path(input_file)
            if output_file is None:
                output_file = self.output_dir / f"{input_path.stem}.jsonl"
            else:
                output_file = Path(output_file)
            data = self.load_json_from_file(input_path)
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        resource_id = data.get("resource_id", "unknown")
        resource_name = data.get("resource_name", "unknown")
        resource_unit = data.get("resource_unit", "unknown")
        resource_classifier = data.get("resource_classifier", "unknown")
        period = data.get("period", "unknown")
        start_date = data.get("start_date", data.get("query", {}).get("from", "unknown"))
        end_date = data.get("end_date", data.get("query", {}).get("to", "unknown"))
        
        readings = data.get("readings", [])
        entries_written = 0
        
        with open(output_file, 'w') as f:
            for reading in readings:
                timestamp, value = reading
                
                try:
                    timestamp_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                    dt = datetime.fromtimestamp(timestamp_seconds)
                    timestamp_iso = dt.isoformat()
                except:
                    timestamp_iso = "unknown"
                
                data_object = {
                    "resource_id": resource_id,
                    "resource_name": resource_name,
                    "resource_unit": resource_unit,
                    "resource_classifier": resource_classifier,
                    "period": period,
                    "from_date": start_date,
                    "to_date": end_date,
                    "timestamp": timestamp,
                    "timestamp_iso": timestamp_iso,
                    "value": value
                }
                
                f.write(json.dumps(data_object) + '\n')
                entries_written += 1
        
        logger.info(f"Converted {entries_written} readings to JSONL format at {output_file}")
        return str(output_file)
    
    def batch_convert_to_jsonl(self, input_files: List[Union[str, Path]]) -> List[str]:
        output_files = []
        
        for input_file in input_files:
            output_file = self.convert_to_jsonl(input_file)
            output_files.append(output_file)
        
        return output_files
    
    def merge_consumption_and_cost_data(self, consumption_file: Union[str, Path], cost_file: Union[str, Path]) -> Tuple[Dict[int, Dict], Dict]:
        consumption_data = self.load_json_from_file(consumption_file)
        cost_data = self.load_json_from_file(cost_file)
        
        consumption_readings = consumption_data.get("readings", [])
        cost_readings = cost_data.get("readings", [])
        
        consumption_by_timestamp = {reading[0]: reading[1] for reading in consumption_readings}
        cost_by_timestamp = {reading[0]: reading[1] for reading in cost_readings}
        
        merged_readings = {}
        
        for timestamp in set(consumption_by_timestamp.keys()) | set(cost_by_timestamp.keys()):
            consumption_value = consumption_by_timestamp.get(timestamp, None)
            cost_value = cost_by_timestamp.get(timestamp, None)
            
            if consumption_value is None and cost_value is None:
                continue
            
            try:
                timestamp_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                dt = datetime.fromtimestamp(timestamp_seconds)
                timestamp_iso = dt.isoformat()
            except:
                timestamp_iso = "unknown"
            
            merged_readings[timestamp] = {
                "timestamp": timestamp,
                "timestamp_iso": timestamp_iso,
                "consumption_value": consumption_value,
                "cost_value": cost_value
            }
        
        if "electricity" in Path(consumption_file).name.lower():
            resource_type = "electricity"
        elif "gas" in Path(consumption_file).name.lower():
            resource_type = "gas"
        elif "water" in Path(consumption_file).name.lower():
            resource_type = "water"
        else:
            resource_type = "unknown"
        
        resource_metadata = {
            "resource_type": resource_type,
            "consumption_id": consumption_data.get("resource_id", "unknown"),
            "consumption_name": consumption_data.get("resource_name", "unknown"),
            "consumption_classifier": consumption_data.get("resource_classifier", "unknown"),
            "consumption_unit": consumption_data.get("resource_unit", "unknown"),
            "cost_id": cost_data.get("resource_id", "unknown"),
            "cost_name": cost_data.get("resource_name", "unknown"),
            "cost_classifier": cost_data.get("resource_classifier", "unknown"),
            "cost_unit": cost_data.get("resource_unit", "unknown"),
            "period": consumption_data.get("period", cost_data.get("period", "unknown")),
            "from_date": consumption_data.get("start_date", consumption_data.get("query", {}).get("from", "unknown")),
            "to_date": consumption_data.get("end_date", consumption_data.get("query", {}).get("to", "unknown"))
        }
        
        return merged_readings, resource_metadata
    
    def combine_consumption_and_cost(self, consumption_file: Union[str, Path], cost_file: Union[str, Path], output_file: Optional[Union[str, Path]] = None) -> str:
        merged_readings, resource_metadata = self.merge_consumption_and_cost_data(consumption_file, cost_file)
        
        resource_type = resource_metadata["resource_type"]
        
        if output_file is None:
            consumption_path = Path(consumption_file)
            filename = f"{resource_type}_combined_{consumption_path.name.split('_', 1)[1]}"
            if not filename.endswith('.jsonl'):
                filename = filename.rsplit('.', 1)[0] + '.jsonl'
            output_file = self.output_dir / filename
        else:
            output_file = Path(output_file)
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        entries_written = 0
        with open(output_file, 'w') as file_handle:
            for timestamp, reading in sorted(merged_readings.items()):
                data_object = {
                    "resource_type": resource_type,
                    "consumption_id": resource_metadata["consumption_id"],
                    "consumption_name": resource_metadata["consumption_name"],
                    "consumption_classifier": resource_metadata["consumption_classifier"],
                    "consumption_unit": resource_metadata["consumption_unit"],
                    "cost_id": resource_metadata["cost_id"],
                    "cost_name": resource_metadata["cost_name"],
                    "cost_classifier": resource_metadata["cost_classifier"],
                    "cost_unit": resource_metadata["cost_unit"],
                    "period": resource_metadata["period"],
                    "from_date": resource_metadata["from_date"],
                    "to_date": resource_metadata["to_date"],
                    "timestamp": reading["timestamp"],
                    "timestamp_iso": reading["timestamp_iso"],
                    "consumption_value": reading["consumption_value"],
                    "cost_value": reading["cost_value"]
                }
                
                file_handle.write(json.dumps(data_object) + '\n')
                entries_written += 1
        
        logger.info(f"Combined {entries_written} readings into JSONL format at {output_file}")
        return str(output_file)
    
    def find_matching_resource_files(self, directory: Union[str, Path]) -> List[Tuple[str, str]]:
        directory = Path(directory)
        all_files = list(directory.glob("*.json"))
        
        consumption_files_by_key = {}
        cost_files_by_key = {}
        
        for file_path in all_files:
            filename = file_path.name.lower()
            
            if "electricity" in filename:
                resource_type = "electricity"
            elif "gas" in filename:
                resource_type = "gas"
            elif "water" in filename:
                resource_type = "water"
            else:
                resource_type = "unknown"
            
            date_range = None
            
            date_pattern = r'(\d{8})_to_(\d{8})'
            match = re.search(date_pattern, filename)
            if match:
                date_range = f"{match.group(1)}_to_{match.group(2)}"
            
            if not date_range:
                base_name = filename.replace("consumption", "").replace("cost", "").replace(".json", "")
                date_range = base_name
            
            file_key = f"{resource_type}_{date_range}"
            
            if "consumption" in filename:
                consumption_files_by_key[file_key] = str(file_path)
            elif "cost" in filename:
                cost_files_by_key[file_key] = str(file_path)
        
        matching_file_pairs = []
        
        for file_key in consumption_files_by_key:
            if file_key in cost_files_by_key:
                matching_file_pairs.append((consumption_files_by_key[file_key], cost_files_by_key[file_key]))
        
        logger.info(f"Found {len(matching_file_pairs)} matching consumption-cost file pairs in {directory}")
        return matching_file_pairs
    
    def combine_all_resources_into_single_file(self, directory: Union[str, Path] = "data/glowmarkt_api_raw", output_file: Optional[Union[str, Path]] = None, split_by_month: bool = True) -> Union[str, List[str]]:
        directory = Path(directory)
        matching_file_pairs = self.find_matching_resource_files(directory)
        
        if not matching_file_pairs:
            logger.warning(f"No matching consumption-cost pairs found in {directory}")
            return None
        
        combined_readings_by_timestamp = {}
        resource_metadata_by_type = {}
        
        for consumption_file, cost_file in matching_file_pairs:
            merged_readings, resource_metadata = self.merge_consumption_and_cost_data(consumption_file, cost_file)
            resource_type = resource_metadata["resource_type"]
            
            resource_metadata_by_type[resource_type] = resource_metadata
            
            for timestamp, reading in merged_readings.items():
                if timestamp not in combined_readings_by_timestamp:
                    combined_readings_by_timestamp[timestamp] = {
                        "timestamp": timestamp,
                        "timestamp_iso": reading["timestamp_iso"]
                    }
                
                combined_readings_by_timestamp[timestamp][f"{resource_type}_consumption"] = reading["consumption_value"]
                combined_readings_by_timestamp[timestamp][f"{resource_type}_cost"] = reading["cost_value"]
        
        consolidated_metadata = {
            "period": next(iter(resource_metadata_by_type.values())).get("period", "unknown"),
            "from_date": next(iter(resource_metadata_by_type.values())).get("from_date", "unknown"),
            "to_date": next(iter(resource_metadata_by_type.values())).get("to_date", "unknown"),
        }
        
        for resource_type, metadata in resource_metadata_by_type.items():
            consolidated_metadata[f"{resource_type}_consumption_id"] = metadata["consumption_id"]
            consolidated_metadata[f"{resource_type}_consumption_name"] = metadata["consumption_name"]
            consolidated_metadata[f"{resource_type}_consumption_unit"] = metadata["consumption_unit"]
            consolidated_metadata[f"{resource_type}_consumption_classifier"] = metadata["consumption_classifier"]
            consolidated_metadata[f"{resource_type}_cost_id"] = metadata["cost_id"]
            consolidated_metadata[f"{resource_type}_cost_name"] = metadata["cost_name"]
            consolidated_metadata[f"{resource_type}_cost_unit"] = metadata["cost_unit"]
            consolidated_metadata[f"{resource_type}_cost_classifier"] = metadata["cost_classifier"]
        
        if not split_by_month:
            if output_file is None:
                first_consumption_file = matching_file_pairs[0][0]
                first_data = self.load_json_from_file(first_consumption_file)
                
                from_date = first_data.get("query", {}).get("from", first_data.get("start_date", "unknown"))
                to_date = first_data.get("query", {}).get("to", first_data.get("end_date", "unknown"))
                
                start_date_str = from_date.split("T")[0].replace("-", "") if isinstance(from_date, str) else "unknown"
                end_date_str = to_date.split("T")[0].replace("-", "") if isinstance(to_date, str) else "unknown"
                
                filename = f"all_resources_{start_date_str}_to_{end_date_str}.jsonl"
                output_file = self.output_dir / filename
            else:
                output_file = Path(output_file)
            
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            entries_written = 0
            with open(output_file, 'w') as file_handle:
                for timestamp, reading in sorted(combined_readings_by_timestamp.items()):
                    data_object = {**consolidated_metadata, **reading}
                    file_handle.write(json.dumps(data_object) + '\n')
                    entries_written += 1
            
            logger.info(f"Combined {entries_written} readings across {len(resource_metadata_by_type)} resource types into JSONL format at {output_file}")
            return str(output_file)
        
        readings_by_month = {}
        output_files = []
        
        for timestamp, reading in combined_readings_by_timestamp.items():
            try:
                timestamp_value = timestamp
                if isinstance(timestamp_value, (int, float)):
                    timestamp_seconds = timestamp_value / 1000 if timestamp_value > 9999999999 else timestamp_value
                    date_time = datetime.fromtimestamp(timestamp_seconds)
                elif isinstance(timestamp_value, str) and 'T' in timestamp_value:
                    date_time = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                else:
                    from dateutil import parser
                    date_time = parser.parse(str(timestamp_value))
                    
                month_key = f"{date_time.year}-{date_time.month:02d}"
                
                if month_key not in readings_by_month:
                    readings_by_month[month_key] = []
                
                readings_by_month[month_key].append((timestamp, reading))
            except Exception as e:
                logger.warning(f"Could not parse timestamp {timestamp}: {e}")
                if 'unknown' not in readings_by_month:
                    readings_by_month['unknown'] = []
                readings_by_month['unknown'].append((timestamp, reading))
        
        for month_key, readings in readings_by_month.items():
            if not readings:
                continue
                
            if month_key == 'unknown':
                month_filename = f"all_resources_unknown_dates.jsonl"
            else:
                year, month = month_key.split('-')
                month_start = f"{year}{month}01"
                
                import calendar
                _, last_day = calendar.monthrange(int(year), int(month))
                month_end = f"{year}{month}{last_day}"
                
                month_filename = f"all_resources_{month_start}_to_{month_end}.jsonl"
            
            month_output_file = self.output_dir / month_filename
            month_output_file.parent.mkdir(parents=True, exist_ok=True)
            
            month_metadata = consolidated_metadata.copy()
            if month_key != 'unknown':
                year, month = month_key.split('-')
                import calendar
                _, last_day = calendar.monthrange(int(year), int(month))
                month_metadata["from_date"] = f"{year}-{month}-01T00:00:00.000Z"
                month_metadata["to_date"] = f"{year}-{month}-{last_day}T23:59:59.999Z"
            
            entries_written = 0
            with open(month_output_file, 'w') as file_handle:
                for timestamp, reading in sorted(readings):
                    data_object = {**month_metadata, **reading}
                    file_handle.write(json.dumps(data_object) + '\n')
                    entries_written += 1
            
            logger.info(f"Created monthly file for {month_key} with {entries_written} readings at {month_output_file}")
            output_files.append(str(month_output_file))
        
        logger.info(f"Split data into {len(output_files)} monthly files in {self.output_dir}")
        return output_files
    
    def extract_resource_type(self, resource_name: str) -> str:
        resource_name = resource_name.lower()
        if "electricity" in resource_name:
            return "electricity"
        elif "gas" in resource_name:
            return "gas"
        elif "water" in resource_name:
            return "water"
        else:
            return "energy"
    
    def batch_combine_resource_files(self, directory: Union[str, Path]) -> List[str]:
        matching_file_pairs = self.find_matching_resource_files(directory)
        
        output_files = []
        for consumption_file, cost_file in matching_file_pairs:
            output_file = self.combine_consumption_and_cost(consumption_file, cost_file)
            output_files.append(output_file)
        
        return output_files