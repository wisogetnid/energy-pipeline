#!/usr/bin/env python
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnergyDataConverter:
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/processed")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def load_json_from_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        path = Path(file_path)
        logger.info(f"Loading data from {path}")
        with open(path, 'r') as file_handle:
            return json.load(file_handle)
    
    def extract_resource_type(self, resource_name: str) -> str:
        if not resource_name:
            return "energy"
        
        words = resource_name.lower().split()
        if words and words[0] in ["electricity", "gas", "water"]:
            return words[0]
        return "energy"
    
    def merge_consumption_and_cost_data(self, consumption_file: Union[str, Path], cost_file: Union[str, Path]) -> Tuple[Dict[int, Dict], Dict]:
        consumption_data = self.load_json_from_file(consumption_file)
        cost_data = self.load_json_from_file(cost_file)
        
        resource_name = consumption_data.get("name", consumption_data.get("resource_name", "energy consumption"))
        resource_type = self.extract_resource_type(resource_name)
        
        resource_metadata = {
            "resource_type": resource_type,
            "consumption_id": consumption_data.get("resourceId", consumption_data.get("resource_id", "unknown")),
            "consumption_name": consumption_data.get("name", consumption_data.get("resource_name", "energy consumption")),
            "consumption_unit": consumption_data.get("units", consumption_data.get("resource_unit", "kWh")),
            "consumption_classifier": consumption_data.get("classifier", consumption_data.get("resource_classifier", "unknown")),
            "cost_id": cost_data.get("resourceId", cost_data.get("resource_id", "unknown")),
            "cost_name": cost_data.get("name", cost_data.get("resource_name", "energy cost")),
            "cost_unit": cost_data.get("units", cost_data.get("resource_unit", "pence")),
            "cost_classifier": cost_data.get("classifier", cost_data.get("resource_classifier", "unknown")),
            "period": consumption_data.get("query", {}).get("period", consumption_data.get("period", "unknown")),
            "from_date": consumption_data.get("query", {}).get("from", consumption_data.get("start_date", "unknown")),
            "to_date": consumption_data.get("query", {}).get("to", consumption_data.get("end_date", "unknown")),
        }
        
        consumption_readings = consumption_data.get("data", consumption_data.get("readings", []))
        cost_readings = cost_data.get("data", cost_data.get("readings", []))
        
        merged_readings_by_timestamp = {}
        
        for reading in consumption_readings:
            if isinstance(reading, list) and len(reading) >= 2:
                timestamp, value = reading[0], reading[1]
                
                iso_timestamp = None
                if isinstance(timestamp, (int, float)):
                    try:
                        time_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                        date_time = datetime.fromtimestamp(time_seconds)
                        iso_timestamp = date_time.isoformat()
                    except (ValueError, TypeError, OverflowError):
                        iso_timestamp = str(timestamp)
                else:
                    iso_timestamp = str(timestamp)
                
                merged_readings_by_timestamp[timestamp] = {
                    "timestamp": timestamp,
                    "timestamp_iso": iso_timestamp,
                    "consumption_value": value,
                    "cost_value": None
                }
        
        for reading in cost_readings:
            if isinstance(reading, list) and len(reading) >= 2:
                timestamp, value = reading[0], reading[1]
                
                if timestamp in merged_readings_by_timestamp:
                    merged_readings_by_timestamp[timestamp]["cost_value"] = value
                else:
                    iso_timestamp = None
                    if isinstance(timestamp, (int, float)):
                        try:
                            time_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                            date_time = datetime.fromtimestamp(time_seconds)
                            iso_timestamp = date_time.isoformat()
                        except (ValueError, TypeError, OverflowError):
                            iso_timestamp = str(timestamp)
                    else:
                        iso_timestamp = str(timestamp)
                    
                    merged_readings_by_timestamp[timestamp] = {
                        "timestamp": timestamp,
                        "timestamp_iso": iso_timestamp,
                        "consumption_value": None,
                        "cost_value": value
                    }
        
        return merged_readings_by_timestamp, resource_metadata
    
    def convert_to_jsonl(
        self, 
        data: Union[Dict[str, Any], str, Path], 
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        original_filename = None
        
        if isinstance(data, (str, Path)):
            file_path = Path(data)
            logger.info(f"Loading data from {file_path}")
            original_filename = file_path.stem
            with open(file_path, 'r') as file_handle:
                data = json.load(file_handle)
        
        resource_id = data.get("resourceId", data.get("resource_id", "unknown"))
        resource_name = data.get("name", data.get("resource_name", "energy consumption"))
        resource_type = data.get("resourceTypeId", data.get("resource_type", "unknown"))
        classifier = data.get("classifier", data.get("resource_classifier", "unknown"))
        units = data.get("units", data.get("resource_unit", "kWh"))
        
        query = data.get("query", {})
        period = query.get("period", data.get("period", "unknown"))
        from_date = query.get("from", data.get("start_date", "unknown"))
        to_date = query.get("to", data.get("end_date", "unknown"))
        
        if output_file is None:
            if original_filename:
                output_file = self.output_dir / f"{original_filename}.jsonl"
            else:
                resource_name_safe = resource_name.lower().replace(" ", "_").replace(".", "_")
                start_date_str = from_date.strftime("%Y%m%d") if isinstance(from_date, datetime) else "unknown"
                end_date_str = to_date.strftime("%Y%m%d") if isinstance(to_date, datetime) else "unknown"
                filename = f"{resource_name_safe}_{start_date_str}_to_{end_date_str}"
                output_file = self.output_dir / f"{filename}.jsonl"
        else:
            output_file = Path(output_file)
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        readings = data.get("data", data.get("readings", []))
        
        entries_written = 0
        
        with open(output_file, 'w') as file_handle:
            for reading in readings:
                if isinstance(reading, list) and len(reading) >= 2:
                    timestamp, value = reading[0], reading[1]
                    
                    iso_timestamp = None
                    if isinstance(timestamp, (int, float)):
                        try:
                            time_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                            date_time = datetime.fromtimestamp(time_seconds)
                            iso_timestamp = date_time.isoformat()
                        except (ValueError, TypeError, OverflowError):
                            iso_timestamp = str(timestamp)
                    else:
                        iso_timestamp = str(timestamp)
                    
                    data_object = {
                        "resource_id": resource_id,
                        "resource_name": resource_name,
                        "resource_type": resource_type,
                        "classifier": classifier,
                        "units": units,
                        "period": period,
                        "from_date": from_date,
                        "to_date": to_date,
                        "timestamp": timestamp,
                        "timestamp_iso": iso_timestamp,
                        "value": value
                    }
                    
                    file_handle.write(json.dumps(data_object) + '\n')
                    entries_written += 1
        
        logger.info(f"Converted {entries_written} readings to JSONL format at {output_file}")
        return str(output_file)
    
    def combine_consumption_and_cost(
        self,
        consumption_file: Union[str, Path],
        cost_file: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        merged_readings, resource_metadata = self.merge_consumption_and_cost_data(consumption_file, cost_file)
        
        resource_type = resource_metadata["resource_type"]
        from_date = resource_metadata["from_date"]
        to_date = resource_metadata["to_date"]
        
        if output_file is None:
            start_date_str = from_date.split("T")[0].replace("-", "") if isinstance(from_date, str) else "unknown"
            end_date_str = to_date.split("T")[0].replace("-", "") if isinstance(to_date, str) else "unknown"
            filename = f"{resource_type}_combined_{start_date_str}_to_{end_date_str}.jsonl"
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
    
    def find_matching_resource_files(self, directory: Union[str, Path] = "data/glowmarkt_api_raw") -> List[Tuple[str, str]]:
        directory = Path(directory)
        all_files = list(directory.glob("*.json"))
        
        consumption_files_by_key = {}
        cost_files_by_key = {}
        
        for file_path in all_files:
            filename = file_path.name
            
            if "electricity" in filename.lower():
                resource_type = "electricity"
            elif "gas" in filename.lower():
                resource_type = "gas"
            elif "water" in filename.lower():
                resource_type = "water"
            else:
                resource_type = "unknown"
            
            filename_parts = filename.split('_')
            if len(filename_parts) >= 3:
                date_range = '_'.join(filename_parts[2:]).replace('.json', '')
                
                file_key = f"{resource_type}_{date_range}"
                
                if "consumption" in filename.lower():
                    consumption_files_by_key[file_key] = str(file_path)
                elif "cost" in filename.lower():
                    cost_files_by_key[file_key] = str(file_path)
        
        matching_file_pairs = []
        
        for file_key in consumption_files_by_key:
            if file_key in cost_files_by_key:
                matching_file_pairs.append((consumption_files_by_key[file_key], cost_files_by_key[file_key]))
        
        return matching_file_pairs
    
    def batch_combine_resource_files(
        self,
        directory: Union[str, Path] = "data/glowmarkt_api_raw",
        output_dir: Optional[Union[str, Path]] = None
    ) -> List[str]:
        matching_file_pairs = self.find_matching_resource_files(directory)
        
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.output_dir
        
        output_files = []
        
        for consumption_file, cost_file in matching_file_pairs:
            try:
                logger.info(f"Combining files: {consumption_file} and {cost_file}")
                output_file = self.combine_consumption_and_cost(consumption_file, cost_file)
                output_files.append(output_file)
            except Exception as error:
                logger.error(f"Error combining {consumption_file} and {cost_file}: {str(error)}")
        
        return output_files
    
    def batch_convert_to_jsonl(
        self, 
        file_patterns: List[str], 
        output_dir: Optional[Union[str, Path]] = None
    ) -> List[str]:
        import glob
        
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.output_dir
        
        output_files = []
        
        for pattern in file_patterns:
            for input_file in glob.glob(pattern):
                file_path = Path(input_file)
                output_file = output_path / f"{file_path.stem}.jsonl"
                
                try:
                    result = self.convert_to_jsonl(file_path, output_file)
                    output_files.append(result)
                except Exception as error:
                    logger.error(f"Error converting {input_file}: {str(error)}")
        
        return output_files
    
    def combine_all_resources_into_single_file(
        self,
        directory: Union[str, Path] = "data/glowmarkt_api_raw",
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        directory = Path(directory)
        matching_file_pairs = self.find_matching_resource_files(directory)
        
        if not matching_file_pairs:
            logger.warning(f"No matching consumption-cost pairs found in {directory}")
            return None
        
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
        
        entries_written = 0
        with open(output_file, 'w') as file_handle:
            for timestamp, reading in sorted(combined_readings_by_timestamp.items()):
                data_object = {**consolidated_metadata, **reading}
                file_handle.write(json.dumps(data_object) + '\n')
                entries_written += 1
        
        logger.info(f"Combined {entries_written} readings across {len(resource_metadata_by_type)} resource types into JSONL format at {output_file}")
        return str(output_file)