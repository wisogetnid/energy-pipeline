"""Module for converting energy data from API format to JSONL format."""

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
    
    def _load_data_from_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        path = Path(file_path)
        logger.info(f"Loading data from {path}")
        with open(path, 'r') as f:
            return json.load(f)
    
    def _extract_resource_type(self, resource_name: str) -> str:
        if not resource_name:
            return "energy"
        
        words = resource_name.lower().split()
        if words and words[0] in ["electricity", "gas", "water"]:
            return words[0]
        return "energy"
    
    def _combine_consumption_and_cost(self, consumption_file: Union[str, Path], cost_file: Union[str, Path]) -> Tuple[Dict[int, Dict], Dict]:
        consumption_data = self._load_data_from_file(consumption_file)
        cost_data = self._load_data_from_file(cost_file)
        
        resource_name = consumption_data.get("name", consumption_data.get("resource_name", "energy consumption"))
        resource_type = self._extract_resource_type(resource_name)
        
        combined_metadata = {
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
        
        combined_readings = {}
        
        # Process consumption readings
        for reading in consumption_readings:
            if isinstance(reading, list) and len(reading) >= 2:
                timestamp, value = reading[0], reading[1]
                
                iso_timestamp = None
                if isinstance(timestamp, (int, float)):
                    try:
                        ts = timestamp / 1000 if timestamp > 9999999999 else timestamp
                        dt = datetime.fromtimestamp(ts)
                        iso_timestamp = dt.isoformat()
                    except (ValueError, TypeError, OverflowError):
                        iso_timestamp = str(timestamp)
                else:
                    iso_timestamp = str(timestamp)
                
                combined_readings[timestamp] = {
                    "timestamp": timestamp,
                    "timestamp_iso": iso_timestamp,
                    "consumption_value": value,
                    "cost_value": None
                }
        
        # Process cost readings and merge with consumption
        for reading in cost_readings:
            if isinstance(reading, list) and len(reading) >= 2:
                timestamp, value = reading[0], reading[1]
                
                if timestamp in combined_readings:
                    combined_readings[timestamp]["cost_value"] = value
                else:
                    iso_timestamp = None
                    if isinstance(timestamp, (int, float)):
                        try:
                            ts = timestamp / 1000 if timestamp > 9999999999 else timestamp
                            dt = datetime.fromtimestamp(ts)
                            iso_timestamp = dt.isoformat()
                        except (ValueError, TypeError, OverflowError):
                            iso_timestamp = str(timestamp)
                    else:
                        iso_timestamp = str(timestamp)
                    
                    combined_readings[timestamp] = {
                        "timestamp": timestamp,
                        "timestamp_iso": iso_timestamp,
                        "consumption_value": None,
                        "cost_value": value
                    }
        
        return combined_readings, combined_metadata
    
    def convert_to_jsonl(
        self, 
        data: Union[Dict[str, Any], str, Path], 
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        original_filename = None
        
        if isinstance(data, (str, Path)):
            data_path = Path(data)
            logger.info(f"Loading data from {data_path}")
            original_filename = data_path.stem
            with open(data_path, 'r') as f:
                data = json.load(f)
        
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
        
        count = 0
        
        with open(output_file, 'w') as f:
            for reading in readings:
                if isinstance(reading, list) and len(reading) >= 2:
                    timestamp, value = reading[0], reading[1]
                    
                    iso_timestamp = None
                    if isinstance(timestamp, (int, float)):
                        try:
                            ts = timestamp / 1000 if timestamp > 9999999999 else timestamp
                            dt = datetime.fromtimestamp(ts)
                            iso_timestamp = dt.isoformat()
                        except (ValueError, TypeError, OverflowError):
                            iso_timestamp = str(timestamp)
                    else:
                        iso_timestamp = str(timestamp)
                    
                    data_obj = {
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
                    
                    f.write(json.dumps(data_obj) + '\n')
                    count += 1
        
        logger.info(f"Converted {count} readings to JSONL format at {output_file}")
        return str(output_file)
    
    def combine_resource_files(
        self,
        consumption_file: Union[str, Path],
        cost_file: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        combined_readings, metadata = self._combine_consumption_and_cost(consumption_file, cost_file)
        
        resource_type = metadata["resource_type"]
        from_date = metadata["from_date"]
        to_date = metadata["to_date"]
        
        if output_file is None:
            start_date_str = from_date.split("T")[0].replace("-", "") if isinstance(from_date, str) else "unknown"
            end_date_str = to_date.split("T")[0].replace("-", "") if isinstance(to_date, str) else "unknown"
            filename = f"{resource_type}_combined_{start_date_str}_to_{end_date_str}.jsonl"
            output_file = self.output_dir / filename
        else:
            output_file = Path(output_file)
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        count = 0
        
        with open(output_file, 'w') as f:
            for timestamp, reading in sorted(combined_readings.items()):
                data_obj = {
                    "resource_type": resource_type,
                    "consumption_id": metadata["consumption_id"],
                    "consumption_name": metadata["consumption_name"],
                    "consumption_classifier": metadata["consumption_classifier"],
                    "consumption_unit": metadata["consumption_unit"],
                    "cost_id": metadata["cost_id"],
                    "cost_name": metadata["cost_name"],
                    "cost_classifier": metadata["cost_classifier"],
                    "cost_unit": metadata["cost_unit"],
                    "period": metadata["period"],
                    "from_date": metadata["from_date"],
                    "to_date": metadata["to_date"],
                    "timestamp": reading["timestamp"],
                    "timestamp_iso": reading["timestamp_iso"],
                    "consumption_value": reading["consumption_value"],
                    "cost_value": reading["cost_value"]
                }
                
                f.write(json.dumps(data_obj) + '\n')
                count += 1
        
        logger.info(f"Combined {count} readings into JSONL format at {output_file}")
        return str(output_file)
    
    def find_matching_resource_files(self, directory: Union[str, Path] = "data/glowmarkt_api_raw") -> List[Tuple[str, str]]:
        directory = Path(directory)
        all_files = list(directory.glob("*.json"))
        
        # Group files by resource type and date range
        consumption_files = {}
        cost_files = {}
        
        for file_path in all_files:
            filename = file_path.name
            
            # Extract resource type (electricity, gas, etc.)
            if "electricity" in filename.lower():
                resource_type = "electricity"
            elif "gas" in filename.lower():
                resource_type = "gas"
            elif "water" in filename.lower():
                resource_type = "water"
            else:
                resource_type = "unknown"
            
            # Extract date range
            parts = filename.split('_')
            if len(parts) >= 3:
                date_range = '_'.join(parts[2:]).replace('.json', '')
                
                key = f"{resource_type}_{date_range}"
                
                # Categorize as consumption or cost
                if "consumption" in filename.lower():
                    consumption_files[key] = str(file_path)
                elif "cost" in filename.lower():
                    cost_files[key] = str(file_path)
        
        # Find consumption and cost pairs
        matched_pairs = []
        
        for key in consumption_files:
            if key in cost_files:
                matched_pairs.append((consumption_files[key], cost_files[key]))
        
        return matched_pairs
    
    def combine_batch_resources(
        self,
        directory: Union[str, Path] = "data/glowmarkt_api_raw",
        output_dir: Optional[Union[str, Path]] = None
    ) -> List[str]:
        matched_pairs = self.find_matching_resource_files(directory)
        
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.output_dir
        
        output_files = []
        
        for consumption_file, cost_file in matched_pairs:
            try:
                logger.info(f"Combining files: {consumption_file} and {cost_file}")
                output_file = self.combine_resource_files(consumption_file, cost_file)
                output_files.append(output_file)
            except Exception as e:
                logger.error(f"Error combining {consumption_file} and {cost_file}: {str(e)}")
        
        return output_files
    
    def convert_batch_to_jsonl(
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
                except Exception as e:
                    logger.error(f"Error converting {input_file}: {str(e)}")
        
        return output_files