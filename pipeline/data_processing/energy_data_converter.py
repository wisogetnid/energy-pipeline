"""Module for converting energy data from API format to JSONL format."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnergyDataConverter:
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/processed")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def convert_to_jsonl(
        self, 
        data: Union[Dict[str, Any], str, Path], 
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        if isinstance(data, (str, Path)):
            data_path = Path(data)
            logger.info(f"Loading data from {data_path}")
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
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            safe_name = resource_name.lower().replace(" ", "_").replace(".", "_")
            output_file = self.output_dir / f"{safe_name}_{timestamp}.jsonl"
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