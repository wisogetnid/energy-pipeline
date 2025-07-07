#!/usr/bin/env python

import os
import json
import logging
from pathlib import Path
from typing import List, Optional, Union, Dict, Any

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JsonlToParquetConverter:
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/parquet")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def convert_jsonl_to_parquet_file(
        self, 
        jsonl_file: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None
    ) -> str:
        jsonl_path = Path(jsonl_file)
        
        if not jsonl_path.exists():
            raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")
        
        logger.info(f"Loading JSONL data from {jsonl_path}")
        
        if output_file is None:
            output_file = self.output_dir / f"{jsonl_path.stem}.parquet"
        else:
            output_file = Path(output_file)
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        jsonl_rows = []
        with open(jsonl_path, 'r') as jsonl_file_handle:
            for line in jsonl_file_handle:
                if line.strip():
                    try:
                        jsonl_rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping invalid JSON line: {line[:50]}...")
        
        if not jsonl_rows:
            logger.warning(f"No data found in {jsonl_path}")
            pd.DataFrame().to_parquet(output_file)
            return str(output_file)
        
        dataframe = pd.DataFrame(jsonl_rows)
        
        self.optimize_dataframe_numeric_types(dataframe)
        
        dataframe.to_parquet(
            output_file,
            compression='snappy',
            index=False
        )
        
        input_size = jsonl_path.stat().st_size
        output_size = output_file.stat().st_size
        logger.info(f"Converted {len(dataframe)} records to Parquet format at {output_file}")
        logger.info(f"File size reduced from {input_size} bytes to {output_size} bytes")
        
        return str(output_file)
    
    def optimize_dataframe_numeric_types(self, dataframe: pd.DataFrame) -> None:
        if 'timestamp' in dataframe.columns:
            dataframe['timestamp'] = pd.to_numeric(dataframe['timestamp'])
        
        if 'value' in dataframe.columns:
            dataframe['value'] = pd.to_numeric(dataframe['value'])
        
        if 'consumption_value' in dataframe.columns:
            dataframe['consumption_value'] = pd.to_numeric(dataframe['consumption_value'], errors='coerce')
        
        if 'cost_value' in dataframe.columns:
            dataframe['cost_value'] = pd.to_numeric(dataframe['cost_value'], errors='coerce')
        
        resource_types = ['electricity', 'gas', 'water']
        for resource_type in resource_types:
            consumption_column = f"{resource_type}_consumption"
            cost_column = f"{resource_type}_cost"
            
            if consumption_column in dataframe.columns:
                dataframe[consumption_column] = pd.to_numeric(dataframe[consumption_column], errors='coerce')
            
            if cost_column in dataframe.columns:
                dataframe[cost_column] = pd.to_numeric(dataframe[cost_column], errors='coerce')
    
    def convert_multiple_jsonl_files(self, file_patterns: List[str]) -> List[str]:
        import glob
        
        converted_files = []
        
        for pattern in file_patterns:
            for input_file in glob.glob(pattern):
                try:
                    result_file = self.convert_jsonl_to_parquet_file(input_file)
                    converted_files.append(result_file)
                except Exception as error:
                    logger.error(f"Error converting {input_file}: {str(error)}")
        
        return converted_files