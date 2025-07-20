#!/usr/bin/env python

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JsonlToParquetConverter:
    
    def __init__(self, output_dir: Union[str, Path] = "data/parquet"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_jsonl_to_parquet_file(self, jsonl_file: Union[str, Path], output_file: Optional[Union[str, Path]] = None) -> Optional[str]:
        jsonl_path = Path(jsonl_file)
        
        if not jsonl_path.exists():
            logger.error(f"JSONL file not found: {jsonl_path}")
            raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")
        
        try:
            # Read JSONL file into a pandas DataFrame
            records = []
            with open(jsonl_path, 'r') as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line in {jsonl_path}: {e}")
                        continue
            
            if not records:
                logger.warning(f"No records found in {jsonl_path}")
                # Create empty DataFrame to handle empty files gracefully
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(records)
            
            # Create output file path
            if output_file is None:
                output_file = self.output_dir / f"{jsonl_path.stem}.parquet"
            else:
                output_file = Path(output_file)
            
            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert timestamp columns if needed
            if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
                try:
                    df['timestamp'] = pd.to_numeric(df['timestamp'])
                except:
                    pass
            
            # Write to Parquet
            df.to_parquet(output_file, index=False)
            
            logger.info(f"Converted {jsonl_path} to Parquet format at {output_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error converting {jsonl_path} to Parquet: {e}")
            return None
    
    def convert_multiple_jsonl_files(self, jsonl_files: List[Union[str, Path]]) -> List[str]:
        converted_files = []
        
        for jsonl_file in jsonl_files:
            try:
                output_file = self.convert_jsonl_to_parquet_file(jsonl_file)
                if output_file:
                    converted_files.append(output_file)
            except FileNotFoundError:
                logger.warning(f"Skipping non-existent file: {jsonl_file}")
            except Exception as e:
                logger.error(f"Error converting {jsonl_file}: {e}")
        
        return converted_files