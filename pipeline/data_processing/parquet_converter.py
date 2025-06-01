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
    
    def convert_to_parquet(
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
        
        data_rows = []
        with open(jsonl_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        data_rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping invalid JSON line: {line[:50]}...")
        
        if not data_rows:
            logger.warning(f"No data found in {jsonl_path}")
            # Create empty parquet file
            pd.DataFrame().to_parquet(output_file)
            return str(output_file)
        
        df = pd.DataFrame(data_rows)
        
        # Optimize types for better compression
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_numeric(df['timestamp'])
        
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'])
        
        df.to_parquet(
            output_file,
            compression='snappy',
            index=False
        )
        
        logger.info(f"Converted {len(df)} records to Parquet format at {output_file}")
        logger.info(f"File size reduced from {jsonl_path.stat().st_size} bytes to {output_file.stat().st_size} bytes")
        
        return str(output_file)
    
    def convert_batch_to_parquet(
        self,
        file_patterns: List[str]
    ) -> List[str]:
        import glob
        
        output_files = []
        
        for pattern in file_patterns:
            for input_file in glob.glob(pattern):
                try:
                    result = self.convert_to_parquet(input_file)
                    output_files.append(result)
                except Exception as e:
                    logger.error(f"Error converting {input_file}: {str(e)}")
        
        return output_files