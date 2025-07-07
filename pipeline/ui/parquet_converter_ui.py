#!/usr/bin/env python

from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

class ParquetConverterUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.jsonl_dir = Path("data/processed")
        self.output_dir = Path("data/parquet")
    
    def convert_to_parquet(self, jsonl_filepath):
        try:
            converter = JsonlToParquetConverter(output_dir=str(self.output_dir))
            
            parquet_filepath = converter.convert_jsonl_to_parquet_file(jsonl_filepath)
            
            print(f"\nData successfully converted to Parquet format: {parquet_filepath}")
            self._display_parquet_info(parquet_filepath)
            return parquet_filepath
        except Exception as e:
            print(f"Error converting to Parquet: {str(e)}")
            return None
    
    def _format_size(self, size_bytes):
        """Format file size in a human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024 or unit == 'GB':
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
    
    def _display_parquet_info(self, parquet_filepath):
        try:
            import pandas as pd
            
            df = pd.read_parquet(parquet_filepath)
            
            print(f"\nParquet file information:")
            print(f"- Records: {len(df)}")
            print(f"- Columns: {len(df.columns)}")
            
            file_size = Path(parquet_filepath).stat().st_size
            print(f"- File size: {self._format_size(file_size)}")
            
            # Detect file type - we're only interested in multi-resource files now
            resources = set()
            for col in df.columns:
                for resource in ['electricity', 'gas', 'water']:
                    if col.startswith(f"{resource}_"):
                        resources.add(resource)
            
            if resources:
                print(f"- Resource types: {', '.join(resources)}")
                
                # Count non-null readings for each resource
                for resource in resources:
                    consumption_col = f"{resource}_consumption"
                    cost_col = f"{resource}_cost"
                    
                    if consumption_col in df.columns:
                        consumption_count = df[consumption_col].count()
                        print(f"- {resource.capitalize()} consumption readings: {consumption_count}")
                    
                    if cost_col in df.columns:
                        cost_count = df[cost_col].count()
                        print(f"- {resource.capitalize()} cost readings: {cost_count}")
            
        except Exception as e:
            print(f"Could not analyze parquet file: {str(e)}")
    
    def run(self):
        self.print_header("Convert Combined File to Parquet")
        
        # Look for the most recent combined file
        combined_files = list(self.jsonl_dir.glob("all_resources_*.jsonl"))
        
        if not combined_files:
            print("No combined resource files found.")
            return None
        
        # Sort by modification time, newest first
        combined_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        latest_file = combined_files[0]
        
        print(f"Converting most recent combined file: {latest_file.name}")
        return self.convert_to_parquet(latest_file)