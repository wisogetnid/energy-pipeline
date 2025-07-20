#!/usr/bin/env python

from pathlib import Path
from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

class ParquetUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.converter = JsonlToParquetConverter(output_dir="data/parquet")
    
    def run(self):
        self.print_header("Convert to Parquet")
        
        print("\nThis utility converts JSONL files to Parquet format.")
        print("Parquet files are more efficient for data analysis and visualization.")
        
        # Find all JSONL files in the processed directory
        processed_dir = Path("data/processed")
        jsonl_files = list(processed_dir.glob("*.jsonl"))
        
        if not jsonl_files:
            print("\nNo JSONL files found in data/processed directory.")
            return False
        
        print("\nAvailable JSONL files:")
        for i, file_path in enumerate(jsonl_files, 1):
            print(f"{i}. {file_path.name}")
        
        print(f"{len(jsonl_files) + 1}. All files")
        print(f"{len(jsonl_files) + 2}. Back")
        
        choice = self.get_int_input("\nSelect a file to convert (or 'all'): ", 1, len(jsonl_files) + 2)
        
        if choice == len(jsonl_files) + 2:
            return False
        
        if choice == len(jsonl_files) + 1:
            print("\nConverting all JSONL files to Parquet...")
            converted_files = self.converter.convert_multiple_jsonl_files([str(f) for f in jsonl_files])
            
            if converted_files:
                print(f"\nSuccessfully converted {len(converted_files)} files to Parquet format:")
                for file_path in converted_files:
                    print(f"- {file_path}")
                return converted_files
            else:
                print("\nNo files were successfully converted.")
                return False
        else:
            selected_file = jsonl_files[choice - 1]
            print(f"\nConverting {selected_file.name} to Parquet...")
            
            output_file = self.converter.convert_jsonl_to_parquet_file(str(selected_file))
            
            if output_file:
                print(f"\nSuccessfully converted to Parquet format: {output_file}")
                return [output_file]
            else:
                print("\nFailed to convert file.")
                return False