#!/usr/bin/env python

import os
import glob

from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

class ParquetConverterUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.jsonl_dir = os.path.join("data", "processed")
        self.output_dir = os.path.join("data", "parquet")
    
    def convert_to_parquet(self, jsonl_filepath):
        try:
            converter = JsonlToParquetConverter(output_dir=self.output_dir)
            
            parquet_filepath = converter.convert_to_parquet(jsonl_filepath)
            
            print(f"\nData successfully converted to Parquet format: {parquet_filepath}")
            return parquet_filepath
        except Exception as e:
            print(f"Error converting to Parquet: {str(e)}")
            return None
    
    def batch_convert_existing_files(self):
        self.print_header("Convert JSONL Files to Parquet")
        
        if not os.path.exists(self.jsonl_dir):
            print(f"Error: Directory '{self.jsonl_dir}' does not exist.")
            return False
        
        jsonl_files = glob.glob(os.path.join(self.jsonl_dir, "*.jsonl"))
        
        if not jsonl_files:
            print(f"No JSONL files found in '{self.jsonl_dir}'.")
            return False
        
        print("\nAvailable JSONL files:")
        for i, file_path in enumerate(jsonl_files, 1):
            file_name = os.path.basename(file_path)
            print(f"{i}. {file_name}")
        
        print("\nOptions:")
        print("1. Convert a single file")
        print("2. Convert all files")
        print("3. Go back")
        
        choice = input("\nEnter your choice: ")
        
        if choice == "1":
            while True:
                try:
                    file_num = int(input("\nEnter file number to convert: "))
                    if 1 <= file_num <= len(jsonl_files):
                        selected_file = jsonl_files[file_num - 1]
                        print(f"\nConverting {os.path.basename(selected_file)}...")
                        self.convert_to_parquet(selected_file)
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(jsonl_files)}")
                except ValueError:
                    print("Please enter a valid number")
                
        elif choice == "2":
            print("\nConverting all JSONL files to Parquet...")
            
            converter = JsonlToParquetConverter(output_dir=self.output_dir)
            
            converted_files = converter.convert_batch_to_parquet(jsonl_files)
            
            print(f"\nConverted {len(converted_files)} files to Parquet format.")
            
        elif choice == "3":
            return False
        else:
            print("Invalid choice. Going back to main menu.")
            return False
        
        return True
    
    def run(self):
        return self.batch_convert_existing_files()