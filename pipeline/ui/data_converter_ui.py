#!/usr/bin/env python

import os
import glob

from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.jsonl_converter import EnergyDataConverter

class DataConverterUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.data_dir = os.path.join("data", "glowmarkt_api_raw")
        self.output_dir = os.path.join("data", "processed")
    
    def convert_to_jsonl(self, json_filepath):
        try:
            converter = EnergyDataConverter(output_dir=self.output_dir)
            
            jsonl_filepath = converter.convert_to_jsonl(json_filepath)
            
            print(f"\nData successfully converted to JSONL format: {jsonl_filepath}")
            print("Each reading is now a separate JSON line with complete metadata.")
            return jsonl_filepath
        except Exception as e:
            print(f"Error converting to JSONL: {str(e)}")
            return None
    
    def batch_convert_existing_files(self):
        self.print_header("Convert Existing JSON Files to JSONL")
        
        if not os.path.exists(self.data_dir):
            print(f"Error: Directory '{self.data_dir}' does not exist.")
            return False
        
        json_files = glob.glob(os.path.join(self.data_dir, "*.json"))
        
        if not json_files:
            print(f"No JSON files found in '{self.data_dir}'.")
            return False
        
        print("\nAvailable JSON files:")
        for i, file_path in enumerate(json_files, 1):
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
                    if 1 <= file_num <= len(json_files):
                        selected_file = json_files[file_num - 1]
                        print(f"\nConverting {os.path.basename(selected_file)}...")
                        self.convert_to_jsonl(selected_file)
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(json_files)}")
                except ValueError:
                    print("Please enter a valid number")
                
        elif choice == "2":
            print("\nConverting all files...")
            
            converter = EnergyDataConverter(output_dir=self.output_dir)
            
            converted_files = converter.convert_batch_to_jsonl(json_files)
            
            print(f"\nConverted {len(converted_files)} files to JSONL format.")
            
        elif choice == "3":
            return False
        else:
            print("Invalid choice. Going back to main menu.")
            return False
        
        return True
    
    def run(self):
        return self.batch_convert_existing_files()