#!/usr/bin/env python

import os
import glob
from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.jsonl_converter import EnergyDataConverter

class DataConverterUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/glowmarkt_api_raw")
        self.output_dir = Path("data/processed")
    
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
    
    def combine_resource_files(self, consumption_file, cost_file):
        try:
            converter = EnergyDataConverter(output_dir=self.output_dir)
            
            combined_filepath = converter.combine_resource_files(consumption_file, cost_file)
            
            print(f"\nFiles successfully combined into JSONL format: {combined_filepath}")
            print("Each line contains both consumption and cost data for the same timestamp.")
            return combined_filepath
        except Exception as e:
            print(f"Error combining files: {str(e)}")
            return None
    
    def find_matching_pairs(self):
        converter = EnergyDataConverter()
        return converter.find_matching_resource_files(self.data_dir)
    
    def batch_convert_existing_files(self):
        self.print_header("Convert Data to JSONL")
        
        if not self.data_dir.exists():
            print(f"Error: Directory '{self.data_dir}' does not exist.")
            return False
        
        json_files = list(self.data_dir.glob("*.json"))
        
        if not json_files:
            print(f"No JSON files found in '{self.data_dir}'.")
            return False
        
        matching_pairs = self.find_matching_pairs()
        
        print("\nOptions:")
        print("1. Convert a single file")
        print("2. Convert all files individually")
        print("3. Combine matching consumption and cost files")
        print("4. Go back")
        
        choice = self.get_int_input("\nEnter your choice: ", 1, 4)
        
        if choice == 1:
            print("\nAvailable JSON files:")
            for i, file_path in enumerate(json_files, 1):
                file_name = file_path.name
                print(f"{i}. {file_name}")
                
            file_num = self.get_int_input("\nEnter file number to convert: ", 1, len(json_files))
            selected_file = json_files[file_num - 1]
            print(f"\nConverting {selected_file.name}...")
            return self.convert_to_jsonl(selected_file)
                
        elif choice == 2:
            print("\nConverting all files individually...")
            
            converter = EnergyDataConverter(output_dir=self.output_dir)
            
            converted_files = converter.convert_batch_to_jsonl([str(f) for f in json_files])
            
            print(f"\nConverted {len(converted_files)} files to JSONL format.")
            return converted_files[0] if converted_files else None
            
        elif choice == 3:
            if not matching_pairs:
                print("\nNo matching consumption and cost pairs found.")
                print("Please ensure you have both consumption and cost data for the same resource and date range.")
                return False
            
            print("\nAvailable resource pairs:")
            for i, (consumption_file, cost_file) in enumerate(matching_pairs, 1):
                cons_name = Path(consumption_file).name
                cost_name = Path(cost_file).name
                print(f"{i}. {cons_name} + {cost_name}")
            
            print("\nOptions:")
            print("1. Combine a specific pair")
            print("2. Combine all matching pairs")
            print("3. Go back")
            
            subchoice = self.get_int_input("\nEnter your choice: ", 1, 3)
            
            if subchoice == 1:
                pair_num = self.get_int_input("Enter pair number to combine: ", 1, len(matching_pairs))
                selected_pair = matching_pairs[pair_num - 1]
                print(f"\nCombining {Path(selected_pair[0]).name} and {Path(selected_pair[1]).name}...")
                return self.combine_resource_files(selected_pair[0], selected_pair[1])
                
            elif subchoice == 2:
                print("\nCombining all matching pairs...")
                
                converter = EnergyDataConverter(output_dir=self.output_dir)
                
                combined_files = converter.combine_batch_resources(self.data_dir)
                
                print(f"\nCombined {len(combined_files)} pairs into JSONL format.")
                return combined_files[0] if combined_files else None
                
            elif subchoice == 3:
                return self.batch_convert_existing_files()
            
        elif choice == 4:
            return False
        
        return False
    
    def run(self):
        return self.batch_convert_existing_files()
    
    def get_int_input(self, prompt, min_value, max_value):
        while True:
            try:
                value = int(input(prompt))
                if min_value <= value <= max_value:
                    return value
                print(f"Please enter a number between {min_value} and {max_value}")
            except ValueError:
                print("Please enter a valid number")