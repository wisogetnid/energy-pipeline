#!/usr/bin/env python

from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.jsonl_converter import EnergyDataConverter

class DataConverterUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/glowmarkt_api_raw")
        self.output_dir = Path("data/processed")
    
    def combine_all_resources(self, directory=None):
        try:
            converter = EnergyDataConverter(output_dir=self.output_dir)
            
            # Use the provided directory or the default one
            data_dir = Path(directory) if directory else self.data_dir
            
            combined_filepath = converter.combine_all_resources_into_single_file(data_dir)
            
            if not combined_filepath:
                print(f"\nNo matching resources found to combine in {data_dir}.")
                return None
                
            print(f"\nAll resources successfully combined into JSONL format: {combined_filepath}")
            print("Each line contains data for all resource types (electricity, gas, etc.) for the same timestamp.")
            return combined_filepath
        except Exception as e:
            print(f"Error combining resources: {str(e)}")
            return None
    
    def run(self):
        self.print_header("Combine All Resources")
        
        # Ask for which directory to process
        print("\nWhich directory would you like to process?")
        print("1. Default directory (data/glowmarkt_api_raw)")
        print("2. Specify a different directory")
        
        choice = self.get_int_input("\nEnter your choice: ", 1, 2)
        
        if choice == 1:
            print(f"\nUsing default directory: {self.data_dir}")
            directory = self.data_dir
        else:
            directory_input = input("\nEnter the directory path: ")
            directory = Path(directory_input)
            
            if not directory.exists() or not directory.is_dir():
                print(f"\nError: {directory} is not a valid directory.")
                return None
        
        print(f"\nCombining ALL resources from {directory} into a single file...")
        return self.combine_all_resources(directory)