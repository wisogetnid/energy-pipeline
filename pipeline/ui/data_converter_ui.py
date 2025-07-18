#!/usr/bin/env python

from pathlib import Path

from pipeline.data_processing.yearly_jsonl_converter import YearlyEnergyDataConverter  # Updated import
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

    def convert_to_yearly(self, directory=None):  # Updated method name
        try:
            converter = YearlyEnergyDataConverter(output_dir=self.output_dir)  # Updated class name
            data_dir = Path(directory) if directory else self.data_dir
            file_pairs = converter.find_matching_resource_files(data_dir)
            if not file_pairs:
                print(f"\nNo matching resources found in {data_dir}.")
                return None

            output_files = converter.convert_to_yearly_jsonl(file_pairs)
            print(f"\nSuccessfully converted data into {len(output_files)} yearly JSONL files.")
            for file in output_files:
                print(f" - {file}")
            return output_files
        except Exception as e:
            print(f"Error converting data to yearly format: {str(e)}")  # Updated error message
            return None

    def run(self):
        self.print_header("Data Converter")

        menu_options = {
            "1": "Combine all resources into a single JSONL file",
            "2": "Convert data to yearly JSONL files",  # Updated menu option
            "3": "Exit"
        }

        while True:
            choice = self.get_choice(menu_options)

            if choice == "1":
                self.run_combination()
            elif choice == "2":
                self.run_yearly_conversion()  # Updated method name
            elif choice == "3":
                break

    def run_combination(self):
        self.print_header("Combine All Resources")
        directory = self.get_directory()
        if directory:
            print(f"\nCombining ALL resources from {directory} into a single file...")
            self.combine_all_resources(directory)

    def run_yearly_conversion(self):
        self.print_header("Convert to Yearly JSONL")
        
        directory = self.get_directory()
        if not directory:
            return None
            
        print(f"\nConverting data from {directory} to yearly JSONL files...")
        result = self.convert_to_yearly(directory)
        
        if result:
            print("\nYearly conversion complete! The data is now available as yearly summaries.")
            print("These files can be used for year-over-year comparisons and annual reporting.")
            
            # Calculate some basic stats for the user
            num_years = len(result)
            years_covered = [Path(file).name.split('_')[0] for file in result]
            
            print(f"\nSummary files created for {num_years} years: {', '.join(years_covered)}")
            print(f"Files saved to: {self.output_dir}")
        
        return result

    def get_directory(self):
        print("\nWhich directory would you like to process?")
        print("1. Default directory (data/glowmarkt_api_raw)")
        print("2. Specify a different directory")

        choice = self.get_int_input("\nEnter your choice: ", 1, 2)

        if choice == 1:
            print(f"\nUsing default directory: {self.data_dir}")
            return self.data_dir
        else:
            directory_input = input("\nEnter the directory path: ")
            directory = Path(directory_input)

            if not directory.exists() or not directory.is_dir():
                print(f"\nError: {directory} is not a valid directory.")
                return None
            return directory