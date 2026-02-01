from pathlib import Path

from pipeline.data_processing.yearly_jsonl_converter import YearlyEnergyDataConverter
from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.jsonl_converter import EnergyDataConverter
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter


class DataConverterUI(BaseUI):
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/processed")
        self.output_dir = Path("data/processed")

    def combine_all_resources(self, directory=None):
        try:
            energy_data_converter = EnergyDataConverter(output_dir=self.output_dir)
            target_data_directory = Path(directory) if directory else self.data_dir

            combined_jsonl_filepath = (
                energy_data_converter.combine_all_resources_into_single_file(
                    target_data_directory
                )
            )

            if not combined_jsonl_filepath:
                print(
                    f"\nNo matching resources found to combine in {target_data_directory}."
                )
                return None

            print(
                f"\nAll resources successfully combined into JSONL format: {combined_jsonl_filepath}"
            )
            print(
                "Each line contains data for all resource types (electricity, gas, etc.) for the same timestamp."
            )
            return combined_jsonl_filepath
        except Exception as resource_combination_error:
            print(f"Error combining resources: {str(resource_combination_error)}")
            return None

    def convert_to_yearly(self, directory=None):
        try:
            yearly_converter_instance = YearlyEnergyDataConverter(
                output_dir=self.output_dir
            )
            target_data_directory = Path(directory) if directory else self.data_dir
            matching_file_pairs = (
                yearly_converter_instance.find_matching_resource_files(
                    target_data_directory
                )
            )
            if not matching_file_pairs:
                print(f"\nNo matching resources found in {target_data_directory}.")
                return None

            converted_yearly_files = yearly_converter_instance.convert_to_yearly_jsonl(
                matching_file_pairs
            )
            print(
                f"\nSuccessfully converted data into {len(converted_yearly_files)} yearly JSONL files."
            )
            for file_path in converted_yearly_files:
                print(f" - {file_path}")
            return converted_yearly_files
        except Exception as yearly_conversion_error:
            print(
                f"Error converting data to yearly format: {str(yearly_conversion_error)}"
            )
            return None

    def run(self):
        self.print_header("Data Converter")

        converter_menu_options = {
            "1": "Combine all resources into a single JSONL file",
            "2": "Convert Monthly to Yearly (JSONL & Parquet)",
            "3": "Exit",
        }

        while True:
            user_choice = self.get_choice(converter_menu_options)

            if user_choice == "1":
                self.run_combination()
            elif user_choice == "2":
                self.run_yearly_conversion()
            elif user_choice == "3":
                break

    def run_combination(self):
        self.print_header("Combine All Resources")
        selected_directory = self.get_directory()
        if selected_directory:
            print(
                f"\nCombining ALL resources from {selected_directory} into a single file..."
            )
            self.combine_all_resources(selected_directory)

    def run_yearly_conversion(self):
        target_directory = self.data_dir
        available_jsonl_files = list(target_directory.glob("*.jsonl"))

        if not available_jsonl_files:
            print(f"\nNo JSONL files found in {target_directory}.")
            print(
                "You need to combine resources into monthly JSONL files first (Option 1)."
            )
            return None

        print(
            f"\nFound {len(available_jsonl_files)} JSONL files in {target_directory}."
        )
        print("Files to process:")
        for jsonl_file in available_jsonl_files:
            print(f" - {jsonl_file.name}")

        print(f"\nConverting data from {target_directory} to yearly JSONL and Parquet files...")

        yearly_converter = YearlyEnergyDataConverter(output_dir=self.output_dir)

        conversion_result_files = yearly_converter.convert_to_yearly_jsonl(
            available_jsonl_files
        )

        if conversion_result_files:
            print(
                "\nYearly conversion complete! The data is now available as yearly summaries."
            )
            print(
                "These files can be used for year-over-year comparisons and annual reporting."
            )

            total_years_processed = len(conversion_result_files)
            years_list = [
                Path(file_path).name.split("_")[0]
                for file_path in conversion_result_files
            ]

            print(
                f"\nSummary files created for {total_years_processed} years: {', '.join(years_list)}"
            )
            print(f"Files saved to: {self.output_dir}")

            print("\nConverting yearly JSONL files to Parquet format...")
            parquet_converter = JsonlToParquetConverter()
            successfully_converted_parquet_files = (
                parquet_converter.convert_multiple_jsonl_files(
                    conversion_result_files
                )
            )

            if successfully_converted_parquet_files:
                print(
                    f"\nSuccessfully converted {len(successfully_converted_parquet_files)} files to Parquet format:"
                )
                for parquet_file_path in successfully_converted_parquet_files:
                    print(f" - {parquet_file_path}")

        return conversion_result_files

    def get_directory(self):
        print("\nWhich directory would you like to process?")
        print("1. Default directory (data/processed)")
        print("2. Specify a different directory")

        directory_option_choice = self.get_int_input("\nEnter your choice: ", 1, 2)

        if directory_option_choice == 1:
            print(f"\nUsing default directory: {self.data_dir}")
            return self.data_dir
        else:
            custom_directory_path_input = input("\nEnter the directory path: ")
            resolved_custom_directory = Path(custom_directory_path_input)

            if (
                not resolved_custom_directory.exists()
                or not resolved_custom_directory.is_dir()
            ):
                print(f"\nError: {resolved_custom_directory} is not a valid directory.")
                return None
            return resolved_custom_directory
