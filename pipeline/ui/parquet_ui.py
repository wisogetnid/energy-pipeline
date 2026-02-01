from pathlib import Path
from pipeline.ui.base_ui import BaseUI
from pipeline.data_processing.parquet_converter import JsonlToParquetConverter


class ParquetUI(BaseUI):
    def __init__(self):
        super().__init__()
        self.parquet_converter = JsonlToParquetConverter(output_dir="data/parquet")

    def run(self):
        self.print_header("Convert to Parquet")

        print("\nThis utility converts JSONL files to Parquet format.")
        print("Parquet files are more efficient for data analysis and visualization.")

        source_processed_directory = Path("data/processed")
        available_jsonl_files_list = list(source_processed_directory.glob("*.jsonl"))

        if not available_jsonl_files_list:
            print("\nNo JSONL files found in data/processed directory.")
            return False

        print("\nAvailable JSONL files:")
        for index, jsonl_file_path in enumerate(available_jsonl_files_list, 1):
            print(f"{index}. {jsonl_file_path.name}")

        total_files_count = len(available_jsonl_files_list)
        all_files_option_index = total_files_count + 1
        back_option_index = total_files_count + 2

        print(f"{all_files_option_index}. All files")
        print(f"{back_option_index}. Back")

        user_selection_index = self.get_int_input(
            "\nSelect a file to convert (or 'all'): ", 1, back_option_index
        )

        if user_selection_index == back_option_index:
            return False

        if user_selection_index == all_files_option_index:
            print("\nConverting all JSONL files to Parquet...")
            converted_parquet_files_paths = (
                self.parquet_converter.convert_multiple_jsonl_files(
                    [str(jsonl_file) for jsonl_file in available_jsonl_files_list]
                )
            )

            if converted_parquet_files_paths:
                print(
                    f"\nSuccessfully converted {len(converted_parquet_files_paths)} files to Parquet format:"
                )
                for converted_file_path in converted_parquet_files_paths:
                    print(f"- {converted_file_path}")
                return converted_parquet_files_paths
            else:
                print("\nNo files were successfully converted.")
                return False
        else:
            selected_jsonl_file_path = available_jsonl_files_list[
                user_selection_index - 1
            ]
            print(f"\nConverting {selected_jsonl_file_path.name} to Parquet...")

            resulting_parquet_file_path = (
                self.parquet_converter.convert_jsonl_to_parquet_file(
                    str(selected_jsonl_file_path)
                )
            )

            if resulting_parquet_file_path:
                print(
                    f"\nSuccessfully converted to Parquet format: {resulting_parquet_file_path}"
                )
                return [resulting_parquet_file_path]
            else:
                print("\nFailed to convert file.")
                return False
