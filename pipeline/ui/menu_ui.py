#!/usr/bin/env python

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.ui.data_converter_ui import DataConverterUI
from pipeline.ui.parquet_converter_ui import ParquetConverterUI
from pathlib import Path

class MenuUI(BaseUI):
    
    def __init__(self, username=None, password=None, token=None):
        super().__init__()
        self.username = username
        self.password = password
        self.token = token
        self.retrieval_ui = DataRetrievalUI()
        self.converter_ui = DataConverterUI()
        self.parquet_ui = ParquetConverterUI()
        self.last_retrieved_file = None
        self.last_jsonl_file = None
        self.last_parquet_file = None
        
        if username or token:
            self.retrieval_ui.setup_client(username, password, token)
    
    def show_pipeline_status(self):
        print("\n--- Pipeline Status ---")
        if self.last_parquet_file:
            print(f"Combined resources file: {self.last_parquet_file}")
            print("All data has been successfully combined into a single file.")
        else:
            print("No combined file created yet.")
        print("----------------------")
    
    def run(self):
        try:
            while True:
                self.print_header("Energy Data Pipeline")
                self.show_pipeline_status()
                
                print("\nWhat would you like to do?")
                print("1. Fetch and combine all resources into one file")
                print("2. Exit")
                
                menu_choice = self.get_int_input("\nEnter your choice: ", 1, 2)
                
                if menu_choice == 1:
                    print("\nFetching and combining all resources...")
                    retrieval_result = self.retrieval_ui.fetch_and_combine_resources()
                    
                    if isinstance(retrieval_result, list) and len(retrieval_result) > 0:
                        print("\nResource processing complete.")
                        print(f"Combined Parquet file: {retrieval_result[0]}")
                        print(f"Combined JSONL file: {retrieval_result[1]}")
                        print(f"Individual resource files: {len(retrieval_result) - 2}")
                        
                        # Store the combined parquet file as the last processed file
                        self.last_parquet_file = retrieval_result[0]
                        self.last_jsonl_file = retrieval_result[1]
                        self.last_retrieved_file = retrieval_result[2] if len(retrieval_result) > 2 else None
                    
                    elif retrieval_result:
                        print("\nResource retrieval completed, but combination failed.")
                        print("Individual resource files are available.")
                    else:
                        print("\nFailed to retrieve resources.")
                
                elif menu_choice == 2:
                    print("\nThank you for using the Energy Data Pipeline!")
                    break
                
                input("\nPress Enter to continue...")
            
        except Exception as e:
            print(f"Error in pipeline: {str(e)}")