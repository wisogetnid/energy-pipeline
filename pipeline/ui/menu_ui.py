#!/usr/bin/env python

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.ui.data_converter_ui import DataConverterUI
from pipeline.ui.parquet_converter_ui import ParquetConverterUI

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
        
        if username or token:
            self.retrieval_ui.setup_client(username, password, token)
    
    def show_pipeline_status(self):
        print("\n--- Pipeline Status ---")
        if self.last_retrieved_file:
            print(f"Last retrieved file: {self.last_retrieved_file}")
            print("Ready for conversion to JSONL")
        else:
            print("No data retrieved yet")
            
        if self.last_jsonl_file:
            print(f"Last JSONL file: {self.last_jsonl_file}")
            print("Ready for conversion to Parquet")
        print("----------------------")
    
    def run(self):
        try:
            while True:
                self.print_header("Energy Data Pipeline")
                self.show_pipeline_status()
                
                print("\nWhat would you like to do?")
                print("1. Retrieve new data from Glowmarkt API")
                print("2. Convert JSON data to JSONL format")
                print("3. Convert JSONL data to Parquet format")
                print("4. Run complete pipeline (retrieve → JSONL → Parquet)")
                print("5. Exit")
                
                menu_choice = self.get_int_input("\nEnter your choice: ", 1, 5)
                
                if menu_choice == 1:
                    # Retrieve data only
                    self.last_retrieved_file = self.retrieval_ui.run()
                    
                    if self.last_retrieved_file:
                        print(f"\nData successfully retrieved to: {self.last_retrieved_file}")
                        print("You can now convert this data using option 2")
                    
                elif menu_choice == 2:
                    # Convert data to JSONL
                    if self.last_retrieved_file:
                        print(f"\nUsing last retrieved file: {self.last_retrieved_file}")
                        if self.get_yes_no_input("Would you like to convert this file?"):
                            self.last_jsonl_file = self.converter_ui.convert_to_jsonl(self.last_retrieved_file)
                        else:
                            self.converter_ui.run()
                    else:
                        print("\nNo recently retrieved file available.")
                        self.converter_ui.run()
                    
                elif menu_choice == 3:
                    # Convert JSONL to Parquet
                    if self.last_jsonl_file:
                        print(f"\nUsing last JSONL file: {self.last_jsonl_file}")
                        if self.get_yes_no_input("Would you like to convert this file to Parquet?"):
                            self.parquet_ui.convert_to_parquet(self.last_jsonl_file)
                        else:
                            self.parquet_ui.run()
                    else:
                        print("\nNo recently created JSONL file available.")
                        self.parquet_ui.run()
                    
                elif menu_choice == 4:
                    # Run complete pipeline
                    print("\nRunning complete pipeline: retrieve → JSONL → Parquet")
                    
                    self.last_retrieved_file = self.retrieval_ui.run()
                    
                    if self.last_retrieved_file:
                        print(f"\nData successfully retrieved to: {self.last_retrieved_file}")
                        print("Now converting to JSONL format...")
                        
                        self.last_jsonl_file = self.converter_ui.convert_to_jsonl(self.last_retrieved_file)
                        
                        if self.last_jsonl_file:
                            print("\nNow converting to Parquet format...")
                            parquet_filepath = self.parquet_ui.convert_to_parquet(self.last_jsonl_file)
                            
                            if parquet_filepath:
                                print("\nPipeline completed successfully!")
                                print(f"Original data: {self.last_retrieved_file}")
                                print(f"JSONL data: {self.last_jsonl_file}")
                                print(f"Parquet data: {parquet_filepath}")
                        else:
                            print("\nJSONL conversion failed. Pipeline could not complete.")
                    else:
                        print("\nData retrieval failed. Pipeline could not complete.")
                    
                elif menu_choice == 5:
                    print("\nThank you for using the Energy Data Pipeline!")
                    break
                
                input("\nPress Enter to continue...")
            
        except Exception as e:
            print(f"Error in pipeline: {str(e)}")