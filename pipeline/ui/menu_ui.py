#!/usr/bin/env python

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI

class MenuUI(BaseUI):
    
    def __init__(self, username=None, password=None, token=None):
        super().__init__()
        self.retrieval_ui = DataRetrievalUI()
        
        if username or password or token:
            self.retrieval_ui.client_type = 'glowmarkt'
            self.retrieval_ui.setup_glowmarkt_client(username, password, token)
    
    def display_welcome(self):
        print("\n" + "=" * 80)
        print("Energy Pipeline - Data Processing Tool".center(80))
        print("=" * 80)
        print("\nWelcome to the Energy Pipeline interactive client!")
        print("This tool helps you fetch and process energy consumption data.")
        print("\nWhat would you like to do?")
    
    def display_menu(self):
        self.display_welcome()
        
        while True:
            print("\nMain Menu:")
            print("1. Download energy data from Glowmarkt API")
            print("2. Process N3rgy CSV files")
            print("3. Combine existing resources into a single file")
            print("4. Convert combined file to Parquet")
            print("5. Run data analysis")
            print("6. Exit")
            
            choice = self.get_int_input("\nEnter choice (1-6): ", 1, 6)
            
            if choice == 1:
                if self.retrieval_ui.client_type != 'glowmarkt':
                    self.retrieval_ui.client_type = 'glowmarkt'
                    self.retrieval_ui.setup_glowmarkt_client()
                
                result = self.retrieval_ui.run()
                if result:
                    print("\nSuccess! Data downloaded successfully.")
                else:
                    print("\nOperation failed or was cancelled.")
                
                self.wait_for_user()
                
            elif choice == 2:
                if self.retrieval_ui.client_type != 'n3rgy':
                    self.retrieval_ui.client_type = 'n3rgy'
                    self.retrieval_ui.setup_n3rgy_client()
                
                if self.retrieval_ui.client:
                    print("\nProcessing N3rgy CSV files...")
                    json_files = self.retrieval_ui.client.process_all_files(extract_cost=True, combine_to_jsonl=True)
                    
                    if json_files:
                        print(f"\nSuccess! Processed {len(json_files)} files.")
                        
                        print("\nProcessed files:")
                        for file_path in json_files:
                            print(f"- {file_path}")
                    else:
                        print("\nNo files were processed or no CSV files found.")
                else:
                    print("\nN3rgy client setup failed.")
                
                self.wait_for_user()
                
            elif choice == 3:
                result = self.retrieval_ui.fetch_and_combine_resources()
                if result:
                    if isinstance(result, list):
                        print(f"\nSuccess! Created {len(result)} files.")
                    else:
                        print("\nSuccess! Resources combined successfully.")
                else:
                    print("\nOperation failed or was cancelled.")
                
                self.wait_for_user()
                
            elif choice == 4:
                from pipeline.ui.parquet_ui import ParquetUI
                parquet_ui = ParquetUI()
                result = parquet_ui.run()
                
                if result:
                    print("\nSuccess! Parquet file created successfully.")
                else:
                    print("\nOperation failed or was cancelled.")
                
                self.wait_for_user()
                
            elif choice == 5:
                from pipeline.ui.analysis_ui import AnalysisUI
                analysis_ui = AnalysisUI()
                analysis_ui.run()
                
                self.wait_for_user()
                
            elif choice == 6:
                print("\nExiting Energy Pipeline. Goodbye!")
                break
    
    def wait_for_user(self):
        input("\nPress Enter to continue...")
    
    def run(self):
        self.display_menu()