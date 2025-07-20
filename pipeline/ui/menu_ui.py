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
    
    def display_main_menu(self):
        self.display_welcome()
        
        while True:
            print("\nMain Menu:")
            print("1. Retrieve Data")
            print("2. Convert Data")
            print("3. Visualise Data")
            print("4. Exit")
            
            choice = self.get_int_input("\nEnter choice (1-4): ", 1, 4)
            
            if choice == 1:
                self.retrieve_data_menu()
            elif choice == 2:
                self.convert_data_menu()
            elif choice == 3:
                self.visualisation_menu()
            elif choice == 4:
                print("\nExiting Energy Pipeline. Goodbye!")
                break
    
    def retrieve_data_menu(self):
        self.print_header("Retrieve Data")
        
        print("1. Use Glowmarkt Client (online API)")
        print("2. Use N3rgy CSV Converter (local files)")
        print("3. Back to Main Menu")
        
        choice = self.get_int_input("\nEnter choice (1-3): ", 1, 3)
        
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
    
    def convert_data_menu(self):
        self.print_header("Convert Data")
        
        print("1. Combine Raw .json Resources to Monthly .jsonl Files")
        print("2. Combine Monthly .jsonl Files into Yearly .jsonl Files")
        print("3. Convert Processed .jsonl Files into .parquet Files")
        print("4. Back to Main Menu")
        
        choice = self.get_int_input("\nEnter choice (1-4): ", 1, 4)
        
        if choice == 1:
            self.combine_to_monthly_files()
        elif choice == 2:
            self.combine_to_yearly_files()
        elif choice == 3:
            self.convert_to_parquet()
    
    def combine_to_monthly_files(self):
        result = self.retrieval_ui.fetch_and_combine_resources()
        if result:
            if isinstance(result, list):
                print(f"\nSuccess! Created {len(result)} files.")
            else:
                print("\nSuccess! Resources combined successfully.")
        else:
            print("\nOperation failed or was cancelled.")
        
        self.wait_for_user()
    
    def combine_to_yearly_files(self):
        from pipeline.ui.data_converter_ui import DataConverterUI
        converter_ui = DataConverterUI()
        result = converter_ui.run_yearly_conversion()
        
        if result:
            print("\nSuccess! Created yearly summary files.")
        else:
            print("\nOperation failed or was cancelled.")
        
        self.wait_for_user()
    
    def convert_to_parquet(self):
        from pipeline.ui.parquet_ui import ParquetUI
        parquet_ui = ParquetUI()
        result = parquet_ui.run()
        
        if result:
            print("\nSuccess! Parquet file(s) created successfully.")
        else:
            print("\nOperation failed or was cancelled.")
        
        self.wait_for_user()
    
    def visualisation_menu(self):
        print("\nVisualisation Menu:")
        print("1. Monthly Consumption/Cost Comparison")
        print("2. Back to Main Menu")
        choice = self.get_int_input("\nEnter choice (1-2): ", 1, 2)
        if choice == 1:
            from pipeline.ui.visualization_ui import VisualizationUI
            VisualizationUI().run_monthly_summary_barchart()
    
    def wait_for_user(self):
        input("\nPress Enter to continue...")
    
    def run(self):
        self.display_main_menu()