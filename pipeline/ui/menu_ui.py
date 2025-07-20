from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.data_retrieval import GlowmarktClient

class MenuUI(BaseUI):
    
    def __init__(self, username=None, password=None, token=None):
        super().__init__()
        self.retrieval_ui = DataRetrievalUI()
    
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
            print("4. Retrieve Token")
            print("5. Exit")
            
            choice = self.get_int_input("\nEnter choice (1-5): ", 1, 5)
            
            if choice == 1:
                self.retrieve_data_menu()
            elif choice == 2:
                self.convert_data_menu()
            elif choice == 3:
                self.visualisation_menu()
            elif choice == 4:
                self.retrieve_token()
            elif choice == 5:
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
            
            if self.retrieval_ui.client and self.retrieval_ui.client_type == 'n3rgy':
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
                print("Error: process_all_files is not supported for the current client type or client is not initialized.")
            
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
        print("1. Monthly Consumption/Cost Comparison Across Years")
        print("2. Theoretical Cost Comparison (last 12 months, electricity & gas)")
        print("3. Back to Main Menu")
        choice = self.get_int_input("\nEnter choice (1-3): ", 1, 3)
        if choice == 1:
            from pipeline.ui.visualization_ui import VisualizationUI
            result = VisualizationUI().run_monthly_summary_barchart()
            if result:
                print("\nYearly comparison visualizations have been created successfully.")
                print("Check the data/visualisations/monthly_summary directory for the charts.")
            else:
                print("\nFailed to generate visualizations. Please ensure you have annual summary data files.")
                print("You may need to run the 'Convert to Yearly JSONL files' option in the Convert Data menu first.")
            self.wait_for_user()
        elif choice == 2:
            from pipeline.ui.visualization_ui import VisualizationUI
            result = VisualizationUI().compare_theoretical_costs_multi_plans_cli()
            if result:
                print("\nTheoretical cost comparison chart has been created successfully.")
                print("Check the data/visualisations/theoretical_comparison directory for the chart.")
            else:
                print("\nFailed to generate theoretical cost comparison. Please ensure you have annual summary data files.")
            self.wait_for_user()
    
    def wait_for_user(self):
        input("\nPress Enter to continue...")
    
    def retrieve_token(self):
        self.print_header("Retrieve Token")

        from pipeline.utils.credentials import get_credentials
        username, password, token = get_credentials()

        if token:
            print(f"\nSuccess! Your token is: {token}")
        else:
            try:
                # Use the client to authenticate and get a new token
                if not self.retrieval_ui.client or not isinstance(self.retrieval_ui.client, GlowmarktClient):
                    self.retrieval_ui.client_type = 'glowmarkt'
                    self.retrieval_ui.setup_glowmarkt_client(username, password)
                
                if self.retrieval_ui.client and isinstance(self.retrieval_ui.client, GlowmarktClient):
                    token = self.retrieval_ui.client.authenticate()
                    print(f"\nSuccess! Your token is: {token}")
                else:
                    print("\nFailed to initialize client properly.")
            except Exception as e:
                print(f"\nFailed to retrieve token: {str(e)}")

        self.wait_for_user()
    
    def run(self):
        self.display_main_menu()