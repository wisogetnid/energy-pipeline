#!/usr/bin/env python

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.ui.data_converter_ui import DataConverterUI
from pipeline.ui.parquet_converter_ui import ParquetConverterUI
from pipeline.ui.visualization_ui import VisualizationUI

class MenuUI(BaseUI):
    
    def __init__(self, username=None, password=None, token=None):
        super().__init__()
        self.username = username
        self.password = password
        self.token = token
        self.retrieval_ui = DataRetrievalUI()
        self.converter_ui = DataConverterUI()
        self.parquet_ui = ParquetConverterUI()
        self.visualization_ui = VisualizationUI()
        self.last_retrieved_file = None
        self.last_jsonl_file = None
        self.last_parquet_file = None
        
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
            
        if self.last_parquet_file:
            print(f"Last Parquet file: {self.last_parquet_file}")
            print("Ready for visualization")
            
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
                print("4. Generate visualizations")
                print("5. Run complete pipeline (retrieve → JSONL → Parquet → Visualize)")
                print("6. Exit")
                
                menu_choice = self.get_int_input("\nEnter your choice: ", 1, 6)
                
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
                            self.last_parquet_file = self.parquet_ui.convert_to_parquet(self.last_jsonl_file)
                        else:
                            self.parquet_ui.run()
                    else:
                        print("\nNo recently created JSONL file available.")
                        self.parquet_ui.run()
                
                elif menu_choice == 4:
                    # Generate visualizations
                    self.visualization_ui.run()
                    
                elif menu_choice == 5:
                    # Run complete pipeline
                    print("\nRunning complete pipeline: retrieve → JSONL → Parquet → Visualize")
                    
                    self.last_retrieved_file = self.retrieval_ui.run()
                    
                    if self.last_retrieved_file:
                        print(f"\nData successfully retrieved to: {self.last_retrieved_file}")
                        print("Now converting to JSONL format...")
                        
                        self.last_jsonl_file = self.converter_ui.convert_to_jsonl(self.last_retrieved_file)
                        
                        if self.last_jsonl_file:
                            print("\nNow converting to Parquet format...")
                            self.last_parquet_file = self.parquet_ui.convert_to_parquet(self.last_jsonl_file)
                            
                            if self.last_parquet_file:
                                print("\nNow generating visualizations...")
                                
                                if "consumption" in self.last_parquet_file:
                                    consumption_file = self.last_parquet_file
                                    
                                    # Generate efficiency visualizations
                                    from pipeline.data_visualisation.energy_efficiency import load_and_process_consumption_data, generate_consumption_patterns, generate_weekly_comparison, generate_weekday_weekend_pattern
                                    
                                    try:
                                        resource_name = Path(consumption_file).stem
                                        output_dir = Path("data/visualisations") / resource_name
                                        output_dir.mkdir(parents=True, exist_ok=True)
                                        
                                        df, resource_type, unit = load_and_process_consumption_data(consumption_file)
                                        
                                        print(f"Generating consumption patterns for {resource_type}...")
                                        generate_consumption_patterns(df, resource_type, unit, output_dir)
                                        
                                        print(f"Generating weekly comparison for {resource_type}...")
                                        generate_weekly_comparison(df, resource_type, unit, output_dir)
                                        
                                        print(f"Generating weekday vs weekend patterns for {resource_type}...")
                                        generate_weekday_weekend_pattern(df, resource_type, unit, output_dir)
                                        
                                        print("\nVisualization generation successful!")
                                        print(f"Visualizations saved to {output_dir}")
                                        
                                    except Exception as e:
                                        print(f"\nVisualization generation failed: {str(e)}")
                                else:
                                    print("\nThe last converted file is not consumption data.")
                                    print("Visualizations require consumption data.")
                                    print("Please convert consumption data before generating visualizations.")
                                
                                print("\nPipeline completed successfully!")
                                print(f"Original data: {self.last_retrieved_file}")
                                print(f"JSONL data: {self.last_jsonl_file}")
                                print(f"Parquet data: {self.last_parquet_file}")
                            else:
                                print("\nParquet conversion failed. Pipeline could not complete.")
                        else:
                            print("\nJSONL conversion failed. Pipeline could not complete.")
                    else:
                        print("\nData retrieval failed. Pipeline could not complete.")
                    
                elif menu_choice == 6:
                    print("\nThank you for using the Energy Data Pipeline!")
                    break
                
                input("\nPress Enter to continue...")
            
        except Exception as e:
            print(f"Error in pipeline: {str(e)}")