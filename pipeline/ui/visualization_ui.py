#!/usr/bin/env python

import os
import glob
from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_visualisation.energy_efficiency import generate_consumption_patterns, generate_weekly_comparison, generate_weekday_weekend_pattern, load_and_process_consumption_data

class VisualizationUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/processed")
        self.output_dir = Path("data/visualisations")
    
    def find_consumption_files(self):
        all_files = glob.glob(str(self.data_dir / "*_consumption_*.parquet"))
        if not all_files:
            all_files = glob.glob(str(self.data_dir / "*_consumption_*.jsonl"))
        
        resources = {}
        
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            resource_name = file_name.split('_')[0]
            date_part = '_'.join(file_name.split('_')[2:]).replace('.parquet', '').replace('.jsonl', '')
            
            key = f"{resource_name}_{date_part}"
            resources[key] = file_path
        
        return resources
    
    def run_visualization(self):
        self.print_header("Energy Efficiency Visualization")
        
        consumption_files = self.find_consumption_files()
        
        if not consumption_files:
            print("No consumption data files found. Please retrieve and convert data first.")
            return False
        
        print("\nAvailable consumption data for visualization:")
        
        file_items = list(consumption_files.items())
        for i, (key, file_path) in enumerate(file_items, 1):
            resource_name = key.split('_')[0]
            date_part = '_'.join(key.split('_')[1:])
            print(f"{i}. {resource_name} ({date_part})")
        
        print("\nOptions:")
        print("1. Visualize a specific resource")
        print("2. Visualize all resources")
        print("3. Go back")
        
        choice = self.get_int_input("\nEnter your choice: ", 1, 3)
        
        if choice == 1:
            file_idx = self.get_int_input("Enter resource number: ", 1, len(file_items))
            selected_key, selected_file = file_items[file_idx - 1]
            
            return self.generate_efficiency_charts(selected_file, selected_key)
                
        elif choice == 2:
            success_count = 0
            for key, file_path in file_items:
                result = self.generate_efficiency_charts(file_path, key)
                if result:
                    success_count += 1
            
            print(f"\nGenerated visualizations for {success_count} out of {len(file_items)} resources.")
            print(f"Visualizations are saved in separate folders under {self.output_dir}")
            return success_count > 0
                
        elif choice == 3:
            return False
        
        return False
    
    def generate_efficiency_charts(self, consumption_file_path, resource_key):
        output_folder = self.output_dir / resource_key
        output_folder.mkdir(parents=True, exist_ok=True)
        
        try:
            df, resource_type, unit = load_and_process_consumption_data(consumption_file_path)
            
            print(f"\nGenerating consumption patterns for {resource_type}...")
            pattern_file = generate_consumption_patterns(df, resource_type, unit, output_folder)
            
            print(f"Generating weekly comparison for {resource_type}...")
            weekly_file = generate_weekly_comparison(df, resource_type, unit, output_folder)
            
            print(f"Generating weekday vs weekend patterns for {resource_type}...")
            weekday_weekend_file = generate_weekday_weekend_pattern(df, resource_type, unit, output_folder)
            
            print(f"Visualizations saved to {output_folder}")
            return True
            
        except Exception as e:
            print(f"Error generating visualizations: {str(e)}")
            return False
    
    def run(self):
        return self.run_visualization()