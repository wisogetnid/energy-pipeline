#!/usr/bin/env python

import os
import glob
from pathlib import Path

from pipeline.ui.base_ui import BaseUI
from pipeline.data_visualisation.monthly_resource_pair_charts import generate_visualizations

class VisualizationUI(BaseUI):
    
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/processed")
        self.output_dir = Path("data/visualisations")
    
    def find_data_pairs(self):
        all_files = glob.glob(str(self.data_dir / "*.parquet"))
        if not all_files:
            all_files = glob.glob(str(self.data_dir / "*.jsonl"))
        
        resources = {}
        
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            resource_type = "unknown"
            
            if "consumption" in file_name:
                resource_type = "consumption"
            elif "cost" in file_name:
                resource_type = "cost"
            
            resource_name = file_name.split('_')[0]
            date_part = '_'.join(file_name.split('_')[2:]).replace('.parquet', '').replace('.jsonl', '')
            
            key = f"{resource_name}_{date_part}"
            
            if key not in resources:
                resources[key] = {}
            
            resources[key][resource_type] = file_path
        
        return resources
    
    def run_visualization(self):
        self.print_header("Energy Data Visualization")
        
        resource_pairs = self.find_data_pairs()
        
        if not resource_pairs:
            print("No data files found. Please retrieve and convert data first.")
            return False
        
        print("\nAvailable resource pairs for visualization:")
        
        valid_pairs = []
        for i, (key, files) in enumerate(resource_pairs.items(), 1):
            resource_name = key.split('_')[0]
            date_part = '_'.join(key.split('_')[1:])
            
            has_consumption = "consumption" in files
            has_cost = "cost" in files
            
            status = []
            if has_consumption:
                status.append("consumption")
            if has_cost:
                status.append("cost")
            
            print(f"{i}. {resource_name} ({date_part}) - Available: {', '.join(status)}")
            
            if has_consumption and has_cost:
                valid_pairs.append((key, files))
        
        if not valid_pairs:
            print("\nNo complete pairs (consumption + cost) found. Please ensure you have both types of data.")
            return False
        
        print("\nOptions:")
        print("1. Visualize a specific resource pair")
        print("2. Visualize all valid pairs")
        print("3. Go back")
        
        choice = self.get_int_input("\nEnter your choice: ", 1, 3)
        
        if choice == 1:
            pair_idx = self.get_int_input("Enter resource pair number: ", 1, len(valid_pairs))
            selected_key, selected_files = valid_pairs[pair_idx - 1]
            
            output_folder = self.output_dir / selected_key
            output_folder.mkdir(parents=True, exist_ok=True)
            
            print(f"\nGenerating visualizations for {selected_key}...")
            result = generate_visualizations(
                selected_files["cost"],
                selected_files["consumption"],
                output_folder
            )
            
            if result:
                print(f"Visualizations saved to {output_folder}")
                return True
            else:
                print("Visualization generation failed.")
                return False
                
        elif choice == 2:
            success_count = 0
            for key, files in valid_pairs:
                output_folder = self.output_dir / key
                output_folder.mkdir(parents=True, exist_ok=True)
                
                print(f"\nGenerating visualizations for {key}...")
                result = generate_visualizations(
                    files["cost"],
                    files["consumption"],
                    output_folder
                )
                if result:
                    success_count += 1
            
            print(f"\nGenerated visualizations for {success_count} out of {len(valid_pairs)} resource pairs.")
            print(f"Visualizations are saved in separate folders under {self.output_dir}")
            return success_count > 0
                
        elif choice == 3:
            return False
        
        return False
    
    def run(self):
        return self.run_visualization()