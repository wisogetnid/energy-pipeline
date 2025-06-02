#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import matplotlib.dates as mdates
import glob

def find_resource_data(resource_type="all"):
    parquet_dir = Path("data/parquet")
    consumption_files = []
    
    file_pattern = f"{resource_type.lower()}_consumption_*.parquet" if resource_type != "all" else "*_consumption_*.parquet"
    
    for file_path in glob.glob(str(parquet_dir / file_pattern)):
        consumption_files.append(file_path)
    
    return consumption_files

def load_and_process_consumption_data(file_path):
    file_path = Path(file_path)
    
    if file_path.suffix == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        df = pd.read_json(file_path, lines=True)
    
    resource_type = df['resource_name'].iloc[0].split()[0].capitalize()
    consumption_unit = df['units'].iloc[0] if 'units' in df.columns else 'kWh'
    
    df['timestamp'] = pd.to_datetime(df['timestamp_iso'])
    df['hour'] = df['timestamp'].dt.hour
    df['day'] = df['timestamp'].dt.day
    df['date'] = df['timestamp'].dt.date
    df['weekday'] = df['timestamp'].dt.day_name()
    df['week'] = df['timestamp'].dt.isocalendar().week
    df['month'] = df['timestamp'].dt.month
    
    return df, resource_type, consumption_unit

def generate_consumption_patterns(df, resource_type, consumption_unit, output_dir):
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    df['day_name'] = df['timestamp'].dt.day_name()
    pivot_data = df.pivot_table(
        index='day_name', 
        columns='hour', 
        values='value', 
        aggfunc='mean'
    )
    
    pivot_data = pivot_data.reindex(day_order)
    
    plt.figure(figsize=(15, 8))
    heatmap = sns.heatmap(pivot_data, cmap='YlOrRd', annot=False, fmt=".2f", cbar_kws={'label': consumption_unit})
    plt.title(f'{resource_type} Consumption by Day of Week and Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Day of Week')
    plt.tight_layout()
    
    file_path = output_dir / f'{resource_type.lower()}_weekly_patterns.png'
    plt.savefig(file_path, dpi=300)
    plt.close()
    
    return file_path

def generate_weekly_comparison(df, resource_type, consumption_unit, output_dir):
    weekly_consumption = df.groupby(['week', 'date']).agg({
        'value': ['sum', 'first']
    }).reset_index()
    
    weekly_consumption.columns = ['week', 'date', 'total_consumption', 'first_value']
    
    weekly_summary = weekly_consumption.groupby('week').agg({
        'total_consumption': 'mean',
        'date': 'min'
    }).reset_index()
    
    weekly_summary = weekly_summary.sort_values('date')
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(weekly_summary)), weekly_summary['total_consumption'], color='green')
    
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{height:.1f}',
                ha='center', va='bottom', rotation=0)
    
    plt.xlabel('Week')
    plt.ylabel(f'Average Daily Consumption ({consumption_unit})')
    plt.title(f'Weekly {resource_type} Consumption Comparison')
    plt.xticks(range(len(weekly_summary)), 
              [d.strftime('%d %b') for d in weekly_summary['date']], 
              rotation=45)
    plt.tight_layout()
    
    file_path = output_dir / f'{resource_type.lower()}_weekly_comparison.png'
    plt.savefig(file_path, dpi=300)
    plt.close()
    
    return file_path

def generate_weekday_weekend_pattern(df, resource_type, consumption_unit, output_dir):
    df['is_weekend'] = df['weekday'].isin(['Saturday', 'Sunday'])
    
    hourly_by_day_type = df.groupby(['hour', 'is_weekend']).agg({
        'value': 'mean'
    }).reset_index()
    
    plt.figure(figsize=(12, 6))
    
    weekend_data = hourly_by_day_type[hourly_by_day_type['is_weekend']]
    weekday_data = hourly_by_day_type[~hourly_by_day_type['is_weekend']]
    
    plt.plot(weekend_data['hour'], weekend_data['value'], 'r-', marker='o', linewidth=2, label='Weekend')
    plt.plot(weekday_data['hour'], weekday_data['value'], 'b-', marker='s', linewidth=2, label='Weekday')
    
    plt.xlabel('Hour of Day')
    plt.ylabel(f'Average Consumption ({consumption_unit})')
    plt.title(f'{resource_type} Usage Pattern: Weekday vs Weekend')
    plt.xticks(range(0, 24))
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    file_path = output_dir / f'{resource_type.lower()}_weekday_weekend_pattern.png'
    plt.savefig(file_path, dpi=300)
    plt.close()
    
    return file_path

def generate_consumption_visualizations(resource_types=["electricity", "gas"]):
    output_dir = Path("data/visualisations/efficiency")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    created_files = []
    
    for resource_type in resource_types:
        consumption_files = find_resource_data(resource_type)
        
        if not consumption_files:
            print(f"No {resource_type} consumption data found.")
            continue
        
        for file_path in consumption_files:
            try:
                df, detected_resource, unit = load_and_process_consumption_data(file_path)
                
                print(f"Generating patterns for {detected_resource} from {Path(file_path).name}")
                
                pattern_file = generate_consumption_patterns(df, detected_resource, unit, output_dir)
                created_files.append(pattern_file)
                
                weekly_file = generate_weekly_comparison(df, detected_resource, unit, output_dir)
                created_files.append(weekly_file)
                
                weekday_weekend_file = generate_weekday_weekend_pattern(df, detected_resource, unit, output_dir)
                created_files.append(weekday_weekend_file)
                
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
    
    if created_files:
        print("\nCreated visualization files:")
        for file in created_files:
            print(f"- {file}")
        return True
    else:
        print("No visualization files created.")
        return False

if __name__ == "__main__":
    generate_consumption_visualizations()