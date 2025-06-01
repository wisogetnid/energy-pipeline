#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

def generate_visualizations(cost_file_path, consumption_file_path, output_dir=None):
    try:
        if output_dir is None:
            vis_dir = Path("data/visualisations")
        else:
            vis_dir = Path(output_dir)
        
        vis_dir.mkdir(parents=True, exist_ok=True)
        
        cost_path = Path(cost_file_path)
        consumption_path = Path(consumption_file_path)
        
        # Read the data files
        if cost_path.suffix == '.parquet':
            cost_df = pd.read_parquet(cost_path)
            consumption_df = pd.read_parquet(consumption_path)
        else:
            cost_df = pd.read_json(cost_path, lines=True)
            consumption_df = pd.read_json(consumption_path, lines=True)
            
        # Extract resource type from the filename or data
        resource_type = cost_df['resource_name'].iloc[0].split()[0].capitalize()
        
        # Process the data
        cost_df['timestamp'] = pd.to_datetime(cost_df['timestamp_iso'])
        consumption_df['timestamp'] = pd.to_datetime(consumption_df['timestamp_iso'])
        
        # Add date features for analysis
        for df in [cost_df, consumption_df]:
            df['hour'] = df['timestamp'].dt.hour
            df['day'] = df['timestamp'].dt.day
            df['weekday'] = df['timestamp'].dt.day_name()
            df['date'] = df['timestamp'].dt.date
        
        # Merge the dataframes on timestamp
        merged_df = pd.merge(
            cost_df[['timestamp', 'timestamp_iso', 'value', 'hour', 'day', 'weekday', 'date', 'units']], 
            consumption_df[['timestamp', 'value', 'units']], 
            on='timestamp', 
            suffixes=('_cost', '_consumption')
        )
        
        # Get units for labels
        cost_unit = cost_df['units'].iloc[0] if 'units' in cost_df.columns else 'pence'
        consumption_unit = consumption_df['units'].iloc[0] if 'units' in consumption_df.columns else 'kWh'
        
        # Set style for plots
        sns.set_style("whitegrid")
        plt.rcParams.update({'font.size': 12})
        
        # 1. Time series plot of cost and consumption
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)
        
        # Cost plot
        ax1.plot(merged_df['timestamp'], merged_df['value_cost'], color='red', linewidth=1.5)
        ax1.set_ylabel(f'Cost ({cost_unit})')
        ax1.set_title(f'{resource_type} Cost Over Time')
        ax1.grid(True)
        
        # Consumption plot
        ax2.plot(merged_df['timestamp'], merged_df['value_consumption'], color='blue', linewidth=1.5)
        ax2.set_ylabel(f'Consumption ({consumption_unit})')
        ax2.set_xlabel('Date')
        ax2.set_title(f'{resource_type} Consumption Over Time')
        ax2.grid(True)
        
        # Format x-axis date labels
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(vis_dir / f'{resource_type.lower()}_time_series.png', dpi=300)
        plt.close()
        
        # 2. Daily patterns - Hourly average cost and consumption
        hourly_data = merged_df.groupby('hour').agg({
            'value_cost': 'mean',
            'value_consumption': 'mean'
        }).reset_index()
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        ax1.set_xlabel('Hour of Day')
        ax1.set_ylabel(f'Cost ({cost_unit})', color='red')
        ax1.plot(hourly_data['hour'], hourly_data['value_cost'], color='red', marker='o')
        ax1.tick_params(axis='y', labelcolor='red')
        ax1.grid(True, alpha=0.3)
        
        ax2 = ax1.twinx()
        ax2.set_ylabel(f'Consumption ({consumption_unit})', color='blue')
        ax2.plot(hourly_data['hour'], hourly_data['value_consumption'], color='blue', marker='s')
        ax2.tick_params(axis='y', labelcolor='blue')
        
        ax1.set_xticks(range(0, 24))
        ax1.set_xlim(-0.5, 23.5)
        plt.title(f'Average Hourly {resource_type} Cost and Consumption')
        plt.tight_layout()
        plt.savefig(vis_dir / f'{resource_type.lower()}_hourly_patterns.png', dpi=300)
        plt.close()
        
        # 3. Heatmap of consumption by day and hour
        daily_hourly_consumption = merged_df.pivot_table(
            index='day', 
            columns='hour', 
            values='value_consumption', 
            aggfunc='mean'
        )
        
        plt.figure(figsize=(15, 8))
        sns.heatmap(daily_hourly_consumption, cmap='YlGnBu', annot=False, fmt=".2f", cbar_kws={'label': consumption_unit})
        plt.title(f'{resource_type} Consumption by Day and Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Day of Month')
        plt.tight_layout()
        plt.savefig(vis_dir / f'{resource_type.lower()}_consumption_heatmap.png', dpi=300)
        plt.close()
        
        # 4. Daily total cost and consumption
        daily_totals = merged_df.groupby('date').agg({
            'value_cost': 'sum',
            'value_consumption': 'sum'
        }).reset_index()
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        x = range(len(daily_totals))
        ax1.set_xlabel('Date')
        ax1.set_ylabel(f'Total Daily Cost ({cost_unit})', color='red')
        bars1 = ax1.bar(x, daily_totals['value_cost'], color='red', alpha=0.6, label='Cost')
        ax1.tick_params(axis='y', labelcolor='red')
        
        ax2 = ax1.twinx()
        ax2.set_ylabel(f'Total Daily Consumption ({consumption_unit})', color='blue')
        line = ax2.plot(x, daily_totals['value_consumption'], color='blue', marker='o', linestyle='-', label='Consumption')
        ax2.tick_params(axis='y', labelcolor='blue')
        
        # Add date labels on x-axis
        plt.xticks(x, [d.strftime('%d-%b') for d in daily_totals['date']], rotation=45)
        
        # Add legend
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc='upper left')
        
        plt.title(f'Daily {resource_type} Cost and Consumption')
        plt.tight_layout()
        plt.savefig(vis_dir / f'{resource_type.lower()}_daily_totals.png', dpi=300)
        plt.close()
        
        return True
        
    except Exception as e:
        print(f"Error generating visualizations: {str(e)}")
        return False

if __name__ == "__main__":
    cost_parquet = Path("data/processed/electricity_cost_20250401_to_20250430.parquet")
    consumption_parquet = Path("data/processed/electricity_consumption_20250401_to_20250430.parquet")
    
    if not cost_parquet.exists():
        cost_parquet = Path("data/processed/electricity_cost_20250401_to_20250430.jsonl")
    
    if not consumption_parquet.exists():
        consumption_parquet = Path("data/processed/electricity_consumption_20250401_to_20250430.jsonl")
    
    generate_visualizations(cost_parquet, consumption_parquet)