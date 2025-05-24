#!/usr/bin/env python
"""Interactive Glowmarkt API client."""

import sys
import os
import json
from datetime import datetime, timedelta
from dateutil import parser
from dotenv import load_dotenv
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient
from pipeline.data_retrieval.batch_retrieval import get_historical_readings

def main():
    """Run the interactive Glowmarkt API client."""
    print("====================================")
    print("   Interactive Glowmarkt Client    ")
    print("====================================\n")
    
    # Load environment variables from .env file
    load_dotenv(override=True)
    
    # Get credentials from environment variables
    username = os.environ.get("GLOWMARKT_USERNAME")
    password = os.environ.get("GLOWMARKT_PASSWORD")
    token = os.environ.get("GLOWMARKT_TOKEN")
    
    if not token and not (username and password):
        print("Error: Please provide either GLOWMARKT_TOKEN or both GLOWMARKT_USERNAME and GLOWMARKT_PASSWORD in your .env file")
        sys.exit(1)
    
    # Create client
    client = GlowmarktClient(username=username, password=password, token=token)
    
    # If we don't have a token but have credentials, authenticate to get one
    if not token and username and password:
        try:
            print("Authenticating with Glowmarkt API...")
            token = client.authenticate()
            print(f"Successfully authenticated. Token: {token[:10]}...")
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            sys.exit(1)
    
    # Get virtual entities
    try:
        print("\nFetching virtual entities...")
        virtual_entities = client.get_virtual_entities()
        
        if not virtual_entities:
            print("No virtual entities found.")
            sys.exit(0)
        
        # Display available entities
        print("\nAvailable Virtual Entities:")
        for i, entity in enumerate(virtual_entities, 1):
            print(f"{i}. {entity.get('name', 'Unknown Entity')} (ID: {entity.get('veId')})")
        
        # Let user choose an entity
        while True:
            try:
                choice = int(input("\nSelect an entity number: "))
                if 1 <= choice <= len(virtual_entities):
                    selected_entity = virtual_entities[choice-1]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(virtual_entities)}")
            except ValueError:
                print("Please enter a valid number")
        
        print(f"\nSelected: {selected_entity.get('name')}")
        
        # Display resources for the selected entity
        resources = selected_entity.get('resources', [])
        if not resources:
            print("No resources found for this entity.")
            sys.exit(0)
        
        print("\nAvailable Resources:")
        for i, resource in enumerate(resources, 1):
            resource_id = resource.get('resourceId')
            resource_type = resource.get('resourceTypeId')
            print(f"{i}. ID: {resource_id}")
            print(f"   Type: {resource_type}")
        
        # Let user choose a resource
        while True:
            try:
                choice = int(input("\nSelect a resource number: "))
                if 1 <= choice <= len(resources):
                    selected_resource = resources[choice-1]
                    selected_resource_id = selected_resource.get('resourceId')
                    break
                else:
                    print(f"Please enter a number between 1 and {len(resources)}")
            except ValueError:
                print("Please enter a valid number")
        
        print(f"\nSelected resource ID: {selected_resource_id}")
        
        # Let user choose a time range
        print("\nTime Range Options:")
        print("1. Last day")
        print("2. Last week")
        print("3. Last month")
        print("4. Last year")
        print("5. Custom range")
        
        while True:
            try:
                choice = int(input("\nSelect a time range: "))
                if 1 <= choice <= 5:
                    break
                else:
                    print("Please enter a number between 1 and 5")
            except ValueError:
                print("Please enter a valid number")
        
        # Set date range based on user choice
        # Set end_date to midnight of the current day
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if choice == 1:
            # Last day
            start_date = end_date - timedelta(hours=24)
            date_range = "last day"
        elif choice == 2:
            # Last week
            start_date = end_date - timedelta(days=7)
            date_range = "last 7 days"
        elif choice == 3:
            # Last month
            start_date = end_date - timedelta(days=30)
            date_range = "last 30 days"
        else:
            # Custom range
            while True:
                try:
                    start_input = input("\nEnter start date (YYYY-MM-DD): ")
                    start_date = parser.parse(start_input)
                    
                    end_input = input("Enter end date (YYYY-MM-DD or press Enter for current date): ")
                    if end_input.strip():
                        end_date = parser.parse(end_input)
                    
                    if start_date > end_date:
                        print("Start date must be before end date")
                        continue
                    
                    date_range = f"{start_date.date()} to {end_date.date()}"
                    break
                except Exception as e:
                    print(f"Invalid date format: {e}")
        
        # Let user choose data granularity
        print("\nData Granularity Options:")
        print("1. 30 minutes (PT30M)")
        print("2. 1 hour (PT1H)")
        print("3. 1 day (P1D)")
        print("4. 1 week (P1W)")
        print("5. 1 month (P1M)")
        
        while True:
            try:
                choice = int(input("\nSelect data granularity: "))
                if 1 <= choice <= 5:
                    break
                else:
                    print("Please enter a number between 1 and 5")
            except ValueError:
                print("Please enter a valid number")
        
        # Set period based on user choice
        if choice == 1:
            period = "PT30M"
        elif choice == 2:
            period = "PT1H"
        elif choice == 3:
            period = "P1D"
        elif choice == 4:
            period = "P1W"
        else:
            period = "P1M"
        
        # Let user choose timezone offset
        print("\nTimezone Options:")
        print("1. UTC (+0)")
        print("2. BST (UTC+1)")
        print("3. EST (UTC-5)")
        print("4. Custom offset")
        
        while True:
            try:
                choice = int(input("\nSelect timezone: "))
                if 1 <= choice <= 4:
                    break
                else:
                    print("Please enter a number between 1 and 4")
            except ValueError:
                print("Please enter a valid number")
        
        # Set offset based on user choice
        if choice == 1:
            offset = 0
            timezone_name = "UTC"
        elif choice == 2:
            offset = -60  # BST
            timezone_name = "BST (UTC+1)"
        elif choice == 3:
            offset = 300  # EST
            timezone_name = "EST (UTC-5)"
        else:
            while True:
                try:
                    hours = float(input("\nEnter timezone offset in hours from UTC (e.g., 1 for UTC+1, -5 for UTC-5): "))
                    offset = int(-hours * 60)  # Convert to minutes and invert (API uses negative values for positive offsets)
                    timezone_name = f"UTC{'+' if hours >= 0 else ''}{hours}"
                    break
                except ValueError:
                    print("Please enter a valid number")
        
        # Confirm selections
        print("\n=== Data Retrieval Summary ===")
        print(f"Resource ID: {selected_resource_id}")
        print(f"Time Range: {date_range}")
        print(f"From: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"To: {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Granularity: {period}")
        print(f"Timezone: {timezone_name}")
        
        confirm = input("\nRetrieve data with these settings? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled")
            sys.exit(0)
        
        # Fetch data
        print("\nRetrieving data in batches...")
        batch_days = 10
        
        # Calculate total days to estimate batches
        total_days = (end_date - start_date).days
        estimated_batches = (total_days // batch_days) + 1
        print(f"Date range spans {total_days} days, will use approximately {estimated_batches} batches")
        
        try:
            start_time = datetime.now()
            all_readings = get_historical_readings(
                client,
                selected_resource_id,
                start_date,
                end_date,
                period=period,
                offset=offset,
                batch_days=batch_days
            )
            end_time = datetime.now()
            retrieval_time = (end_time - start_time).total_seconds()
            
            print(f"\nRetrieved {len(all_readings)} readings in {retrieval_time:.2f} seconds")
            
            # Display data
            if all_readings:
                print("\nFirst 5 readings:")
                for reading in all_readings[:5]:
                    timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    value = reading[1]
                    print(f"  {timestamp}: {value}")
                
                print("\nLast 5 readings:")
                for reading in all_readings[-5:]:
                    timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    value = reading[1]
                    print(f"  {timestamp}: {value}")
                
                # Ask if user wants to save the data
                save = input("\nSave data to file? (y/n): ")
                if save.lower() == 'y':
                    # Create filename based on resource and date range
                    filename = f"readings_{selected_resource_id}_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.json"
                    
                    # Format data for saving
                    output_data = {
                        "resource_id": selected_resource_id,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                        "period": period,
                        "timezone_offset": offset,
                        "readings": all_readings
                    }
                    
                    with open(filename, 'w') as f:
                        json.dump(output_data, f, indent=2)
                    
                    print(f"Data saved to {filename}")
            else:
                print("No readings were retrieved for the specified period")
                
        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
    
    except Exception as e:
        print(f"Failed to interact with Glowmarkt API: {str(e)}")
        sys.exit(1)
    
    print("\nThank you for using the Interactive Glowmarkt Client!")

if __name__ == "__main__":
    main()