#!/usr/bin/env python
"""Main entry point for the Glowmarkt API client."""

import sys
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

def main():
    """Run the Glowmarkt API client."""
    print("Glowmarkt API Client")
    
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
            token = client.authenticate()
            print(f"Successfully authenticated. Token: {token[:10]}...")
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            sys.exit(1)
    
    # Get virtual entities and their resources
    try:
        virtual_entities = client.get_virtual_entities()
        print("\nVirtual Entities:")
        
        if not virtual_entities:
            print("No virtual entities found.")
            sys.exit(0)
            
        # Process each virtual entity
        for entity in virtual_entities:
            print(f"\n- {entity.get('name', 'Unknown Entity')} (ID: {entity.get('veId')})")
            
            # List resources associated with this entity
            resources = entity.get('resources', [])
            print(f"  Resources ({len(resources)}):")
            
            for resource in resources:
                resource_id = resource.get('resourceId')
                resource_type = resource.get('resourceTypeId')
                print(f"    - Resource ID: {resource_id}")
                print(f"      Type: {resource_type}")
                
                # Get and display readings for one resource as an example
                if resource_id:
                    try:
                        # Get last 7 days of data
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=7)
                        
                        # Set timezone offset for BST (UTC+1) as an example
                        offset = -60
                        
                        print(f"\n    Fetching readings for resource {resource_id}...")
                        print(f"    Period: {start_date.date()} to {end_date.date()}")
                        print(f"    Using timezone offset: {offset} minutes from UTC")
                        
                        readings = client.get_readings(
                            resource_id, 
                            start_date=start_date, 
                            end_date=end_date,
                            period="PT30M",
                            function="sum",
                            offset=offset
                        )
                        
                        # Print a sample of the readings
                        reading_data = readings.get("readings", [])
                        if reading_data:
                            print("\n    Sample readings:")
                            for i, reading in enumerate(reading_data[:3]):
                                timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                                value = reading[1]
                                print(f"      {timestamp}: {value}")
                                
                            if len(reading_data) > 3:
                                print(f"      ... and {len(reading_data) - 3} more readings")
                        else:
                            print("    No readings available for this resource.")
                            
                        # Only process one resource as an example
                        break
                    except Exception as e:
                        print(f"    Failed to get readings: {str(e)}")
            
            # Only process the first entity for demonstration
            break
            
    except Exception as e:
        print(f"Failed to get virtual entities: {str(e)}")
        sys.exit(1)
    
if __name__ == "__main__":
    main()