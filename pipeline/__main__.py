#!/usr/bin/env python
"""Main entry point for the Glowmarkt API client."""

import sys
import os
import json
from datetime import datetime, timedelta
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

def main():
    """Run the Glowmarkt API client."""
    print("Glowmarkt API Client")
    
    # Get credentials from environment variables
    username = os.environ.get("GLOWMARKT_USERNAME")
    password = os.environ.get("GLOWMARKT_PASSWORD")
    token = os.environ.get("GLOWMARKT_TOKEN")
    
    if not token and not (username and password):
        print("Error: Please provide either GLOWMARKT_TOKEN or both GLOWMARKT_USERNAME and GLOWMARKT_PASSWORD environment variables")
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
    
    # Get available resources
    try:
        resources = client.get_resources()
        print("\nAvailable Resources:")
        for resource in resources.get("resources", []):
            print(f"- {resource.get('name', 'Unknown')}: {resource.get('id')}")
    except Exception as e:
        print(f"Failed to get resources: {str(e)}")
        sys.exit(1)
    
    # Example: Get readings for the first resource
    if resources.get("resources"):
        resource_id = resources["resources"][0]["id"]
        try:
            # Get last 7 days of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            print(f"\nFetching readings for resource {resource_id} from {start_date.date()} to {end_date.date()}...")
            readings = client.get_readings(resource_id, start_date=start_date, end_date=end_date)
            
            # Print a sample of the readings
            print("\nSample readings:")
            for i, reading in enumerate(readings.get("readings", [])[:5]):
                timestamp = datetime.fromtimestamp(reading[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                value = reading[1]
                print(f"{timestamp}: {value}")
                
            if len(readings.get("readings", [])) > 5:
                print(f"... and {len(readings.get('readings', [])) - 5} more readings")
        except Exception as e:
            print(f"Failed to get readings: {str(e)}")
    
if __name__ == "__main__":
    main()