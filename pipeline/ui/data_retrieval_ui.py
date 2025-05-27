#!/usr/bin/env python
"""UI module for data retrieval workflow."""

import os
import json
from datetime import datetime, timedelta
from dateutil import parser

from pipeline.ui.base_ui import BaseUI
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient
from pipeline.data_retrieval.batch_retrieval import get_historical_readings

class DataRetrievalUI(BaseUI):
    """UI for interacting with the Glowmarkt API data retrieval."""

    def __init__(self, client=None):
        """Initialize the data retrieval UI component."""
        super().__init__()
        self.client = client
        self.selected_entity = None
        self.selected_resource_id = None
        self.selected_resource_name = None
        self.selected_resource_unit = None
        self.selected_resource_classifier = None
        self.start_date = None
        self.end_date = None
        self.period = "PT30M"
        self.offset = 0
        self.timezone_name = "UTC"
        self.date_range = ""
        self.batch_days = 10
    
    def setup_client(self, username=None, password=None, token=None):
        """Set up the Glowmarkt client with authentication."""
        self.print_header("Glowmarkt API Authentication")
        
        # Create client
        self.client = GlowmarktClient(username=username, password=password, token=token)
        
        # If we don't have a token but have credentials, authenticate to get one
        if not token and username and password:
            try:
                print("Authenticating with Glowmarkt API...")
                token = self.client.authenticate()
                print(f"Successfully authenticated. Token: {token[:10]}...")
                return True
            except Exception as e:
                print(f"Authentication failed: {str(e)}")
                return False
        return True

    # ... include all the existing methods from GlowmarktUI for data retrieval ...
    # select_entity, select_resource, _select_resource_basic, select_time_range,
    # select_granularity, select_timezone, confirm_settings, retrieve_data,
    # display_readings, save_data methods should be moved here

    def run(self):
        """Run the data retrieval workflow."""
        # Check if client is set up
        if not self.client:
            if not self.setup_client():
                return
        
        print("\nNote: First virtual entity will be automatically selected.")
        if not self.select_entity():
            return
        
        while True:  # Loop to allow multiple resource retrievals
            if not self.select_resource():
                break
            
            if not self.select_time_range():
                break
            
            if not self.select_granularity():
                break
            
            if not self.select_timezone():
                break
            
            if not self.confirm_settings():
                print("Operation cancelled")
                break
            
            readings = self.retrieve_data()
            if readings:
                self.display_readings(readings)
                json_filepath = self.save_data(readings)
                
                # Return the filepath so the main menu can offer conversion
                if json_filepath:
                    return json_filepath
                
                # Ask if user wants to retrieve data for another resource
                another = self.get_yes_no_input("\nWould you like to retrieve data for another resource?")
                if not another:
                    break
            else:
                print("Failed to retrieve readings. Please try again.")
                break
        
        return None