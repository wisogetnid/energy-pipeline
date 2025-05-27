#!/usr/bin/env python
"""Interactive UI for Glowmarkt API client."""

import sys
import os
from dotenv import load_dotenv

from pipeline.ui.menu_ui import MenuUI

def start_interactive_client(username=None, password=None, token=None):
    """Start the interactive client with credentials."""
    ui = MenuUI(username, password, token)
    ui.run()

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv(override=True)
    
    # Get credentials from environment variables
    username = os.environ.get("GLOWMARKT_USERNAME")
    password = os.environ.get("GLOWMARKT_PASSWORD")
    token = os.environ.get("GLOWMARKT_TOKEN")
    
    if not token and not (username and password):
        print("Error: Please provide either GLOWMARKT_TOKEN or both GLOWMARKT_USERNAME and GLOWMARKT_PASSWORD in your .env file")
        sys.exit(1)
    
    # Start the interactive client
    start_interactive_client(username, password, token)