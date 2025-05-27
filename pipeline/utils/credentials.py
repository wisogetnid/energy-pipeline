# Create a new file: pipeline/utils/credentials.py
"""Utilities for handling credentials."""

import os
import sys
from dotenv import load_dotenv

def get_credentials():
    """
    Load and validate credentials from environment variables.
    
    Returns:
        tuple: (username, password, token)
    """
    # Load environment variables from .env file
    load_dotenv(override=True)
    
    # Get credentials from environment variables
    username = os.environ.get("GLOWMARKT_USERNAME")
    password = os.environ.get("GLOWMARKT_PASSWORD")
    token = os.environ.get("GLOWMARKT_TOKEN")
    
    if not token and not (username and password):
        print("Error: Please provide either GLOWMARKT_TOKEN or both GLOWMARKT_USERNAME and GLOWMARKT_PASSWORD in your .env file")
        sys.exit(1)
    
    return username, password, token