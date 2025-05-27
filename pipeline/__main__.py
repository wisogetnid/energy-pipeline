#!/usr/bin/env python
"""Main entry point for the Glowmarkt API client."""

from pipeline.utils.credentials import get_credentials
from pipeline.ui.glowmarkt_interactive import start_interactive_client

def main():
    """Run the Glowmarkt API client."""
    # Get credentials from environment variables
    username, password, token = get_credentials()
    
    # Start the interactive client
    start_interactive_client(username, password, token)

if __name__ == "__main__":
    main()