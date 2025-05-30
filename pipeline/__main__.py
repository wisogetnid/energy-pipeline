#!/usr/bin/env python

from pipeline.utils.credentials import get_credentials
from pipeline.ui.glowmarkt_interactive import start_interactive_client

def main():
    username, password, token = get_credentials()
    
    start_interactive_client(username, password, token)

if __name__ == "__main__":
    main()