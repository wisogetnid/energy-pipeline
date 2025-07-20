import os
import sys
from dotenv import load_dotenv

def get_credentials():
    load_dotenv(override=True)
    
    username = os.environ.get("GLOWMARKT_USERNAME")
    password = os.environ.get("GLOWMARKT_PASSWORD")
    token = os.environ.get("GLOWMARKT_TOKEN")
    
    if not token and not (username and password):
        print("Error: Please provide either GLOWMARKT_TOKEN or both GLOWMARKT_USERNAME and GLOWMARKT_PASSWORD in your .env file")
        sys.exit(1)
    
    return username, password, token

def get_token():
    """Get the Glowmarkt API token from environment variables or by authenticating with username and password."""
    username, password, token = get_credentials()
    
    if token:
        return token
    
    # If we have a username and password but no token, we could authenticate
    # but this would require importing the GlowmarktClient here
    # For now, just return what we have
    from pipeline.data_retrieval import GlowmarktClient
    client = GlowmarktClient(username=username, password=password)
    return client.authenticate()