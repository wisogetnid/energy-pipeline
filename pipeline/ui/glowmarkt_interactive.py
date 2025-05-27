#!/usr/bin/env python
"""Interactive UI for Glowmarkt API client."""

from pipeline.ui.menu_ui import MenuUI

def start_interactive_client(username=None, password=None, token=None):
    """Start the interactive client with credentials."""
    ui = MenuUI(username, password, token)
    ui.run()

if __name__ == "__main__":
    # When run directly, use the main entry point
    from pipeline.__main__ import main
    main()