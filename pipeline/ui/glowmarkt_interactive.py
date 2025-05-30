#!/usr/bin/env python

from pipeline.ui.menu_ui import MenuUI

def start_interactive_client(username=None, password=None, token=None):
    ui = MenuUI(username, password, token)
    ui.run()

if __name__ == "__main__":
    from pipeline.__main__ import main
    main()