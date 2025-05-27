#!/usr/bin/env python
"""Main menu UI module for energy pipeline."""

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.ui.data_converter_ui import DataConverterUI

class MenuUI(BaseUI):
    """Main menu UI for the energy pipeline."""

    def __init__(self, username=None, password=None, token=None):
        """Initialize the main menu UI component."""
        super().__init__()
        self.username = username
        self.password = password
        self.token = token
        self.retrieval_ui = DataRetrievalUI()
        self.converter_ui = DataConverterUI()
        
        # Pre-configure client if credentials are provided
        if username or token:
            self.retrieval_ui.setup_client(username, password, token)
    
    def run(self):
        """Run the main menu workflow."""
        try:
            # Main menu
            while True:
                self.print_header("Interactive Glowmarkt Client")
                print("\nWhat would you like to do?")
                print("1. Retrieve new data from Glowmarkt API")
                print("2. Convert existing JSON files to JSONL format")
                print("3. Exit")
                
                choice = self.get_int_input("\nEnter your choice: ", 1, 3)
                
                if choice == 1:
                    # Retrieve new data
                    json_filepath = self.retrieval_ui.run()
                    
                    # Ask if user wants to convert to JSONL format
                    if json_filepath:
                        if self.get_yes_no_input("\nWould you like to convert this data to JSONL format?"):
                            self.converter_ui.convert_to_jsonl(json_filepath)
                            
                elif choice == 2:
                    # Convert existing files
                    self.converter_ui.run()
                    
                elif choice == 3:
                    # Exit
                    print("\nThank you for using the Interactive Glowmarkt Client!")
                    break
            
        except Exception as e:
            print(f"Error in interactive client: {str(e)}")