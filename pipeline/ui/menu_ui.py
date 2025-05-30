#!/usr/bin/env python

from pipeline.ui.base_ui import BaseUI
from pipeline.ui.data_retrieval_ui import DataRetrievalUI
from pipeline.ui.data_converter_ui import DataConverterUI

class MenuUI(BaseUI):
    
    def __init__(self, username=None, password=None, token=None):
        super().__init__()
        self.username = username
        self.password = password
        self.token = token
        self.retrieval_ui = DataRetrievalUI()
        self.converter_ui = DataConverterUI()
        
        if username or token:
            self.retrieval_ui.setup_client(username, password, token)
    
    def run(self):
        try:
            while True:
                self.print_header("Interactive Glowmarkt Client")
                print("\nWhat would you like to do?")
                print("1. Retrieve new data from Glowmarkt API")
                print("2. Convert existing JSON files to JSONL format")
                print("3. Exit")
                
                menu_choice = self.get_int_input("\nEnter your choice: ", 1, 3)
                
                if menu_choice == 1:
                    json_filepath = self.retrieval_ui.run()
                    
                    if json_filepath:
                        if self.get_yes_no_input("\nWould you like to convert this data to JSONL format?"):
                            self.converter_ui.convert_to_jsonl(json_filepath)
                            
                elif menu_choice == 2:
                    self.converter_ui.run()
                    
                elif menu_choice == 3:
                    print("\nThank you for using the Interactive Glowmarkt Client!")
                    break
            
        except Exception as e:
            print(f"Error in interactive client: {str(e)}")