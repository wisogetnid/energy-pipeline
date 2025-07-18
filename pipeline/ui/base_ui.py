#!/usr/bin/env python

import os
import sys
from datetime import datetime

class BaseUI:
    
    def __init__(self):
        self.header_width = 36
    
    def print_header(self, title):
        print(f"\n{'=' * self.header_width}")
        print(f"{title.center(self.header_width)}")
        print(f"{'=' * self.header_width}")
    
    def get_int_input(self, prompt, min_val, max_val):
        while True:
            try:
                choice = int(input(prompt))
                if min_val <= choice <= max_val:
                    return choice
                else:
                    print(f"Please enter a number between {min_val} and {max_val}")
            except ValueError:
                print("Please enter a valid number")

    def get_choice(self, options: dict) -> str:
        while True:
            for key, value in options.items():
                print(f"{key}. {value}")
            choice = input("\nEnter your choice: ")
            if choice in options:
                return choice
            else:
                print("Invalid choice. Please try again.")
    
    def get_yes_no_input(self, prompt):
        response = input(f"{prompt} (y/n): ")
        return response.lower() == 'y'
    
    def ensure_directory(self, directory):
        os.makedirs(directory, exist_ok=True)
        return directory