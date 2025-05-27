#!/usr/bin/env python
"""Base UI components for interactive modules."""

import os
import sys
from datetime import datetime

class BaseUI:
    """Base class for UI components with common utilities."""
    
    def __init__(self):
        """Initialize base UI component."""
        self.header_width = 36
    
    def print_header(self, title):
        """Print a formatted header."""
        print(f"\n{'=' * self.header_width}")
        print(f"{title.center(self.header_width)}")
        print(f"{'=' * self.header_width}")
    
    def get_int_input(self, prompt, min_val, max_val):
        """Get integer input within range."""
        while True:
            try:
                choice = int(input(prompt))
                if min_val <= choice <= max_val:
                    return choice
                else:
                    print(f"Please enter a number between {min_val} and {max_val}")
            except ValueError:
                print("Please enter a valid number")
    
    def get_yes_no_input(self, prompt):
        """Get yes/no input from user."""
        response = input(f"{prompt} (y/n): ")
        return response.lower() == 'y'
    
    def ensure_directory(self, directory):
        """Ensure directory exists."""
        os.makedirs(directory, exist_ok=True)
        return directory