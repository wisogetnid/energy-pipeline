# AGENTS.md for ui

This document provides detailed information about the components in the `ui` directory.

## Overview

The `ui` directory contains the modules that make up the command-line interface (CLI) for the data pipeline. The CLI allows users to interact with the pipeline and execute various tasks, such as retrieving data, converting data formats, and generating visualizations.

## Key Components

### `menu_ui.py`

-   **Purpose:** This is the main entry point for the CLI.
-   **Functionality:** It displays a menu of options to the user and allows them to choose which task they want to perform.

### `data_retrieval_ui.py`

-   **Purpose:** This module provides the UI for the data retrieval functionality.
-   **Functionality:** It automates the selection of resources and time ranges, detecting the latest available local data to fetch only missing records.

### `data_converter_ui.py`

-   **Purpose:** This module provides the UI for the data conversion functionality.
-   **Functionality:** It allows the user to choose which format they want to convert the data to and then initiates the conversion process.

### `visualization_ui.py`

-   **Purpose:** This module provides the UI for the data visualization functionality.
-   **Functionality:** It allows the user to choose which visualization they want to generate and then initiates the visualization process.

### `glowmarkt_interactive.py`

-   **Purpose:** This module provides an interactive CLI for the Glowmarkt API.
-   **Functionality:** It allows users to make direct requests to the API and view the responses.

### `base_ui.py`, `parquet_ui.py`

-   **Purpose:** These modules provide base classes and specialized UI components for the other UI modules.

## How to Add a New UI Component

To add a new UI component, you will need to create a new module in this directory. The new module should contain a class that inherits from one of the base UI classes and implements the necessary functionality for the new component.

## GLOBAL DIRECTIVES (CRITICAL)

Adhere to the **Global Directives** (including the **No Comments Policy**) defined in the root [AGENTS.md](../../AGENTS.md).
