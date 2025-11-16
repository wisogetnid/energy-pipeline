# AGENTS.md for data_retrieval

This document provides detailed information about the components in the `data_retrieval` directory.

## Overview

The `data_retrieval` directory contains modules responsible for fetching energy consumption data from various external sources. Each module is designed to handle the specific API or data format of a single source.

## Key Components

### `glowmarkt_client.py`

-   **Purpose:** This module contains the `GlowmarktClient` class, which is responsible for interacting with the Hildebrand Glowmarkt API.
-   **Functionality:** It handles authentication, makes requests to the API, and retrieves energy consumption data for a specified period.
-   **Usage:** To use the `GlowmarktClient`, you need to provide it with valid API credentials. The client will then be able to fetch data and return it as a structured data object.

### `n3rgy_csv_client.py`

-   **Purpose:** This module provides the `N3rgyCsvClient` class, which is designed to parse and import energy consumption data from CSV files provided by N3rgy.
-   **Functionality:** It reads the CSV files, extracts the relevant data, and transforms it into a format that is consistent with the rest of the pipeline.
-   **Usage:** The `N3rgyCsvClient` takes a file path to a CSV file as input and returns a structured data object.

### `batch_retrieval.py`

-   **Purpose:** This module contains logic for retrieving data in batches.
-   **Functionality:** It is designed to handle large data requests by breaking them down into smaller, more manageable chunks. This helps to avoid timeouts and other issues that can occur when retrieving a large amount of data at once.
-   **Usage:** The functions in this module can be used in conjunction with the data retrieval clients to fetch data for a long period.

## How to Add a New Data Source

To add a new data source, you will need to create a new client in this directory. The new client should follow a similar structure to the existing clients and implement a consistent interface for fetching data.
