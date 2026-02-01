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
-   **Usage:** The `N3rgyCsvClient` takes a directory of CSV files and processes them.

### `batch_retrieval.py`

-   **Purpose:** This module contains logic for retrieving data in batches.
-   **Functionality:** It handles large data requests by breaking them down into smaller, more manageable chunks.
-   **Usage:** The `BatchRetriever` class can be used to fetch data for long periods without hitting API limits or timeouts.

### `latest_date_service.py`

-   **Purpose:** Provides a service to detect the latest available local data.
-   **Functionality:** Scans raw data directories to find the most recent file for each resource and determines a safe starting point ("pivot") for fetching new data.
-   **Usage:** Used by the UI to implement the "Get latest data" feature.

## GLOBAL DIRECTIVES (CRITICAL)

Adhere to the **Global Directives** (including the **No Comments Policy**) defined in the root [AGENTS.md](../../AGENTS.md).

## How to Add a New Data Source

To add a new data source, you will need to create a new client in this directory. The new client should follow a similar structure to the existing clients and implement a consistent interface for fetching data.
