# AGENTS.md

This document provides instructions and guidelines for AI agents working on this codebase.

## Project Overview

This project is a Python-based data pipeline for retrieving, processing, and visualizing energy data. It is designed to be modular and extensible, allowing for the addition of new data sources, processing steps, and visualizations.

## High-Level Architecture

The pipeline is divided into the following key components:

-   **Data Retrieval:** Fetches data from external sources like the Hildebrand Glowmarkt API.
-   **Data Processing:** Transforms and enriches the raw data.
-   **Data Visualisation:** Generates charts and graphs to help users understand their energy consumption.
-   **UI:** A command-line interface for interacting with the pipeline.
-   **Utils:** Shared utilities and helper functions.
-   **Tests:** Unit tests to ensure the reliability of the pipeline.

For a visual representation of the architecture, please refer to the diagram in `docs/high-level-architecture.png`.

## Getting Started

To get started with the project, please refer to the instructions in `copilot-instructions.md`. This file provides a comprehensive guide on how to set up the environment, install dependencies, and run the application.

## How to Contribute

The following is a step-by-step guide for adding a new data source to the pipeline.

### Step 1: Create a New Data Retrieval Client

Create a new Python module in the `pipeline/data_retrieval` directory. This module should contain a class that inherits from a base client (if one exists) or implements a similar interface to the existing clients. The new client should be responsible for fetching data from the new data source and returning it in a consistent format.

### Step 2: Integrate the New Client into the Pipeline

Modify the `pipeline/__main__.py` file to include the new data source as an option in the CLI. This will involve adding a new command or a new option to an existing command.

### Step 3: Add Data Processing Logic

If the new data source requires any special processing, add a new module to the `pipeline/data_processing` directory. This module should contain functions for transforming the data into the desired format.

### Step 4: Add a New Visualisation (Optional)

If you want to add a new visualization for the new data source, create a new module in the `pipeline/data_visualisation` directory. This module should contain a function that generates a chart or graph from the processed data.

### Step 5: Add Unit Tests

Create a new test file in the `tests` directory to test the new data retrieval client, processing logic, and visualization. This will ensure that the new functionality is working as expected and does not introduce any regressions.

## Detailed Documentation

For more detailed information on each component, please refer to the `AGENTS.md` files in the following directories:

-   [`pipeline/data_retrieval/AGENTS.md`](pipeline/data_retrieval/AGENTS.md)
-   [`pipeline/data_processing/AGENTS.md`](pipeline/data_processing/AGENTS.md)
-   [`pipeline/data_visualisation/AGENTS.md`](pipeline/data_visualisation/AGENTS.md)
-   [`pipeline/ui/AGENTS.md`](pipeline/ui/AGENTS.md)
-   [`pipeline/utils/AGENTS.md`](pipeline/utils/AGENTS.md)
-   [`pipeline/tests/AGENTS.md`](pipeline/tests/AGENTS.md)
