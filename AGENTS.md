# AGENTS.md

This document provides instructions and guidelines for AI agents working on this codebase.

## CRITICAL: Agent Command Constraints

**AI Agents MUST ALWAYS use the `./go.sh` script for all interactions with the application, environment, and development tools.**

-   **Testing:** Use `./go.sh test` instead of `pytest`.
-   **Execution:** Use `./go.sh run` instead of `python -m pipeline`.
-   **Linting/Formatting:** Use `./go.sh lint` or `./go.sh format` instead of calling `pylint` or `black` directly.
-   **Setup:** Use `./go.sh install` or `./go.sh create-env` for environment management.

Directly executing the underlying tools is prohibited as the `go.sh` script ensures correct environment variables, paths, and configurations are applied.

## Project Overview

This project is a Python-based data pipeline for retrieving, processing, and visualizing energy data from different data sources. It allows users to fetch their energy consumption data, convert it into different formats (JSONL, Parquet), and generate visualizations to analyze energy usage patterns. The pipeline is designed to be modular and extensible, allowing for the addition of new data sources, processing steps, and visualizations.

### Supported data sources

Currently supported data sources are
- Hildebrand Glowmarkt API
- n3rgy (personal) data in .csv/.json format

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

### Environment Setup

1.  **Python Version:** The project uses Python 3.8 or higher. It is recommended to use a version manager like `asdf` to manage Python versions. The `.tool-versions` file specifies the exact Python version.
2.  **Virtual Environment:** Use a virtual environment to manage project dependencies. Create a new environment by running:
    ```bash
    ./go.sh create-env
    ```
    Activate the environment with:
    ```bash
    source .venv/bin/activate
    ```
3.  **Install Dependencies:** Install the required dependencies from `pyproject.toml`:
    ```bash
    ./go.sh install
    ```

### Running the Application

The main application can be run using the following command:

```bash
./go.sh run
```

This will launch an interactive command-line interface (CLI) that allows you to choose from various options, such as retrieving data, converting data formats, and visualizing data.

## Available Commands

The `go.sh` script in the root directory provides a convenient way to run common tasks. Run go without arguments to see a list of available commands:

-   `./go.sh install` - Install required dependencies
-   `./go.sh run` - Run the complete energy pipeline
-   `./go.sh test` - Run all unit tests
-   `./go.sh test-coverage` - Run tests with coverage report
-   `./go.sh clean` - Clean up generated files
-   `./go.sh create-env` - Create a new virtual environment
-   `./go.sh lint` - Lint code with pylint
-   `./go.sh format` - Format code with black
-   `./go.sh check` - Run tests and lint code

You might need to execute `chmod +x go.sh` to make the `go.sh` script executable.

## Code Style and Conventions

-   **Formatting:** The project uses `black` for code formatting. Please run `./go.sh format` before committing any changes.
-   **Linting:** `pylint` is used for linting. Run `./go.sh lint` to check for any linting errors.
-   **Dependencies:** Project dependencies are managed in `pyproject.toml`.
-   **Testing:** The project uses `pytest` for unit testing. Tests are located in the `tests` directory. Run `./go.sh test` to execute all tests.
-   **Code Conventions:** Follow PEP 8 guidelines for Python code. Use meaningful variable and function names instead of comments or docstrings.

## Key Components

-   `pipeline/data_retrieval`: Contains modules for fetching data from the Glowmarkt API.
-   `pipeline/data_processing`: Includes scripts for converting data into different formats (e.g., JSONL, Parquet).
-   `pipeline/data_visualisation`: Contains modules for generating charts and visualizations of the energy data.
-   `pipeline/ui`: Provides the command-line interface for interacting with the pipeline.
-   `pipeline/tests`: Contains all the unit tests for the project.
-   `go.sh`: Defines the commands for managing the project.
-   `pyproject.toml`: Specifies the project metadata and dependencies.

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
