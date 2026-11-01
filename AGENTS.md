# AGENTS.md

This document provides instructions and guidelines for AI agents working on this codebase.

## GLOBAL DIRECTIVES (CRITICAL)

### No Comments Policy
Use **highly expressive and descriptive** variable and function names. Comments and docstrings **MUST be avoided entirely** in favor of self-documenting code. If you feel the need to add a comment, refactor the code to be more readable instead. This is a strict, non-negotiable directive.

### Command Constraints
**AI Agents MUST ALWAYS use the `mise` tasks for all interactions with the application, environment, and development tools.**

-   **Testing:** Use `mise run test` instead of `pytest`.
-   **Execution:** Use `mise run run` instead of `python -m pipeline`.
-   **Linting/Formatting:** Use `mise run lint` or `mise run format` instead of calling `pylint` or `black` directly.
-   **Setup:** Use `mise install` and `mise run install` for environment management.

Directly executing the underlying tools is prohibited as the `mise` tasks ensure the pinned tool versions, paths, and configurations are applied.

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

1.  **Tool Versions:** The project uses [mise](https://mise.jdx.dev/) to manage tool versions. The `[tools]` section in `mise.toml` pins the exact Python and uv versions. Install them by running:
    ```bash
    mise install
    ```
2.  **Install Dependencies:** Install the required dependencies from `pyproject.toml`. uv creates and manages the `.venv` virtual environment automatically:
    ```bash
    mise run install
    ```

### Running the Application

The main application can be run using the following command:

```bash
mise run run
```

This will launch an interactive command-line interface (CLI) that allows you to choose from various options, such as retrieving data, converting data formats, and visualizing data.

## Available Commands

The `mise.toml` file in the root directory defines the tasks for common development work. Run `mise tasks` to see a list of available tasks:

-   `mise install` - Install the pinned Python and uv versions
-   `mise run install` - Install required dependencies
-   `mise run run` - Run the complete energy pipeline
-   `mise run test` - Run all unit tests
-   `mise run test-coverage` - Run tests with coverage report
-   `mise run clean` - Clean up generated files
-   `mise run lint` - Lint code with pylint
-   `mise run format` - Format code with black
-   `mise run check` - Run tests and lint code

## Code Style and Conventions

-   **Formatting:** The project uses `black` for code formatting. Please run `mise run format` before committing any changes.
-   **Linting:** `pylint` is used for linting. Run `mise run lint` to check for any linting errors.
-   **Dependencies:** Project dependencies are managed in `pyproject.toml`.
-   **Testing:** The project uses `pytest` for unit testing. Tests are located in the `tests` directory. Run `mise run test` to execute all tests.
-   **Code Conventions:** Follow PEP 8 guidelines for Python code.

## Key Components

-   `pipeline/data_retrieval`: Contains modules for fetching data from the Glowmarkt API.
-   `pipeline/data_processing`: Includes scripts for converting data into different formats (e.g., JSONL, Parquet).
-   `pipeline/data_visualisation`: Contains modules for generating charts and visualizations of the energy data.
-   `pipeline/ui`: Provides the command-line interface for interacting with the pipeline.
-   `pipeline/tests`: Contains all the unit tests for the project.
-   `mise.toml`: Pins tool versions and defines the tasks for managing the project.
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
