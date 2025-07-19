# Project Overview

This project is a Python-based data pipeline for retrieving, processing, and visualizing energy data from the Hildebrand Glowmarkt API. It allows users to fetch their energy consumption data, convert it into different formats (JSONL, Parquet), and generate visualizations to analyze energy usage patterns.

# Getting Started

## Environment Setup

1.  **Python Version:** The project uses Python 3.8 or higher. It is recommended to use a version manager like `asdf` to manage Python versions. The `.tool-versions` file specifies the exact Python version.
2.  **Virtual Environment:** Use a virtual environment to manage project dependencies. Create a new environment by running:
    ```bash
    ./go create-env
    ```
    Activate the environment with:
    ```bash
    source .venv/bin/activate
    ```
3.  **Install Dependencies:** Install the required dependencies from `requirements.txt`:
    ```bash
    ./go install
    ```

## Running the Application

The main application can be run using the following command:

```bash
./go run
```

This will launch an interactive command-line interface (CLI) that allows you to choose from various options, such as retrieving data, converting data formats, and visualizing data.

# Available Commands

The `go.sh` script in the root directory provides a convenient way to run common tasks. Run go without arguments to see a list of available commands:

`./go.sh install` - Install required dependencies
`./go.sh run` - Run the complete energy pipeline
`./go.sh test` - Run all unit tests
`./go.sh test-coverage` - Run tests with coverage report
`./go.sh clean` - Clean up generated files
`./go.sh create-env` - Create a new virtual environment
`./go.sh lint` - Lint code with pylint
`./go.sh format` - Format code with black
`./go.sh check` - Run tests and lint code

 You might need to execute `chmod +x go.sh` to make the `go.sh` script executable.

# Code Style and Conventions

-   **Formatting:** The project uses `black` for code formatting. Please run `just format` before committing any changes.
-   **Linting:** `pylint` is used for linting. Run `just lint` to check for any linting errors.
-   **Dependencies:** Project dependencies are managed in `requirements.txt` and `pyproject.toml`. Please keep these files in sync.

# Key Components

-   `pipeline/data_retrieval`: Contains modules for fetching data from the Glowmarkt API.
-   `pipeline/data_processing`: Includes scripts for converting data into different formats (e.g., JSONL, Parquet).
-   `pipeline/data_visualisation`: Contains modules for generating charts and visualizations of the energy data.
-   `pipeline/ui`: Provides the command-line interface for interacting with the pipeline.
-   `pipeline/tests`: Contains all the unit tests for the project.
-   `justfile`: Defines the commands for managing the project.
-   `pyproject.toml`: Specifies the project metadata and dependencies.
-   `requirements.txt`: Lists the project dependencies.