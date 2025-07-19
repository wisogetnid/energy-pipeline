# Project Overview

This project is a Python-based data pipeline for retrieving, processing, and visualizing energy data from the Hildebrand Glowmarkt API. It allows users to fetch their energy consumption data, convert it into different formats (JSONL, Parquet), and generate visualizations to analyze energy usage patterns.

# Getting Started

## Environment Setup

1.  **Python Version:** The project uses Python 3.8 or higher. It is recommended to use a version manager like `asdf` to manage Python versions. The `.tool-versions` file specifies the exact Python version.
2.  **Virtual Environment:** Use a virtual environment to manage project dependencies. Create a new environment by running:
    ```bash
    python -m venv .venv
    ```
    Activate the environment with:
    ```bash
    source .venv/bin/activate
    ```
3.  **Install Dependencies:** Install the required dependencies from `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

The main application can be run using the following command:

```bash
python -m pipeline
```

This will launch an interactive command-line interface (CLI) that allows you to choose from various options, such as retrieving data, converting data formats, and visualizing data.

# Available Commands

The `justfile` in the root directory provides a convenient way to run common tasks. Use `just` to see a list of available commands.

-   `just install`: Install project dependencies.
-   `just test`: Run all unit tests.
-   `just test-coverage`: Run tests with a coverage report.
-   `just lint`: Lint the codebase using `pylint`.
-   `just format`: Format the code using `black`.
-   `just run`: Run the main energy pipeline.
-   `just clean`: Remove temporary files and directories.

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

# Inconsistencies and Suggested Improvements

## Dependency Management

The project has dependencies listed in both `requirements.txt` and `pyproject.toml`. This can lead to inconsistencies.

-   **`requirements.txt`:**
    ```
    requests
    pytest
    pandas
    requests
    pyarrow
    fastparquet
    python-dotenv
    python-dateutil
    ```
-   **`pyproject.toml`:**
    ```toml
    dependencies = [
        "pandas",
        "pyarrow",
        "python-dotenv",
        "matplotlib",
        "requests",
        "python-dateutil",
    ]
    [project.optional-dependencies]
    dev = [
        "pytest",
        "pytest-cov",
    ]
    ```

**Inconsistencies:**

-   `requests` is listed twice in `requirements.txt`.
-   `fastparquet` is in `requirements.txt` but not in `pyproject.toml`.
-   `matplotlib` is in `pyproject.toml` but not in `requirements.txt`.
-   `pytest-cov` is a development dependency in `pyproject.toml` but not included in `requirements.txt`.

**Suggested Tasks:**

1.  **Consolidate Dependencies:** Choose a single source of truth for dependencies. Given the use of `pyproject.toml` for project metadata, it is recommended to manage all dependencies there.
2.  **Remove `requirements.txt`:** Once all dependencies are moved to `pyproject.toml`, delete `requirements.txt` to avoid confusion.
3.  **Update Installation Instructions:** Modify the documentation to use `pip install .` or `pip install -e .` for installing dependencies from `pyproject.toml`.

## Code Duplication

There may be opportunities to reduce code duplication in the data visualization scripts. For example, the `visualize-all` command in the `justfile` suggests that similar processing steps are applied to multiple files.

**Suggested Task:**

-   **Refactor Visualization Code:** Create a reusable function that encapsulates the common data loading, processing, and visualization logic. This function can then be called with different data files as input.

## Testing

The project has a good test structure, with separate test modules for data processing and data retrieval. However, the overall test coverage is unknown.

**Suggested Tasks:**

1.  **Measure Test Coverage:** Use a tool like `pytest-cov` to measure the test coverage of the project. The `test-coverage` command is already available in the `justfile`.
2.  **Increase Test Coverage:** Based on the coverage report, identify areas with low test coverage and add new tests to improve it.
