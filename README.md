# Energy pipeline

A small CLI app to collect energy data from different providers, convert their data into a shared data format and create visualisations from the data.

It's also a playground to explore and refine my agentic coding setup.

## Currently Supported Providers

- Hildebrand Glowmarkt API
- n3rgy .json files (downloaded from their personal account)

## Tools used

- [Bruno](https://www.usebruno.com/) scripts to manually explore the API
- Python for the data pipeline

## Getting Started

The `go.sh` script in the root directory provides a convenient way to run common tasks. Run go without arguments to see a list of available commands

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

### Testing the Application

The `go.sh` command has a few arguments to run the tests, run the linter and `go.sh check` to run tests and linting together.