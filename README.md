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

The project uses [mise](https://mise.jdx.dev/) to pin tool versions and run common tasks. Run `mise tasks` to see a list of available tasks.

### Environment Setup

1.  **Tool Versions:** The `[tools]` section in `mise.toml` pins the exact Python and uv versions. Install them by running:
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

### Testing the Application

Use `mise run test` to run the tests, `mise run lint` to run the linter and `mise run check` to run tests and linting together.