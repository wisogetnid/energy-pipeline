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
