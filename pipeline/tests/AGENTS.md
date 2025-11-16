# AGENTS.md for tests

This document provides an overview of the testing strategy for the data pipeline.

## Overview

The `tests` directory contains all the unit tests for the project. The tests are written using the `pytest` framework and are designed to ensure the reliability and correctness of the pipeline.

## Test Structure

The tests are organized into subdirectories that mirror the structure of the `pipeline` directory. For example, the tests for the `data_retrieval` modules are located in the `tests/data_retrieval` directory.

## Key Components

### `conftest.py`

-   **Purpose:** This file contains shared fixtures and configuration for the tests.
-   **Functionality:** It provides a way to define and share test data, mock objects, and other resources that are used by multiple tests.

## How to Run the Tests

To run all the tests, you can use the following command:

```bash
./go.sh test
```

## How to Add New Tests

To add a new test, you will need to create a new test file in the appropriate subdirectory. The new test file should follow the `pytest` conventions and should include tests for all the new functionality.
