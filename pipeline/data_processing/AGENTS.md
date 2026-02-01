# AGENTS.md for data_processing

This document provides detailed information about the components in the `data_processing` directory.

## Overview

The `data_processing` directory contains modules for converting the raw energy consumption data into different formats. These formats are designed to be more efficient for storage and analysis.

## Key Components

### `jsonl_converter.py`

-   **Purpose:** This module is responsible for converting the raw data into the JSONL (JSON Lines) format.
-   **Functionality:** It takes the structured data object from the data retrieval step and writes it to a file, with each line in the file being a valid JSONL object.
-   **Example:** Following two lines in the unified .jsonl file: `{"period": "PT30M", "from_date": "2024-02-01T00:00:00.000Z", "to_date": "2024-02-29T23:59:59.999Z", "gas_consumption_id": "n3rgy-gas", "gas_consumption_name": "gas consumption", "gas_consumption_unit": "kWh", "gas_consumption_classifier": "gas.consumption", "gas_cost_id": "n3rgy-gas-cost", "gas_cost_name": "gas cost", "gas_cost_unit": "pence", "gas_cost_classifier": "gas.consumption.cost", "electricity_consumption_id": "n3rgy-electricity", "electricity_consumption_name": "electricity consumption", "electricity_consumption_unit": "kWh", "electricity_consumption_classifier": "electricity.consumption", "electricity_cost_id": "n3rgy-electricity-cost", "electricity_cost_name": "electricity cost", "electricity_cost_unit": "pence", "electricity_cost_classifier": "electricity.consumption.cost", "timestamp": 1707892200, "timestamp_iso": "2024-02-14T06:30:00", "gas_consumption": 5.3808508, "gas_cost": 30.019766609999998}
{"period": "PT30M", "from_date": "2024-02-01T00:00:00.000Z", "to_date": "2024-02-29T23:59:59.999Z", "gas_consumption_id": "n3rgy-gas", "gas_consumption_name": "gas consumption", "gas_consumption_unit": "kWh", "gas_consumption_classifier": "gas.consumption", "gas_cost_id": "n3rgy-gas-cost", "gas_cost_name": "gas cost", "gas_cost_unit": "pence", "gas_cost_classifier": "gas.consumption.cost", "electricity_consumption_id": "n3rgy-electricity", "electricity_consumption_name": "electricity consumption", "electricity_consumption_unit": "kWh", "electricity_consumption_classifier": "electricity.consumption", "electricity_cost_id": "n3rgy-electricity-cost", "electricity_cost_name": "electricity cost", "electricity_cost_unit": "pence", "electricity_cost_classifier": "electricity.consumption.cost", "timestamp": 1707894000, "timestamp_iso": "2024-02-14T07:00:00", "gas_consumption": 4.0496216, "gas_cost": 22.592838909999998}
`
-   **Usage:** This is useful for creating a simple, line-delimited log of the energy consumption data.

### `parquet_converter.py`

-   **Purpose:** This module converts the raw data into the Apache Parquet format.
-   **Functionality:** Parquet is a columnar storage format that is optimized for use with big data processing frameworks. This module takes the structured data object and writes it to a Parquet file.
-   **Usage:** This format is ideal for large-scale data analysis and can be used with tools like Apache Spark and Pandas.

### `yearly_jsonl_converter.py`

-   **Purpose:** This module is a specialized version of the JSONL converter that splits the data into separate files for each year.
-   **Functionality:** It takes the structured data object and groups the data by year before writing it to a separate JSONL file for each year.
-   **Usage:** This is useful for organizing the data into more manageable chunks and can make it easier to perform year-over-year analysis.

## How to Add a New Data Format

To add a new data format, you will need to create a new converter module in this directory. The new module should follow a similar structure to the existing converters and implement a consistent interface for converting the data.

## GLOBAL DIRECTIVES (CRITICAL)

Adhere to the **Global Directives** (including the **No Comments Policy**) defined in the root [AGENTS.md](../../AGENTS.md).
