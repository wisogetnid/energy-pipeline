# AGENTS.md for data_visualisation

This document provides detailed information about the components in the `data_visualisation` directory.

## Overview

The `data_visualisation` directory contains modules for generating charts and other visualizations from the processed energy consumption data. These visualizations are designed to help users understand their energy usage patterns and identify areas for improvement.

## Key Components

### `monthly_resource_pair_charts.py`

-   **Purpose:** This module is responsible for generating monthly charts that compare the consumption of two different resources (e.g., gas and electricity).
-   **Functionality:** It takes the processed data and creates a series of charts, with each chart showing the daily consumption of the two resources for a single month.
-   **Usage:** This is useful for comparing the usage of different resources and identifying any correlations between them.

### `energy_efficiency.py`

-   **Purpose:** This module contains functions for analyzing the energy efficiency of a household.
-   **Functionality:** It calculates various metrics, such as the average daily consumption and the peak usage time, and provides recommendations for how to improve energy efficiency.
-   **Usage:** This can be used to identify areas where energy is being wasted and to track the impact of any changes that are made to improve efficiency.

## How to Add a New Visualisation

To add a new visualization, you will need to create a new module in this directory. The new module should contain a function that takes the processed data as input and generates a chart or other visualization.
