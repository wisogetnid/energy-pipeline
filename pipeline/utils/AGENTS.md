# AGENTS.md for utils

This document provides detailed information about the components in the `utils` directory.

## Overview

The `utils` directory contains shared utilities and helper functions that are used across the data pipeline.

## Key Components

### `credentials.py`

-   **Purpose:** This module is responsible for managing the credentials for the external APIs.
-   **Functionality:** It provides a secure way to store and access the API keys and other sensitive information. It reads the credentials from a configuration file and makes them available to the rest of the application.
-   **Usage:** The functions in this module can be used to get the credentials for a specific API.

## GLOBAL DIRECTIVES (CRITICAL)

Adhere to the **Global Directives** (including the **No Comments Policy**) defined in the root [AGENTS.md](../../AGENTS.md).

## How to Add a New Utility

To add a new utility, you can create a new module in this directory or add a new function to an existing module. If the new utility requires any sensitive information, be sure to add it to the credentials file and not to the source code.
