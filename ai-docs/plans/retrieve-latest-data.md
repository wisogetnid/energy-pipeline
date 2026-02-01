# Implementation Plan: "Get Latest Data" Feature

## Context

The goal is to improve the data retrieval process by adding an automated "Get latest data" option. This feature will automatically detect the last date of available local data and fetch only the missing records from the selected provider.

1.  **Selection:** Add a "Get latest data" option to the Time Range Selection in the CLI.
2.  **Detection:** Identify the latest date for which raw data exists in the `/data` folder for the chosen provider (e.g., `data/glowmarkt_api_raw`).
3.  **Retrieval:** Fetch all data from the day after the last local record up to the current date.
4.  **Storage:** Save the new data in monthly files following the existing naming convention.

---

## Implementation Details

### 1. UI Changes
- **Extend Time Range Selection:** In the CLI (likely in `data_retrieval_ui.py`), add "Get latest data" as a new menu option.
- **Trigger Logic:** Upon selection, invoke the date detection service before proceeding to data fetching.

### 2. Determine Last Available Date
- **Provider-Specific Folders:**
  - **Glowmarkt:** `data/glowmarkt_api_raw`
  - **n3rgy:** `data/n3rgy_raw`
- **Latest Date Logic:**
  - Scan folders for files matching patterns like `[resource]_[type]_YYYYMMDD_to_YYYYMMDD.json`.
  - **Cross-Resource Sync:** Identify the latest end date for *all* resource types (electricity consumption/cost, gas consumption/cost). To ensure data integrity, use the **earliest** of these "latest dates" as the global start point for the new fetch.
  - **File Start-Date Pivot:** Once the latest file is identified, use its **start date** as the starting point for the API request. This ensures any partial or corrupted data in the most recent file is corrected.
  - **Fallback:** If no files are found, default to a predefined "earliest date" for the provider.

### 3. Fetch New Data
- **Client Integration:** Reuse existing `GlowmarktClient` and associated logic. API communication, batching, and rate limiting are already handled in the codebase and must be leveraged without modification.
- **Date Range:** 
  - **Start date:** The start date of the latest existing file (to trigger an overwrite/refresh).
  - **End date:** `today`.

### 4. Store Data
- **Format:** Save data in monthly chunks.
- **Overwrite Policy:** If a file with the same name already exists (including the "pivot" file identified in step 2), **always overwrite it** with the fresh data from the API.
- **Naming Convention:** Maintain consistency with existing files (e.g., `electricity_consumption_YYYYMMDD_to_YYYYMMDD.json`).
- **Location:** Write directly to the provider's raw data directory.

### 5. Integration & Error Handling
- **User Feedback:** Display the detected last date and the range being fetched.
- **Error Handling:** If a failure occurs during a multi-month fetch, stop immediately. Inform the user of the error and the last successfully saved file. All data retrieved up to the point of failure must remain saved.
- **Validation:** Add unit tests for the date detection logic and the overwrite behavior.

### 6. Documentation & Constraints
- **n3rgy Provider:** Note that the n3rgy provider is file-based and works differently; the "latest data" logic should be scoped primarily to the Glowmarkt API retrieval flow initially, or adapted to n3rgy's specific file-drop structure.
- Update `AGENTS.md` and CLI help text to reflect the new functionality.

---

## File Changes & Responsibilities

| File Path | Description of Changes |
| :--- | :--- |
| `pipeline/data_retrieval/latest_date_service.py` | **(New File)** Implement `get_latest_available_date(data_dir, resources)` to scan raw data folders, parse filenames, and determine the earliest "start-date pivot" across all selected resources. |
| `pipeline/ui/data_retrieval_ui.py` | Update `select_time_range()` to add "Get latest data" option. Modify `_download_all_resources()` to support fetching data in monthly chunks when the "latest" flag is set. |
| `pipeline/data_retrieval/batch_retrieval.py` | Enhance or add a wrapper to `get_readings_in_batches` to support grouping results by month to facilitate monthly file saving. |
| `pipeline/tests/data_retrieval/test_latest_date_service.py` | **(New File)** Unit tests for the date parsing and cross-resource synchronization logic. |

---

## Status of Clarifications

- **Overwrite Policy:** Confirmed. We will start from the **start date** of the latest file and overwrite existing files to ensure content completeness.
- **API Logic:** Confirmed. Reuse existing client implementation for communication and rate limiting.
- **Failure Mode:** Stop and notify user. Keep data saved up to the failure point.
- **n3rgy:** Recognized as a different file-based flow.
- **Retrospective Changes:** Not a priority; "latest data" from API is the source of truth.