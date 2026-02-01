# Implementation Plan: Simplify Monthly to Yearly Conversion

## Context
The goal is to streamline the "Monthly to Yearly" conversion process in the data converter UI. The user wants to remove manual steps and always use the default directory.

1.  **Always use default directory:** Remove the prompt that asks the user to select a directory.
2.  **Remove "Convert to Yearly JSONL" step:** This refers to the explicit UI step/header and the manual confirmation to convert to Parquet. The process should be a single, automated action.

---

## Implementation Details

### 1. UI Simplification in `data_converter_ui.py`
- **Remove Header:** Remove `self.print_header("Convert to Yearly JSONL")` in `run_yearly_conversion`.
- **Remove Directory Selection:** In `run_yearly_conversion`, replace `self.get_directory()` with a direct reference to `self.data_dir`.
- **Automate Parquet Conversion:** 
  - Remove the `get_yes_no_input` prompt asking if the user wants to convert to Parquet.
  - Automatically call the Parquet conversion logic after the yearly JSONL files are created.
- **Cleanup Output:** Ensure the process clearly communicates its progress without redundant headers.

### 2. Menu Update in `menu_ui.py`
- **Rename Option:** Update the label for Option 2 in `convert_data_menu` to better reflect the new automated process (e.g., "Convert Monthly JSONL Files to Yearly Parquet").

---

## File Changes

| File Path | Description of Changes |
| :--- | :--- |
| `pipeline/ui/data_converter_ui.py` | Modify `run_yearly_conversion` to remove the header, skip directory selection, and automate Parquet conversion. |
| `pipeline/ui/menu_ui.py` | Update menu text for Option 2 in `convert_data_menu`. |

---

## Status of Clarifications
- **Yearly JSONL files:** We will still create these files in the background because they are required by the visualization modules (as noted in `menu_ui.py`'s error messages). However, the "step" of doing it will no longer be presented as a separate manual action or header.
- **Default Directory:** `data/processed` will be used as the source for monthly JSONL files and the destination for yearly JSONL files. Parquet files will continue to go to `data/parquet` as per `JsonlToParquetConverter` default.
