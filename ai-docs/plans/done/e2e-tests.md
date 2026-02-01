# E2E Test Implementation Plan - Glowmarkt Happy Path

## Goal
Implement an end-to-end test that simulates a user interacting with the CLI to retrieve energy data from the Glowmarkt API and convert it into processed formats.

## Global Directives
- No comments in code.
- Use `./go.sh` for all interactions.
- Mock Glowmarkt API.
- Use 2 records/entries where multiple are needed.
- Use temporary directories for data to avoid pollution.

## Proposed Scenario: Happy Path Data Retrieval & Conversion
1. **Setup:**
   - Use `pytest`'s `tmp_path` fixture to manage a isolated test environment.
   - Mock `pathlib.Path` or change the working directory to `tmp_path` to ensure hardcoded `data/` paths point to the temporary test directory.
   - Mock `datetime.datetime` (specifically `now()`) to return a static date (e.g., `2025-03-01`) to control the automatic date range detection and limit API batching.
   - Mock Glowmarkt API responses with 2 months of data (e.g., January and February 2025), and 2 readings per month per resource.
   - Mock `input()` to drive the CLI with the sequence below.
   - Set dummy credentials via environment variables.
2. **Execution:**
   - Start `pipeline.__main__`.
   - **Menu Sequence:**
     - `1`: Select "Retrieve Data"
     - `1`: Select "Use Glowmarkt Client"
     - `""`: Press Enter after successful retrieval (Wait for user)
     - `2`: Select "Convert Data"
     - `1`: Select "Combine Raw .json Resources to Monthly .jsonl Files"
     - `1`: Select source directory (e.g., `glowmarkt_api_raw`)
     - `n`: Select "n" when asked "Convert to Parquet?" (Focus on JSON path only)
     - `""`: Press Enter after successful conversion (Wait for user)
     - `5`: Select "Exit"
3. **Verification:**
   - Check that raw JSON files exist in the temp `data/glowmarkt_api_raw` directory.
   - Check that combined JSONL files exist in the temp `data/processed` directory.
   - Verify that JSONL files contain exactly 2 records (lines) per month.
   - Verify that both Electricity and Gas resources (if mocked) are merged into the same JSONL records.

## Technical Details
- **Test File:** `pipeline/tests/e2e/test_glowmarkt_happy_path.py`
- **Mocks:**
  - `unittest.mock.patch('builtins.input')` for UI interaction.
  - `unittest.mock.patch` on `requests` for API calls.
  - `unittest.mock.patch('pipeline.ui.data_retrieval_ui.datetime')` and other modules to return a static `now()`.
  - `monkeypatch` to change `os.getcwd()` or mock `Path` to redirect `data/` to `tmp_path`.
- **Environment Variables:**
  - `GLOWMARKT_USERNAME`, `GLOWMARKT_PASSWORD`, etc. for credentials.
- **Data Cleanup:** `tmp_path` handles deletion automatically.