import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.ui.menu_ui import MenuUI

@pytest.fixture
def mock_now_val():
    return datetime(2025, 3, 1)

@pytest.fixture
def PatchedDateTime(mock_now_val):
    class _PatchedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(mock_now_val.year, mock_now_val.month, mock_now_val.day)
        
        @classmethod
        def strptime(cls, date_string, format):
            dt = datetime.strptime(date_string, format)
            return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)

        @classmethod
        def fromtimestamp(cls, timestamp, tz=None):
            dt = datetime.fromtimestamp(timestamp, tz)
            return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)
            
    return _PatchedDateTime

@pytest.fixture
def mock_glowmarkt_responses():
    def mock_post(url, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        if url.endswith("/auth"):
            mock_response.json.return_value = {"token": "test-token"}
        return mock_response

    def mock_get(url, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        
        if url.endswith("/virtualentity"):
            mock_response.json.return_value = [
                {"veId": "ve-123", "name": "Test Home"}
            ]
        elif "/virtualentity/ve-123/resources" in url:
            mock_response.json.return_value = {
                "resources": [
                    {"resourceId": "res-elec-con", "name": "Electricity consumption", "baseUnit": "kWh", "classifier": "electricity.consumption"},
                    {"resourceId": "res-elec-cost", "name": "Electricity cost", "baseUnit": "GBP", "classifier": "electricity.cost"},
                    {"resourceId": "res-gas-con", "name": "Gas consumption", "baseUnit": "kWh", "classifier": "gas.consumption"},
                    {"resourceId": "res-gas-cost", "name": "Gas cost", "baseUnit": "GBP", "classifier": "gas.cost"},
                ]
            }
        elif "/readings" in url:
            params = kwargs.get("params", {})
            from_date = params.get("from", "")
            
            if "2025-01-01" in from_date:
                readings = [
                    [1735689600000, 1.1], # 2025-01-01 00:00:00 UTC
                    [1735691400000, 1.2], # 2025-01-01 00:30:00 UTC
                ]
            elif "2025-02-01" in from_date:
                readings = [
                    [1738368000000, 2.1], # 2025-02-01 00:00:00 UTC
                    [1738369800000, 2.2], # 2025-02-01 00:30:00 UTC
                ]
            else:
                readings = []
            
            mock_response.json.return_value = {"data": readings}
        
        return mock_response

    with patch("requests.post", side_effect=mock_post), \
         patch("requests.get", side_effect=mock_get):
        yield

def test_glowmarkt_happy_path(tmp_path, monkeypatch, mock_glowmarkt_responses, PatchedDateTime):
    monkeypatch.chdir(tmp_path)
    
    monkeypatch.setenv("GLOWMARKT_USERNAME", "testuser")
    monkeypatch.setenv("GLOWMARKT_PASSWORD", "testpass")
    
    with patch("pipeline.ui.data_retrieval_ui.datetime", PatchedDateTime), \
         patch("pipeline.data_retrieval.batch_retrieval.datetime", PatchedDateTime), \
         patch("pipeline.data_processing.jsonl_converter.datetime", PatchedDateTime), \
         patch("pipeline.ui.data_retrieval_ui.get_latest_available_date") as mock_latest:
        
        mock_latest.return_value = PatchedDateTime(2025, 1, 1)
        
        # Menu Sequence:
        # 1. Retrieve Data
        # 1. Use Glowmarkt Client
        # "" Press Enter after success
        # 2. Convert Data
        # 1. Combine Raw .json Resources
        # 1. Select glowmarkt_api_raw directory
        # n. Convert to Parquet? n
        # "" Press Enter after success
        # 5. Exit
        
        inputs = ["1", "1", "", "2", "1", "1", "n", "", "5"]
        
        with patch("builtins.input", side_effect=inputs):
            ui = MenuUI()
            ui.run()

    raw_dir = tmp_path / "data" / "glowmarkt_api_raw"
    processed_dir = tmp_path / "data" / "processed"
    
    assert raw_dir.exists()
    assert processed_dir.exists()
    
    raw_files = list(raw_dir.glob("*.json"))
    assert len(raw_files) == 8
    
    processed_files = list(processed_dir.glob("*.jsonl"))
    assert len(processed_files) == 2
    
    jan_file = processed_dir / "all_resources_20250101_to_20250131.jsonl"
    assert jan_file.exists()
    
    with open(jan_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2
        
        first_record = json.loads(lines[0])
        assert "electricity_consumption" in first_record
        assert "electricity_cost" in first_record
        assert "gas_consumption" in first_record
        assert "gas_cost" in first_record
        assert first_record["electricity_consumption"] == 1.1
        assert first_record["gas_consumption"] == 1.1

    feb_file = processed_dir / "all_resources_20250201_to_20250228.jsonl"
    assert feb_file.exists()
    
    with open(feb_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2
        
        first_record = json.loads(lines[0])
        assert first_record["electricity_consumption"] == 2.1
        assert first_record["gas_consumption"] == 2.1
