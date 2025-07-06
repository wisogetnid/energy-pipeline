import json
import os
import pytest
from pathlib import Path

def load_fixture(filename):
    fixture_path = Path(__file__).parent / 'fixtures' / filename
    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def electricity_consumption_data():
    return {
        "resource_id": "04678775-6c72-43c9-8378-c9914756384a",
        "resource_name": "electricity consumption",
        "resource_unit": "kWh",
        "resource_classifier": "electricity.consumption",
        "start_date": "2025-02-01T00:00:00",
        "end_date": "2025-02-28T00:00:00",
        "period": "PT30M",
        "timezone_offset": 0,
        "readings": [
            [1738368000, 0.047],
            [1738369800, 0.059],
            [1738371600, 0.039]
        ]
    }

@pytest.fixture
def electricity_cost_data():
    return {
        "resource_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
        "resource_name": "electricity cost",
        "resource_unit": "pence",
        "resource_classifier": "electricity.consumption.cost",
        "start_date": "2025-01-01T00:00:00",
        "end_date": "2025-01-31T00:00:00",
        "period": "PT30M",
        "timezone_offset": 0,
        "readings": [
            [1738368000, 0.78773],
            [1738369800, 0.44709],
            [1738371600, 0.5123]
        ]
    }