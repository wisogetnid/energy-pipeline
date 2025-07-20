import json
import pytest
import tempfile
from pathlib import Path

from pipeline.data_processing.yearly_jsonl_converter import YearlyEnergyDataConverter


def load_fixture(filename):
    fixture_path = Path(__file__).parent / 'fixtures' / filename
    with open(fixture_path, 'r') as f:
        return json.load(f)


class TestYearlyEnergyDataConverter:

    @pytest.fixture
    def converter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield YearlyEnergyDataConverter(output_dir=temp_dir)

    @pytest.fixture
    def gas_consumption_data(self):
        return load_fixture("gas_consumption_test.json")

    @pytest.fixture
    def gas_cost_data(self):
        consumption_data = load_fixture("gas_consumption_test.json")
        cost_data = consumption_data.copy()
        cost_data["resource_name"] = "gas cost"
        cost_data["resource_classifier"] = "gas.consumption.cost"
        cost_data["resource_unit"] = "pence"

        cost_readings = []
        for reading in cost_data["readings"]:
            cost_readings.append([reading[0], reading[1] * 0.15])
        cost_data["readings"] = cost_readings

        return cost_data

    @pytest.fixture
    def electricity_consumption_data(self):
        return {
            "resource_id": "04678775-6c72-43c9-8378-c9914756384a",
            "resource_name": "electricity consumption",
            "readings": [
                [1738368000, 0.047],
                [1738454400, 0.059],
                [1769980800, 0.039]
            ]
        }

    @pytest.fixture
    def electricity_cost_data(self):
        return {
            "resource_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
            "resource_name": "electricity cost",
            "readings": [
                [1738368000, 0.78773],
                [1738454400, 0.44709],
                [1769980800, 0.5123]
            ]
        }

    @pytest.fixture
    def gas_consumption_file_path(self, tmp_path, gas_consumption_data):
        file_path = tmp_path / "gas_consumption_test.json"
        with open(file_path, 'w') as f:
            json.dump(gas_consumption_data, f)
        return file_path

    @pytest.fixture
    def gas_cost_file_path(self, tmp_path, gas_cost_data):
        file_path = tmp_path / "gas_cost_test.json"
        with open(file_path, 'w') as f:
            json.dump(gas_cost_data, f)
        return file_path

    @pytest.fixture
    def electricity_consumption_file_path(self, tmp_path, electricity_consumption_data):
        file_path = tmp_path / "electricity_consumption_test.json"
        with open(file_path, 'w') as f:
            json.dump(electricity_consumption_data, f)
        return file_path

    @pytest.fixture
    def electricity_cost_file_path(self, tmp_path, electricity_cost_data):
        file_path = tmp_path / "electricity_cost_test.json"
        with open(file_path, 'w') as f:
            json.dump(electricity_cost_data, f)
        return file_path

    def test_convert_to_yearly_jsonl(self, converter, electricity_consumption_file_path, electricity_cost_file_path):
        file_pairs = [(str(electricity_consumption_file_path), str(electricity_cost_file_path))]

        output_files = converter.convert_to_yearly_jsonl(file_pairs)

        assert len(output_files) == 2

        output_2025 = Path(converter.output_dir) / "2025_annual_energy_summary.jsonl"
        output_2026 = Path(converter.output_dir) / "2026_annual_energy_summary.jsonl"

        assert str(output_2025) in output_files
        assert str(output_2026) in output_files

        with open(output_2025, 'r') as f:
            lines = f.readlines()
            assert len(lines) >= 1

            
            daily_records = [json.loads(line) for line in lines if json.loads(line).get('data_type') == 'daily_summary']
            

            feb01 = next((record for record in daily_records if record.get('date') == '2025-02-01'), None)
            feb02 = next((record for record in daily_records if record.get('date') == '2025-02-02'), None)
            
            assert feb01 is not None
            assert feb02 is not None
            assert abs(feb01['consumption_total'] - 0.047) < 0.0001
            assert abs(feb01['cost_total'] - 0.78773) < 0.0001

        with open(output_2026, 'r') as f:
            lines = f.readlines()
            assert len(lines) >= 1

            
            daily_records = [json.loads(line) for line in lines if json.loads(line).get('data_type') == 'daily_summary']
            

            feb01 = next((record for record in daily_records if record.get('date') == '2026-02-01'), None)
            
            assert feb01 is not None
            assert abs(feb01['consumption_total'] - 0.039) < 0.0001
            assert abs(feb01['cost_total'] - 0.5123) < 0.0001
