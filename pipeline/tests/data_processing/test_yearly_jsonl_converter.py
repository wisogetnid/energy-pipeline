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

    def test_convert_to_yearly_jsonl_multiple_times_does_not_duplicate(self, converter, electricity_consumption_file_path, electricity_cost_file_path):
        file_pairs = [(str(electricity_consumption_file_path), str(electricity_cost_file_path))]

        output_files_first = converter.convert_to_yearly_jsonl(file_pairs)
        
        output_2025 = Path(converter.output_dir) / "2025_annual_energy_summary.jsonl"
        
        with open(output_2025, 'r') as f:
            lines_first = f.readlines()
            daily_records_first = [json.loads(line) for line in lines_first if json.loads(line).get('data_type') == 'daily_summary']
            feb01_first = next((record for record in daily_records_first if record.get('date') == '2025-02-01'), None)
            first_consumption = feb01_first['consumption_total']
            first_cost = feb01_first['cost_total']
        
        output_files_second = converter.convert_to_yearly_jsonl(file_pairs)
        
        with open(output_2025, 'r') as f:
            lines_second = f.readlines()
            daily_records_second = [json.loads(line) for line in lines_second if json.loads(line).get('data_type') == 'daily_summary']
            feb01_second = next((record for record in daily_records_second if record.get('date') == '2025-02-01'), None)
            second_consumption = feb01_second['consumption_total']
            second_cost = feb01_second['cost_total']
        
        assert abs(first_consumption - second_consumption) < 0.0001
        assert abs(first_cost - second_cost) < 0.0001
        assert abs(second_consumption - 0.047) < 0.0001
        assert abs(second_cost - 0.78773) < 0.0001

    def test_convert_with_existing_file_does_not_duplicate(self, converter, tmp_path):
        existing_consumption_data = {
            "resource_id": "test-id-1",
            "resource_name": "electricity consumption",
            "readings": [
                [1738368000, 0.047],
                [1738454400, 0.059],
            ]
        }
        existing_cost_data = {
            "resource_id": "test-id-2",
            "resource_name": "electricity cost",
            "readings": [
                [1738368000, 0.78773],
                [1738454400, 0.44709],
            ]
        }
        
        consumption_file = tmp_path / "electricity_consumption.json"
        cost_file = tmp_path / "electricity_cost.json"
        
        with open(consumption_file, 'w') as f:
            json.dump(existing_consumption_data, f)
        with open(cost_file, 'w') as f:
            json.dump(existing_cost_data, f)
        
        file_pairs = [(str(consumption_file), str(cost_file))]
        converter.convert_to_yearly_jsonl(file_pairs)
        
        output_2025 = Path(converter.output_dir) / "2025_annual_energy_summary.jsonl"
        
        with open(output_2025, 'r') as f:
            lines_first = f.readlines()
            yearly_first = json.loads(lines_first[0])
            first_yearly_consumption = yearly_first['consumption_total']
        
        converter.convert_to_yearly_jsonl(file_pairs)
        
        with open(output_2025, 'r') as f:
            lines_second = f.readlines()
            yearly_second = json.loads(lines_second[0])
            second_yearly_consumption = yearly_second['consumption_total']
        
        assert abs(first_yearly_consumption - second_yearly_consumption) < 0.0001
        
        new_consumption_data = {
            "resource_id": "test-id-1",
            "resource_name": "electricity consumption",
            "readings": [
                [1738368000, 0.047],
                [1738454400, 0.059],
                [1738540800, 0.065],
            ]
        }
        new_cost_data = {
            "resource_id": "test-id-2",
            "resource_name": "electricity cost",
            "readings": [
                [1738368000, 0.78773],
                [1738454400, 0.44709],
                [1738540800, 0.55555],
            ]
        }
        
        with open(consumption_file, 'w') as f:
            json.dump(new_consumption_data, f)
        with open(cost_file, 'w') as f:
            json.dump(new_cost_data, f)
        
        converter.convert_to_yearly_jsonl(file_pairs)
        
        with open(output_2025, 'r') as f:
            lines_third = f.readlines()
            yearly_third = json.loads(lines_third[0])
            third_yearly_consumption = yearly_third['consumption_total']
            daily_records_third = [json.loads(line) for line in lines_third if json.loads(line).get('data_type') == 'daily_summary']
        
        expected_total = 0.047 + 0.059 + 0.065
        assert abs(third_yearly_consumption - expected_total) < 0.0001
        assert len(daily_records_third) == 3

    def test_convert_existing_jsonl_file_does_not_duplicate(self, tmp_path):
        converter_output_dir = tmp_path / "output"
        converter_output_dir.mkdir()
        converter = YearlyEnergyDataConverter(output_dir=str(converter_output_dir))
        
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        
        initial_jsonl = input_dir / "2025_data.jsonl"
        with open(initial_jsonl, 'w') as f:
            f.write(json.dumps({"timestamp": 1738368000, "consumption_value": 0.047, "cost_value": 0.78773}) + '\n')
            f.write(json.dumps({"timestamp": 1738454400, "consumption_value": 0.059, "cost_value": 0.44709}) + '\n')
        
        converter.convert_to_yearly_jsonl([str(initial_jsonl)])
        
        output_2025 = converter_output_dir / "2025_annual_energy_summary.jsonl"
        
        with open(output_2025, 'r') as f:
            lines_first = f.readlines()
            yearly_first = json.loads(lines_first[0])
            first_yearly_consumption = yearly_first['consumption_total']
            first_yearly_cost = yearly_first['consumption_total']
        
        expected_consumption = 0.047 + 0.059
        expected_cost = 0.78773 + 0.44709
        assert abs(first_yearly_consumption - expected_consumption) < 0.0001
        
        converter.convert_to_yearly_jsonl([str(output_2025)])
        
        with open(output_2025, 'r') as f:
            lines_second = f.readlines()
            yearly_second = json.loads(lines_second[0])
            second_yearly_consumption = yearly_second['consumption_total']
        
        assert abs(second_yearly_consumption - expected_consumption) < 0.0001

    def test_reprocessing_annual_summary_daily_records_preserves_values(self, tmp_path):
        converter_output_dir = tmp_path / "output"
        converter_output_dir.mkdir()
        converter = YearlyEnergyDataConverter(output_dir=str(converter_output_dir))
        
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        
        annual_summary_with_daily_records = input_dir / "2024_annual_energy_summary.jsonl"
        with open(annual_summary_with_daily_records, 'w') as f:
            f.write(json.dumps({
                'date': '2024-02-14',
                'consumption_total': 193.98,
                'cost_total': 1266.89,
                'reading_count': 36,
                'data_type': 'daily_summary',
                'gas_consumption': 96.99,
                'gas_consumption_unit': 'kWh',
                'gas_cost': 633.44,
                'gas_cost_unit': 'pence'
            }) + '\n')
            
            f.write(json.dumps({
                'date': '2024-02-15',
                'consumption_total': 200.0,
                'cost_total': 1300.0,
                'reading_count': 36,
                'data_type': 'daily_summary',
                'gas_consumption': 100.0,
                'gas_consumption_unit': 'kWh',
                'gas_cost': 650.0,
                'gas_cost_unit': 'pence'
            }) + '\n')
        
        converter.convert_to_yearly_jsonl([str(annual_summary_with_daily_records)])
        
        output_2024 = converter_output_dir / "2024_annual_energy_summary.jsonl"
        assert output_2024.exists()
        
        with open(output_2024, 'r') as f:
            lines = f.readlines()
            yearly_summary = json.loads(lines[0])
            
            expected_consumption = 193.98 + 200.0
            expected_cost = 1266.89 + 1300.0
            
            actual_consumption = yearly_summary['consumption_total']
            actual_cost = yearly_summary['cost_total']
            
            print(f"\nExpected: {expected_consumption}, Actual: {actual_consumption}, Diff: {actual_consumption - expected_consumption}")
            
            assert abs(actual_consumption - expected_consumption) < 0.01
            assert abs(actual_cost - expected_cost) < 0.01

    def test_idempotency_with_existing_annual_summary(self, tmp_path):
        converter_output_dir = tmp_path / "output"
        converter_output_dir.mkdir()
        converter = YearlyEnergyDataConverter(output_dir=str(converter_output_dir))
        
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        
        # 1. Create an initial annual summary file
        annual_summary_file = converter_output_dir / "2024_annual_energy_summary.jsonl"
        with open(annual_summary_file, 'w') as f:
            f.write(json.dumps({
                'year': '2024', 'data_type': 'yearly_summary', 'consumption_total': 100, 'cost_total': 10
            }) + '\n')
            f.write(json.dumps({
                'date': '2024-01-01', 'data_type': 'daily_summary', 'consumption_total': 100, 'cost_total': 10
            }) + '\n')

        # 2. Create a new monthly file to be processed
        monthly_file = input_dir / "2024-02.jsonl"
        with open(monthly_file, 'w') as f:
            f.write(json.dumps({
                'timestamp': 1706745600, 'consumption_value': 50, 'cost_value': 5
            }) + '\n')
            
        # 3. Run the converter with both the new monthly file and the existing annual summary
        # The bug is that it will read the annual summary and add its values to the new calculation
        files_to_process = [str(monthly_file), str(annual_summary_file)]
        converter.convert_to_yearly_jsonl(files_to_process)
        
        # 4. Check the results
        with open(annual_summary_file, 'r') as f:
            lines = f.readlines()
            new_yearly_summary = json.loads(lines[0])
            
            # Expected: The new summary should ONLY contain data from the monthly file (50)
            # Bug: The new summary will contain data from monthly + old annual (50 + 100 = 150)
            expected_consumption = 50
            actual_consumption = new_yearly_summary['consumption_total']
            
            print(f"\nIdempotency Test: Expected={expected_consumption}, Actual={actual_consumption}")
            
            assert abs(actual_consumption - expected_consumption) < 0.01, \
                f"Idempotency bug! Expected {expected_consumption}, got {actual_consumption}"
