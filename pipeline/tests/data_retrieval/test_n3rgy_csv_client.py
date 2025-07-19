import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import datetime
from pathlib import Path
import tempfile
import os
import csv
from pipeline.data_retrieval.n3rgy_csv_client import N3rgyCSVClient


class TestN3rgyCSVClient(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "source"
        self.output_dir = Path(self.temp_dir.name) / "output"
        self.source_dir.mkdir()
        self.output_dir.mkdir()
        
        self.client = N3rgyCSVClient(
            source_dir=str(self.source_dir),
            output_dir=str(self.output_dir)
        )
        
        self.sample_electricity_csv_content = (
            "timestamp,consumption,cost\n"
            "2025-01-01 00:00,0.123,0.0456\n"
            "2025-01-01 00:30,0.234,0.0567\n"
            "2025-01-01 01:00,0.345,0.0678\n"
        )
        
        self.sample_gas_csv_content = (
            "timestamp,consumption,cost\n"
            "2025-01-01 00:00,1.234,0.1234\n"
            "2025-01-01 00:30,2.345,0.2345\n"
            "2025-01-01 01:00,3.456,0.3456\n"
        )
        
        self.electricity_csv = self.source_dir / "electricity_consumption_20250101_to_20250131.csv"
        self.gas_csv = self.source_dir / "gas_consumption_20250101_to_20250131.csv"
        
        with open(self.electricity_csv, "w") as f:
            f.write(self.sample_electricity_csv_content)
        
        with open(self.gas_csv, "w") as f:
            f.write(self.sample_gas_csv_content)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_initialization(self):
        client = N3rgyCSVClient()
        self.assertEqual(client.source_dir, Path("./raw-csv"))
        self.assertEqual(client.output_dir, Path("./processed"))
        
        client = N3rgyCSVClient(source_dir="source", output_dir="output")
        self.assertEqual(client.source_dir, Path("source"))
        self.assertEqual(client.output_dir, Path("output"))
    
    def test_extract_energy_type_from_filename(self):
        self.assertEqual(
            self.client._extract_energy_type_from_filename("electricity_consumption_20250101"),
            "electricity"
        )
        self.assertEqual(
            self.client._extract_energy_type_from_filename("gas_consumption_20250101"),
            "gas"
        )
        self.assertIsNone(
            self.client._extract_energy_type_from_filename("energy_consumption_20250101")
        )
    
    def test_extract_date_range_from_filename(self):
        self.assertEqual(
            self.client._extract_date_range_from_filename("electricity_20250101_to_20250131"),
            "20250101_to_20250131"
        )
        
        self.assertEqual(
            self.client._extract_date_range_from_filename("electricity_202501"),
            "20250101_to_20250131"
        )
        
        self.assertEqual(
            self.client._extract_date_range_from_filename("electricity_2025-02"),
            "20250201_to_20250228"
        )
        
        self.assertEqual(
            self.client._extract_date_range_from_filename("electricity_2024-02"),
            "20240201_to_20240229"
        )
        
        self.assertEqual(
            self.client._extract_date_range_from_filename("electricity_data"),
            "unknown_date_range"
        )
    
    def test_transform_csv_to_json(self):
        consumption_path, cost_path = self.client.transform_csv_to_json(
            self.electricity_csv,
            "electricity"
        )
        
        self.assertTrue(Path(consumption_path).exists())
        self.assertTrue(Path(cost_path).exists())
        
        with open(consumption_path, 'r') as f:
            consumption_data = json.load(f)
            self.assertEqual(consumption_data['resource_name'], 'electricity consumption')
            self.assertEqual(len(consumption_data['readings']), 3)
            timestamp, value = consumption_data['readings'][0]
            self.assertEqual(value, 0.123)
        
        with open(cost_path, 'r') as f:
            cost_data = json.load(f)
            self.assertEqual(cost_data['resource_name'], 'electricity cost')
            self.assertEqual(len(cost_data['readings']), 3)
            timestamp, value = cost_data['readings'][0]
            self.assertAlmostEqual(value, 4.56)
    
    def test_process_all_files(self):
        json_files = self.client.process_all_files(combine_to_jsonl=False)
        
        self.assertEqual(len(json_files), 4)
        
        expected_prefixes = ['electricity_consumption', 'electricity_cost', 
                            'gas_consumption', 'gas_cost']
        
        for prefix in expected_prefixes:
            matching_files = list(self.output_dir.glob(f"{prefix}*.json"))
            self.assertEqual(len(matching_files), 1)
    
    def test_process_all_files_with_combine(self):
        json_files = self.client.process_all_files(combine_to_jsonl=True)
        
        jsonl_files = list(self.output_dir.glob("all_resources_*.jsonl"))
        self.assertEqual(len(jsonl_files), 1)
        
        with open(jsonl_files[0], 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 3)
            
            first_record = json.loads(lines[0])
            self.assertIn('electricity_consumption', first_record)
            self.assertIn('electricity_cost', first_record)
            self.assertIn('gas_consumption', first_record)
            self.assertIn('gas_cost', first_record)
            
            self.assertEqual(first_record['electricity_consumption'], 0.123)
            self.assertAlmostEqual(first_record['electricity_cost'], 4.56)
            self.assertEqual(first_record['gas_consumption'], 1.234)
            self.assertAlmostEqual(first_record['gas_cost'], 12.34)
    
    def test_process_all_files_no_cost(self):
        json_files = self.client.process_all_files(extract_cost=False)
        
        self.assertEqual(len(json_files), 2)
        
        consumption_files = list(self.output_dir.glob("*_consumption_*.json"))
        cost_files = list(self.output_dir.glob("*_cost_*.json"))
        self.assertEqual(len(consumption_files), 2)
        self.assertEqual(len(cost_files), 0)
    
    def test_get_resource_data(self):
        self.client.process_all_files()
        
        electricity_data = self.client.get_resource_data('n3rgy-electricity')
        self.assertIsNotNone(electricity_data)
        self.assertEqual(electricity_data['resource_name'], 'electricity consumption')
        self.assertEqual(len(electricity_data['readings']), 3)
        
        gas_cost_data = self.client.get_resource_data('n3rgy-gas-cost')
        self.assertIsNotNone(gas_cost_data)
        self.assertEqual(gas_cost_data['resource_name'], 'gas cost')
        self.assertEqual(len(gas_cost_data['readings']), 3)
        
        nonexistent_data = self.client.get_resource_data('nonexistent-id')
        self.assertIsNone(nonexistent_data)
    
    def test_get_resource_data_with_date_filter(self):
        self.client.process_all_files()
        
        multi_day_readings = []
        for day in range(1, 4):
            dt = datetime.datetime(2025, 1, day, 12, 0)
            timestamp = int(dt.timestamp())
            multi_day_readings.append([timestamp, day * 0.1])
        
        test_json = {
            "resource_id": "test-resource",
            "resource_name": "test resource",
            "resource_unit": "kWh",
            "resource_classifier": "test.consumption",
            "start_date": "2025-01-01T12:00:00",
            "end_date": "2025-01-03T12:00:00",
            "period": "PT30M",
            "timezone_offset": 0,
            "readings": multi_day_readings
        }
        
        test_json_path = self.output_dir / "test_resource.json"
        with open(test_json_path, 'w') as f:
            json.dump(test_json, f)
        
        filtered_data_start_only = self.client.get_resource_data(
            "test-resource", 
            start_date="2025-01-02"
        )
        self.assertEqual(len(filtered_data_start_only['readings']), 2)
        
        filtered_data_end_only = self.client.get_resource_data(
            "test-resource", 
            end_date="2025-01-02"
        )
        self.assertEqual(len(filtered_data_end_only['readings']), 2)
        
        filtered_data_both_dates = self.client.get_resource_data(
            "test-resource", 
            start_date="2025-01-02",
            end_date="2025-01-02"
        )
        self.assertEqual(len(filtered_data_both_dates['readings']), 1)
        
        filtered_data_datetime_objects = self.client.get_resource_data(
            "test-resource", 
            start_date=datetime.datetime(2025, 1, 2),
            end_date=datetime.datetime(2025, 1, 2)
        )
        self.assertEqual(len(filtered_data_datetime_objects['readings']), 1)
    
    def test_handling_of_malformed_csv(self):
        malformed_csv = self.source_dir / "electricity_malformed.csv"
        with open(malformed_csv, "w") as f:
            f.write("timestamp,consumption,cost\n")
            f.write("2025-01-01 00:00,invalid,invalid\n")
            f.write("invalid_timestamp,0.1,0.2\n")
            f.write("2025-01-01 01:00,0.3,0.4\n")
        
        consumption_path, cost_path = self.client.transform_csv_to_json(
            malformed_csv,
            "electricity"
        )
        
        self.assertTrue(Path(consumption_path).exists())
        
        with open(consumption_path, 'r') as f:
            consumption_data = json.load(f)
            self.assertEqual(len(consumption_data['readings']), 1)
    
    def test_create_jsonl_from_json_files(self):
        json_files = self.client.process_all_files(combine_to_jsonl=False)
        
        jsonl_path = self.client.create_jsonl_from_json_files(json_files)
        
        self.assertTrue(Path(jsonl_path).exists())
        
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 3)
            
            for line in lines:
                record = json.loads(line)
                self.assertIn('electricity_consumption', record)
                self.assertIn('gas_consumption', record)

    # Fix for the n3rgy_csv_client.py file
    def _extract_date_range_from_filename(self, filename):
        """Extract date range from a filename in format YYYYMMDD_to_YYYYMMDD."""
        # Try to find pattern like 20250101_to_20250131
        date_pattern = r'(\d{8})_to_(\d{8})'
        match = re.search(date_pattern, filename)
        if match:
            return f"{match.group(1)}_to_{match.group(2)}"
        
        # Try to find pattern like 202405 (YYYYMM) and convert to full date range
        month_pattern = r'(\d{4})(\d{2})'
        match = re.search(month_pattern, filename)
        if match: 
            year = int(match.group(1))
            month = int(match.group(2))
            import calendar
            # Check if it's a leap year for February
            last_day = calendar.monthrange(year, month)[1]
            return f"{year}{month:02d}01_to_{year}{month:02d}{last_day:02d}"
        
        # Try to find pattern like 2024-05 (YYYY-MM) and convert to full date range
        month_pattern_dash = r'(\d{4})-(\d{2})'
        match = re.search(month_pattern_dash, filename)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            import calendar
            # Check if it's a leap year for February
            last_day = calendar.monthrange(year, month)[1]
            return f"{year}{month:02d}01_to_{year}{month:02d}{last_day:02d}"
        
        return "unknown_date_range"


if __name__ == '__main__':
    unittest.main()