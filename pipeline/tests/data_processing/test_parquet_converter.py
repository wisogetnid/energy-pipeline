import json
import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

class TestJsonlToParquetConverter:
    
    @pytest.fixture
    def converter(self, tmp_path):
        output_dir = tmp_path / "parquet_output"
        return JsonlToParquetConverter(output_dir=str(output_dir))
    
    @pytest.fixture
    def sample_jsonl_file(self, tmp_path):
        data = [
            {
                "resource_id": "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4",
                "resource_name": "gas consumption",
                "resource_type": "gas",
                "classifier": "gas.consumption",
                "units": "kWh",
                "period": "PT30M",
                "from_date": "2025-01-01T00:00:00",
                "to_date": "2025-01-31T23:59:59",
                "timestamp": 1748217600,
                "timestamp_iso": "2025-05-25T00:00:00",
                "value": 0
            },
            {
                "resource_id": "e7d48c5a-b142-49f2-8a65-c79d3fb5c7e4",
                "resource_name": "gas consumption",
                "resource_type": "gas",
                "classifier": "gas.consumption",
                "units": "kWh",
                "period": "PT30M",
                "from_date": "2025-01-01T00:00:00",
                "to_date": "2025-01-31T23:59:59",
                "timestamp": 1748224800,
                "timestamp_iso": "2025-05-25T02:00:00",
                "value": 0.00824216
            }
        ]
        
        file_path = tmp_path / "sample.jsonl"
        with open(file_path, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        
        return file_path
    
    @pytest.fixture
    def combined_jsonl_file(self, tmp_path):
        data = [
            {
                "resource_type": "electricity",
                "consumption_id": "04678775-6c72-43c9-8378-c9914756384a",
                "consumption_name": "electricity consumption",
                "consumption_classifier": "electricity.consumption",
                "consumption_unit": "kWh",
                "cost_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
                "cost_name": "electricity cost",
                "cost_classifier": "electricity.consumption.cost",
                "cost_unit": "pence",
                "period": "PT30M",
                "from_date": "2025-02-01T00:00:00",
                "to_date": "2025-02-28T23:59:59",
                "timestamp": 1738368000,
                "timestamp_iso": "2025-01-01T12:00:00",
                "consumption_value": 0.047,
                "cost_value": 0.78773
            },
            {
                "resource_type": "electricity",
                "consumption_id": "04678775-6c72-43c9-8378-c9914756384a",
                "consumption_name": "electricity consumption",
                "consumption_classifier": "electricity.consumption",
                "consumption_unit": "kWh",
                "cost_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
                "cost_name": "electricity cost",
                "cost_classifier": "electricity.consumption.cost",
                "cost_unit": "pence",
                "period": "PT30M",
                "from_date": "2025-02-01T00:00:00",
                "to_date": "2025-02-28T23:59:59",
                "timestamp": 1738369800,
                "timestamp_iso": "2025-01-01T12:30:00",
                "consumption_value": 0.059,
                "cost_value": 0.44709
            }
        ]
        
        file_path = tmp_path / "combined_resource.jsonl"
        with open(file_path, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        
        return file_path
    
    @pytest.fixture
    def alternate_jsonl_file(self, tmp_path):
        data = [
            {
                "resource_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
                "resource_name": "electricity cost",
                "resource_type": "electricity",
                "classifier": "electricity.consumption.cost",
                "units": "pence",
                "period": "PT30M",
                "from_date": "2025-01-01T00:00:00",
                "to_date": "2025-01-31T23:59:59",
                "timestamp": 1738368000,
                "timestamp_iso": "2025-01-01T12:00:00",
                "value": 0.78773
            }
        ]
        
        file_path = tmp_path / "alternate.jsonl"
        with open(file_path, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        
        return file_path
    
    @pytest.fixture
    def empty_jsonl_file(self, tmp_path):
        file_path = tmp_path / "empty.jsonl"
        with open(file_path, 'w') as f:
            pass
        return file_path
    
    @pytest.fixture
    def malformed_jsonl_file(self, tmp_path):
        file_path = tmp_path / "malformed.jsonl"
        with open(file_path, 'w') as f:
            f.write('{"valid": true}\n')
            f.write('{"invalid\n')
        return file_path
    
    @pytest.fixture
    def large_jsonl_file(self, tmp_path):
        data = {
            "resource_id": "936f529b-1b68-4110-9fd9-b227eced10ae",
            "resource_name": "electricity cost",
            "resource_type": "electricity",
            "classifier": "electricity.consumption.cost",
            "units": "pence",
            "period": "PT30M",
            "from_date": "2025-01-01T00:00:00",
            "to_date": "2025-01-31T23:59:59"
        }
        
        file_path = tmp_path / "large.jsonl"
        with open(file_path, 'w') as f:
            for i in range(1000):
                entry = data.copy()
                entry["timestamp"] = 1738368000 + (i * 1800)
                entry["timestamp_iso"] = f"2025-01-01T{12 + (i // 2):02d}:{(i % 2) * 30:02d}:00"
                entry["value"] = i * 0.1
                f.write(json.dumps(entry) + '\n')
        
        return file_path
    
    def test_constructor_creates_output_directory(self, tmp_path):
        nonexistent_dir_path = tmp_path / "test_parquet_output"
        assert not nonexistent_dir_path.exists()
        
        converter = JsonlToParquetConverter(output_dir=str(nonexistent_dir_path))
        assert nonexistent_dir_path.exists()
    
    def test_convert_jsonl_to_parquet(self, converter, sample_jsonl_file):
        output_file_path = Path(converter.output_dir) / "test_output.parquet"
        
        result_path = converter.convert_to_parquet(
            sample_jsonl_file, 
            output_file=output_file_path
        )
        
        assert result_path == str(output_file_path)
        assert output_file_path.exists()
        
        df = pd.read_parquet(output_file_path)
        assert len(df) > 0
        
        first_row = df.iloc[0]
        assert first_row["resource_name"] == "gas consumption"
        assert pd.api.types.is_numeric_dtype(first_row["timestamp"])
        assert pd.api.types.is_numeric_dtype(first_row["value"])
    
    def test_convert_combined_jsonl_to_parquet(self, converter, combined_jsonl_file):
        output_file_path = Path(converter.output_dir) / "combined_output.parquet"
        
        result_path = converter.convert_to_parquet(
            combined_jsonl_file, 
            output_file=output_file_path
        )
        
        assert result_path == str(output_file_path)
        assert output_file_path.exists()
        
        df = pd.read_parquet(output_file_path)
        assert len(df) > 0
        
        first_row = df.iloc[0]
        assert first_row["resource_type"] == "electricity"
        assert pd.api.types.is_numeric_dtype(first_row["timestamp"])
        assert pd.api.types.is_numeric_dtype(first_row["consumption_value"])
        assert pd.api.types.is_numeric_dtype(first_row["cost_value"])
        
        # Check that the consumption and cost values were preserved
        assert first_row["consumption_value"] == 0.047
        assert first_row["cost_value"] == 0.78773
    
    def test_auto_generated_output_filename(self, converter, sample_jsonl_file):
        result_path = converter.convert_to_parquet(sample_jsonl_file)
        
        output_file = Path(result_path)
        assert output_file.exists()
        assert output_file.suffix == ".parquet"
        assert Path(sample_jsonl_file).stem in output_file.stem
        
        df = pd.read_parquet(output_file)
        assert len(df) > 0
    
    def test_numeric_type_optimization(self, converter, sample_jsonl_file):
        result_path = converter.convert_to_parquet(sample_jsonl_file)
        
        df = pd.read_parquet(result_path)
        
        assert pd.api.types.is_numeric_dtype(df["timestamp"])
        assert pd.api.types.is_numeric_dtype(df["value"])
    
    def test_numeric_type_optimization_for_combined_data(self, converter, combined_jsonl_file):
        result_path = converter.convert_to_parquet(combined_jsonl_file)
        
        df = pd.read_parquet(result_path)
        
        assert pd.api.types.is_numeric_dtype(df["timestamp"])
        assert pd.api.types.is_numeric_dtype(df["consumption_value"])
        assert pd.api.types.is_numeric_dtype(df["cost_value"])
    
    def test_batch_conversion_of_multiple_files(self, converter, sample_jsonl_file, alternate_jsonl_file, combined_jsonl_file):
        output_file_paths = converter.convert_batch_to_parquet([
            str(sample_jsonl_file),
            str(alternate_jsonl_file),
            str(combined_jsonl_file)
        ])
        
        assert len(output_file_paths) == 3
        
        for file_path in output_file_paths:
            assert Path(file_path).exists()
            assert Path(file_path).suffix == ".parquet"
    
    def test_handles_empty_jsonl_file(self, converter, empty_jsonl_file):
        result_path = converter.convert_to_parquet(empty_jsonl_file)
        
        assert Path(result_path).exists()
        
        df = pd.read_parquet(result_path)
        assert len(df) == 0
    
    def test_handles_malformed_jsonl_lines(self, converter, malformed_jsonl_file):
        result_path = converter.convert_to_parquet(malformed_jsonl_file)
        
        assert Path(result_path).exists()
        
        df = pd.read_parquet(result_path)
        assert len(df) == 1
        assert "valid" in df.columns
    
    def test_file_size_reduction(self, converter, large_jsonl_file):
        result_path = converter.convert_to_parquet(str(large_jsonl_file))
        
        jsonl_size = os.path.getsize(large_jsonl_file)
        parquet_size = os.path.getsize(result_path)
        
        assert parquet_size < jsonl_size
        
        df = pd.read_parquet(result_path)
        assert len(df) > 0
    
    def test_raises_error_for_nonexistent_file(self, converter):
        with pytest.raises(FileNotFoundError):
            converter.convert_to_parquet("non_existent_file.jsonl")
    
    def test_preserves_all_columns(self, converter, sample_jsonl_file):
        result_path = converter.convert_to_parquet(sample_jsonl_file)
        
        with open(sample_jsonl_file, 'r') as f:
            first_line = f.readline().strip()
            if first_line:
                jsonl_columns = set(json.loads(first_line).keys())
                
                df = pd.read_parquet(result_path)
                parquet_columns = set(df.columns)
                
                assert jsonl_columns.issubset(parquet_columns)
    
    def test_preserves_all_columns_for_combined_data(self, converter, combined_jsonl_file):
        result_path = converter.convert_to_parquet(combined_jsonl_file)
        
        with open(combined_jsonl_file, 'r') as f:
            first_line = f.readline().strip()
            if first_line:
                jsonl_columns = set(json.loads(first_line).keys())
                
                df = pd.read_parquet(result_path)
                parquet_columns = set(df.columns)
                
                assert jsonl_columns.issubset(parquet_columns)
                assert "consumption_value" in parquet_columns
                assert "cost_value" in parquet_columns