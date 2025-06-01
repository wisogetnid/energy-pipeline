import json
import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from pipeline.data_processing.parquet_converter import JsonlToParquetConverter

class TestJsonlToParquetConverter:
    
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
    
    def test_batch_conversion_of_multiple_files(self, converter, sample_jsonl_file, alternate_jsonl_file):
        output_file_paths = converter.convert_batch_to_parquet([
            str(sample_jsonl_file),
            str(alternate_jsonl_file)
        ])
        
        assert len(output_file_paths) == 2
        
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