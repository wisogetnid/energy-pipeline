"""Tests for the batch retrieval module."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from pipeline.data_retrieval.batch_retrieval import BatchRetriever, get_historical_readings
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestBatchRetriever:
    
    def test_calculate_batch_date_ranges(self):
        client = Mock(spec=GlowmarktClient)
        retriever = BatchRetriever(client)
        
        month_start = datetime(2023, 1, 1)
        month_end = datetime(2023, 1, 31)
        ten_day_batch_size = 10
        
        calculated_date_ranges = retriever._calculate_batch_date_ranges(month_start, month_end, ten_day_batch_size)
        
        assert len(calculated_date_ranges) == 3
        assert calculated_date_ranges[0] == (datetime(2023, 1, 1), datetime(2023, 1, 11))
        assert calculated_date_ranges[1] == (datetime(2023, 1, 11), datetime(2023, 1, 21))
        assert calculated_date_ranges[2] == (datetime(2023, 1, 21), datetime(2023, 1, 31))
    
    def test_get_readings_in_batches(self, mock_client, mock_readings_response):
        retriever = BatchRetriever(mock_client)
        resource_id = "test-resource-123"
        five_day_start = datetime(2023, 1, 1)
        five_day_end = datetime(2023, 1, 5)
        two_day_batch_size = 2
        
        mock_client.get_readings.side_effect = [
            {"data": [[1672531200000, 1.0], [1672617600000, 2.0]]},
            {"data": [[1672704000000, 3.0], [1672790400000, 4.0]]}
        ]
        
        combined_readings = retriever.get_readings_in_batches(
            resource_id, 
            five_day_start, 
            five_day_end, 
            batch_days=two_day_batch_size
        )
        
        assert len(combined_readings) == 4
        assert mock_client.get_readings.call_count == 2
        
        reading_timestamps = [reading[0] for reading in combined_readings]
        assert reading_timestamps == sorted(reading_timestamps)
    
    def test_get_readings_handles_string_dates(self, mock_client):
        retriever = BatchRetriever(mock_client)
        resource_id = "test-resource-123"
        
        date_string_start = "2023-01-01T00:00:00"
        date_string_end = "2023-01-05T00:00:00"
        
        mock_client.get_readings.return_value = {"readings": []}
        
        retriever.get_readings_in_batches(
            resource_id, 
            date_string_start, 
            date_string_end, 
            batch_days=2
        )
        
        _, call_kwargs = mock_client.get_readings.call_args_list[0]
        assert isinstance(call_kwargs['start_date'], datetime)
        assert isinstance(call_kwargs['end_date'], datetime)
    
    def test_convenience_function(self, mock_client):
        resource_id = "test-resource-123"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)
        
        mock_client.get_readings.return_value = {"readings": []}
        
        with patch('pipeline.data_retrieval.batch_retrieval.BatchRetriever') as MockRetrieverClass:
            mock_retriever = Mock()
            MockRetrieverClass.return_value = mock_retriever
            mock_retriever.get_readings_in_batches.return_value = []
            
            get_historical_readings(
                mock_client,
                resource_id,
                start_date,
                end_date
            )
            
            MockRetrieverClass.assert_called_once_with(mock_client)
            
            mock_retriever.get_readings_in_batches.assert_called_once_with(
                resource_id,
                start_date,
                end_date,
                "PT30M",
                "sum",
                None,
                10
            )