"""Tests for the batch retrieval module."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from pipeline.data_retrieval.batch_retrieval import BatchRetriever, get_historical_readings
from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

class TestBatchRetriever:
    
    def test_calculate_batch_date_ranges(self):
        """Test that batch date ranges are calculated correctly."""
        client = Mock(spec=GlowmarktClient)
        retriever = BatchRetriever(client)
        
        # Test with a 30-day range and 10-day batches
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)
        batch_days = 10
        
        date_ranges = retriever._calculate_batch_date_ranges(start_date, end_date, batch_days)
        
        # Should have 3 batches with updated implementation: 1-11, 11-21, 21-31
        assert len(date_ranges) == 3
        assert date_ranges[0] == (datetime(2023, 1, 1), datetime(2023, 1, 11))
        assert date_ranges[1] == (datetime(2023, 1, 11), datetime(2023, 1, 21))
        assert date_ranges[2] == (datetime(2023, 1, 21), datetime(2023, 1, 31))
    
    def test_get_readings_in_batches(self, mock_client, mock_readings_response):
        """Test that readings are fetched in batches and combined."""
        # Setup
        retriever = BatchRetriever(mock_client)
        resource_id = "test-resource-123"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)  # 5-day range
        batch_days = 2  # 2-day batches
        
        # Configure mock to return different readings for each batch with "data" key
        mock_client.get_readings.side_effect = [
            # Batch 1: Jan 1-3
            {"data": [[1672531200000, 1.0], [1672617600000, 2.0]]},
            # Batch 2: Jan 3-5
            {"data": [[1672704000000, 3.0], [1672790400000, 4.0]]}
        ]
        
        # Call the method
        results = retriever.get_readings_in_batches(
            resource_id, 
            start_date, 
            end_date, 
            batch_days=batch_days
        )
        
        # Verify
        assert len(results) == 4  # Total of 4 readings from all batches
        assert mock_client.get_readings.call_count == 2  # Called once per batch
        
        # Verify readings are in chronological order
        timestamps = [reading[0] for reading in results]
        assert timestamps == sorted(timestamps)
    
    def test_get_readings_handles_string_dates(self, mock_client):
        """Test that string dates are converted properly."""
        retriever = BatchRetriever(mock_client)
        resource_id = "test-resource-123"
        
        # Use string dates
        start_date = "2023-01-01T00:00:00"
        end_date = "2023-01-05T00:00:00"
        
        # Configure mock
        mock_client.get_readings.return_value = {"readings": []}
        
        # Call the method
        retriever.get_readings_in_batches(
            resource_id, 
            start_date, 
            end_date, 
            batch_days=2
        )
        
        # Verify the first call used datetime objects converted from strings
        _, kwargs = mock_client.get_readings.call_args_list[0]
        assert isinstance(kwargs['start_date'], datetime)
        assert isinstance(kwargs['end_date'], datetime)
    
    def test_convenience_function(self, mock_client):
        """Test the convenience function."""
        resource_id = "test-resource-123"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)
        
        # Configure mock
        mock_client.get_readings.return_value = {"readings": []}
        
        # Call the convenience function
        with patch('pipeline.data_retrieval.batch_retrieval.BatchRetriever') as MockRetriever:
            # Configure the mock retriever
            mock_retriever_instance = Mock()
            MockRetriever.return_value = mock_retriever_instance
            mock_retriever_instance.get_readings_in_batches.return_value = []
            
            # Call the function
            get_historical_readings(
                mock_client,
                resource_id,
                start_date,
                end_date
            )
            
            # Verify BatchRetriever was instantiated with our client
            MockRetriever.assert_called_once_with(mock_client)
            
            # Verify get_readings_in_batches was called with our parameters
            mock_retriever_instance.get_readings_in_batches.assert_called_once_with(
                resource_id,
                start_date,
                end_date,
                "PT30M",
                "sum",
                None,
                10
            )