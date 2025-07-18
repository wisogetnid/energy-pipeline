import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from pipeline.data_retrieval.batch_retrieval import BatchRetriever, get_historical_readings

class TestBatchRetriever:
    
    @pytest.fixture
    def retriever(self, mock_client):
        return BatchRetriever(mock_client)

    def test_calculate_batch_date_ranges(self, retriever):
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)
        batch_size = 10
        
        date_ranges = retriever._calculate_batch_date_ranges(start_date, end_date, batch_size)
        
        assert len(date_ranges) == 3
        assert date_ranges[0] == (datetime(2023, 1, 1), datetime(2023, 1, 11))
        assert date_ranges[1] == (datetime(2023, 1, 11), datetime(2023, 1, 21))
        assert date_ranges[2] == (datetime(2023, 1, 21), datetime(2023, 1, 31))
    
    def test_get_readings_in_batches(self, retriever, mock_client, mock_readings_response):
        resource_id = "test-resource-123"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)
        batch_size = 2
        
        mock_client.get_readings.side_effect = [
            {"data": [[1672531200000, 1.0], [1672617600000, 2.0]]},
            {"data": [[1672704000000, 3.0], [1672790400000, 4.0]]}
        ]
        
        readings = retriever.get_readings_in_batches(
            resource_id, 
            start_date,
            end_date,
            batch_days=batch_size
        )
        
        assert len(readings) == 4
        assert mock_client.get_readings.call_count == 2
        
        timestamps = [reading[0] for reading in readings]
        assert timestamps == sorted(timestamps)
    
    def test_get_readings_handles_string_dates(self, retriever, mock_client):
        resource_id = "test-resource-123"
        start_date_str = "2023-01-01T00:00:00"
        end_date_str = "2023-01-05T00:00:00"
        
        mock_client.get_readings.return_value = {"readings": []}
        
        retriever.get_readings_in_batches(
            resource_id, 
            start_date_str,
            end_date_str,
            batch_days=2
        )
        
        _, call_kwargs = mock_client.get_readings.call_args_list[0]
        assert isinstance(call_kwargs['start_date'], datetime)
        assert isinstance(call_kwargs['end_date'], datetime)
    
    def test_get_historical_readings_convenience_function(self, mock_client):
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