"""Module for retrieving Glowmarkt data in batches."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union, Optional, Tuple
import requests

from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchRetriever:
    
    def __init__(self, client: GlowmarktClient):
        self.client = client
    
    def get_readings_in_batches(
        self,
        resource_id: str,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        period: str = "PT30M",
        function: str = "sum",
        offset: Optional[int] = None,
        batch_days: int = 10
    ) -> List[List[Union[int, float]]]:
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            
        if start_date > end_date:
            raise ValueError("Start date must be before end date")
            
        logger.info(f"Retrieving readings for resource {resource_id} from {start_date} to {end_date}")
        logger.info(f"Using period {period}, batch size {batch_days} days")
        
        date_ranges = self._calculate_batch_date_ranges(start_date, end_date, batch_days)
        
        all_readings = []
        batch_count = len(date_ranges)
        
        for i, (batch_start, batch_end) in enumerate(date_ranges):
            logger.info(f"Fetching batch {i+1}/{batch_count}: {batch_start.date()} to {batch_end.date()}")
            
            try:
                batch_data = self.client.get_readings(
                    resource_id,
                    start_date=batch_start,
                    end_date=batch_end,
                    period=period,
                    function=function,
                    offset=offset
                )
                
                readings = batch_data.get("data", batch_data.get("readings", []))
                logger.info(f"Received {len(readings)} readings in batch {i+1}")
                
                all_readings.extend(readings)
                
            except Exception as e:
                logger.error(f"Error fetching batch {i+1}: {str(e)}")
                
        logger.info(f"Retrieved a total of {len(all_readings)} readings")
        
        all_readings.sort(key=lambda x: x[0])
        
        return all_readings
    
    def _calculate_batch_date_ranges(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        batch_days: int
    ) -> List[Tuple[datetime, datetime]]:
        date_ranges = []
        
        if start_date == end_date:
            return [(start_date, end_date)]
        
        batch_start = start_date
        while batch_start < end_date:
            batch_end = min(batch_start + timedelta(days=batch_days), end_date)
            date_ranges.append((batch_start, batch_end))
            
            if batch_end == end_date:
                break
                
            batch_start = batch_end
        
        return date_ranges

def get_historical_readings(
    client: GlowmarktClient,
    resource_id: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    period: str = "PT30M",
    function: str = "sum",
    offset: Optional[int] = None,
    batch_days: int = 10
) -> List[List[Union[int, float]]]:
    retriever = BatchRetriever(client)
    return retriever.get_readings_in_batches(
        resource_id,
        start_date,
        end_date,
        period,
        function,
        offset,
        batch_days
    )