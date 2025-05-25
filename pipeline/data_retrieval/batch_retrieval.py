"""Module for retrieving Glowmarkt data in batches."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union, Optional, Tuple
import requests

from pipeline.data_retrieval.glowmarkt_client import GlowmarktClient

# Set up basic logging if not already configured
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchRetriever:
    """Class for retrieving Glowmarkt data in batches."""
    
    def __init__(self, client: GlowmarktClient):
        """Initialize the batch retriever with a GlowmarktClient.
        
        Args:
            client: An authenticated GlowmarktClient instance
        """
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
    ) -> List[List[Union[int, float]]]:  # Updated return type
        """Get readings for a resource in batches.
        
        Args:
            resource_id: The resource ID to fetch data for
            start_date: Start date for readings
            end_date: End date for readings
            period: Data granularity (e.g., "PT30M" for 30 minutes)
            function: Aggregation function to apply
            offset: Timezone offset in minutes from UTC
            batch_days: Number of days per batch
            
        Returns:
            List of readings data objects merged from all batches
            
        Raises:
            ValueError: If the date range is invalid
        """
        # Convert string dates to datetime objects if needed
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            
        # Validate date range
        if start_date > end_date:
            raise ValueError("Start date must be before end date")
            
        logger.info(f"Retrieving readings for resource {resource_id} from {start_date} to {end_date}")
        logger.info(f"Using period {period}, batch size {batch_days} days")
        
        # Calculate batch boundaries
        date_ranges = self._calculate_batch_date_ranges(start_date, end_date, batch_days)
        
        # Initialize results container
        all_readings = []
        batch_count = len(date_ranges)
        
        # Fetch each batch
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
                
                # Extract readings from the response - try both "data" and "readings" keys
                readings = batch_data.get("data", batch_data.get("readings", []))
                logger.info(f"Received {len(readings)} readings in batch {i+1}")
                
                # Append to results
                all_readings.extend(readings)
                
            except Exception as e:
                logger.error(f"Error fetching batch {i+1}: {str(e)}")
                # Continue with next batch instead of failing completely
                
        logger.info(f"Retrieved a total of {len(all_readings)} readings")
        
        # Sort readings by timestamp to ensure they're in chronological order
        all_readings.sort(key=lambda x: x[0])
        
        return all_readings
    
    def _calculate_batch_date_ranges(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        batch_days: int
    ) -> List[Tuple[datetime, datetime]]:
        """Calculate the date ranges for each batch."""
        date_ranges = []
        
        # Special case: if start_date equals end_date, return a single range
        if start_date == end_date:
            return [(start_date, end_date)]
        
        # First, create all the full-sized batches
        batch_start = start_date
        while batch_start < end_date:
            batch_end = min(batch_start + timedelta(days=batch_days), end_date)
            date_ranges.append((batch_start, batch_end))
            
            # If we've reached the end date, break without adding another range
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
) -> List[List[Union[int, float]]]:  # Updated return type
    """Convenience function to get historical readings in batches.
    
    Args:
        client: An authenticated GlowmarktClient instance
        resource_id: The resource ID to fetch data for
        start_date: Start date for readings
        end_date: End date for readings
        period: Data granularity (e.g., "PT30M" for 30 minutes)
        function: Aggregation function to apply
        offset: Timezone offset in minutes from UTC
        batch_days: Number of days per batch
        
    Returns:
        List of readings merged from all batches
    """
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