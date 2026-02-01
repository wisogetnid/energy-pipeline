import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

def get_latest_available_date(data_dir: str, resource_names: List[str], default_date: datetime = datetime(2024, 1, 1)) -> datetime:
    """
    Identifies the latest date for which raw data exists in the data_dir for the given resources.
    Returns the start date of the latest file to use as a pivot for fresh fetching.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return default_date

    # Pattern: resource_name_YYYYMMDD_to_YYYYMMDD.json
    # We need to be careful with resource names that might contain spaces or special characters
    # In the UI, they seem to be converted to lowercase and spaces replaced with underscores.
    
    resource_latest_dates = {}
    resource_pivot_starts = {}

    for resource in resource_names:
        safe_name = resource.lower().replace(" ", "_")
        # Match files starting with safe_name followed by two date patterns
        pattern = re.compile(rf"^{re.escape(safe_name)}_(\d{{8}})_to_(\d{{8}})\.json$")
        
        latest_end = None
        pivot_start = None

        for file in data_path.glob("*.json"):
            match = pattern.match(file.name)
            if match:
                start_str, end_str = match.groups()
                try:
                    start_dt = datetime.strptime(start_str, "%Y%m%d")
                    end_dt = datetime.strptime(end_str, "%Y%m%d")
                    
                    if latest_end is None or end_dt > latest_end:
                        latest_end = end_dt
                        pivot_start = start_dt
                except ValueError:
                    continue
        
        if latest_end:
            resource_latest_dates[resource] = latest_end
            resource_pivot_starts[resource] = pivot_start
        else:
            # If ANY requested resource is missing local data, start from the default date
            return default_date

    if not resource_latest_dates:
        return default_date

    # Cross-Resource Sync: Use the earliest of these "latest dates" to find which resource is furthest behind
    furthest_behind_resource = min(resource_latest_dates, key=resource_latest_dates.get)
    
    # Return the start date of that file to trigger an overwrite/refresh of the latest month
    return resource_pivot_starts[furthest_behind_resource]
