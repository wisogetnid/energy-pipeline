import re
from datetime import datetime
from pathlib import Path
from typing import List


def get_latest_available_date(
    data_dir: str,
    resource_names: List[str],
    default_date: datetime = datetime(2024, 1, 1),
) -> datetime:
    data_path = Path(data_dir)
    if not data_path.exists():
        return default_date

    resource_to_latest_end_date = {}
    resource_to_latest_file_start_date = {}

    for resource in resource_names:
        safe_resource_name = resource.lower().replace(" ", "_")
        filename_pattern = re.compile(
            rf"^{re.escape(safe_resource_name)}_(\d{{8}})_to_(\d{{8}})\.json$"
        )

        latest_end_date = None
        pivot_start_date = None

        for file in data_path.glob("*.json"):
            match = filename_pattern.match(file.name)
            if match:
                start_str, end_str = match.groups()
                try:
                    start_dt = datetime.strptime(start_str, "%Y%m%d")
                    end_dt = datetime.strptime(end_str, "%Y%m%d")

                    if latest_end_date is None or end_dt > latest_end_date:
                        latest_end_date = end_dt
                        pivot_start_date = start_dt
                except ValueError:
                    continue

        if latest_end_date:
            resource_to_latest_end_date[resource] = latest_end_date
            resource_to_latest_file_start_date[resource] = pivot_start_date
        else:
            return default_date

    if not resource_to_latest_end_date:
        return default_date

    furthest_behind_resource = min(
        resource_to_latest_end_date, key=resource_to_latest_end_date.get
    )

    return resource_to_latest_file_start_date[furthest_behind_resource]
