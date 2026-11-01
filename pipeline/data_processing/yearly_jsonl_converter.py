import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime

from pipeline.data_processing.jsonl_converter import EnergyDataConverter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class YearlyEnergyDataConverter(EnergyDataConverter):

    def __init__(self, output_dir: Optional[Union[str, Path]] = None):
        if output_dir:
            super().__init__(output_dir)
        else:
            super().__init__()
        self.resource_metadata = {}

    def find_matching_resource_files(
        self, directory: Union[str, Path]
    ) -> List[Tuple[str, str]]:
        directory = Path(directory)
        jsonl_files = list(directory.glob("*.jsonl"))

        if not jsonl_files:
            json_files = list(directory.glob("*.json"))
            return [(str(f), str(f)) for f in json_files]

        logger.info(f"Found {len(jsonl_files)} JSONL files in {directory}")
        return [(str(f), str(f)) for f in jsonl_files]

    def _group_and_sum_by_day(
        self, merged_readings: Dict[int, Dict]
    ) -> Dict[str, Dict]:
        daily_data = defaultdict(
            lambda: {"consumption_value": 0, "cost_value": 0, "count": 0}
        )
        for ts, reading in merged_readings.items():
            try:
                if isinstance(ts, (int, float)):
                    timestamp_seconds = ts / 1000 if ts > 9999999999 else ts
                    day = datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d")
                else:
                    day = datetime.fromisoformat(
                        str(ts).replace("Z", "+00:00")
                    ).strftime("%Y-%m-%d")
            except:
                try:
                    if "timestamp_iso" in reading and reading["timestamp_iso"]:
                        day = reading["timestamp_iso"].split("T")[0]
                    else:
                        continue
                except:
                    continue

            consumption = None
            cost = None

            if (
                "consumption_value" in reading
                and reading["consumption_value"] is not None
            ):
                consumption = reading["consumption_value"]
            if "cost_value" in reading and reading["cost_value"] is not None:
                cost = reading["cost_value"]

            resource_types = ["electricity", "gas", "water"]
            for resource in resource_types:
                consumption_key = f"{resource}_consumption"
                cost_key = f"{resource}_cost"

                if consumption_key in reading and reading[consumption_key] is not None:
                    if consumption is None:
                        consumption = 0
                    consumption += reading[consumption_key]
                    daily_data[day][consumption_key] = (
                        daily_data[day].get(consumption_key, 0)
                        + reading[consumption_key]
                    )

                if cost_key in reading and reading[cost_key] is not None:
                    if cost is None:
                        cost = 0
                    cost += reading[cost_key]
                    daily_data[day][cost_key] = (
                        daily_data[day].get(cost_key, 0) + reading[cost_key]
                    )

            if consumption is not None:
                daily_data[day]["consumption_value"] += consumption
            if cost is not None:
                daily_data[day]["cost_value"] += cost
            daily_data[day]["count"] += 1

        return daily_data

    def _extract_year_from_first_record(self, first_record: Dict) -> Optional[str]:
        if "year" in first_record and isinstance(first_record["year"], str):
            return first_record["year"]

        for date_field in ["date", "from_date", "timestamp_iso"]:
            if date_field in first_record and isinstance(first_record[date_field], str):
                return first_record[date_field].split("-")[0]

        timestamp = first_record.get("timestamp")
        if isinstance(timestamp, (int, float)):
            try:
                timestamp_seconds = (
                    timestamp / 1000 if timestamp > 9999999999 else timestamp
                )
                return datetime.fromtimestamp(timestamp_seconds).strftime("%Y")
            except:
                pass

        return None

    def _extract_year_from_file(self, file_path: Path) -> str:
        filename = file_path.name.lower()

        compact_date_pattern = r"(\d{4})\d{4}"
        match = re.search(compact_date_pattern, filename)
        if match:
            return match.group(1)

        dashed_year_month_pattern = r"(\d{4})-\d{2}"
        match = re.search(dashed_year_month_pattern, filename)
        if match:
            return match.group(1)

        if file_path.suffix.lower() == ".jsonl":
            try:
                with open(file_path, "r") as f:
                    first_record = json.loads(f.readline().strip())
                year_from_first_record = self._extract_year_from_first_record(
                    first_record
                )
                if year_from_first_record is not None:
                    return year_from_first_record
            except:
                pass

        return "unknown"

    def _accumulate_daily_summary_record(
        self, yearly_data: Dict[str, Dict[str, Dict]], data: Dict
    ) -> None:
        date_str = data["date"]
        year = date_str.split("-")[0]

        if date_str not in yearly_data[year]:
            yearly_data[year][date_str] = {
                "consumption_value": 0,
                "cost_value": 0,
                "count": 0,
                "readings": [],
            }

        consumption = data.get("consumption_total", 0)
        cost = data.get("cost_total", 0)

        yearly_data[year][date_str]["consumption_value"] += consumption
        yearly_data[year][date_str]["cost_value"] += cost
        yearly_data[year][date_str]["count"] += data.get("reading_count", 0)

        resource_types = ["electricity", "gas", "water"]
        for resource in resource_types:
            consumption_key = f"{resource}_consumption"
            cost_key = f"{resource}_cost"

            if f"{resource}_consumption" in data:
                if consumption_key not in yearly_data[year][date_str]:
                    yearly_data[year][date_str][consumption_key] = 0
                yearly_data[year][date_str][consumption_key] += data[
                    f"{resource}_consumption"
                ]

                if (
                    resource not in self.resource_metadata
                    and f"{resource}_consumption_unit" in data
                ):
                    self.resource_metadata[resource] = {
                        "consumption_unit": data[f"{resource}_consumption_unit"],
                        "cost_unit": data.get(f"{resource}_cost_unit", "unknown"),
                    }

            if f"{resource}_cost" in data:
                if cost_key not in yearly_data[year][date_str]:
                    yearly_data[year][date_str][cost_key] = 0
                yearly_data[year][date_str][cost_key] += data[f"{resource}_cost"]

    def _process_jsonl_file(self, file_path: Path) -> Dict[str, Dict[str, Dict]]:
        yearly_data = defaultdict(lambda: defaultdict(dict))

        try:
            with open(file_path, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)

                        if data.get("data_type") == "daily_summary":
                            self._accumulate_daily_summary_record(yearly_data, data)
                            continue
                        if data.get("data_type") in [
                            "yearly_summary",
                            "monthly_summary",
                        ]:
                            continue

                        timestamp = None
                        if "timestamp" in data:
                            timestamp = data["timestamp"]
                        elif "date" in data:
                            try:
                                date = datetime.fromisoformat(data["date"])
                                timestamp = date.timestamp()
                            except:
                                pass

                        if timestamp is None:
                            continue

                        try:
                            if isinstance(timestamp, (int, float)):
                                timestamp_seconds = (
                                    timestamp / 1000
                                    if timestamp > 9999999999
                                    else timestamp
                                )
                                dt = datetime.fromtimestamp(timestamp_seconds)
                            else:
                                dt = datetime.fromisoformat(
                                    str(timestamp).replace("Z", "+00:00")
                                )

                            date_str = dt.strftime("%Y-%m-%d")
                            year = dt.strftime("%Y")
                        except:
                            if "date" in data and isinstance(data["date"], str):
                                date_str = data["date"]
                                year = date_str.split("-")[0]
                            elif "timestamp_iso" in data and isinstance(
                                data["timestamp_iso"], str
                            ):
                                date_str = data["timestamp_iso"].split("T")[0]
                                year = date_str.split("-")[0]
                            else:
                                continue

                        if date_str not in yearly_data[year]:
                            yearly_data[year][date_str] = {
                                "consumption_value": 0,
                                "cost_value": 0,
                                "count": 0,
                                "readings": [],
                            }

                        consumption = None
                        cost = None

                        consumption_fields = [
                            "consumption_value",
                            "consumption",
                            "consumption_total",
                            "value",
                        ]
                        cost_fields = ["cost_value", "cost", "cost_total"]

                        for field in consumption_fields:
                            if field in data and data[field] is not None:
                                consumption = float(data[field])
                                break

                        for field in cost_fields:
                            if field in data and data[field] is not None:
                                cost = float(data[field])
                                break

                        resource_types = ["electricity", "gas", "water"]
                        if consumption is None and cost is None:
                            for resource in resource_types:
                                consumption_key = f"{resource}_consumption"
                                cost_key = f"{resource}_cost"

                                if (
                                    consumption_key in data
                                    and data[consumption_key] is not None
                                ):
                                    if consumption is None:
                                        consumption = 0
                                    consumption += float(data[consumption_key])

                                if cost_key in data and data[cost_key] is not None:
                                    if cost is None:
                                        cost = 0
                                    cost += float(data[cost_key])

                        if consumption is None and cost is None:
                            continue

                        if consumption is not None:
                            yearly_data[year][date_str][
                                "consumption_value"
                            ] += consumption
                        if cost is not None:
                            yearly_data[year][date_str]["cost_value"] += cost

                        yearly_data[year][date_str]["count"] += 1
                        yearly_data[year][date_str]["readings"].append(
                            {
                                "timestamp": timestamp,
                                "consumption": consumption,
                                "cost": cost,
                            }
                        )

                        for resource in resource_types:
                            consumption_key = f"{resource}_consumption"
                            cost_key = f"{resource}_cost"

                            if (
                                consumption_key in data
                                and data[consumption_key] is not None
                            ):
                                if consumption_key not in yearly_data[year][date_str]:
                                    yearly_data[year][date_str][consumption_key] = 0
                                yearly_data[year][date_str][consumption_key] += float(
                                    data[consumption_key]
                                )

                                if resource not in self.resource_metadata:
                                    self.resource_metadata[resource] = {
                                        "consumption_unit": data.get(
                                            f"{resource}_consumption_unit", "unknown"
                                        ),
                                        "cost_unit": data.get(
                                            f"{resource}_cost_unit", "unknown"
                                        ),
                                    }

                            if cost_key in data and data[cost_key] is not None:
                                if cost_key not in yearly_data[year][date_str]:
                                    yearly_data[year][date_str][cost_key] = 0
                                yearly_data[year][date_str][cost_key] += float(
                                    data[cost_key]
                                )
                    except Exception as e:
                        logger.warning(f"Error processing line in {file_path}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

        return yearly_data

    @staticmethod
    def _add_daily_values_to_yearly_totals(
        yearly_totals: Dict[str, Dict[str, Dict]],
        year: str,
        day: str,
        daily_values: Dict,
    ) -> None:
        if day not in yearly_totals[year]:
            yearly_totals[year][day] = {
                "consumption_value": 0,
                "cost_value": 0,
                "count": 0,
            }

        summable_value_names = [
            "consumption_value",
            "cost_value",
            "count",
            "consumption_total",
            "cost_total",
            "reading_count",
        ]
        for value_name, value in daily_values.items():
            if isinstance(value, (int, float)) and (
                value_name in summable_value_names
                or value_name.endswith("_consumption")
                or value_name.endswith("_cost")
            ):
                yearly_totals[year][day][value_name] = (
                    yearly_totals[year][day].get(value_name, 0) + value
                )
            elif value_name not in yearly_totals[year][day]:
                yearly_totals[year][day][value_name] = value

    def _daily_data_from_consumption_and_cost_files(
        self, consumption_file: Union[Path, str], cost_file: Union[Path, str]
    ) -> Dict[str, Dict]:
        merged_readings, resource_metadata = self.merge_consumption_and_cost_data(
            consumption_file, cost_file
        )
        daily_data = self._group_and_sum_by_day(merged_readings)
        resource_type = resource_metadata.get("resource_type", "unknown")
        if resource_type == "unknown":
            return daily_data

        self.resource_metadata[resource_type] = resource_metadata
        for values in daily_data.values():
            if f"{resource_type}_consumption" not in values:
                values[f"{resource_type}_consumption"] = values.get(
                    "consumption_value", 0
                )
            if f"{resource_type}_cost" not in values:
                values[f"{resource_type}_cost"] = values.get("cost_value", 0)
        return daily_data

    def convert_to_yearly_jsonl(
        self, files: List[Union[Path, str, tuple]]
    ) -> List[str]:
        input_files = []
        potential_output_files = []

        for file_item in files:
            file_path = Path(
                file_item[0] if isinstance(file_item, tuple) else file_item
            )

            if file_path.name.endswith("_annual_energy_summary.jsonl"):
                potential_output_files.append(file_item)
            else:
                input_files.append(file_item)

        yearly_data_combined = defaultdict(lambda: defaultdict(dict))
        self.resource_metadata = {}

        for file_item in input_files:
            if isinstance(file_item, tuple):
                file_path = Path(file_item[0])
            else:
                file_path = Path(file_item)
            logger.info(f"Processing input file: {file_path}")
            if file_path.suffix.lower() == ".jsonl":
                file_yearly_data = self._process_jsonl_file(file_path)
                for year, days in file_yearly_data.items():
                    for day, data in days.items():
                        self._add_daily_values_to_yearly_totals(
                            yearly_data_combined, year, day, data
                        )
            elif file_path.suffix.lower() == ".json":
                if isinstance(file_item, tuple) and len(file_item) == 2:
                    consumption_file, cost_file = file_item
                    daily_data = self._daily_data_from_consumption_and_cost_files(
                        consumption_file, cost_file
                    )
                    for day, values in daily_data.items():
                        year = day.split("-")[0]
                        self._add_daily_values_to_yearly_totals(
                            yearly_data_combined, year, day, values
                        )

        years_with_input_data = set(yearly_data_combined.keys())

        for file_item in potential_output_files:
            if isinstance(file_item, tuple):
                file_path = Path(file_item[0])
            else:
                file_path = Path(file_item)

            year = self._extract_year_from_file(file_path)

            if year not in years_with_input_data:
                logger.info(
                    f"Processing output file (no input data for {year}): {file_path}"
                )
                file_yearly_data = self._process_jsonl_file(file_path)
                for year_key, days in file_yearly_data.items():
                    for day, data in days.items():
                        self._add_daily_values_to_yearly_totals(
                            yearly_data_combined, year_key, day, data
                        )
            else:
                logger.info(
                    f"Skipping output file (input data exists for {year}): {file_path}"
                )

        for year in yearly_data_combined.keys():
            output_file = self.output_dir / f"{year}_annual_energy_summary.jsonl"
            if output_file.exists():
                logger.info(
                    f"Deleting existing annual summary file to prevent duplication: {output_file}"
                )
                output_file.unlink()
        output_files = []
        for year, daily_readings in yearly_data_combined.items():
            if not daily_readings:
                continue
            output_file = self.output_dir / f"{year}_annual_energy_summary.jsonl"
            output_files.append(str(output_file))
            monthly_data = defaultdict(dict)
            for day, data in daily_readings.items():
                month = day[:7]
                if month not in monthly_data:
                    monthly_data[month] = {
                        "consumption_value": 0,
                        "cost_value": 0,
                        "count": 0,
                    }

                for k, v in data.items():
                    if isinstance(v, (int, float)) and (
                        k
                        in [
                            "consumption_value",
                            "cost_value",
                            "count",
                            "consumption_total",
                            "cost_total",
                            "reading_count",
                        ]
                        or k.endswith("_consumption")
                        or k.endswith("_cost")
                    ):
                        monthly_data[month][k] = monthly_data[month].get(k, 0) + v
                    elif k not in monthly_data[month]:
                        monthly_data[month][k] = v
            with open(output_file, "w") as f:
                yearly_summary = {
                    "year": year,
                    "consumption_total": sum(
                        d["consumption_value"]
                        for d in daily_readings.values()
                        if "consumption_value" in d
                    ),
                    "cost_total": sum(
                        d["cost_value"]
                        for d in daily_readings.values()
                        if "cost_value" in d
                    ),
                    "reading_count": sum(d["count"] for d in daily_readings.values()),
                    "days_with_readings": len(daily_readings),
                    "from_date": f"{year}-01-01",
                    "to_date": f"{year}-12-31",
                    "data_type": "yearly_summary",
                }
                for resource, meta in self.resource_metadata.items():
                    c_key = f"{resource}_consumption"
                    cost_key = f"{resource}_cost"
                    yearly_summary[f"{resource}_consumption_total"] = sum(
                        d.get(c_key, 0) for d in daily_readings.values()
                    )
                    yearly_summary[f"{resource}_consumption_unit"] = meta.get(
                        "consumption_unit", "unknown"
                    )
                    yearly_summary[f"{resource}_cost_total"] = sum(
                        d.get(cost_key, 0) for d in daily_readings.values()
                    )
                    yearly_summary[f"{resource}_cost_unit"] = meta.get(
                        "cost_unit", "unknown"
                    )
                f.write(json.dumps(yearly_summary) + "\n")

                for month, data in sorted(monthly_data.items()):
                    monthly_summary = {
                        "year": year,
                        "month": month,
                        "consumption_total": data.get("consumption_value", 0),
                        "cost_total": data.get("cost_value", 0),
                        "reading_count": data["count"],
                        "data_type": "monthly_summary",
                    }
                    for resource, meta in self.resource_metadata.items():
                        c_key = f"{resource}_consumption"
                        cost_key = f"{resource}_cost"
                        monthly_summary[f"{resource}_consumption_total"] = data.get(
                            c_key, 0
                        )
                        monthly_summary[f"{resource}_consumption_unit"] = meta.get(
                            "consumption_unit", "unknown"
                        )
                        monthly_summary[f"{resource}_cost_total"] = data.get(
                            cost_key, 0
                        )
                        monthly_summary[f"{resource}_cost_unit"] = meta.get(
                            "cost_unit", "unknown"
                        )
                    f.write(json.dumps(monthly_summary) + "\n")

                for day, data in sorted(daily_readings.items()):
                    daily_summary = {
                        "date": day,
                        "consumption_total": data.get("consumption_value", 0),
                        "cost_total": data.get("cost_value", 0),
                        "reading_count": data["count"],
                        "data_type": "daily_summary",
                    }
                    for resource, meta in self.resource_metadata.items():
                        c_key = f"{resource}_consumption"
                        cost_key = f"{resource}_cost"
                        if c_key in data:
                            daily_summary[f"{resource}_consumption"] = data[c_key]
                            daily_summary[f"{resource}_consumption_unit"] = meta.get(
                                "consumption_unit", "unknown"
                            )
                        if cost_key in data:
                            daily_summary[f"{resource}_cost"] = data[cost_key]
                            daily_summary[f"{resource}_cost_unit"] = meta.get(
                                "cost_unit", "unknown"
                            )
                    f.write(json.dumps(daily_summary) + "\n")
            logger.info(
                f"Written yearly summary for {year} with {len(daily_readings)} days to {output_file}"
            )
        return output_files
