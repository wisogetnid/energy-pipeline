import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnergyDataConverter:
    def __init__(self, output_dir: Union[str, Path] = "data/processed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_json_from_file(self, file_path: Union[str, Path]) -> Dict:
        with open(file_path, "r") as file_handle:
            return json.load(file_handle)

    def convert_to_jsonl(
        self,
        input_file: Union[str, Path, Dict],
        output_file: Optional[Union[str, Path]] = None,
    ) -> str:
        if isinstance(input_file, dict):
            input_data = input_file
            if output_file is None:
                raise ValueError(
                    "output_file must be provided when input_file is a dictionary"
                )
            output_jsonl_path = Path(output_file)
        else:
            input_path = Path(input_file)
            if output_file is None:
                output_jsonl_path = self.output_dir / f"{input_path.stem}.jsonl"
            else:
                output_jsonl_path = Path(output_file)
            input_data = self.load_json_from_file(input_path)

        output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)

        resource_id = input_data.get("resource_id", "unknown")
        resource_name = input_data.get("resource_name", "unknown")
        resource_unit = input_data.get("resource_unit", "unknown")
        resource_classifier = input_data.get("resource_classifier", "unknown")
        period = input_data.get("period", "unknown")
        start_date = input_data.get(
            "start_date", input_data.get("query", {}).get("from", "unknown")
        )
        end_date = input_data.get(
            "end_date", input_data.get("query", {}).get("to", "unknown")
        )

        raw_readings = input_data.get("readings", [])
        total_entries_written = 0

        with open(output_jsonl_path, "w") as file_handle:
            for timestamp, value in raw_readings:
                try:
                    timestamp_seconds = (
                        timestamp / 1000 if timestamp > 9999999999 else timestamp
                    )
                    reading_datetime = datetime.fromtimestamp(timestamp_seconds)
                    timestamp_iso = reading_datetime.isoformat()
                except:
                    timestamp_iso = "unknown"

                jsonl_record = {
                    "resource_id": resource_id,
                    "resource_name": resource_name,
                    "resource_unit": resource_unit,
                    "resource_classifier": resource_classifier,
                    "period": period,
                    "from_date": start_date,
                    "to_date": end_date,
                    "timestamp": timestamp,
                    "timestamp_iso": timestamp_iso,
                    "value": value,
                }

                file_handle.write(json.dumps(jsonl_record) + "\n")
                total_entries_written += 1

        logger.info(
            f"Converted {total_entries_written} readings to JSONL format at {output_jsonl_path}"
        )
        return str(output_jsonl_path)

    def batch_convert_to_jsonl(self, input_files: List[Union[str, Path]]) -> List[str]:
        output_files_list = []
        for input_file in input_files:
            output_file_path = self.convert_to_jsonl(input_file)
            output_files_list.append(output_file_path)
        return output_files_list

    def merge_consumption_and_cost_data(
        self, consumption_file: Union[str, Path], cost_file: Union[str, Path]
    ) -> Tuple[Dict[int, Dict], Dict]:
        consumption_data = self.load_json_from_file(consumption_file)
        cost_data = self.load_json_from_file(cost_file)

        consumption_readings = consumption_data.get("readings", [])
        cost_readings = cost_data.get("readings", [])

        consumption_values_by_timestamp = {
            reading[0]: reading[1] for reading in consumption_readings
        }
        cost_values_by_timestamp = {reading[0]: reading[1] for reading in cost_readings}

        merged_readings_map = {}

        for timestamp in set(consumption_values_by_timestamp.keys()) | set(
            cost_values_by_timestamp.keys()
        ):
            consumption_val = consumption_values_by_timestamp.get(timestamp, None)
            cost_val = cost_values_by_timestamp.get(timestamp, None)

            if consumption_val is None and cost_val is None:
                continue

            try:
                timestamp_seconds = (
                    timestamp / 1000 if timestamp > 9999999999 else timestamp
                )
                reading_datetime = datetime.fromtimestamp(timestamp_seconds)
                timestamp_iso = reading_datetime.isoformat()
            except:
                timestamp_iso = "unknown"

            merged_readings_map[timestamp] = {
                "timestamp": timestamp,
                "timestamp_iso": timestamp_iso,
                "consumption_value": consumption_val,
                "cost_value": cost_val,
            }

        consumption_filename_lower = Path(consumption_file).name.lower()
        if "electricity" in consumption_filename_lower:
            resource_type_label = "electricity"
        elif "gas" in consumption_filename_lower:
            resource_type_label = "gas"
        elif "water" in consumption_filename_lower:
            resource_type_label = "water"
        else:
            resource_type_label = "unknown"

        resource_metadata_details = {
            "resource_type": resource_type_label,
            "consumption_id": consumption_data.get("resource_id", "unknown"),
            "consumption_name": consumption_data.get("resource_name", "unknown"),
            "consumption_classifier": consumption_data.get(
                "resource_classifier", "unknown"
            ),
            "consumption_unit": consumption_data.get("resource_unit", "unknown"),
            "cost_id": cost_data.get("resource_id", "unknown"),
            "cost_name": cost_data.get("resource_name", "unknown"),
            "cost_classifier": cost_data.get("resource_classifier", "unknown"),
            "cost_unit": cost_data.get("resource_unit", "unknown"),
            "period": consumption_data.get(
                "period", cost_data.get("period", "unknown")
            ),
            "from_date": consumption_data.get(
                "start_date", consumption_data.get("query", {}).get("from", "unknown")
            ),
            "to_date": consumption_data.get(
                "end_date", consumption_data.get("query", {}).get("to", "unknown")
            ),
        }

        return merged_readings_map, resource_metadata_details

    def combine_consumption_and_cost(
        self,
        consumption_file: Union[str, Path],
        cost_file: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None,
    ) -> str:
        merged_readings_map, resource_metadata_details = (
            self.merge_consumption_and_cost_data(consumption_file, cost_file)
        )

        resource_type = resource_metadata_details["resource_type"]

        if output_file is None:
            consumption_path = Path(consumption_file)
            generated_filename = (
                f"{resource_type}_combined_{consumption_path.name.split('_', 1)[1]}"
            )
            if not generated_filename.endswith(".jsonl"):
                generated_filename = generated_filename.rsplit(".", 1)[0] + ".jsonl"
            output_jsonl_path = self.output_dir / generated_filename
        else:
            output_jsonl_path = Path(output_file)

        output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)

        total_entries_written = 0
        with open(output_jsonl_path, "w") as file_handle:
            for timestamp, reading in sorted(merged_readings_map.items()):
                combined_record = {
                    "resource_type": resource_type,
                    "consumption_id": resource_metadata_details["consumption_id"],
                    "consumption_name": resource_metadata_details["consumption_name"],
                    "consumption_classifier": resource_metadata_details[
                        "consumption_classifier"
                    ],
                    "consumption_unit": resource_metadata_details["consumption_unit"],
                    "cost_id": resource_metadata_details["cost_id"],
                    "cost_name": resource_metadata_details["cost_name"],
                    "cost_classifier": resource_metadata_details["cost_classifier"],
                    "cost_unit": resource_metadata_details["cost_unit"],
                    "period": resource_metadata_details["period"],
                    "from_date": resource_metadata_details["from_date"],
                    "to_date": resource_metadata_details["to_date"],
                    "timestamp": reading["timestamp"],
                    "timestamp_iso": reading["timestamp_iso"],
                    "consumption_value": reading["consumption_value"],
                    "cost_value": reading["cost_value"],
                }

                file_handle.write(json.dumps(combined_record) + "\n")
                total_entries_written += 1

        logger.info(
            f"Combined {total_entries_written} readings into JSONL format at {output_jsonl_path}"
        )
        return str(output_jsonl_path)

    def find_matching_resource_files(
        self, directory: Union[str, Path]
    ) -> List[Tuple[str, str]]:
        directory_path = Path(directory)
        all_json_files = list(directory_path.glob("*.json"))

        consumption_files_by_unique_key = {}
        cost_files_by_unique_key = {}

        for file_path in all_json_files:
            lowercase_filename = file_path.name.lower()

            if "electricity" in lowercase_filename:
                resource_category = "electricity"
            elif "gas" in lowercase_filename:
                resource_category = "gas"
            elif "water" in lowercase_filename:
                resource_category = "water"
            else:
                resource_category = "unknown"

            extracted_date_range = None
            date_range_regex_pattern = r"(\d{8})_to_(\d{8})"
            regex_match = re.search(date_range_regex_pattern, lowercase_filename)
            if regex_match:
                extracted_date_range = (
                    f"{regex_match.group(1)}_to_{regex_match.group(2)}"
                )

            if not extracted_date_range:
                fallback_base_name = (
                    lowercase_filename.replace("consumption", "")
                    .replace("cost", "")
                    .replace(".json", "")
                )
                extracted_date_range = fallback_base_name

            unique_resource_key = f"{resource_category}_{extracted_date_range}"

            if "consumption" in lowercase_filename:
                consumption_files_by_unique_key[unique_resource_key] = str(file_path)
            elif "cost" in lowercase_filename:
                cost_files_by_unique_key[unique_resource_key] = str(file_path)

        matching_file_pairs_list = []
        for key in consumption_files_by_unique_key:
            if key in cost_files_by_unique_key:
                matching_file_pairs_list.append(
                    (
                        consumption_files_by_unique_key[key],
                        cost_files_by_unique_key[key],
                    )
                )

        logger.info(
            f"Found {len(matching_file_pairs_list)} matching consumption-cost file pairs in {directory}"
        )
        return matching_file_pairs_list

    def combine_all_resources(
        self,
        directory: Union[str, Path] = "data/glowmarkt_api_raw",
        output_file: Optional[Union[str, Path]] = None,
    ) -> List[str]:
        directory_path = Path(directory)
        matching_file_pairs_list = self.find_matching_resource_files(directory_path)

        if not matching_file_pairs_list:
            logger.warning(f"No matching consumption-cost pairs found in {directory}")
            return []

        combined_readings_map = {}
        metadata_by_resource_type = {}

        for consumption_file, cost_file in matching_file_pairs_list:
            readings_map, metadata = self.merge_consumption_and_cost_data(
                consumption_file, cost_file
            )
            res_type = metadata["resource_type"]
            metadata_by_resource_type[res_type] = metadata

            for timestamp, reading in readings_map.items():
                if timestamp not in combined_readings_map:
                    combined_readings_map[timestamp] = {
                        "timestamp": timestamp,
                        "timestamp_iso": reading["timestamp_iso"],
                    }

                combined_readings_map[timestamp][f"{res_type}_consumption"] = reading[
                    "consumption_value"
                ]
                combined_readings_map[timestamp][f"{res_type}_cost"] = reading[
                    "cost_value"
                ]

        first_metadata_entry = next(iter(metadata_by_resource_type.values()))
        consolidated_global_metadata = {
            "period": first_metadata_entry.get("period", "unknown"),
            "from_date": first_metadata_entry.get("from_date", "unknown"),
            "to_date": first_metadata_entry.get("to_date", "unknown"),
        }

        for res_type, metadata in metadata_by_resource_type.items():
            consolidated_global_metadata[f"{res_type}_consumption_id"] = metadata[
                "consumption_id"
            ]
            consolidated_global_metadata[f"{res_type}_consumption_name"] = metadata[
                "consumption_name"
            ]
            consolidated_global_metadata[f"{res_type}_consumption_unit"] = metadata[
                "consumption_unit"
            ]
            consolidated_global_metadata[f"{res_type}_consumption_classifier"] = (
                metadata["consumption_classifier"]
            )
            consolidated_global_metadata[f"{res_type}_cost_id"] = metadata["cost_id"]
            consolidated_global_metadata[f"{res_type}_cost_name"] = metadata[
                "cost_name"
            ]
            consolidated_global_metadata[f"{res_type}_cost_unit"] = metadata[
                "cost_unit"
            ]
            consolidated_global_metadata[f"{res_type}_cost_classifier"] = metadata[
                "cost_classifier"
            ]

        monthly_binned_readings = {}
        output_files_paths = []

        for timestamp, reading_data in combined_readings_map.items():
            try:
                if isinstance(timestamp, (int, float)):
                    ts_seconds = (
                        timestamp / 1000 if timestamp > 9999999999 else timestamp
                    )
                    dt_object = datetime.fromtimestamp(ts_seconds)
                elif isinstance(timestamp, str) and "T" in timestamp:
                    dt_object = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                else:
                    from dateutil import parser

                    dt_object = parser.parse(str(timestamp))

                year_month_key = f"{dt_object.year}-{dt_object.month:02d}"

                if year_month_key not in monthly_binned_readings:
                    monthly_binned_readings[year_month_key] = []

                monthly_binned_readings[year_month_key].append(
                    (timestamp, reading_data)
                )
            except Exception as timestamp_parse_error:
                logger.warning(
                    f"Could not parse timestamp {timestamp}: {timestamp_parse_error}"
                )
                if "unknown" not in monthly_binned_readings:
                    monthly_binned_readings["unknown"] = []
                monthly_binned_readings["unknown"].append((timestamp, reading_data))

        for key, readings_list in monthly_binned_readings.items():
            if not readings_list:
                continue

            if key == "unknown":
                output_filename = f"all_resources_unknown_dates.jsonl"
            else:
                year_str, month_str = key.split("-")
                range_start_suffix = f"{year_str}{month_str}01"

                import calendar

                _, last_day_int = calendar.monthrange(int(year_str), int(month_str))
                range_end_suffix = f"{year_str}{month_str}{last_day_int}"

                output_filename = (
                    f"all_resources_{range_start_suffix}_to_{range_end_suffix}.jsonl"
                )

            monthly_output_path = self.output_dir / output_filename
            monthly_output_path.parent.mkdir(parents=True, exist_ok=True)

            month_specific_metadata = consolidated_global_metadata.copy()
            if key != "unknown":
                y, m = key.split("-")
                import calendar

                _, last_d = calendar.monthrange(int(y), int(m))
                month_specific_metadata["from_date"] = f"{y}-{m}-01T00:00:00.000Z"
                month_specific_metadata["to_date"] = f"{y}-{m}-{last_d}T23:59:59.999Z"

            monthly_entries_written = 0
            with open(monthly_output_path, "w") as file_handle:
                for timestamp, reading_entry in sorted(readings_list):
                    monthly_combined_record = {
                        **month_specific_metadata,
                        **reading_entry,
                    }
                    file_handle.write(json.dumps(monthly_combined_record) + "\n")
                    monthly_entries_written += 1

            logger.info(
                f"Created monthly file for {key} with {monthly_entries_written} readings at {monthly_output_path}"
            )
            output_files_paths.append(str(monthly_output_path))

        logger.info(
            f"Split data into {len(output_files_paths)} monthly files in {self.output_dir}"
        )
        return output_files_paths

    def extract_resource_type(self, resource_name: str) -> str:
        lowercase_resource_name = resource_name.lower()
        if "electricity" in lowercase_resource_name:
            return "electricity"
        elif "gas" in lowercase_resource_name:
            return "gas"
        elif "water" in lowercase_resource_name:
            return "water"
        else:
            return "energy"

    def batch_combine_resource_files(self, directory: Union[str, Path]) -> List[str]:
        matching_file_pairs_list = self.find_matching_resource_files(directory)

        batch_output_files_list = []
        for consumption_file, cost_file in matching_file_pairs_list:
            output_file_path = self.combine_consumption_and_cost(
                consumption_file, cost_file
            )
            batch_output_files_list.append(output_file_path)

        return batch_output_files_list
