import json
import logging
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JsonlToParquetConverter:
    def __init__(self, output_dir: Union[str, Path] = "data/parquet"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def convert_jsonl_to_parquet_file(
        self,
        jsonl_file: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None,
    ) -> Optional[str]:
        jsonl_input_path = Path(jsonl_file)

        if not jsonl_input_path.exists():
            logger.error(f"JSONL file not found: {jsonl_input_path}")
            raise FileNotFoundError(f"JSONL file not found: {jsonl_input_path}")

        try:
            data_records_list = []
            with open(jsonl_input_path, "r") as file_handle:
                for line in file_handle:
                    try:
                        record_object = json.loads(line)
                        data_records_list.append(record_object)
                    except json.JSONDecodeError as decode_error:
                        logger.warning(
                            f"Skipping invalid JSON line in {jsonl_input_path}: {decode_error}"
                        )
                        continue

            if not data_records_list:
                logger.warning(f"No records found in {jsonl_input_path}")
                energy_dataframe = pd.DataFrame()
            else:
                energy_dataframe = pd.DataFrame(data_records_list)

            if output_file is None:
                parquet_output_path = (
                    self.output_dir / f"{jsonl_input_path.stem}.parquet"
                )
            else:
                parquet_output_path = Path(output_file)

            parquet_output_path.parent.mkdir(parents=True, exist_ok=True)

            if (
                "timestamp" in energy_dataframe.columns
                and energy_dataframe["timestamp"].dtype == "object"
            ):
                try:
                    energy_dataframe["timestamp"] = pd.to_numeric(
                        energy_dataframe["timestamp"]
                    )
                except:
                    pass

            energy_dataframe.to_parquet(parquet_output_path, index=False)

            logger.info(
                f"Converted {jsonl_input_path} to Parquet format at {parquet_output_path}"
            )
            return str(parquet_output_path)

        except Exception as conversion_error:
            logger.error(
                f"Error converting {jsonl_input_path} to Parquet: {conversion_error}"
            )
            return None

    def convert_multiple_jsonl_files(
        self, jsonl_files: List[Union[str, Path]]
    ) -> List[str]:
        converted_parquet_files_list = []

        for jsonl_file in jsonl_files:
            try:
                converted_file_path = self.convert_jsonl_to_parquet_file(jsonl_file)
                if converted_file_path:
                    converted_parquet_files_list.append(converted_file_path)
            except FileNotFoundError:
                logger.warning(f"Skipping non-existent file: {jsonl_file}")
            except Exception as loop_error:
                logger.error(f"Error converting {jsonl_file}: {loop_error}")

        return converted_parquet_files_list
