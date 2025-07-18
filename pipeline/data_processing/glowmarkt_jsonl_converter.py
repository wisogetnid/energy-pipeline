#!/usr/bin/env python
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from pipeline.data_processing.jsonl_converter import EnergyDataConverter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GlowmarktEnergyDataConverter(EnergyDataConverter):

    def __init__(self, output_dir: Optional[str] = None):
        super().__init__(output_dir)

    def _group_and_sum_by_day(self, merged_readings: Dict[int, Dict]) -> Dict[str, Dict]:
        daily_data = defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0})
        for ts, reading in merged_readings.items():
            day = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            if reading['consumption_value'] is not None:
                daily_data[day]['consumption_value'] += reading['consumption_value']
            if reading['cost_value'] is not None:
                daily_data[day]['cost_value'] += reading['cost_value']
            daily_data[day]['count'] += 1
        return daily_data

    def convert_to_yearly_jsonl(self, file_pairs: List[tuple[str, str]], output_dir: Optional[Union[str, Path]] = None):
        if output_dir:
            self.output_dir = Path(output_dir)

        yearly_data = defaultdict(lambda: defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0}))

        for consumption_file, cost_file in file_pairs:
            merged_readings, resource_metadata = self.merge_consumption_and_cost_data(consumption_file, cost_file)
            daily_data = self._group_and_sum_by_day(merged_readings)

            for day, values in daily_data.items():
                year = day.split('-')[0]
                yearly_data[year][day]['consumption_value'] += values['consumption_value']
                yearly_data[year][day]['cost_value'] += values['cost_value']
                yearly_data[year][day]['count'] += values['count']

        output_files = []
        for year, daily_readings in yearly_data.items():
            output_file = self.output_dir / f"{year}_glowmarkt_data.jsonl"
            output_files.append(str(output_file))
            with open(output_file, 'w') as f:
                for day, data in sorted(daily_readings.items()):
                    json_line = {
                        'date': day,
                        'consumption_total': data['consumption_value'],
                        'cost_total': data['cost_value'],
                        'reading_count': data['count']
                    }
                    f.write(json.dumps(json_line) + '\n')
            logger.info(f"Written {len(daily_readings)} days of data to {output_file}")

        return output_files
