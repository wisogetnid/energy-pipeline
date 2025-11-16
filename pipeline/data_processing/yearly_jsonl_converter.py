import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

from pipeline.data_processing.jsonl_converter import EnergyDataConverter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class YearlyEnergyDataConverter(EnergyDataConverter):
    
    def __init__(self, output_dir: Optional[Union[str, Path]] = None):
        if output_dir:
            super().__init__(output_dir)
        else:
            super().__init__()
        self.resource_metadata = {}
    
    def find_matching_resource_files(self, directory: Union[str, Path]) -> List[Tuple[str, str]]:
        directory = Path(directory)
        jsonl_files = list(directory.glob("*.jsonl"))
        
        if not jsonl_files:
            json_files = list(directory.glob("*.json"))
            return [(str(f), str(f)) for f in json_files]
        
        logger.info(f"Found {len(jsonl_files)} JSONL files in {directory}")
        return [(str(f), str(f)) for f in jsonl_files]
    
    def _group_and_sum_by_day(self, merged_readings: Dict[int, Dict]) -> Dict[str, Dict]:
        daily_data = defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0})
        for ts, reading in merged_readings.items():
            try:

                if isinstance(ts, int) or isinstance(ts, float):
                    timestamp_seconds = ts / 1000 if ts > 9999999999 else ts
                    day = datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d')
                else:

                    day = datetime.fromisoformat(str(ts).replace('Z', '+00:00')).strftime('%Y-%m-%d')
            except:
                try:

                    if 'timestamp_iso' in reading and reading['timestamp_iso']:
                        day = reading['timestamp_iso'].split('T')[0]
                    else:
                        continue
                except:
                    continue

            
            consumption = None
            cost = None
            

            if 'consumption_value' in reading and reading['consumption_value'] is not None:
                consumption = reading['consumption_value']
            if 'cost_value' in reading and reading['cost_value'] is not None:
                cost = reading['cost_value']
            

            resource_types = ['electricity', 'gas', 'water']
            for resource in resource_types:
                if f'{resource}_consumption' in reading and reading[f'{resource}_consumption'] is not None:
                    if consumption is None:
                        consumption = 0
                    consumption += reading[f'{resource}_consumption']
                if f'{resource}_cost' in reading and reading[f'{resource}_cost'] is not None:
                    if cost is None:
                        cost = 0
                    cost += reading[f'{resource}_cost']
            

            if consumption is not None:
                daily_data[day]['consumption_value'] += consumption
            if cost is not None:
                daily_data[day]['cost_value'] += cost
            daily_data[day]['count'] += 1
            
        return daily_data
    
    def _extract_year_from_file(self, file_path: Path) -> str:
        filename = file_path.name.lower()
        

        year_pattern = r'(\d{4})\d{4}'
        match = re.search(year_pattern, filename)
        if match:
            return match.group(1)
            

        year_pattern2 = r'(\d{4})-\d{2}'
        match = re.search(year_pattern2, filename)
        if match:
            return match.group(1)
        

        if file_path.suffix.lower() == '.jsonl':
            try:
                with open(file_path, 'r') as f:
                    first_line = f.readline().strip()
                    data = json.loads(first_line)
                    

                    if 'date' in data and isinstance(data['date'], str):
                        return data['date'].split('-')[0]
                    

                    if 'from_date' in data and isinstance(data['from_date'], str):
                        return data['from_date'].split('-')[0]
                    

                    if 'timestamp_iso' in data and isinstance(data['timestamp_iso'], str):
                        return data['timestamp_iso'].split('-')[0]
            except:
                pass
        

        return "unknown"
    
    def _process_jsonl_file(self, file_path: Path) -> Dict[str, Dict[str, Dict]]:
        yearly_data = defaultdict(lambda: defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0, 'readings': []}))
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        

                        timestamp = None
                        if 'timestamp' in data:
                            timestamp = data['timestamp']
                        elif 'date' in data:

                            try:
                                date = datetime.fromisoformat(data['date'])
                                timestamp = date.timestamp()
                            except:
                                pass
                        
                        if timestamp is None:
                            continue
                        

                        try:
                            if isinstance(timestamp, (int, float)):
                                timestamp_seconds = timestamp / 1000 if timestamp > 9999999999 else timestamp
                                dt = datetime.fromtimestamp(timestamp_seconds)
                            else:
                                dt = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                            
                            date_str = dt.strftime('%Y-%m-%d')
                            year = dt.strftime('%Y')
                        except:

                            if 'date' in data and isinstance(data['date'], str):
                                date_str = data['date']
                                year = date_str.split('-')[0]
                            elif 'timestamp_iso' in data and isinstance(data['timestamp_iso'], str):
                                date_str = data['timestamp_iso'].split('T')[0]
                                year = date_str.split('-')[0]
                            else:
                                continue
                        

                        consumption = None
                        cost = None
                        

                        consumption_fields = ['consumption_value', 'consumption', 'consumption_total', 'value']
                        cost_fields = ['cost_value', 'cost', 'cost_total']
                        
                        for field in consumption_fields:
                            if field in data and data[field] is not None:
                                consumption = float(data[field])
                                break
                        
                        for field in cost_fields:
                            if field in data and data[field] is not None:
                                cost = float(data[field])
                                break
                        

                        resource_types = ['electricity', 'gas', 'water']
                        if consumption is None and cost is None:
                            for resource in resource_types:
                                consumption_key = f'{resource}_consumption'
                                cost_key = f'{resource}_cost'
                                
                                if consumption_key in data and data[consumption_key] is not None:
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
                            yearly_data[year][date_str]['consumption_value'] += consumption
                        if cost is not None:
                            yearly_data[year][date_str]['cost_value'] += cost
                        
                        yearly_data[year][date_str]['count'] += 1
                        yearly_data[year][date_str]['readings'].append({
                            'timestamp': timestamp,
                            'consumption': consumption,
                            'cost': cost
                        })
                        
                        # Store resource-specific consumption and cost
                        for resource in resource_types:
                            consumption_key = f'{resource}_consumption'
                            cost_key = f'{resource}_cost'
                            
                            if consumption_key in data and data[consumption_key] is not None:
                                if consumption_key not in yearly_data[year][date_str]:
                                    yearly_data[year][date_str][consumption_key] = 0
                                yearly_data[year][date_str][consumption_key] += float(data[consumption_key])
                                
                                # Store resource metadata
                                if resource not in self.resource_metadata:
                                    self.resource_metadata[resource] = {
                                        'consumption_unit': data.get(f'{resource}_consumption_unit', 'unknown'),
                                        'cost_unit': data.get(f'{resource}_cost_unit', 'unknown')
                                    }
                            
                            if cost_key in data and data[cost_key] is not None:
                                if cost_key not in yearly_data[year][date_str]:
                                    yearly_data[year][date_str][cost_key] = 0
                                yearly_data[year][date_str][cost_key] += float(data[cost_key])
                    except Exception as e:
                        logger.warning(f"Error processing line in {file_path}: {e}")
                        continue
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
        
        return yearly_data
    
    def convert_to_yearly_jsonl(self, files: List[Union[Path, str, tuple]]) -> List[str]:
        yearly_data_combined = defaultdict(lambda: defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0}))
        self.resource_metadata = {}
        for file_item in files:
            if isinstance(file_item, tuple):
                file_path = Path(file_item[0])
            else:
                file_path = Path(file_item)
            logger.info(f"Processing file: {file_path}")
            if file_path.suffix.lower() == '.jsonl':
                file_yearly_data = self._process_jsonl_file(file_path)
                for year, days in file_yearly_data.items():
                    for day, data in days.items():
                        for k, v in data.items():
                            if k in yearly_data_combined[year][day] and isinstance(v, (int, float)):
                                yearly_data_combined[year][day][k] += v
                            elif k not in yearly_data_combined[year][day]:
                                yearly_data_combined[year][day][k] = v
            elif file_path.suffix.lower() == '.json':
                if isinstance(file_item, tuple) and len(file_item) == 2:
                    consumption_file, cost_file = file_item
                    merged_readings, resource_metadata = self.merge_consumption_and_cost_data(consumption_file, cost_file)
                    daily_data = self._group_and_sum_by_day(merged_readings)
                    resource_type = resource_metadata.get('resource_type', 'unknown')
                    if resource_type != 'unknown':
                        self.resource_metadata[resource_type] = resource_metadata
                    for day, values in daily_data.items():
                        year = day.split('-')[0]
                        for k, v in values.items():
                            if k in yearly_data_combined[year][day] and isinstance(v, (int, float)):
                                yearly_data_combined[year][day][k] += v
                            elif k not in yearly_data_combined[year][day]:
                                yearly_data_combined[year][day][k] = v
        output_files = []
        for year, daily_readings in yearly_data_combined.items():
            if not daily_readings:
                continue
            output_file = self.output_dir / f"{year}_annual_energy_summary.jsonl"
            output_files.append(str(output_file))
            monthly_data = defaultdict(lambda: {'consumption_value': 0, 'cost_value': 0, 'count': 0})
            for day, data in daily_readings.items():
                month = day[:7]
                for k, v in data.items():
                    if k in monthly_data[month] and isinstance(v, (int, float)):
                        monthly_data[month][k] += v
                    elif k not in monthly_data[month]:
                        monthly_data[month][k] = v
            with open(output_file, 'w') as f:
                # Yearly summary
                summary = {
                    'year': year,
                    'consumption_total': sum(d['consumption_value'] for d in daily_readings.values()),
                    'cost_total': sum(d['cost_value'] for d in daily_readings.values()),
                    'reading_count': sum(d['count'] for d in daily_readings.values()),
                    'days_with_readings': len(daily_readings),
                    'from_date': f"{year}-01-01",
                    'to_date': f"{year}-12-31",
                    'data_type': 'yearly_summary'
                }
                for resource, meta in self.resource_metadata.items():
                    c_key = f'{resource}_consumption'
                    cost_key = f'{resource}_cost'
                    summary[f'{resource}_consumption_total'] = sum(d.get(c_key, 0) for d in daily_readings.values())
                    summary[f'{resource}_consumption_unit'] = meta.get('consumption_unit', 'unknown')
                    summary[f'{resource}_cost_total'] = sum(d.get(cost_key, 0) for d in daily_readings.values())
                    summary[f'{resource}_cost_unit'] = meta.get('cost_unit', 'unknown')
                f.write(json.dumps(summary) + '\n')
                # Monthly summaries
                for month, data in sorted(monthly_data.items()):
                    month_summary = {
                        'year': year,
                        'month': month,
                        'consumption_total': data['consumption_value'],
                        'cost_total': data['cost_value'],
                        'reading_count': data['count'],
                        'data_type': 'monthly_summary'
                    }
                    for resource, meta in self.resource_metadata.items():
                        c_key = f'{resource}_consumption'
                        cost_key = f'{resource}_cost'
                        month_summary[f'{resource}_consumption_total'] = data.get(c_key, 0)
                        month_summary[f'{resource}_consumption_unit'] = meta.get('consumption_unit', 'unknown')
                        month_summary[f'{resource}_cost_total'] = data.get(cost_key, 0)
                        month_summary[f'{resource}_cost_unit'] = meta.get('cost_unit', 'unknown')
                    f.write(json.dumps(month_summary) + '\n')
                # Daily summaries
                for day, data in sorted(daily_readings.items()):
                    day_summary = {
                        'date': day,
                        'consumption_total': data['consumption_value'],
                        'cost_total': data['cost_value'],
                        'reading_count': data['count'],
                        'data_type': 'daily_summary'
                    }
                    for resource, meta in self.resource_metadata.items():
                        c_key = f'{resource}_consumption'
                        cost_key = f'{resource}_cost'
                        if c_key in data:
                            day_summary[f'{resource}_consumption'] = data[c_key]
                            day_summary[f'{resource}_consumption_unit'] = meta.get('consumption_unit', 'unknown')
                        if cost_key in data:
                            day_summary[f'{resource}_cost'] = data[cost_key]
                            day_summary[f'{resource}_cost_unit'] = meta.get('cost_unit', 'unknown')
                    f.write(json.dumps(day_summary) + '\n')
            logger.info(f"Written yearly summary for {year} with {len(daily_readings)} days to {output_file}")
        return output_files
