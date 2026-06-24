import os
import glob
from pathlib import Path
import matplotlib.pyplot as plt
import json
from pipeline.ui.base_ui import BaseUI
from pipeline.data_visualisation.energy_efficiency import (
    generate_consumption_patterns,
    generate_weekly_comparison,
    generate_weekday_weekend_pattern,
    load_and_process_consumption_data,
)


class VisualizationUI(BaseUI):
    def __init__(self):
        super().__init__()
        self.data_dir = Path("data/processed")
        self.output_dir = Path("data/visualisations")

    def find_consumption_files(self):
        all_files = glob.glob(str(self.data_dir / "*_consumption_*.parquet"))
        if not all_files:
            all_files = glob.glob(str(self.data_dir / "*_consumption_*.jsonl"))
        resources = {}
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            resource_name = file_name.split("_")[0]
            date_part = (
                "_".join(file_name.split("_")[2:])
                .replace(".parquet", "")
                .replace(".jsonl", "")
            )
            key = f"{resource_name}_{date_part}"
            resources[key] = file_path
        return resources

    def run_visualization(self):
        self.print_header("Energy Efficiency Visualization")
        consumption_files = self.find_consumption_files()
        if not consumption_files:
            print(
                "No consumption data files found. Please retrieve and convert data first."
            )
            return False
        print("\nAvailable consumption data for visualization:")
        file_items = list(consumption_files.items())
        for i, (key, file_path) in enumerate(file_items, 1):
            resource_name = key.split("_")[0]
            date_part = "_".join(key.split("_")[1:])
            print(f"{i}. {resource_name} ({date_part})")
        print("\nOptions:")
        print("1. Visualize a specific resource")
        print("2. Visualize all resources")
        print("3. Go back")
        choice = self.get_int_input("\nEnter your choice: ", 1, 3)
        if choice == 1:
            file_idx = self.get_int_input("Enter resource number: ", 1, len(file_items))
            selected_key, selected_file = file_items[file_idx - 1]
            return self.generate_efficiency_charts(selected_file, selected_key)
        elif choice == 2:
            success_count = 0
            for key, file_path in file_items:
                result = self.generate_efficiency_charts(file_path, key)
                if result:
                    success_count += 1
            print(
                f"\nGenerated visualizations for {success_count} out of {len(file_items)} resources."
            )
            print(
                f"Visualizations are saved in separate folders under {self.output_dir}"
            )
            return success_count > 0
        elif choice == 3:
            return False
        return False

    def generate_efficiency_charts(self, consumption_file_path, resource_key):
        output_folder = self.output_dir / resource_key
        output_folder.mkdir(parents=True, exist_ok=True)
        try:
            df, resource_type, unit = load_and_process_consumption_data(
                consumption_file_path
            )
            print(f"\nGenerating consumption patterns for {resource_type}...")
            pattern_file = generate_consumption_patterns(
                df, resource_type, unit, output_folder
            )
            print(f"Generating weekly comparison for {resource_type}...")
            weekly_file = generate_weekly_comparison(
                df, resource_type, unit, output_folder
            )
            print(f"Generating weekday vs weekend patterns for {resource_type}...")
            weekday_weekend_file = generate_weekday_weekend_pattern(
                df, resource_type, unit, output_folder
            )
            print(f"Visualizations saved to {output_folder}")
            return True
        except Exception as e:
            print(f"Error generating visualizations: {str(e)}")
            return False

    def run_monthly_summary_barchart(self):
        self.print_header("Monthly Consumption/Cost Comparison")
        jsonlSummaryFiles = sorted(
            Path("data/processed").glob("*_annual_energy_summary.jsonl")
        )
        parquetProcessedFiles = sorted(
            Path("data/processed").glob("*_annual_energy_summary.parquet")
        )
        parquetDirFiles = sorted(
            Path("data/parquet").glob("*_annual_energy_summary.parquet")
        )
        allFiles = []
        yearlyData = {}
        if jsonlSummaryFiles:
            allFiles.extend(jsonlSummaryFiles)
        if parquetProcessedFiles or parquetDirFiles:
            allParquetFiles = parquetProcessedFiles + parquetDirFiles
            allFiles.extend(allParquetFiles)
        if not allFiles:
            print("No annual summary files found in data/processed or data/parquet.")
            return False
        print(f"Found {len(allFiles)} annual summary files.")
        for dataFile in allFiles:
            print(f"Processing file: {dataFile.name}")
            year = dataFile.name.split("_")[0]
            if dataFile.suffix.lower() == ".jsonl":
                months = []
                consumptionTotals = []
                costTotals = []
                resource_consumptions = {}
                resource_costs = {}
                with open(dataFile, "r") as f:
                    for line in f:
                        entry = json.loads(line)
                        if entry.get("data_type") == "monthly_summary":
                            monthLabel = entry["month"].split("-")[1]
                            months.append(monthLabel)
                            consumptionTotals.append(entry["consumption_total"])
                            costTotals.append(entry["cost_total"])
                            for k, v in entry.items():
                                if isinstance(k, str) and k.endswith(
                                    "_consumption_total"
                                ):
                                    resource = k.replace("_consumption_total", "")
                                    if resource not in resource_consumptions:
                                        resource_consumptions[resource] = []
                                    resource_consumptions[resource].append(v)
                                if isinstance(k, str) and k.endswith("_cost_total"):
                                    resource = k.replace("_cost_total", "")
                                    if resource not in resource_costs:
                                        resource_costs[resource] = []
                                    resource_costs[resource].append(v)
                if months:
                    print(f"  Year {year}: Found months {months}")
                    yearlyData[year] = {
                        "months": months,
                        "consumption": consumptionTotals,
                        "cost": costTotals,
                        "resource_consumptions": resource_consumptions,
                        "resource_costs": resource_costs,
                    }
            elif dataFile.suffix.lower() == ".parquet":
                try:
                    import pandas as pd

                    df = pd.read_parquet(dataFile)
                    monthlyDf = df[df["data_type"] == "monthly_summary"]
                    if not monthlyDf.empty:
                        months = []
                        resource_consumptions = {}
                        resource_costs = {}
                        for idx, row in monthlyDf.iterrows():
                            monthLabel = row["month"].split("-")[1]
                            months.append(monthLabel)
                            for k, v in row.items():
                                if isinstance(k, str) and k.endswith(
                                    "_consumption_total"
                                ):
                                    resource = k.replace("_consumption_total", "")
                                    if resource not in resource_consumptions:
                                        resource_consumptions[resource] = []
                                    resource_consumptions[resource].append(v)
                                if isinstance(k, str) and k.endswith("_cost_total"):
                                    resource = k.replace("_cost_total", "")
                                    if resource not in resource_costs:
                                        resource_costs[resource] = []
                                    resource_costs[resource].append(v)
                        yearlyData[year] = {
                            "months": months,
                            "resource_consumptions": resource_consumptions,
                            "resource_costs": resource_costs,
                        }
                        print(f"  Year {year} (parquet): Found months {months}")
                except Exception as e:
                    print(f"Error processing parquet file {dataFile}: {str(e)}")
        if not yearlyData:
            print("No monthly summary data found in any files.")
            return False
        self._create_overlay_charts(yearlyData)
        return True

    def _create_overlay_charts(self, yearlyData):
        import numpy as np
        import matplotlib.pyplot as plt

        resource_keys = set()
        for year, data in yearlyData.items():
            if "resource_consumptions" in data:
                for r in data["resource_consumptions"]:
                    resource_keys.add(r)
        if not resource_keys:
            for year, data in yearlyData.items():
                for k in data.keys():
                    if k not in ["months", "consumption", "cost"] and k.endswith(
                        "_consumption"
                    ):
                        resource_keys.add(k.replace("_consumption", ""))
        resource_keys = sorted(resource_keys)
        year_resource_month = {}
        year_resource_cost_month = {}
        for year, data in yearlyData.items():
            months = data["months"]
            year_resource_month[year] = {}
            year_resource_cost_month[year] = {}
            for resource in resource_keys:
                values = [0] * 12
                cost_values = [0] * 12
                if (
                    "resource_consumptions" in data
                    and resource in data["resource_consumptions"]
                ):
                    for i, m in enumerate(months):
                        try:
                            month_idx = int(m) - 1
                            if 0 <= month_idx < 12 and i < len(
                                data["resource_consumptions"][resource]
                            ):
                                values[month_idx] = data["resource_consumptions"][
                                    resource
                                ][i]
                        except (ValueError, TypeError):
                            continue
                if "resource_costs" in data and resource in data["resource_costs"]:
                    for i, m in enumerate(months):
                        try:
                            month_idx = int(m) - 1
                            if 0 <= month_idx < 12 and i < len(
                                data["resource_costs"][resource]
                            ):
                                cost_values[month_idx] = data["resource_costs"][
                                    resource
                                ][i]
                        except (ValueError, TypeError):
                            continue
                year_resource_month[year][resource] = values
                year_resource_cost_month[year][resource] = cost_values
        print("\nTotal energy consumption per resource (last 12 months):")
        from collections import defaultdict

        resource_months = defaultdict(list)
        for year in sorted(year_resource_month.keys()):
            for month_idx in range(12):
                for resource in resource_keys:
                    value = year_resource_month[year][resource][month_idx]
                    resource_months[resource].append(
                        ((int(year), month_idx + 1), value)
                    )
        print(f"Debug: Found {len(resource_months)} resources with monthly data")
        for resource in resource_keys:
            sorted_months = sorted(
                resource_months[resource], key=lambda x: (x[0][0], x[0][1])
            )
            print(f"Debug: {resource} has {len(sorted_months)} month entries")
            print(f"Debug: Last few months for {resource}: {sorted_months[-6:]}")
            last12_with_zero = [v for (_, v) in sorted_months][-12:]
            last12 = [v for (_, v) in sorted_months if v is not None and v > 0][-12:]
            total = sum(last12)
            print(
                f"  {resource}: {total:.2f} (last 12 non-zero months out of {len(last12_with_zero)} total)"
            )
        plt.figure(figsize=(14, 8))
        bar_width = 0.7 / max(1, len(year_resource_month))
        x = np.arange(12)
        for idx, (year, resource_months_) in enumerate(
            sorted(year_resource_month.items())
        ):
            bottom = np.zeros(12)
            for resource in resource_keys:
                vals = resource_months_[resource]
                plt.bar(
                    x + idx * bar_width,
                    vals,
                    bar_width,
                    label=f"{year} {resource}",
                    bottom=bottom,
                )
                bottom += np.array(vals)
        plt.title("Monthly Resource Consumption Comparison (Stacked by Resource)")
        plt.xlabel("Month")
        plt.ylabel("Consumption")
        plt.xticks(
            x + bar_width * (len(year_resource_month) - 1) / 2,
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        visualisationsDir = Path("data/visualisations") / "monthly_summary"
        visualisationsDir.mkdir(parents=True, exist_ok=True)
        consumptionPath = visualisationsDir / "yearly_resource_consumption_stacked.png"
        plt.tight_layout()
        plt.savefig(consumptionPath)
        plt.close()
        plt.figure(figsize=(14, 8))
        for idx, (year, resource_cost_months) in enumerate(
            sorted(year_resource_cost_month.items())
        ):
            bottom = np.zeros(12)
            for resource in resource_keys:
                vals = resource_cost_months[resource]
                plt.bar(
                    x + idx * bar_width,
                    vals,
                    bar_width,
                    label=f"{year} {resource}",
                    bottom=bottom,
                )
                bottom += np.array(vals)
        plt.title("Monthly Resource Cost Comparison (Stacked by Resource)")
        plt.xlabel("Month")
        plt.ylabel("Cost")
        plt.xticks(
            x + bar_width * (len(year_resource_cost_month) - 1) / 2,
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        costStackedPath = visualisationsDir / "yearly_resource_cost_stacked.png"
        plt.tight_layout()
        plt.savefig(costStackedPath)
        plt.close()
        plt.figure(figsize=(12, 7))
        for year, data in sorted(yearlyData.items()):
            if len(data.get("months", [])) == 0:
                continue
            cost_padded = [0] * 12
            months = data["months"]
            costs = data.get("cost", None)
            if not costs or all(c == 0 for c in costs):
                resource_costs = data.get("resource_costs", {})
                for i, m in enumerate(months):
                    try:
                        idx = int(m) - 1
                        if 0 <= idx < 12:
                            cost_padded[idx] = sum(
                                (
                                    resource_costs[r][i]
                                    if i < len(resource_costs[r])
                                    else 0
                                )
                                for r in resource_costs
                            )
                    except Exception:
                        continue
            else:
                for i, m in enumerate(months):
                    try:
                        idx = int(m) - 1
                        if 0 <= idx < 12 and i < len(costs):
                            cost_padded[idx] = costs[i]
                    except Exception:
                        continue
            plt.plot(range(12), cost_padded, marker="o", label=year)
        plt.title("Monthly Cost Comparison Across Years")
        plt.xlabel("Month")
        plt.ylabel("Cost")
        plt.xticks(
            range(12),
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        costPath = visualisationsDir / "yearly_cost_comparison.png"
        plt.tight_layout()
        plt.savefig(costPath)
        plt.close()
        import matplotlib.colors as mcolors

        plt.figure(figsize=(14, 8))
        base_colors = list(mcolors.TABLEAU_COLORS.values())
        year_list = list(sorted(year_resource_month.keys()))
        bar_width = 0.7 / max(1, len(year_list))
        x = np.arange(12)
        for idx, year in enumerate(year_list):
            resource_months_ = year_resource_month[year]
            bottom = np.zeros(12)
            base_color = base_colors[idx % len(base_colors)]
            n_resources = len(resource_keys)
            shades = [
                mcolors.to_rgba(
                    base_color, alpha=0.7 - 0.5 * (i / max(1, n_resources - 1))
                )
                for i in range(n_resources)
            ]
            for r_idx, resource in enumerate(resource_keys):
                vals = resource_months_[resource]
                plt.bar(
                    x + idx * bar_width,
                    vals,
                    bar_width,
                    label=f"{year} {resource}",
                    bottom=bottom,
                    color=shades[r_idx],
                )
                bottom += np.array(vals)
        plt.title("Monthly Resource Consumption Comparison (Stacked by Resource)")
        plt.xlabel("Month")
        plt.ylabel("Consumption")
        plt.xticks(
            x + bar_width * (len(year_list) - 1) / 2,
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        consumptionPath = visualisationsDir / "yearly_resource_consumption_stacked.png"
        plt.tight_layout()
        plt.savefig(consumptionPath)
        plt.close()
        plt.figure(figsize=(14, 8))
        for idx, year in enumerate(year_list):
            resource_cost_months = year_resource_cost_month[year]
            bottom = np.zeros(12)
            base_color = base_colors[idx % len(base_colors)]
            n_resources = len(resource_keys)
            shades = [
                mcolors.to_rgba(
                    base_color, alpha=0.7 - 0.5 * (i / max(1, n_resources - 1))
                )
                for i in range(n_resources)
            ]
            for r_idx, resource in enumerate(resource_keys):
                vals = resource_cost_months[resource]
                plt.bar(
                    x + idx * bar_width,
                    vals,
                    bar_width,
                    label=f"{year} {resource}",
                    bottom=bottom,
                    color=shades[r_idx],
                )
                bottom += np.array(vals)
        plt.title("Monthly Resource Cost Comparison (Stacked by Resource)")
        plt.xlabel("Month")
        plt.ylabel("Cost")
        plt.xticks(
            x + bar_width * (len(year_list) - 1) / 2,
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        costStackedPath = visualisationsDir / "yearly_resource_cost_stacked.png"
        plt.tight_layout()
        plt.savefig(costStackedPath)
        plt.close()

        for resource_type in ["electricity", "gas"]:
            if resource_type in resource_keys:
                plt.figure(figsize=(14, 8))
                for idx, year in enumerate(year_list):
                    resource_months_ = year_resource_month[year]
                    vals = resource_months_.get(resource_type, [0] * 12)
                    plt.bar(
                        x + idx * bar_width,
                        vals,
                        bar_width,
                        label=f"{year} {resource_type}",
                        color=base_colors[idx % len(base_colors)],
                    )
                plt.title(
                    f"Monthly {resource_type.capitalize()} Consumption Comparison"
                )
                plt.xlabel("Month")
                plt.ylabel("Consumption")
                plt.xticks(
                    x + bar_width * (len(year_list) - 1) / 2,
                    [
                        "Jan",
                        "Feb",
                        "Mar",
                        "Apr",
                        "May",
                        "Jun",
                        "Jul",
                        "Aug",
                        "Sep",
                        "Oct",
                        "Nov",
                        "Dec",
                    ],
                )
                plt.legend()
                plt.grid(True, linestyle="--", alpha=0.7)
                specific_path = (
                    visualisationsDir
                    / f"yearly_resource_consumption_{resource_type}.png"
                )
                plt.tight_layout()
                plt.savefig(specific_path)
                plt.close()

        print(f"Created overlaid yearly comparison charts:")
        print(f"- Resource Consumption (stacked): {consumptionPath}")
        print(
            f"- Electricity Consumption: {visualisationsDir / 'yearly_resource_consumption_electricity.png'}"
        )
        print(
            f"- Gas Consumption: {visualisationsDir / 'yearly_resource_consumption_gas.png'}"
        )
        print(f"- Resource Cost (stacked): {costStackedPath}")
        print(f"- Cost (line): {costPath}")

    def get_tariff_config(self):
        config_path = Path("tariff_config.json")
        if not config_path.exists():
            default_config = {
                "current_plan": {
                    "name": "Current Plan",
                    "electricity": {
                        "standing_charge_pence_per_day": 0.0,
                        "unit_rate_pence_per_kwh": 0.0,
                    },
                    "gas": {
                        "standing_charge_pence_per_day": 0.0,
                        "unit_rate_pence_per_kwh": 0.0,
                    },
                },
                "comparison_plans": [
                    {
                        "name": "Example Comparison Plan",
                        "electricity": {
                            "standing_charge_pence_per_day": 0.0,
                            "unit_rate_pence_per_kwh": 0.0,
                        },
                        "gas": {
                            "standing_charge_pence_per_day": 0.0,
                            "unit_rate_pence_per_kwh": 0.0,
                        },
                    }
                ],
            }
            with open(config_path, "w") as f:
                json.dump(default_config, f, indent=4)
            print(f"\nCreated default tariff configuration file at {config_path}.")
            print(
                "Please update it with your actual current rates and comparison plans."
            )
            return default_config

        with open(config_path, "r") as f:
            return json.load(f)

    def compare_theoretical_costs_cli(self):
        import calendar
        from collections import defaultdict

        jsonlSummaryFiles = sorted(
            Path("data/processed").glob("*_annual_energy_summary.jsonl")
        )
        if not jsonlSummaryFiles:
            print("No annual summary JSONL files found in data/processed.")
            return False
        month_entries = []
        for dataFile in jsonlSummaryFiles:
            year = dataFile.name.split("_")[0]
            with open(dataFile, "r") as f:
                for line in f:
                    entry = json.loads(line)
                    if entry.get("data_type") == "monthly_summary":
                        monthLabel = entry["month"].split("-")[1]
                        monthNum = int(monthLabel)
                        resource_consumptions = {}
                        resource_costs = {}
                        for k, v in entry.items():
                            if isinstance(k, str) and k.endswith("_consumption_total"):
                                resource = k.replace("_consumption_total", "")
                                resource_consumptions[resource] = v
                            if isinstance(k, str) and k.endswith("_cost_total"):
                                resource = k.replace("_cost_total", "")
                                resource_costs[resource] = v
                        month_entries.append(
                            (int(year), monthNum, resource_consumptions, resource_costs)
                        )
        if not month_entries:
            print("No monthly summary data found in any JSONL files.")
            return False
        month_entries.sort()
        last_12 = month_entries[-12:]
        config = self.get_tariff_config()
        current_tariff = config.get("current_plan", {})
        print("\nEnter new rates:")
        elec_standing = self.get_float_input(
            "Electricity standing charge (pence per day): "
        )
        elec_unit = self.get_float_input("Electricity unit rate (pence per kWh): ")
        gas_standing = self.get_float_input("Gas standing charge (pence per day): ")
        gas_unit = self.get_float_input("Gas unit rate (pence per kWh): ")
        current_tariff_costs = {"electricity": [], "gas": []}
        theoretical_costs = {"electricity": [], "gas": []}
        xlabels = []
        for year, month, resource_consumptions, resource_costs in last_12:
            label = f"{year}-{month:02d}"
            xlabels.append(label)
            for resource in ["electricity", "gas"]:
                consumption = resource_consumptions.get(resource, 0)
                days_in_month = calendar.monthrange(year, month)[1]
                if resource == "electricity":
                    theo = (consumption * elec_unit) + (elec_standing * days_in_month)
                    curr = (
                        consumption
                        * current_tariff.get("electricity", {}).get(
                            "unit_rate_pence_per_kwh", 0.0
                        )
                    ) + (
                        current_tariff.get("electricity", {}).get(
                            "standing_charge_pence_per_day", 0.0
                        )
                        * days_in_month
                    )
                else:
                    theo = (consumption * gas_unit) + (gas_standing * days_in_month)
                    curr = (
                        consumption
                        * current_tariff.get("gas", {}).get(
                            "unit_rate_pence_per_kwh", 0.0
                        )
                    ) + (
                        current_tariff.get("gas", {}).get(
                            "standing_charge_pence_per_day", 0.0
                        )
                        * days_in_month
                    )
                current_tariff_costs[resource].append(curr)
                theoretical_costs[resource].append(theo)
        import matplotlib.pyplot as plt

        plt.figure(figsize=(15, 8))
        x = range(12)
        for resource, color in zip(["electricity", "gas"], ["tab:blue", "tab:orange"]):
            plt.plot(
                x,
                current_tariff_costs[resource],
                marker="s",
                linestyle="-.",
                label=f"Current {resource.capitalize()} ({current_tariff.get('name', 'Config')})",
                color=color,
                alpha=0.8,
            )
            plt.plot(
                x,
                theoretical_costs[resource],
                marker="^",
                linestyle="--",
                label=f"Theoretical {resource.capitalize()} Cost",
                color=color,
                alpha=0.6,
            )
            for i in x:
                plt.annotate(
                    f"{current_tariff_costs[resource][i]:.2f}",
                    (i, current_tariff_costs[resource][i]),
                    textcoords="offset points",
                    xytext=(0, 6),
                    ha="center",
                    fontsize=8,
                    color=color,
                )
                plt.annotate(
                    f"{theoretical_costs[resource][i]:.2f}",
                    (i, theoretical_costs[resource][i]),
                    textcoords="offset points",
                    xytext=(0, -12),
                    ha="center",
                    fontsize=8,
                    color=color,
                    alpha=0.7,
                )
        input_text = (
            f"Current ({current_tariff.get('name', 'Config')}):\n"
            f"  Elec: {current_tariff.get('electricity', {}).get('standing_charge_pence_per_day', 0.0)}p/d, {current_tariff.get('electricity', {}).get('unit_rate_pence_per_kwh', 0.0)}p/kWh\n"
            f"  Gas: {current_tariff.get('gas', {}).get('standing_charge_pence_per_day', 0.0)}p/d, {current_tariff.get('gas', {}).get('unit_rate_pence_per_kwh', 0.0)}p/kWh\n"
            f"New Plan:\n"
            f"  Elec: {elec_standing}p/d, {elec_unit}p/kWh\n"
            f"  Gas: {gas_standing}p/d, {gas_unit}p/kWh"
        )
        plt.gcf().text(
            0.99,
            0.01,
            input_text,
            fontsize=10,
            ha="right",
            va="bottom",
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="gray"),
        )
        plt.title(
            f"Monthly Cost Comparison (Actual vs. Theoretical) for Last 12 Months"
        )
        plt.xlabel("Month")
        plt.ylabel("Cost")
        plt.xticks(x, xlabels, rotation=45)
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        visualisationsDir = Path("data/visualisations") / "theoretical_comparison"
        visualisationsDir.mkdir(parents=True, exist_ok=True)
        outpath = (
            visualisationsDir / f"last12_actual_vs_theoretical_electricity_gas.png"
        )
        plt.tight_layout()
        plt.savefig(outpath)
        plt.close()
        print(f"\nComparison chart saved to: {outpath}")

        print("\nYearly Total Cost Summary (Last 12 Months):")

        curr_elec_total = round(sum(current_tariff_costs["electricity"]) / 100, 2)
        curr_gas_total = round(sum(current_tariff_costs["gas"]) / 100, 2)
        curr_total = curr_elec_total + curr_gas_total
        print(f"\nCurrent Tariff ({current_tariff.get('name', 'Config')}):")
        print(f"  Electricity: £{curr_elec_total:.2f}")
        print(f"  Gas: £{curr_gas_total:.2f}")
        print(f"  Total: £{curr_total:.2f}")

        theo_elec_total = round(sum(theoretical_costs["electricity"]) / 100, 2)
        theo_gas_total = round(sum(theoretical_costs["gas"]) / 100, 2)
        theo_total = theo_elec_total + theo_gas_total
        print(f"\nTheoretical:")
        print(f"  Electricity: £{theo_elec_total:.2f}")
        print(f"  Gas: £{theo_gas_total:.2f}")
        print(f"  Total: £{theo_total:.2f}")

        return True

    def compare_theoretical_costs_multi_plans_cli(self):
        import calendar

        jsonlSummaryFiles = sorted(
            Path("data/processed").glob("*_annual_energy_summary.jsonl")
        )
        if not jsonlSummaryFiles:
            print("No annual summary JSONL files found in data/processed.")
            return False
        month_entries = []
        for dataFile in jsonlSummaryFiles:
            year = dataFile.name.split("_")[0]
            with open(dataFile, "r") as f:
                for line in f:
                    entry = json.loads(line)
                    if entry.get("data_type") == "monthly_summary":
                        monthLabel = entry["month"].split("-")[1]
                        monthNum = int(monthLabel)
                        resource_consumptions = {}
                        resource_costs = {}
                        for k, v in entry.items():
                            if isinstance(k, str) and k.endswith("_consumption_total"):
                                resource = k.replace("_consumption_total", "")
                                resource_consumptions[resource] = v
                            if isinstance(k, str) and k.endswith("_cost_total"):
                                resource = k.replace("_cost_total", "")
                                resource_costs[resource] = v
                        month_entries.append(
                            (int(year), monthNum, resource_consumptions, resource_costs)
                        )
        if not month_entries:
            print("No monthly summary data found in any JSONL files.")
            return False
        month_entries.sort()
        last_12 = month_entries[-12:]
        config = self.get_tariff_config()
        current_tariff = config.get("current_plan", {})
        comparison_plans_config = config.get("comparison_plans", [])

        print("\n" + "=" * 50)
        print("Current Tariff Configuration:")
        print(f"  Name: {current_tariff.get('name', 'Unnamed')}")
        print(
            f"  Elec: {current_tariff.get('electricity', {}).get('standing_charge_pence_per_day', 0)}p/d, {current_tariff.get('electricity', {}).get('unit_rate_pence_per_kwh', 0)}p/kWh"
        )
        print(
            f"  Gas:  {current_tariff.get('gas', {}).get('standing_charge_pence_per_day', 0)}p/d, {current_tariff.get('gas', {}).get('unit_rate_pence_per_kwh', 0)}p/kWh"
        )

        print("\nComparison Plans in Config:")
        if not comparison_plans_config:
            print("  (None)")
        else:
            for idx, p in enumerate(comparison_plans_config, 1):
                print(f"  {idx}. {p.get('name', 'Unnamed')}")
                print(
                    f"     Elec: {p.get('electricity', {}).get('standing_charge_pence_per_day', 0)}p/d, {p.get('electricity', {}).get('unit_rate_pence_per_kwh', 0)}p/kWh"
                )
                print(
                    f"     Gas:  {p.get('gas', {}).get('standing_charge_pence_per_day', 0)}p/d, {p.get('gas', {}).get('unit_rate_pence_per_kwh', 0)}p/kWh"
                )
        print("=" * 50)

        print("\nOptions:")
        print("1. Calculate using only the plans from the config")
        print("2. Add more plans to compare (and optionally save to config)")
        print("3. Cancel")

        choice = self.get_int_input("\nEnter choice (1-3): ", 1, 3)
        if choice == 3:
            return False

        plans = []
        for p in comparison_plans_config:
            plans.append(
                {
                    "name": p.get("name", "Unnamed Plan"),
                    "elec_standing": p.get("electricity", {}).get(
                        "standing_charge_pence_per_day", 0.0
                    ),
                    "elec_unit": p.get("electricity", {}).get(
                        "unit_rate_pence_per_kwh", 0.0
                    ),
                    "gas_standing": p.get("gas", {}).get(
                        "standing_charge_pence_per_day", 0.0
                    ),
                    "gas_unit": p.get("gas", {}).get("unit_rate_pence_per_kwh", 0.0),
                }
            )

        if choice == 2:
            num_new = self.get_int_input(
                "How many additional plans do you want to add? (1-5): ", 1, 5
            )
            new_plans_config_format = []
            for i in range(num_new):
                print(f"\nEnter rates for new plan {i+1}:")
                plan_name = input("Plan name: ") or f"New Plan {i+1}"
                elec_standing = self.get_float_input(
                    "  Electricity standing charge (pence per day): "
                )
                elec_unit = self.get_float_input(
                    "  Electricity unit rate (pence per kWh): "
                )
                gas_standing = self.get_float_input(
                    "  Gas standing charge (pence per day): "
                )
                gas_unit = self.get_float_input("  Gas unit rate (pence per kWh): ")

                plans.append(
                    {
                        "name": plan_name,
                        "elec_standing": elec_standing,
                        "elec_unit": elec_unit,
                        "gas_standing": gas_standing,
                        "gas_unit": gas_unit,
                    }
                )

                new_plans_config_format.append(
                    {
                        "name": plan_name,
                        "electricity": {
                            "standing_charge_pence_per_day": elec_standing,
                            "unit_rate_pence_per_kwh": elec_unit,
                        },
                        "gas": {
                            "standing_charge_pence_per_day": gas_standing,
                            "unit_rate_pence_per_kwh": gas_unit,
                        },
                    }
                )

            save_choice = (
                input(
                    "\nDo you want to save these new plans to tariff_config.json? (y/N): "
                )
                .strip()
                .lower()
            )
            if save_choice == "y":
                if "comparison_plans" not in config:
                    config["comparison_plans"] = []
                config["comparison_plans"].extend(new_plans_config_format)
                with open(Path("tariff_config.json"), "w") as f:
                    json.dump(config, f, indent=4)
                print("Saved new plans to config.")

        if not plans:
            print("\nNo comparison plans available. Please add at least one plan.")
            return False

        current_tariff_costs = {"electricity": [], "gas": []}
        xlabels = []
        for year, month, resource_consumptions, resource_costs in last_12:
            label = f"{year}-{month:02d}"
            xlabels.append(label)
            for resource in ["electricity", "gas"]:
                consumption = resource_consumptions.get(resource, 0)
                days_in_month = calendar.monthrange(year, month)[1]
                if resource == "electricity":
                    curr = (
                        consumption
                        * current_tariff.get("electricity", {}).get(
                            "unit_rate_pence_per_kwh", 0.0
                        )
                    ) + (
                        current_tariff.get("electricity", {}).get(
                            "standing_charge_pence_per_day", 0.0
                        )
                        * days_in_month
                    )
                else:
                    curr = (
                        consumption
                        * current_tariff.get("gas", {}).get(
                            "unit_rate_pence_per_kwh", 0.0
                        )
                    ) + (
                        current_tariff.get("gas", {}).get(
                            "standing_charge_pence_per_day", 0.0
                        )
                        * days_in_month
                    )
                current_tariff_costs[resource].append(curr)
        theoretical_costs = []
        for plan in plans:
            plan_costs = {"electricity": [], "gas": []}
            for idx, (year, month, resource_consumptions, resource_costs) in enumerate(
                last_12
            ):
                for resource in ["electricity", "gas"]:
                    consumption = resource_consumptions.get(resource, 0)
                    if resource == "electricity":
                        theo = (consumption * plan["elec_unit"]) + (
                            plan["elec_standing"] * calendar.monthrange(year, month)[1]
                        )
                    else:
                        theo = (consumption * plan["gas_unit"]) + (
                            plan["gas_standing"] * calendar.monthrange(year, month)[1]
                        )
                    plan_costs[resource].append(theo)
            theoretical_costs.append(plan_costs)
        import matplotlib.pyplot as plt

        plt.figure(figsize=(15, 8))
        x = range(12)
        for resource, color in zip(["electricity", "gas"], ["tab:blue", "tab:orange"]):
            plt.plot(
                x,
                current_tariff_costs[resource],
                marker="s",
                linestyle="-.",
                label=f"Current {resource.capitalize()} ({current_tariff.get('name', 'Config')})",
                color=color,
                alpha=0.8,
            )
            for i in x:
                plt.annotate(
                    f"{current_tariff_costs[resource][i]:.2f}",
                    (i, current_tariff_costs[resource][i]),
                    textcoords="offset points",
                    xytext=(0, 6),
                    ha="center",
                    fontsize=8,
                    color=color,
                )
        plan_colors = ["tab:green", "tab:red", "tab:purple", "tab:brown", "tab:gray"]
        for pidx, plan in enumerate(plans):
            color = plan_colors[pidx % len(plan_colors)]
            for resource in ["electricity", "gas"]:
                linestyle = "--" if resource == "electricity" else ":"
                label = f"{plan['name']} {resource.capitalize()} (theoretical)"
                plt.plot(
                    x,
                    theoretical_costs[pidx][resource],
                    marker="o",
                    linestyle=linestyle,
                    label=label,
                    color=color,
                    alpha=0.7,
                )
                for i in x:
                    plt.annotate(
                        f"{theoretical_costs[pidx][resource][i]:.2f}",
                        (i, theoretical_costs[pidx][resource][i]),
                        textcoords="offset points",
                        xytext=(0, -12 - 10 * pidx),
                        ha="center",
                        fontsize=8,
                        color=color,
                        alpha=0.7,
                    )
        input_text = f"Current ({current_tariff.get('name', 'Config')}): Elec Stand {current_tariff.get('electricity', {}).get('standing_charge_pence_per_day', 0.0)}p/d, Elec Unit {current_tariff.get('electricity', {}).get('unit_rate_pence_per_kwh', 0.0)}p/kWh, Gas Stand {current_tariff.get('gas', {}).get('standing_charge_pence_per_day', 0.0)}p/d, Gas Unit {current_tariff.get('gas', {}).get('unit_rate_pence_per_kwh', 0.0)}p/kWh\n"
        input_text += "\n".join(
            [
                f"{plan['name']}: Elec Stand {plan['elec_standing']}p/d, Elec Unit {plan['elec_unit']}p/kWh, Gas Stand {plan['gas_standing']}p/d, Gas Unit {plan['gas_unit']}p/kWh"
                for plan in plans
            ]
        )
        plt.gcf().text(
            0.99,
            0.01,
            input_text,
            fontsize=10,
            ha="right",
            va="bottom",
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="gray"),
        )
        plt.title(
            f"Monthly Cost Comparison (Actual vs. Theoretical Plans) for Last 12 Months"
        )
        plt.xlabel("Month")
        plt.ylabel("Cost (pence)")
        plt.xticks(x, xlabels, rotation=45)
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.7)
        visualisationsDir = Path("data/visualisations") / "theoretical_comparison"
        visualisationsDir.mkdir(parents=True, exist_ok=True)
        outpath = visualisationsDir / f"last12_actual_vs_theoretical_multi_plans.png"
        plt.tight_layout()
        plt.savefig(outpath)
        plt.close()
        print(f"\nComparison chart saved to: {outpath}")

        print("\nYearly Total Cost Summary (Last 12 Months):")

        curr_elec_total = round(sum(current_tariff_costs["electricity"]) / 100, 2)
        curr_gas_total = round(sum(current_tariff_costs["gas"]) / 100, 2)
        curr_total = curr_elec_total + curr_gas_total
        print(f"\nCurrent Tariff ({current_tariff.get('name', 'Config')}):")
        print(f"  Electricity: £{curr_elec_total:.2f}")
        print(f"  Gas: £{curr_gas_total:.2f}")
        print(f"  Total: £{curr_total:.2f}")

        for pidx, plan in enumerate(plans):
            theo_elec_total = round(
                sum(theoretical_costs[pidx]["electricity"]) / 100, 2
            )
            theo_gas_total = round(sum(theoretical_costs[pidx]["gas"]) / 100, 2)
            theo_total = theo_elec_total + theo_gas_total
            print(f"\n{plan['name']} (Theoretical):")
            print(f"  Electricity: £{theo_elec_total:.2f}")
            print(f"  Gas: £{theo_gas_total:.2f}")
            print(f"  Total: £{theo_total:.2f}")

        return True

    def get_float_input(self, prompt, min_value=None, max_value=None):
        while True:
            try:
                value = input(prompt)
                value = float(value)
                if min_value is not None and value < min_value:
                    print(f"Value must be at least {min_value}.")
                    continue
                if max_value is not None and value > max_value:
                    print(f"Value must be at most {max_value}.")
                    continue
                return value
            except ValueError:
                print("Please enter a valid number.")
