import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator


def generate_visualizations(cost_file_path, consumption_file_path, output_dir=None):
    try:
        if output_dir is None:
            vis_dir = Path("data/visualisations")
        else:
            vis_dir = Path(output_dir)

        vis_dir.mkdir(parents=True, exist_ok=True)

        cost_path = Path(cost_file_path)
        consumption_path = Path(consumption_file_path)

        if cost_path.suffix == ".parquet":
            cost_df = pd.read_parquet(cost_path)
            consumption_df = pd.read_parquet(consumption_path)
        else:
            cost_df = pd.read_json(cost_path, lines=True)
            consumption_df = pd.read_json(consumption_path, lines=True)

        detected_resource_type = (
            cost_df["resource_name"].iloc[0].split()[0].capitalize()
        )

        cost_df["timestamp"] = pd.to_datetime(cost_df["timestamp_iso"])
        consumption_df["timestamp"] = pd.to_datetime(consumption_df["timestamp_iso"])

        for df in [cost_df, consumption_df]:
            df["hour"] = df["timestamp"].dt.hour
            df["day"] = df["timestamp"].dt.day
            df["weekday"] = df["timestamp"].dt.day_name()
            df["date"] = df["timestamp"].dt.date

        merged_metrics_df = pd.merge(
            cost_df[
                [
                    "timestamp",
                    "timestamp_iso",
                    "value",
                    "hour",
                    "day",
                    "weekday",
                    "date",
                    "units",
                ]
            ],
            consumption_df[["timestamp", "value", "units"]],
            on="timestamp",
            suffixes=("_cost", "_consumption"),
        )

        cost_unit_label = (
            cost_df["units"].iloc[0] if "units" in cost_df.columns else "pence"
        )
        consumption_unit_label = (
            consumption_df["units"].iloc[0]
            if "units" in consumption_df.columns
            else "kWh"
        )

        sns.set_style("whitegrid")
        plt.rcParams.update({"font.size": 12})

        fig, (cost_axis, consumption_axis) = plt.subplots(
            2, 1, figsize=(15, 10), sharex=True
        )

        cost_axis.plot(
            merged_metrics_df["timestamp"],
            merged_metrics_df["value_cost"],
            color="red",
            linewidth=1.5,
        )
        cost_axis.set_ylabel(f"Cost ({cost_unit_label})")
        cost_axis.set_title(f"{detected_resource_type} Cost Over Time")
        cost_axis.grid(True)

        consumption_axis.plot(
            merged_metrics_df["timestamp"],
            merged_metrics_df["value_consumption"],
            color="blue",
            linewidth=1.5,
        )
        consumption_axis.set_ylabel(f"Consumption ({consumption_unit_label})")
        consumption_axis.set_xlabel("Date")
        consumption_axis.set_title(f"{detected_resource_type} Consumption Over Time")
        consumption_axis.grid(True)

        consumption_axis.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        consumption_axis.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig(
            vis_dir / f"{detected_resource_type.lower()}_time_series.png", dpi=300
        )
        plt.close()

        hourly_averaged_data = (
            merged_metrics_df.groupby("hour")
            .agg({"value_cost": "mean", "value_consumption": "mean"})
            .reset_index()
        )

        fig, cost_secondary_axis = plt.subplots(figsize=(12, 6))

        cost_secondary_axis.set_xlabel("Hour of Day")
        cost_secondary_axis.set_ylabel(f"Cost ({cost_unit_label})", color="red")
        cost_secondary_axis.plot(
            hourly_averaged_data["hour"],
            hourly_averaged_data["value_cost"],
            color="red",
            marker="o",
        )
        cost_secondary_axis.tick_params(axis="y", labelcolor="red")
        cost_secondary_axis.grid(True, alpha=0.3)

        consumption_secondary_axis = cost_secondary_axis.twinx()
        consumption_secondary_axis.set_ylabel(
            f"Consumption ({consumption_unit_label})", color="blue"
        )
        consumption_secondary_axis.plot(
            hourly_averaged_data["hour"],
            hourly_averaged_data["value_consumption"],
            color="blue",
            marker="s",
        )
        consumption_secondary_axis.tick_params(axis="y", labelcolor="blue")

        cost_secondary_axis.set_xticks(range(0, 24))
        cost_secondary_axis.set_xlim(-0.5, 23.5)
        plt.title(f"Average Hourly {detected_resource_type} Cost and Consumption")
        plt.tight_layout()
        plt.savefig(
            vis_dir / f"{detected_resource_type.lower()}_hourly_patterns.png", dpi=300
        )
        plt.close()

        daily_hourly_consumption = merged_metrics_df.pivot_table(
            index="day", columns="hour", values="value_consumption", aggfunc="mean"
        )

        plt.figure(figsize=(15, 8))
        sns.heatmap(
            daily_hourly_consumption,
            cmap="YlGnBu",
            annot=False,
            fmt=".2f",
            cbar_kws={"label": consumption_unit_label},
        )
        plt.title(f"{detected_resource_type} Consumption by Day and Hour")
        plt.xlabel("Hour of Day")
        plt.ylabel("Day of Month")
        plt.tight_layout()
        plt.savefig(
            vis_dir / f"{detected_resource_type.lower()}_consumption_heatmap.png",
            dpi=300,
        )
        plt.close()

        daily_totals = (
            merged_metrics_df.groupby("date")
            .agg({"value_cost": "sum", "value_consumption": "sum"})
            .reset_index()
        )

        fig, ax1 = plt.subplots(figsize=(12, 6))

        x = range(len(daily_totals))
        ax1.set_xlabel("Date")
        ax1.set_ylabel(f"Total Daily Cost ({cost_unit_label})", color="red")
        bars1 = ax1.bar(
            x, daily_totals["value_cost"], color="red", alpha=0.6, label="Cost"
        )
        ax1.tick_params(axis="y", labelcolor="red")

        ax2 = ax1.twinx()
        ax2.set_ylabel(
            f"Total Daily Consumption ({consumption_unit_label})", color="blue"
        )
        line = ax2.plot(
            x,
            daily_totals["value_consumption"],
            color="blue",
            marker="o",
            linestyle="-",
            label="Consumption",
        )
        ax2.tick_params(axis="y", labelcolor="blue")

        plt.xticks(x, [d.strftime("%d-%b") for d in daily_totals["date"]], rotation=45)

        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc="upper left")

        plt.title(f"Daily {detected_resource_type} Cost and Consumption")
        plt.tight_layout()
        plt.savefig(
            vis_dir / f"{detected_resource_type.lower()}_daily_totals.png", dpi=300
        )
        plt.close()

        return True

    except Exception as e:
        print(f"Error generating visualizations: {str(e)}")
        return False


if __name__ == "__main__":
    cost_parquet = Path("data/processed/electricity_cost_20250401_to_20250430.parquet")
    consumption_parquet = Path(
        "data/processed/electricity_consumption_20250401_to_20250430.parquet"
    )

    if not cost_parquet.exists():
        cost_parquet = Path(
            "data/processed/electricity_cost_20250401_to_20250430.jsonl"
        )

    if not consumption_parquet.exists():
        consumption_parquet = Path(
            "data/processed/electricity_consumption_20250401_to_20250430.jsonl"
        )

    generate_visualizations(cost_parquet, consumption_parquet)
