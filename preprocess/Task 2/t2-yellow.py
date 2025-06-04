import os
import glob
import re
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow.parquet as pq

# === Setup ===
base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
dataset_name = "yellow"
config = {
    "pattern": "yellow_tripdata_*.parquet",
    "pickup_col": "tpep_pickup_datetime",
    "dropoff_col": "tpep_dropoff_datetime",
    "distance_col": "trip_distance",
    "fare_col": "fare_amount",
    "start_year": 2012
}
start_year = 2012
import matplotlib.colors as mcolors

# Choose a global color palette from matplotlib
base_colors = list(mcolors.TABLEAU_COLORS.values()) + list(mcolors.CSS4_COLORS.values())
all_possible_issues = [
    "invalid_year", "same_pickup_dropoff", "dropoff_before_pickup", "zero_distance",
    "negative_fare", "missing_pickup", "missing_dropoff", "long_trip_duration",
    "zero_trip_duration", "missing_passenger_count", "extreme_passenger_count",
    "negative_tip", "pickup_eq_dropoff_location"
]

# Assign each issue a fixed color
issue_color_map = dict(zip(all_possible_issues, base_colors[:len(all_possible_issues)]))

# === Filter files by year in filename ===
pattern = os.path.join(base_path, config["pattern"])
files = [
    f for f in glob.glob(pattern)
    if re.search(r"(\d{4})", f) and int(re.search(r"(\d{4})", f).group(1)) >= config["start_year"]
]

if not files:
    print(f"No matching files found after {config['start_year']}")
else:
    print(f"Reading {len(files)} filtered YELLOW files")

    try:
        # === Dynamically determine available columns ===
        available_columns = pq.read_table(files[0]).schema.names
        used_columns = [
            col for col in [
                config["pickup_col"].lower(), config["dropoff_col"].lower(),
                config["distance_col"].lower(), config["fare_col"].lower(),
                "passenger_count", "tip_amount", "ratecodeid", "pulocationid", "dolocationid"
            ] if col in available_columns
        ]

        # === Read data with only available columns ===
        ddf = dd.read_parquet(files, engine="pyarrow", columns=used_columns, assume_missing=True)
        ddf = ddf.rename(columns=str.lower)

        # === Ensure all expected columns are present (fill missing with None) ===
        for col in [
            config["pickup_col"].lower(), config["dropoff_col"].lower(),
            config["distance_col"].lower(), config["fare_col"].lower(),
            "passenger_count", "tip_amount", "ratecodeid", "pulocationid", "dolocationid"
        ]:
            if col not in ddf.columns:
                ddf[col] = None

        pickup = config["pickup_col"].lower()
        dropoff = config["dropoff_col"].lower()

        ddf[pickup] = dd.to_datetime(ddf[pickup], errors="coerce")
        ddf[dropoff] = dd.to_datetime(ddf[dropoff], errors="coerce")
        ddf["year"] = ddf[pickup].dt.year

        ddf["invalid_year"] = ~ddf["year"].between(config["start_year"], 2025)
        ddf["same_pickup_dropoff"] = ddf[pickup] == ddf[dropoff]
        ddf["dropoff_before_pickup"] = ddf[dropoff] < ddf[pickup]
        ddf["zero_distance"] = ddf[config["distance_col"].lower()] == 0 if config["distance_col"].lower() in ddf.columns else False
        ddf["negative_fare"] = ddf[config["fare_col"].lower()] < 0 if config["fare_col"].lower() in ddf.columns else False

        ddf["missing_pickup"] = ddf[pickup].isna()
        ddf["missing_dropoff"] = ddf[dropoff].isna()
        ddf["duration_seconds"] = (ddf[dropoff] - ddf[pickup]).dt.total_seconds()
        ddf["long_trip_duration"] = ddf["duration_seconds"] > 86400
        ddf["zero_trip_duration"] = ddf["duration_seconds"] <= 0

        ddf["missing_passenger_count"] = ddf["passenger_count"].isna() if "passenger_count" in ddf.columns else False
        ddf["extreme_passenger_count"] = ddf["passenger_count"] > 6 if "passenger_count" in ddf.columns else False
        ddf["negative_tip"] = ddf["tip_amount"] < 0 if "tip_amount" in ddf.columns else False
        # ddf["invalid_ratecode"] = ~ddf["ratecodeid"].isin([1, 2, 3, 4, 5, 6]) if "ratecodeid" in ddf.columns else False
        ddf["pickup_eq_dropoff_location"] = (
            (ddf["pulocationid"] == ddf["dolocationid"])
            if "pulocationid" in ddf.columns and "dolocationid" in ddf.columns
            else False
        )

        issues = [
            "invalid_year", "same_pickup_dropoff", "dropoff_before_pickup", "zero_distance",
            "negative_fare", "missing_pickup", "missing_dropoff", "long_trip_duration",
            "zero_trip_duration", "missing_passenger_count", "extreme_passenger_count",
            "negative_tip", "pickup_eq_dropoff_location"
        ]

        summary = ddf.groupby("year")[issues].sum().compute().sort_index()
        summary.index = summary.index.astype(int)

        summary.to_csv("summary_yellow.csv")

        total_by_year = ddf.groupby("year").size().compute()
        total_by_year = total_by_year[total_by_year.index.to_series().between(start_year, 2025)]
        normalized_summary = summary.div(total_by_year, axis=0).fillna(0)

        top_issues = normalized_summary.mean().sort_values(ascending=False).head(5).index
        # ax = normalized_summary[top_issues].plot.area(figsize=(14, 7), alpha=0.7)
        colors = [issue_color_map[issue] for issue in top_issues]

        ax = normalized_summary[top_issues].plot.area(
            figsize=(14, 7),
            alpha=0.7,
            color=colors
        )

        ax.set_title(f"Top 5 Data Quality Issues by Year (Proportion) – {dataset_name.upper()} Taxi")
        ax.set_xlabel("Year")
        ax.set_ylabel("Proportion of Records")
        ax.grid(True)
        plt.tight_layout()
        plt.savefig(f"issues_{dataset_name}_top5_area.png")
        plt.close()

        print("Yellow plots saved successfully.")

    except Exception as e:
        print(f"Error: {e}")
