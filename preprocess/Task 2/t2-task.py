import os
import glob
import re
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

# === Setup ===
base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
datasets = {
    "yellow": {
        "pattern": "yellow_tripdata_*.parquet",
        "pickup_col": "tpep_pickup_datetime",
        "dropoff_col": "tpep_dropoff_datetime",
        "distance_col": "trip_distance",
        "fare_col": "fare_amount",
        "start_year": 2012,
    },
    "green": {
        "pattern": "green_tripdata_*.parquet",
        "pickup_col": "lpep_pickup_datetime",
        "dropoff_col": "lpep_dropoff_datetime",
        "distance_col": "trip_distance",
        "fare_col": "fare_amount",
        "start_year": 2014
    },
    "fhv": {
        "pattern": "fhv_tripdata_*.parquet",
        "pickup_col": "pickup_datetime",
        "dropoff_col": "dropoff_datetime",
        "distance_col": "trip_distance",
        "fare_col": "fare_amount",
        "start_year": 2015
    },
    "fhvhv": {
        "pattern": "fhvhv_tripdata_*.parquet",
        "pickup_col": "pickup_datetime",
        "dropoff_col": "dropoff_datetime",
        "distance_col": "trip_distance",
        "fare_col": "base_passenger_fare",
        "start_year": 2019
    }
}


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


for dataset_name, config in datasets.items():
    print(f"🚕 Processing {dataset_name.upper()}")
    
    start_year = config["start_year"]
    try:
        pattern = os.path.join(base_path, config["pattern"])
        files = [
            f for f in glob.glob(pattern)
            if re.search(r"(\d{4})", f) and int(re.search(r"(\d{4})", f).group(1)) >= start_year
        ]

        if not files:
            print(f"No matching files found for {dataset_name}")
            continue

        ddf = dd.read_parquet(files, engine="pyarrow", assume_missing=True)
        ddf = ddf.rename(columns=str.lower)

        pickup = config["pickup_col"].lower()
        dropoff = config["dropoff_col"].lower()
        distance = config["distance_col"].lower()
        fare = config["fare_col"].lower()

        maybe_cols = [pickup, dropoff, distance, fare, "passenger_count", "tip_amount", "pulocationid", "dolocationid"]
        for col in maybe_cols:
            if col not in ddf.columns:
                ddf[col] = None

        ddf[pickup] = dd.to_datetime(ddf[pickup], errors="coerce")
        ddf[dropoff] = dd.to_datetime(ddf[dropoff], errors="coerce")
        ddf["year"] = ddf[pickup].dt.year

        ddf["invalid_year"] = ~ddf["year"].between(start_year, 2025)
        ddf["same_pickup_dropoff"] = ddf[pickup] == ddf[dropoff]
        ddf["dropoff_before_pickup"] = ddf[dropoff] < ddf[pickup]
        ddf["zero_distance"] = ddf[distance] == 0
        ddf["negative_fare"] = ddf[fare] < 0
        ddf["missing_pickup"] = ddf[pickup].isna()
        ddf["missing_dropoff"] = ddf[dropoff].isna()
        ddf["duration_seconds"] = (ddf[dropoff] - ddf[pickup]).dt.total_seconds()
        ddf["long_trip_duration"] = ddf["duration_seconds"] > 86400
        ddf["zero_trip_duration"] = ddf["duration_seconds"] <= 0
        ddf["missing_passenger_count"] = ddf["passenger_count"].isna()
        ddf["extreme_passenger_count"] = ddf["passenger_count"] > 6
        ddf["negative_tip"] = ddf["tip_amount"] < 0
        ddf["pickup_eq_dropoff_location"] = ddf["pulocationid"] == ddf["dolocationid"]

        issues = [
            "invalid_year", "same_pickup_dropoff", "dropoff_before_pickup", "zero_distance",
            "negative_fare", "missing_pickup", "missing_dropoff", "long_trip_duration",
            "zero_trip_duration", "missing_passenger_count", "extreme_passenger_count",
            "negative_tip", "pickup_eq_dropoff_location"
        ]

        summary = ddf.groupby("year")[issues].sum().compute()
        summary = summary[summary.index.to_series().between(start_year, 2025)]
        summary.index = summary.index.astype(int)
        summary.to_csv(f"summary_{dataset_name}.csv")

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

        ax.set_title(f"Data 5 Quality Issues by Year (Proportion) – {dataset_name.upper()} Taxi")
        ax.set_xlabel("Year")
        ax.set_ylabel("Proportion of Records")
        ax.grid(True)
        plt.tight_layout()
        plt.savefig(f"issues_{dataset_name}_top5_area.png")
        plt.close()

        print(f"Done with {dataset_name} — summary + area plot saved.")

    except Exception as e:
        print(f"Error in {dataset_name}: {e}")
