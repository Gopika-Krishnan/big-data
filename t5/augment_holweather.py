import os
import sys
import pandas as pd
import holidays
import glob
from pathlib import Path

# -----------------------------
# Parse CLI args
# -----------------------------
if len(sys.argv) != 3:
    print("Usage: python augment_holweather_by_year.py <dataset> <year>")
    sys.exit(1)

dataset = sys.argv[1]
year = int(sys.argv[2])

dataset_configs = {
    "yellow": {"pickup_col": "tpep_pickup_datetime", "years": range(2012, 2026)},
    "green": {"pickup_col": "lpep_pickup_datetime", "years": range(2016, 2026)},
    "fhv": {"pickup_col": "pickup_datetime", "years": range(2016, 2026)},
    "fhvhv": {"pickup_col": "pickup_datetime", "years": range(2019, 2026)}
}

if dataset not in dataset_configs or year not in dataset_configs[dataset]["years"]:
    print(f"Invalid dataset/year combination: {dataset} {year}")
    sys.exit(1)

pickup_col = dataset_configs[dataset]["pickup_col"].lower()

# -----------------------------
# I/O paths
# -----------------------------
input_path = f"/d/hpc/projects/FRI/jo83525/big-data/T1/{dataset}_partitioned/year={year}"
output_path = f"/d/hpc/projects/FRI/jo83525/big-data/T5/holweather_pandas/{dataset}_partitioned/year={year}"
weather_csv = "nyc_weather_2012_2024.csv"

os.makedirs(output_path, exist_ok=True)

# -----------------------------
# Load weather CSV
# -----------------------------
weather_df = pd.read_csv(weather_csv, parse_dates=["pickup_date"])
weather_df["avg_temp_c"] = pd.to_numeric(weather_df["avg_temp_c"], errors="coerce").fillna(-9999.0)
weather_df["precip_mm"] = pd.to_numeric(weather_df["precip_mm"], errors="coerce").fillna(0.0)

# -----------------------------
# US holidays
# -----------------------------
us_ny_holidays = holidays.US(state='NY', years=range(2012, 2025))

# -----------------------------
# Find Parquet files & filter safe ones
# -----------------------------
parquet_files = sorted(glob.glob(os.path.join(input_path, "*.parquet")))
valid_files = []
bad_files = []

for f in parquet_files:
    if os.path.getsize(f) == 0:
        bad_files.append(f)
        continue
    try:
        pd.read_parquet(f, engine="pyarrow", columns=None)
        valid_files.append(f)
    except Exception as e:
        print(f"Skipped corrupt: {f} — {e}")
        bad_files.append(f)

print(f"Valid files: {len(valid_files)} | Bad files: {len(bad_files)}")

if not valid_files:
    print(f"No valid parquet files for {dataset} {year}")
    sys.exit(0)

# -----------------------------
# Process each file separately and write individually
# -----------------------------
for i, file in enumerate(valid_files):
    print(f" Processing {file}")
    df = pd.read_parquet(file)
    df.columns = df.columns.str.lower()

    if pickup_col not in df.columns:
        print(f" Skipping {file} — missing pickup column")
        continue

    df["pickup_date"] = pd.to_datetime(df[pickup_col], errors="coerce").dt.floor("D")
    df["is_holiday"] = df["pickup_date"].apply(lambda x: x in us_ny_holidays)
    df["holiday_name"] = df["pickup_date"].apply(lambda x: us_ny_holidays.get(x, pd.NA))

    df = df.merge(weather_df, on="pickup_date", how="left")

    df["avg_temp_c"] = pd.to_numeric(df["avg_temp_c"], errors="coerce").fillna(-9999.0)
    df["precip_mm"] = pd.to_numeric(df["precip_mm"], errors="coerce").fillna(0.0)
    df["is_holiday"] = df["is_holiday"].astype("string")
    df["holiday_name"] = df["holiday_name"].astype("string")

    # Write out immediately — one file per source parquet
    output_file = os.path.join(output_path, Path(file).stem + "_augmented.parquet")
    df.to_parquet(output_file, index=False)
    print(f" Wrote {output_file} ({len(df)} rows)")
