import os
import sys
import dask.dataframe as dd
import pandas as pd
import holidays
import glob

if len(sys.argv) != 2:
    print("Usage: python augment_holweather_by_year.py <year>")
    sys.exit(1)

year = int(sys.argv[1])

input_path = f"/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_parquet/year={year}"
weather_csv = "/d/hpc/projects/FRI/bigdata/students/gk40784/DB/nyc_weather_2016_2024.csv"
output_path = f"/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_with_holidayweather/year={year}"

os.makedirs(output_path, exist_ok=True)

weather_df = pd.read_csv(weather_csv, parse_dates=["pickup_date"])
weather_df = weather_df[["pickup_date", "avg_temp_c", "precip_mm"]]

us_ny_holidays = holidays.US(state='NY', years=range(2016, 2025))

parquet_files = glob.glob(os.path.join(input_path, "*.parquet"))
valid_files = [f for f in parquet_files if os.path.getsize(f) > 0]

print(f"Year {year}: Found {len(valid_files)} valid parquet files.")

if not valid_files:
    print(f"Year {year}: No files found, exiting.")
    sys.exit(0)

ddf = dd.read_parquet(valid_files, engine="pyarrow")
ddf["pickup_date"] = dd.to_datetime(ddf["tpep_pickup_datetime"]).dt.floor("D")

meta = ddf._meta.copy()
meta["is_holiday"] = pd.Series([], dtype=bool)
meta["holiday_name"] = pd.Series([], dtype="object")
meta["avg_temp_c"] = pd.Series([], dtype="float64")
meta["precip_mm"] = pd.Series([], dtype="float64")

def enrich(df):
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    df["is_holiday"] = df["pickup_date"].apply(lambda x: x in us_ny_holidays)
    df["holiday_name"] = df["pickup_date"].apply(lambda x: us_ny_holidays.get(x, pd.NA))
    df = df.merge(weather_df, on="pickup_date", how="left")
    return df

ddf = ddf.map_partitions(enrich, meta=meta)

ddf.to_parquet(
    output_path,
    engine="pyarrow",
    write_index=False,
    row_group_size=100000
)

print(f"Year {year}: Augmentation complete.")
