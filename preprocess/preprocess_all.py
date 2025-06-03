from datetime import datetime
import dask.dataframe as dd
import pyarrow as pa
import pandas as pd
import warnings
import os

warnings.filterwarnings("default")
input_base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
output_base_path = "/d/hpc/projects/FRI/jo83525/big-data/bigdata_project_output"
os.makedirs(output_base_path, exist_ok=True)

years_to_include_yellow = [str(y) for y in range(2016, 2026)]

yellow_files = sorted([
    os.path.join(input_base_path, f)
    for f in os.listdir(input_base_path)
    if f.startswith("yellow_tripdata_") and f.endswith(".parquet")
    and any(f"yellow_tripdata_{year}" in f for year in years_to_include_yellow)
])

yellow_schema = pa.schema([
    ("vendorid", pa.int8()),
    ("tpep_pickup_datetime", pa.timestamp("s")),
    ("tpep_dropoff_datetime", pa.timestamp("s")),
    ("passenger_count", pa.int8()),
    ("trip_distance", pa.float32()),
    ("ratecodeid", pa.int8()),
    ("store_and_fwd_flag", pa.string()),
    ("pulocationid", pa.int32()),
    ("dolocationid", pa.int32()),
    ("payment_type", pa.int8()),
    ("fare_amount", pa.float32()),
    ("extra", pa.float32()),
    ("mta_tax", pa.float32()),
    ("tip_amount", pa.float32()),
    ("tolls_amount", pa.float32()),
    ("improvement_surcharge", pa.float32()),
    ("total_amount", pa.float32()),
    ("congestion_surcharge", pa.float32()),
    ("airport_fee", pa.float32()),
])

years_to_include_green = [str(y) for y in range(2016, 2026)]

green_files = sorted([
    os.path.join(input_base_path, f)
    for f in os.listdir(input_base_path)
    if f.startswith("green_tripdata_") and f.endswith(".parquet")
    and any(f"green_tripdata_{year}" in f for year in years_to_include_green)
])

green_schema = pa.schema([
    ("vendorid", pa.int8()),
    ("lpep_pickup_datetime", pa.timestamp("s")),
    ("lpep_dropoff_datetime", pa.timestamp("s")),
    ("store_and_fwd_flag", pa.string()),
    ("ratecodeid", pa.int8()),
    ("pulocationid", pa.int32()),
    ("dolocationid", pa.int32()),
    ("passenger_count", pa.int8()),
    ("trip_distance", pa.float32()),
    ("fare_amount", pa.float32()),
    ("extra", pa.float32()),
    ("mta_tax", pa.float32()),
    ("tip_amount", pa.float32()),
    ("tolls_amount", pa.float32()),
    ("improvement_surcharge", pa.float32()),
    ("total_amount", pa.float32()),
    ("payment_type", pa.int8()),
    ("trip_type", pa.int8()),
    ("congestion_surcharge", pa.float32()),
])

years_to_include_fhv = [str(y) for y in range(2016, 2026)]

fhv_files = sorted([
    os.path.join(input_base_path, f)
    for f in os.listdir(input_base_path)
    if f.startswith("fhv_tripdata_") and f.endswith(".parquet")
    and any(f"fhv_tripdata_{year}" in f for year in years_to_include_fhv)
])

years_to_include_fhvhv = [str(y) for y in range(2019, 2026)]

fhv_schema = pa.schema([
    ("dispatching_base_num", pa.string()),
    ("pickup_datetime", pa.timestamp("s")),
    ("dropoff_datetime", pa.timestamp("s")),
    ("pulocationid", pa.int32()),
    ("dolocationid", pa.int32()),
    ("sr_flag", pa.int8()),
    ("affiliated_base_number", pa.string())
])


fhvhv_files = sorted([
    os.path.join(input_base_path, f)
    for f in os.listdir(input_base_path)
    if f.startswith("fhvhv_tripdata_") and f.endswith(".parquet")
    and any(f"fhvhv_tripdata_{year}" in f for year in years_to_include_fhvhv)
])

fhvhv_schema = pa.schema([
    ("hvfhs_license_num", pa.string()),
    ("dispatching_base_num", pa.string()),
    ("originating_base_num", pa.string()),
    ("request_datetime", pa.timestamp("s")),
    ("on_scene_datetime", pa.timestamp("s")),
    ("pickup_datetime", pa.timestamp("s")),
    ("dropoff_datetime", pa.timestamp("s")),
    ("pulocationid", pa.int32()),
    ("dolocationid", pa.int32()),
    ("trip_miles", pa.float32()),
    ("trip_time", pa.int32()),
    ("base_passenger_fare", pa.float32()),
    ("tolls", pa.float32()),
    ("bcf", pa.float32()),
    ("sales_tax", pa.float32()),
    ("congestion_surcharge", pa.float32()),
    ("airport_fee", pa.float32()),
    ("tips", pa.float32()),
    ("driver_pay", pa.float32()),
    ("shared_request_flag", pa.string()),
    ("shared_match_flag", pa.string()),
    ("access_a_ride_flag", pa.string()),
    ("wav_request_flag", pa.string()),
    ("wav_match_flag", pa.string()),
])


datasets = {
    "yellow": (
        yellow_files,
        yellow_schema,
        "tpep_pickup_datetime",
        {
            "vendorid": "float64",
            "pulocationid": "float64",
            "dolocationid": "float64",
            "payment_type": "float64",
            "congestion_surcharge": "float64",
            "airport_fee": "float64",
        }
    ),
    "green": (
        green_files,
        green_schema,
        "lpep_pickup_datetime",
        {
            "vendorid": "float64",
            "pulocationid": "float64",
            "dolocationid": "float64",
            "payment_type": "float64",
            "trip_type": "float64",
            "congestion_surcharge": "float64",
            "airport_fee": "float64",
        }
    ),
    "fhv": (
        fhv_files,
        fhv_schema,
        "pickup_datetime",
        {
            "pulocationid": "float64",
            "dolocationid": "float64",
            "sr_flag": "float64"
        }
    ),
    "fhvhv": (
        fhvhv_files,
        fhvhv_schema,
        "pickup_datetime",
        {
            "pulocationid": "float64",
            "dolocationid": "float64",
            "trip_miles": "float64",
            "trip_time": "float64",
            "base_passenger_fare": "float64",
            "tolls": "float64",
            "bcf": "float64",
            "sales_tax": "float64",
            "congestion_surcharge": "float64",
            "airport_fee": "float64",
            "tips": "float64",
            "driver_pay": "float64",
            "cbd_congestion_fee": "float64",
        }
    )
}
ddfs = []

for name, (files, schema, pickup_col, safe_cast) in datasets.items():
    print(f"\n==============================")
    print(f"🚕 Starting dataset: {name} with {len(files)} files")
    print(f"==============================\n")

    ddfs = []
    for idx, file in enumerate(files, 1):
        try:
            print(f"[{name.upper()}][{idx}/{len(files)}] Reading: {os.path.basename(file)}")
            ddf = dd.read_parquet(file, engine="pyarrow")
            ddf.columns = [col.lower() for col in ddf.columns]

            print(f"[{name.upper()}][{idx}] Loaded with {len(ddf.columns)} columns")

            # Fill numeric columns with 0
            print(f"[{name.upper()}][{idx}] Filling missing values")
            ddf = ddf.map_partitions(
                lambda df: df.assign(**{
                    col: df[col].fillna(0) if pd.api.types.is_numeric_dtype(df[col]) else df[col]
                    for col in df.columns})
            )

            # Normalize pickup time
            if pickup_col.lower() in ddf.columns:
                print(f"[{name.upper()}][{idx}] Parsing pickup datetime")
                ddf["pickup_datetime_std"] = dd.to_datetime(ddf[pickup_col.lower()], errors="coerce")
            else:
                print(f"[{name.upper()}][{idx}] Pickup column '{pickup_col}' missing, using NaT")
                ddf["pickup_datetime_std"] = pd.NaT
            
            if "pickup_datetime_std" not in ddf.columns or ddf["pickup_datetime_std"].isnull().all().compute():
                print(f"[{name.upper()}][{idx}] ❌ pickup_datetime_std missing or all NaT — skipping this file")
                continue

            ddf = ddf.assign(year=ddf["pickup_datetime_std"].dt.year)
            


            # Safe type casting
            print(f"[{name.upper()}][{idx}] Safe casting fields")
            for col, typ in safe_cast.items():
                if col in ddf.columns:
                    try:
                        ddf[col] = dd.to_numeric(ddf[col], errors="coerce") if "float" in typ else ddf[col].astype(typ)
                    except Exception as e:
                        print(f"[{name.upper()}][{idx}] Failed to cast {col} — {e}")

            ddfs.append(ddf)
            print(f"[{name.upper()}][{idx}] Processed successfully")

        except Exception as e:
            print(f"[{name.upper()}][{idx}] Skipping file due to error: {e}")

    if ddfs:
        print(f"\nConcatenating {len(ddfs)} partitions for {name}")
        combined = dd.concat(ddfs)
        combined = combined.dropna(subset=["year"])

        output_path = os.path.join(output_base_path, f"{name}_partitioned")
        print(f"Saving to: {output_path}")

        try:
            start_time = datetime.now()
            combined.to_parquet(
                output_path,
                engine="pyarrow",
                write_index=False,
                partition_on=["year"],
                row_group_size=2_000_000,
                schema=schema
            )
            elapsed = datetime.now() - start_time
            print(f"Done saving {name} in {elapsed.total_seconds():.2f} seconds\n")
        except Exception as e:
            print(f"Failed to save {name}: {e}")
    else:
        print(f"No usable {name} files found.\n")
