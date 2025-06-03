import os
import dask.dataframe as dd
import pandas as pd
import warnings

warnings.filterwarnings("default")

input_base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
output_base_path = "/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output"
os.makedirs(output_base_path, exist_ok=True)

years_to_include = [str(y) for y in range(2016, 2026)]
yellow_files = sorted([
    os.path.join(input_base_path, f)
    for f in os.listdir(input_base_path)
    if f.startswith("yellow_tripdata_") and f.endswith(".parquet")
    and any(f"yellow_tripdata_{year}" in f for year in years_to_include)
])

ddfs = []
for file in yellow_files:
    try:
        print(f"Processing: {file}")
        ddf = dd.read_parquet(file, engine="pyarrow")
        ddf.columns = [col.lower() for col in ddf.columns]

        # Fill numeric columns with 0
        ddf = ddf.map_partitions(
            lambda df: df.assign(**{
                col: df[col].fillna(0) if pd.api.types.is_numeric_dtype(df[col]) else df[col]
                for col in df.columns})
        )

        # Normalize datetime columns
        if 'pickup_datetime' in ddf.columns:
            ddf['pickup_datetime'] = dd.to_datetime(ddf['pickup_datetime'], errors='coerce')
        if 'dropoff_datetime' in ddf.columns:
            ddf['dropoff_datetime'] = dd.to_datetime(ddf['dropoff_datetime'], errors='coerce')

        # Set pickup time reference
        if 'tpep_pickup_datetime' in ddf.columns:
            ddf['tpep_pickup_datetime'] = dd.to_datetime(ddf['tpep_pickup_datetime'], errors='coerce')
        elif 'pickup_datetime' in ddf.columns:
            ddf['tpep_pickup_datetime'] = dd.to_datetime(ddf['pickup_datetime'], errors='coerce')
        else:
            ddf = ddf.map_partitions(lambda df: df.assign(tpep_pickup_datetime=pd.NaT))

        # Extract year
        ddf = ddf.assign(year=ddf['tpep_pickup_datetime'].dt.year)

        # Safely cast key columns without crashing
        safe_cast = {
            "vendorid": "float64",
            "pulocationid": "float64",
            "dolocationid": "float64",
            "payment_type": "float64",
            "congestion_surcharge": "float64",
            "airport_fee": "float64",
        }

        for col, typ in safe_cast.items():
            if col in ddf.columns:
                try:
                    if typ == "float64":
                        ddf[col] = dd.to_numeric(ddf[col], errors="coerce")
                    else:
                        ddf[col] = ddf[col].astype(typ)
                except Exception as e:
                    print(f"Warning: Could not cast {col} in {file} — {e}")

        ddfs.append(ddf)

    except Exception as e:
        print(f"Skipping {file} due to error: {e}")

if ddfs:
    combined = dd.concat(ddfs)

    # Drop invalid years
    combined = combined.dropna(subset=["year"])
    combined = combined[combined["year"].between(2016, 2025)]

    combined.to_parquet(
        os.path.join(output_base_path, "partitioned_parquet"),
        engine="pyarrow",
        write_index=False,
        partition_on=["year"],
        row_group_size=100000
    )
else:
    print("No usable files found.")
