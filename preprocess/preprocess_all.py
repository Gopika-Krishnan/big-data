import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from data_quality import clean_taxi_data

input_base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
output_base_path = "/d/hpc/projects/FRI/jo83525/big-data/T1"
os.makedirs(output_base_path, exist_ok=True)

# Dataset configurations
dataset_configs = {
    "yellow": {
        "years": range(2012, 2026),
        "pickup_col": "tpep_pickup_datetime",
    },
    "green": {
        "years": range(2016, 2026),
        "pickup_col": "lpep_pickup_datetime",
    },
    "fhv": {
        "years": range(2016, 2026),
        "pickup_col": "pickup_datetime",
    },
    "fhvhv": {
        "years": range(2019, 2026),
        "pickup_col": "pickup_datetime",
    }
}

for dataset_name, config in dataset_configs.items():
    print(f"\n--- Processing {dataset_name.upper()} ---")
    pickup_col = config["pickup_col"].lower()
    output_dir = os.path.join(output_base_path, f"{dataset_name}_partitioned")
    os.makedirs(output_dir, exist_ok=True)

    for file in sorted(os.listdir(input_base_path)):
        if not (file.startswith(f"{dataset_name}_tripdata_") and file.endswith(".parquet")):
            continue
        if not any(str(year) in file for year in config["years"]):
            continue

        file_path = os.path.join(input_base_path, file)
        print(f"[{dataset_name.upper()}] Reading {file}")
        try:
            table = pq.read_table(file_path)
            table = table.rename_columns([col.lower() for col in table.schema.names])

            if pickup_col not in table.schema.names:
                print(f"Pickup column {pickup_col} missing in {file}")
                continue

            df = table.to_pandas()
            initial_rows = len(df)
            df_clean = clean_taxi_data(df, dataset_configs, dataset_type=dataset_name).copy()
            if not isinstance(df_clean, pd.DataFrame):
                raise TypeError(f"[{dataset_name.upper()}] clean_taxi_data() returned a {type(df_clean)} instead of a DataFrame.")
            
            cleaned_rows = len(df_clean)
            dropped_rows = initial_rows - cleaned_rows
            print(f"[{dataset_name.upper()}] Cleaned {file}: {initial_rows} → {cleaned_rows} rows "
              f"({dropped_rows} dropped)")

            if cleaned_rows == 0:
                print(f"No valid rows remaining after cleaning {file}, skipping.")
                continue
            
            df_clean["year"] = pd.to_datetime(df_clean[pickup_col], errors="coerce").dt.year
            table = pa.Table.from_pandas(df_clean, preserve_index=False)

            pq.write_to_dataset(
                table,
                root_path=output_dir,
                partition_cols=["year"],
                row_group_size=2_000_000,
                existing_data_behavior="overwrite_or_ignore"
            )
            print(f"[{dataset_name.upper()}] Saved with year partitioning")
        except Exception as e:
            print(f"[{dataset_name.upper()}] Error processing {file}: {e}")
