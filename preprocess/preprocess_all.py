import os
import pyarrow as pa
import pyarrow.parquet as pq

input_base_path = "/d/hpc/projects/FRI/bigdata/data/Taxi"
output_base_path = "/d/hpc/projects/FRI/gk40784/big-data/T1"
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
                print(f"⚠️ Pickup column {pickup_col} missing in {file}")
                continue

            # Extract year from pickup column
            year_array = pa.compute.year(table[pickup_col])
            table = table.append_column("year", year_array)

            # Write to partitioned dataset
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
