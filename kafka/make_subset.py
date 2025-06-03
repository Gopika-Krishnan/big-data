import pandas as pd

# Set the base path to your 2019 Parquet partition
base_path = "/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_augmented/year=2019"

# Read the data
df = pd.read_parquet(base_path, engine="pyarrow")

# Filter to Dec 22–26, 2019
df = df[df["tpep_pickup_datetime"].between("2019-12-22", "2019-12-26")]

# Sort by pickup time
df = df.sort_values("tpep_pickup_datetime")

# Save to a new file
output_path = "/d/hpc/projects/FRI/bigdata/students/gk40784/yellow_taxi_dec22to26.parquet"
df.to_parquet(output_path)

print("Saved Dec 22–26 subset to:", output_path)
