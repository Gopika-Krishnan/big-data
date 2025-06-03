import os
import glob
import pyarrow.parquet as pq
import dask.dataframe as dd

def load_parquets_safely(base_path):
    # Find all parquet files recursively
    parquet_files = glob.glob(os.path.join(base_path, "**", "*.parquet"), recursive=True)
    print(f"Found {len(parquet_files)} parquet files total.")

    valid_files = []
    bad_files = []

    # Check each file
    for f in parquet_files:
        if os.path.getsize(f) == 0:
            # skip empty files
            bad_files.append(f)
            continue
        try:
            pq.ParquetFile(f)  # Try to open to validate
            valid_files.append(f)
        except Exception:
            bad_files.append(f)

    print(f"Valid parquet files: {len(valid_files)}")
    print(f"Bad parquet files (skipped): {len(bad_files)}")
    if bad_files:
        print("Bad files:")
        for bf in bad_files:
            print("  ", bf)

    # Load only valid files with Dask
    ddf = dd.read_parquet(valid_files, engine="pyarrow")
    return ddf

# Usage
base_path = "/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_with_holidayweather"
ddf = load_parquets_safely(base_path)

# Now you can continue your analysis
print(ddf.head())
# Assuming ddf is already loaded by load_parquets_safely()

# 1. Print columns to verify schema
print("Columns:", ddf.columns.tolist())

# 2. Show first 5 rows
print("First 5 rows:")
print(ddf.head())

# 3. Check data types
print("Data types:")
print(ddf.dtypes)

# 4. Check count of holiday vs non-holiday trips
print("Holiday trip counts:")
print(ddf['is_holiday'].value_counts().compute())

# 5. Basic stats on weather columns
print("Weather stats:")
print(ddf[['avg_temp_c', 'precip_mm']].describe().compute())
