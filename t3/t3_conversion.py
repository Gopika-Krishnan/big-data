#!/usr/bin/env python3
"""
t3_conversion.py

Task 3:
- Read the 2024 Green Taxi Parquet partition (≈16 MB total).
- Export to:
    1) plain CSV
    2) gzipped CSV
    3) HDF5 (table format)
    4) DuckDB
- Record file sizes and Pandas read-times.
- Log everything (INFO+ level) both to console and to a log file.
"""

import os
import glob
import time
import logging
import pandas as pd
import duckdb

# === CONFIGURABLE PATHS ===
# 1) Correct absolute path to your 2024 Green-Taxi Parquet files:
GREEN2024_PARQUET_DIR = "/d/hpc/projects/FRI/jo83525/big-data/T1/green_partitioned/year=2024"

# 2) Where to write all four output formats (CSV, CSV.gz, HDF5, DuckDB).
OUT_DIR = "/d/hpc/projects/FRI/ma76193/big-data/T3"

# 3) Where to write the log file.
LOG_DIR = "/d/hpc/projects/FRI/ma76193/logs"
LOG_FILE = os.path.join(LOG_DIR, "t3_conversion.log")

# =============================================================================
def setup_logger():
    """
    Configure a logger that writes INFO+ messages both to console and to a log file.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("T3Logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid duplicate logs if root logger also prints

    # Formatter (timestamp, log-level, message)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1) StreamHandler (console)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # 2) FileHandler (log file, append mode)
    fh = logging.FileHandler(LOG_FILE, mode="a")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

logger = setup_logger()

def file_size_mb(path: str) -> float:
    """
    Return the file size in megabytes, rounded to two decimals.
    """
    size_bytes = os.path.getsize(path)
    return round(size_bytes / (1024 ** 2), 2)

def timed_read(label: str, load_func):
    """
    Run load_func() to read data, measure elapsed time, and log it.
    Returns the loaded DataFrame so we can inspect shape if desired.
    """
    start = time.time()
    df_local = load_func()
    elapsed = round(time.time() - start, 4)
    logger.info(f"⏱ {label:<10}: {elapsed} s   (rows={len(df_local):,}, cols={df_local.shape[1]})")
    return df_local

def main():
    logger.info("=== TASK 3 START ===")
    logger.info(f"Green-2024 Parquet Dir: {GREEN2024_PARQUET_DIR}")
    logger.info(f"Output Directory:        {OUT_DIR}")
    logger.info(f"Log File:                {LOG_FILE}")

    # Ensure output directory exists
    os.makedirs(OUT_DIR, exist_ok=True)

    # ---- STEP 1: Read Parquet partition into Pandas DF ----
    logger.info("STEP 1: Loading 2024 Green-Taxi Parquet → Pandas DataFrame")
    parquet_paths = sorted(glob.glob(os.path.join(GREEN2024_PARQUET_DIR, "*.parquet")))
    if not parquet_paths:
        logger.error(f"No Parquet files found under {GREEN2024_PARQUET_DIR}")
        raise FileNotFoundError(f"No Parquet files in {GREEN2024_PARQUET_DIR}")

    logger.info(f"Found {len(parquet_paths)} Parquet file(s).")
    df_chunks = []
    for filepath in parquet_paths:
        fname = os.path.basename(filepath)
        logger.info(f"  • Loading {fname}")
        df_part = pd.read_parquet(filepath)
        df_chunks.append(df_part)
    df = pd.concat(df_chunks, ignore_index=True)
    logger.info(f"Total rows loaded: {len(df):,}; columns: {df.shape[1]}")
    logger.info(f"Column names: {df.columns.tolist()}")

    # ---- STEP 2: Write to CSV, CSV.gz, HDF5, DuckDB ----
    logger.info("STEP 2: Writing out to different formats")

    # 2A) Plain CSV
    csv_path = os.path.join(OUT_DIR, "green_2024.csv")
    t0 = time.perf_counter()
    df.to_csv(csv_path, index=False)
    write_csv = round(time.perf_counter() - t0, 2)
    logger.info(f"  • plain CSV → {csv_path}   (write time: {write_csv} s)")

    # 2B) Gzipped CSV
    csv_gz_path = os.path.join(OUT_DIR, "green_2024.csv.gz")
    t0 = time.perf_counter()
    df.to_csv(csv_gz_path, index=False, compression="gzip")
    write_csv_gz = round(time.perf_counter() - t0, 2)
    logger.info(f"  • gzipped CSV → {csv_gz_path}   (write time: {write_csv_gz} s)")

    # 2C) HDF5 (table format)
    hdf5_path = os.path.join(OUT_DIR, "green_2024.h5")
    t0 = time.perf_counter()
    df.to_hdf(hdf5_path, key="data", format="table", mode="w")
    write_h5 = round(time.perf_counter() - t0, 2)
    logger.info(f"  • HDF5 (table) → {hdf5_path}   (write time: {write_h5} s)")

    # 2D) DuckDB
    duckdb_path = os.path.join(OUT_DIR, "green_2024.duckdb")
    t0 = time.perf_counter()
    con = duckdb.connect(duckdb_path, read_only=False)
    con.execute("DROP TABLE IF EXISTS green2024;")
    con.register("temp_df", df)
    con.execute("CREATE TABLE green2024 AS SELECT * FROM temp_df;")
    con.unregister("temp_df")
    con.close()
    write_duckdb = round(time.perf_counter() - t0, 2)
    logger.info(f"  • DuckDB file → {duckdb_path}   (write time: {write_duckdb} s)")

    # ---- STEP 3: Report File Sizes ----
    logger.info("STEP 3: Reporting on-disk file sizes (MB)")
    formats = ["CSV", "CSV.gz", "HDF5", "DuckDB"]
    paths = [csv_path, csv_gz_path, hdf5_path, duckdb_path]
    write_times = [write_csv, write_csv_gz, write_h5, write_duckdb]
    for fmt, pth, wt in zip(formats, paths, write_times):
        sz = file_size_mb(pth)
        logger.info(f"  • {fmt:<7} | size = {sz} MB | write time = {wt} s")

    # ---- STEP 4: Read-Time Benchmark (avg over 3 runs) ----
    logger.info("STEP 4: Benchmarking read times (avg over 3 runs each)")
    N_RUNS = 3
    read_times = {}

    # 4A) Plain CSV
    times = []
    for _ in range(N_RUNS):
        start = time.time()
        _ = pd.read_csv(csv_path)
        end = time.time()
        times.append(end - start)
    read_times["CSV"] = round(sum(times)/N_RUNS, 2)
    logger.info(f"  • CSV       read time (avg) = {read_times['CSV']} s")

    # 4B) Gzipped CSV
    times = []
    for _ in range(N_RUNS):
        start = time.time()
        _ = pd.read_csv(csv_gz_path, compression="gzip")
        end = time.time()
        times.append(end - start)
    read_times["CSV.gz"] = round(sum(times)/N_RUNS, 2)
    logger.info(f"  • CSV.gz    read time (avg) = {read_times['CSV.gz']} s")

    # 4C) HDF5
    times = []
    for _ in range(N_RUNS):
        start = time.time()
        _ = pd.read_hdf(hdf5_path, key="data")
        end = time.time()
        times.append(end - start)
    read_times["HDF5"] = round(sum(times)/N_RUNS, 2)
    logger.info(f"  • HDF5      read time (avg) = {read_times['HDF5']} s")

    # 4D) DuckDB
    times = []
    for _ in range(N_RUNS):
        start = time.time()
        con2 = duckdb.connect(duckdb_path, read_only=True)
        _ = con2.execute("SELECT * FROM green2024").df()
        con2.close()
        end = time.time()
        times.append(end - start)
    read_times["DuckDB"] = round(sum(times)/N_RUNS, 2)
    logger.info(f"  • DuckDB    read time (avg) = {read_times['DuckDB']} s")

    # ---- FINAL SUMMARY TABLE ----
    logger.info("FINAL SUMMARY:")
    header = f"{'Format':<7} | {'Size_MiB':>8} | {'Write_s':>7} | {'Read_s(avg)':>11}"
    logger.info(header)
    logger.info("-" * len(header))
    for fmt, pth, wt in zip(formats, paths, write_times):
        sz = file_size_mb(pth)
        rt = read_times[fmt]
        logger.info(f"{fmt:<7} | {sz:>8.2f} | {wt:>7.2f} | {rt:>11.2f}")

    logger.info("=== TASK 3 COMPLETE ===\n")

if __name__ == "__main__":
    main()
