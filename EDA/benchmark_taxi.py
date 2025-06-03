#!/usr/bin/env python3
"""
benchmark_taxi.py

Benchmarks load + hourly-aggregation for 2020 taxi data in BOTH CSV & Parquet:
 - Pandas
 - Dask
 - DuckDB
 - Dask-SQL (if available)

Produces `benchmark_results_all.csv` with columns:
  engine, format, load_s, agg_hour_s
"""

import sys, os, time
import pandas as pd
import duckdb
import dask.dataframe as dd
import logging

# — Logging setup —
logger = logging.getLogger("bench_parquet")
logger.setLevel(logging.INFO)
if logger.handlers:
    for h in list(logger.handlers):
        logger.removeHandler(h)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
logger.addHandler(ch)

# — Configuration —
PARQUET_DIR  = "/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_augmented/year=2020"
PARQUET_GLOB = os.path.join(PARQUET_DIR, "*.parquet")
CSV_PATH     = "./csv_data/taxi_2020.csv"
REPEATS      = 3
COLUMNS      = ["tpep_pickup_datetime", "fare_amount"]

# — Check Dask-SQL availability —
sys.path.insert(0, os.path.dirname(__file__))
try:
    import dask_sql
    import dask_expr.io.parquet
    from dask_sql import Context
    DASK_SQL_AVAILABLE = True
except Exception as e:
    logger.warning("Dask-SQL disabled: %s", e)
    DASK_SQL_AVAILABLE = False

def ensure_csv():
    """Write out a single CSV if it doesn't exist yet."""
    if os.path.exists(CSV_PATH):
        logger.info("CSV already exists at %s", CSV_PATH)
        return
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    logger.info("Writing CSV to %s (this will take a bit)...", CSV_PATH)
    ddf = dd.read_parquet(PARQUET_GLOB, columns=COLUMNS, engine="pyarrow")
    t0 = time.time()
    ddf.to_csv(CSV_PATH, single_file=True, index=False)
    logger.info("CSV written in %.1f s", time.time() - t0)

def bench_pandas(path, fmt):
    total_load, total_agg = 0.0, 0.0
    for _ in range(REPEATS):
        t0 = time.time()
        if fmt == "csv":
            df = pd.read_csv(path, parse_dates=["tpep_pickup_datetime"])
        else:
            df = pd.read_parquet(path, columns=COLUMNS, engine="pyarrow")
        total_load += time.time() - t0

        t1 = time.time()
        df.groupby(df['tpep_pickup_datetime'].dt.hour)['fare_amount']\
          .agg(['count','mean'])
        total_agg += time.time() - t1

    return total_load/REPEATS, total_agg/REPEATS

def bench_dask(path, fmt):
    total_load, total_agg = 0.0, 0.0
    for _ in range(REPEATS):
        t0 = time.time()
        if fmt == "csv":
            ddf = dd.read_csv(path, parse_dates=["tpep_pickup_datetime"])
        else:
            ddf = dd.read_parquet(PARQUET_GLOB, columns=COLUMNS, engine="pyarrow")
        total_load += time.time() - t0

        t1 = time.time()
        ddf.groupby(ddf['tpep_pickup_datetime'].dt.hour)['fare_amount']\
           .agg(['count','mean']).compute()
        total_agg += time.time() - t1

    return total_load/REPEATS, total_agg/REPEATS

def bench_duckdb(path, fmt):
    total_load, total_agg = 0.0, 0.0
    for _ in range(REPEATS):
        t0 = time.time()
        if fmt == "csv":
            duckdb.sql(f"CREATE OR REPLACE TABLE trips AS SELECT * FROM read_csv_auto('{path}')")
        else:
            duckdb.sql(f"CREATE OR REPLACE TABLE trips AS SELECT {','.join(COLUMNS)} FROM '{PARQUET_GLOB}'")
        total_load += time.time() - t0

        t1 = time.time()
        duckdb.sql("""
            SELECT EXTRACT(hour FROM tpep_pickup_datetime) AS hr,
                   COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare
            FROM trips GROUP BY hr;
        """)
        total_agg += time.time() - t1

    return total_load/REPEATS, total_agg/REPEATS

def bench_dask_sql(path, fmt):
    if not DASK_SQL_AVAILABLE:
        return (None, None)
    ctx = Context()
    total_load, total_agg = 0.0, 0.0
    for _ in range(REPEATS):
        t0 = time.time()
        if fmt == "csv":
            ddf = dd.read_csv(path, parse_dates=["tpep_pickup_datetime"])
        else:
            ddf = dd.read_parquet(PARQUET_GLOB, columns=COLUMNS, engine="pyarrow")
        ctx.create_table("trips", ddf)
        total_load += time.time() - t0

        t1 = time.time()
        ctx.sql("""
            SELECT EXTRACT(hour FROM tpep_pickup_datetime) AS hr,
                   COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare
            FROM trips GROUP BY hr;
        """).compute()
        total_agg += time.time() - t1

    return total_load/REPEATS, total_agg/REPEATS

def main():
    ensure_csv()

    results = []
    for fmt, path in [("CSV", CSV_PATH), ("Parquet", PARQUET_DIR)]:
        l, a = bench_pandas(path, fmt.lower())
        results.append({"engine":"Pandas",   "format":fmt, "load_s":l, "agg_hour_s":a})
        l, a = bench_dask(path, fmt.lower())
        results.append({"engine":"Dask",     "format":fmt, "load_s":l, "agg_hour_s":a})
        l, a = bench_duckdb(path, fmt.lower())
        results.append({"engine":"DuckDB",   "format":fmt, "load_s":l, "agg_hour_s":a})
        l, a = bench_dask_sql(path, fmt.lower())
        if l is not None:
            results.append({"engine":"Dask-SQL","format":fmt, "load_s":l, "agg_hour_s":a})

    df = pd.DataFrame(results)
    logger.info("FINAL SUMMARY:\n%s", df)
    df.to_csv("benchmark_results_all.csv", index=False)

if __name__ == "__main__":
    main()
