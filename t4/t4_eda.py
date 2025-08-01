#!/usr/bin/env python3
"""
t4_eda.py

Task 4: Exploratory Data Analysis on all four TLC datasets (Yellow, Green, FHV, FHVHV).

This script:
  1) Validates each Parquet file under year=*/ (skipping any corrupted or non-Parquet files).
  2) Inspects schema of the first valid file to detect:
       - pickup timestamp column (time_col)
       - trip_distance column (dist_col), if present
       - fare_amount column (fare_col), if present
       - pickup-zone column (zone_col)
  3) Builds a Dask DataFrame from valid files (selecting only the needed columns).
  4) Converts pickup timestamp to datetime, extracts year/month.
  5) Computes and saves CSVs for:
       - trips_per_year
       - avg_trip_distance_per_year (if dist_col exists)
       - avg_fare_per_year (if fare_col exists)
       - trips_per_month 
           • YELLOW uses DuckDB on Parquet (safe for 2+ billion rows)
           • Others (Green, FHV, FHVHV) use Dask groupby
       - top_10_pickup_zones
  6) Generates and saves PNG plots for each of the above aggregates (skipping missing columns).
  7) Performs a cross-dataset similarity analysis at the end:
       - Pairwise Pearson correlation of yearly trip counts
       - Pairwise Jaccard index of top-10 pickup zones
       - Saves results to OUT_DIR/cross_dataset_similarity.txt
  8) Logs every INFO+ message to console and to LOG_DIR/t4_eda.log

Adjust the four PARQUET_ROOT paths, OUT_DIR, and LOG_DIR at the top to match your environment.
"""

import os
import glob
import time
import logging

import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq
import duckdb

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# === CONFIGURABLE PATHS ===
YELLOW_PARQUET_ROOT = "/d/hpc/projects/FRI/jo83525/big-data/T1/yellow_partitioned"
GREEN_PARQUET_ROOT  = "/d/hpc/projects/FRI/jo83525/big-data/T1/green_partitioned"
FHV_PARQUET_ROOT    = "/d/hpc/projects/FRI/jo83525/big-data/T1/fhv_partitioned"
FHVHV_PARQUET_ROOT  = "/d/hpc/projects/FRI/jo83525/big-data/T1/fhvhv_partitioned"

OUT_DIR = "/d/hpc/projects/FRI/ma76193/big-data/T4"
LOG_DIR = "/d/hpc/projects/FRI/ma76193/logs"
LOG_FILE = os.path.join(LOG_DIR, "t4_eda.log")

DATASETS = {
    "yellow":  {"root": YELLOW_PARQUET_ROOT},
    "green":   {"root": GREEN_PARQUET_ROOT},
    "fhv":     {"root": FHV_PARQUET_ROOT},
    "fhvhv":   {"root": FHVHV_PARQUET_ROOT}
}


def setup_logger():
    """
    Configure a logger that writes INFO+ messages both to console and to a log file.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("T4Logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # Console handler
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # File handler (append mode)
    fh = logging.FileHandler(LOG_FILE, mode="a")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

logger = setup_logger()


def collect_valid_parquet_paths(root_dir):
    """
    Recursively find all .parquet under root_dir/year=*/.
    Skip any empty or corrupted files via PyArrow validation.
    Return list of valid file paths, logging any bad ones.
    """
    pattern = os.path.join(root_dir, "year=*/*.parquet")
    all_files = sorted(glob.glob(pattern))
    logger.info("  • Discovered %d total files under %s", len(all_files), root_dir)

    valid_files = []
    bad_files = []

    for f in all_files:
        try:
            if os.path.getsize(f) == 0:
                bad_files.append(f)
                continue
            _ = pq.ParquetFile(f)
            valid_files.append(f)
        except Exception:
            bad_files.append(f)

    logger.info("  • Valid Parquet files:   %d", len(valid_files))
    if bad_files:
        logger.warning("  • Bad/Corrupted files:   %d (skipped)", len(bad_files))
        for bf in bad_files:
            logger.warning("      - %s", bf)

    return valid_files


def inspect_schema(first_parquet_path):
    """
    Open one Parquet file via PyArrow and return its schema.names list.
    """
    try:
        pqfile = pq.ParquetFile(first_parquet_path)
        return pqfile.schema.names
    except Exception as e:
        logger.error("  • Failed to inspect schema from %s: %s", first_parquet_path, e)
        raise


def find_column_variant(schema_names, candidates):
    """
    Given a list of existing schema_names and candidate names (case-insensitive),
    return the exact match if present, else None.
    """
    lowered = {col.lower(): col for col in schema_names}
    for c in candidates:
        if c.lower() in lowered:
            return lowered[c.lower()]
    return None


def process_dataset(name, root_dir):
    """
    Process one dataset (“name”) whose Parquet files live under root_dir/year=*/.
    Steps:
      1) Validate and collect only good Parquet file paths.
      2) Inspect schema of the first good file.
      3) Detect time_col, dist_col, fare_col, zone_col.
      4) Read with Dask (only selected columns).
      5) Convert time_col→datetime, extract year/month.
      6) Count total rows.
      7) Compute & save:
         - trips_per_year
         - avg_trip_distance_per_year (if dist_col exists)
         - avg_fare_per_year (if fare_col exists)
         - trips_per_month 
             • YELLOW: DuckDB on Parquet
             • Others: Dask groupby
         - top_10_pickup_zones
      8) Generate and save corresponding PNG plots.
    """
    logger.info("=== Processing dataset: %s ===", name.upper())

    # 1) Validate & collect good Parquet paths
    valid_files = collect_valid_parquet_paths(root_dir)
    if not valid_files:
        logger.error("[%s] No valid Parquet files found → skipping", name)
        return

    # 2) Inspect schema of the first valid file
    schema_names = inspect_schema(valid_files[0])
    logger.info("[%s] Schema columns: %s", name, schema_names)

    # 3a) Detect pickup-datetime column (“time_col”)
    time_col = None
    for col in schema_names:
        if "pickup" in col.lower() and "datetime" in col.lower():
            time_col = col
            break
    if not time_col:
        logger.error("[%s] No pickup‐datetime column found → skipping", name)
        return
    logger.info("[%s] Detected pickup timestamp column: %s", name, time_col)

    # 3b) Detect trip_distance (“dist_col”)
    dist_col = find_column_variant(schema_names, ["trip_distance"])
    if dist_col:
        logger.info("[%s] Detected trip‐distance column: %s", name, dist_col)
    else:
        logger.info("[%s] No trip‐distance column found; skipping distance‐based aggregates", name)

    # 3c) Detect fare_amount (“fare_col”)
    fare_col = find_column_variant(schema_names, ["fare_amount", "fareamount"])
    if fare_col:
        logger.info("[%s] Detected fare column: %s", name, fare_col)
    else:
        logger.info("[%s] No fare column found; skipping fare‐based aggregates", name)

    # 3d) Detect pickup-zone (“zone_col”), e.g., pulocationid
    zone_col = None
    for col in schema_names:
        if (("pu" in col.lower() or "pickup" in col.lower()) and "location" in col.lower()):
            zone_col = col
            break
    if not zone_col:
        logger.error("[%s] No pickup‐zone column found → skipping", name)
        return
    logger.info("[%s] Detected pickup‐zone column: %s", name, zone_col)

    # 4) Read with Dask (select only needed columns)
    read_cols = [time_col, zone_col]
    if dist_col:
        read_cols.append(dist_col)
    if fare_col:
        read_cols.append(fare_col)

    logger.info("[%s] Reading Dask DataFrame with columns: %s", name, read_cols)
    start = time.time()
    try:
        ddf = dd.read_parquet(
            valid_files,
            engine="pyarrow",
            columns=read_cols,
            assume_missing=True
        )
    except Exception as e:
        logger.error("[%s] Dask.read_parquet failed: %s", name, e)
        return
    elapsed = round(time.time() - start, 2)
    logger.info("[%s] Dask‐read complete in %.2f s", name, elapsed)

    # 5) Convert time_col → datetime, extract year/month
    logger.info("[%s] Converting %s to datetime and extracting year/month …", name, time_col)
    ddf[time_col] = dd.to_datetime(ddf[time_col], errors="coerce", utc=False)
    ddf["year"]  = ddf[time_col].dt.year
    ddf["month"] = ddf[time_col].dt.month

    # 6) Count total rows
    logger.info("[%s] Counting total rows …", name)
    try:
        total_rows = int(ddf.shape[0].compute())
        logger.info("[%s] Total rows = %d", name, total_rows)
    except Exception as e:
        logger.error("[%s] Failed to count rows: %s", name, e)
        return

    # Prepare output directories
    out_base = os.path.join(OUT_DIR, name)
    plot_dir = os.path.join(out_base, "plots")
    os.makedirs(plot_dir, exist_ok=True)

    # 7A) trips_per_year
    s_trips_year = None
    logger.info("[%s] Computing trips_per_year …", name)
    try:
        s_trips_year = ddf.groupby("year").size().compute().rename("trip_count").reset_index()
        out_csv = os.path.join(out_base, f"{name}_trips_per_year.csv")
        s_trips_year.to_csv(out_csv, index=False)
        logger.info("[%s] Saved trips_per_year to %s (rows=%d)", name, out_csv, len(s_trips_year))
    except Exception as e:
        logger.error("[%s] trips_per_year failed: %s", name, e)

    # 7B) avg_trip_distance_per_year (if dist_col exists)
    df_avg_dist = None
    if dist_col:
        logger.info("[%s] Computing avg_trip_distance_per_year …", name)
        try:
            s_avg_dist = ddf.groupby("year")[dist_col].mean().compute()
            df_avg_dist = s_avg_dist.rename(f"avg_{dist_col}").reset_index()
            out_csv = os.path.join(out_base, f"{name}_avg_dist_per_year.csv")
            df_avg_dist.to_csv(out_csv, index=False)
            logger.info("[%s] Saved avg_dist_per_year to %s (rows=%d)", name, out_csv, len(df_avg_dist))
        except Exception as e:
            logger.error("[%s] avg_dist_per_year failed: %s", name, e)

    # 7C) avg_fare_per_year (if fare_col exists)
    df_avg_fare = None
    if fare_col:
        logger.info("[%s] Computing avg_fare_per_year …", name)
        try:
            s_avg_fare = ddf.groupby("year")[fare_col].mean().compute()
            df_avg_fare = s_avg_fare.rename(f"avg_{fare_col}").reset_index()
            out_csv = os.path.join(out_base, f"{name}_avg_fare_per_year.csv")
            df_avg_fare.to_csv(out_csv, index=False)
            logger.info("[%s] Saved avg_fare_per_year to %s (rows=%d)", name, out_csv, len(df_avg_fare))
        except Exception as e:
            logger.error("[%s] avg_fare_per_year failed: %s", name, e)

    # 7D) trips_per_month
    df_trips_month = None
    logger.info("[%s] Computing trips_per_month …", name)
    if name.lower() == "yellow":
        # Use DuckDB on Parquet for Yellow’s 2+ billion rows
        parquet_glob = os.path.join(root_dir, "year=*/*.parquet")
        logger.info("[%s] Using DuckDB on Parquet pattern: %s", name, parquet_glob)
        try:
            con = duckdb.connect()
            query = f"""
                SELECT 
                  YEAR({time_col}) AS year,
                  MONTH({time_col}) AS month,
                  COUNT(*)            AS trip_count
                FROM read_parquet('{parquet_glob}')
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """
            df_trips_month = con.execute(query).df()
            con.close()
            out_csv = os.path.join(out_base, f"{name}_trips_per_month.csv")
            df_trips_month.to_csv(out_csv, index=False)
            logger.info("[%s] (DuckDB) Saved trips_per_month to %s (rows=%d)",
                        name, out_csv, len(df_trips_month))
        except Exception as e:
            logger.error("[%s] DuckDB trips_per_month failed: %s", name, e)
            df_trips_month = None
    else:
        # Use Dask for the smaller datasets
        try:
            df_trips_month = (
                ddf.groupby(["year", "month"])
                .size()
                .compute()
                .rename("trip_count")
                .reset_index()
            )
            out_csv = os.path.join(out_base, f"{name}_trips_per_month.csv")
            df_trips_month.to_csv(out_csv, index=False)
            logger.info("[%s] Saved trips_per_month to %s (rows=%d)",
                        name, out_csv, len(df_trips_month))
        except Exception as e:
            logger.error("[%s] Dask trips_per_month failed: %s", name, e)
            df_trips_month = None

    # 7E) top_10_pickup_zones
    df_top_zones = None
    logger.info("[%s] Computing top_10_pickup_zones …", name)
    try:
        df_top_zones = (
            ddf[zone_col]
            .value_counts()
            .nlargest(10)
            .compute()
            .rename("trip_count")
            .reset_index()
            .rename(columns={"index": zone_col})
        )
        out_csv = os.path.join(out_base, f"{name}_top10_pickup_zones.csv")
        df_top_zones.to_csv(out_csv, index=False)
        logger.info("[%s] Saved top10_pickup_zones to %s (rows=%d)",
                    name, out_csv, len(df_top_zones))
    except Exception as e:
        logger.error("[%s] top10_pickup_zones failed: %s", name, e)

    # 8) Generate & Save Plots (PNG)
    logger.info("[%s] Generating plots …", name)

    # 8A) trips_per_year bar chart
    if s_trips_year is not None and not s_trips_year.empty:
        try:
            plt.figure(figsize=(6, 4))
            plt.bar(s_trips_year["year"], s_trips_year["trip_count"], color="skyblue")
            plt.xlabel("Year")
            plt.ylabel("Number of Trips")
            plt.title(f"{name.upper()}: Trips per Year")
            plt.tight_layout()
            out_png = os.path.join(plot_dir, f"{name}_trips_per_year.png")
            plt.savefig(out_png)
            plt.close()
            logger.info("[%s] Saved plot: %s", name, out_png)
        except Exception as e:
            logger.error("[%s] Plot trips_per_year failed: %s", name, e)

    # 8B) avg_trip_distance_per_year line chart
    if df_avg_dist is not None and not df_avg_dist.empty:
        try:
            plt.figure(figsize=(6, 4))
            plt.plot(df_avg_dist["year"], df_avg_dist[f"avg_{dist_col}"], marker="o")
            plt.xlabel("Year")
            plt.ylabel(f"Avg {dist_col}")
            plt.title(f"{name.upper()}: Avg Trip Distance per Year")
            plt.tight_layout()
            out_png = os.path.join(plot_dir, f"{name}_avg_dist_per_year.png")
            plt.savefig(out_png)
            plt.close()
            logger.info("[%s] Saved plot: %s", name, out_png)
        except Exception as e:
            logger.error("[%s] Plot avg_dist_per_year failed: %s", name, e)

    # 8C) avg_fare_per_year line chart
    if df_avg_fare is not None and not df_avg_fare.empty:
        try:
            plt.figure(figsize=(6, 4))
            plt.plot(df_avg_fare["year"], df_avg_fare[f"avg_{fare_col}"], marker="o", color="green")
            plt.xlabel("Year")
            plt.ylabel(f"Avg {fare_col}")
            plt.title(f"{name.upper()}: Avg Fare per Year")
            plt.tight_layout()
            out_png = os.path.join(plot_dir, f"{name}_avg_fare_per_year.png")
            plt.savefig(out_png)
            plt.close()
            logger.info("[%s] Saved plot: %s", name, out_png)
        except Exception as e:
            logger.error("[%s] Plot avg_fare_per_year failed: %s", name, e)

    # 8D) trips_per_month time-series chart
    if df_trips_month is not None and not df_trips_month.empty:
        try:
            df_trips_month["date"] = pd.to_datetime(
                df_trips_month[["year", "month"]].assign(day=1)
            )
            plt.figure(figsize=(8, 4))
            plt.plot(df_trips_month["date"], df_trips_month["trip_count"], marker=".", linestyle="-")
            plt.xlabel("Date (Year‐Month)")
            plt.ylabel("Number of Trips")
            plt.title(f"{name.upper()}: Trips per Month")
            plt.tight_layout()
            out_png = os.path.join(plot_dir, f"{name}_trips_per_month.png")
            plt.savefig(out_png)
            plt.close()
            logger.info("[%s] Saved plot: %s", name, out_png)
        except Exception as e:
            logger.error("[%s] Plot trips_per_month failed: %s", name, e)

    # 8E) top_10_pickup_zones bar chart
    if df_top_zones is not None and not df_top_zones.empty:
        try:
            plt.figure(figsize=(6, 4))
            plt.bar(df_top_zones[zone_col].astype(str), df_top_zones["trip_count"], color="salmon")
            plt.xlabel("Pickup Zone ID")
            plt.ylabel("Number of Trips")
            plt.title(f"{name.upper()}: Top 10 Pickup Zones")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            out_png = os.path.join(plot_dir, f"{name}_top10_pickup_zones.png")
            plt.savefig(out_png)
            plt.close()
            logger.info("[%s] Saved plot: %s", name, out_png)
        except Exception as e:
            logger.error("[%s] Plot top10_pickup_zones failed: %s", name, e)

    logger.info("=== Completed dataset: %s ===\n", name.upper())


def cross_dataset_similarity():
    """
    After all four datasets produce their:
      - {name}_trips_per_year.csv
      - {name}_top10_pickup_zones.csv
    this function:
      1) Loads each trips_per_year.csv (Pandas Series), computes pairwise Pearson correlation.
      2) Loads each top10_pickup_zones.csv (sets of zone IDs), computes pairwise Jaccard index.
      3) Logs results and writes a summary to OUT_DIR/cross_dataset_similarity.txt.
    """
    logger.info("=== Cross‐Dataset Similarity Analysis START ===")

    # 1) Year‐level trip_count correlation
    df_yearly = {}
    for name in ["yellow", "green", "fhv", "fhvhv"]:
        fpath = os.path.join(OUT_DIR, name, f"{name}_trips_per_year.csv")
        if not os.path.exists(fpath):
            logger.warning("  • Missing %s → skipping correlation for this dataset", fpath)
            continue
        df = pd.read_csv(fpath)  # columns: ["year", "trip_count"]
        df_yearly[name] = df.set_index("year")["trip_count"]

    if len(df_yearly) >= 2:
        yearly_df = pd.DataFrame(df_yearly)
        corr_matrix = yearly_df.corr()
        logger.info("  • Year‐level trip_count correlation matrix:\n%s", corr_matrix.to_string())
    else:
        logger.warning("  • Not enough datasets for year‐level correlation")

    # 2) Jaccard similarity of top‐10 pickup zones
    top_zones = {}
    for name in ["yellow", "green", "fhv", "fhvhv"]:
        fpath = os.path.join(OUT_DIR, name, f"{name}_top10_pickup_zones.csv")
        if not os.path.exists(fpath):
            logger.warning("  • Missing %s → skipping top10 Jaccard for this dataset", fpath)
            continue
        df = pd.read_csv(fpath)  # columns: [zone_col, trip_count]
        top_zones[name] = set(df.iloc[:, 0].astype(str).tolist())

    jaccard_results = {}
    names = sorted(top_zones.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            set_a, set_b = top_zones[a], top_zones[b]
            if not set_a or not set_b:
                score = float("nan")
            else:
                intersection = len(set_a & set_b)
                union = len(set_a | set_b)
                score = intersection / union
            jaccard_results[(a, b)] = score
            logger.info("  • Jaccard(top10_zones, %s vs %s) = %.2f", a, b, score)

    # 3) Save a text summary
    sim_txt = os.path.join(OUT_DIR, "cross_dataset_similarity.txt")
    with open(sim_txt, "w") as f:
        f.write("Yearly Trip‐Count Correlation Matrix:\n")
        if 'corr_matrix' in locals():
            f.write(corr_matrix.to_string())
            f.write("\n\n")
        else:
            f.write("Not enough data for correlation\n\n")
        f.write("Top‐10 Pickup Zones Jaccard Similarity:\n")
        for (a, b), score in jaccard_results.items():
            f.write(f"{a} vs {b}: {score:.2f}\n")

    logger.info("=== Cross‐Dataset Similarity Analysis COMPLETE ===")


def main():
    logger.info("=== TASK 4 START ===")
    logger.info("Output directory = %s", OUT_DIR)
    logger.info("Log file         = %s", LOG_FILE)
    os.makedirs(OUT_DIR, exist_ok=True)

    for ds_name, ds_cfg in DATASETS.items():
        try:
            process_dataset(ds_name, ds_cfg["root"])
        except Exception as e:
            logger.error("[%s] Unexpected error: %s", ds_name, e, exc_info=True)

    # Perform cross‐dataset similarity only after all four are done
    try:
        cross_dataset_similarity()
    except Exception as e:
        logger.error("Cross‐dataset similarity analysis failed: %s", e, exc_info=True)

    logger.info("=== TASK 4 COMPLETE ===")


if __name__ == "__main__":
    main()
