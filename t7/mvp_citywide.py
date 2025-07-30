#!/usr/bin/env python
import os
import time
import argparse
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from lightgbm.dask import DaskLGBMRegressor
from xgboost.dask import DaskXGBRegressor
import warnings

warnings.filterwarnings("ignore")

# Define feature and target columns for model training
FEATURE_COLS = [
    'trip_distance', 'pulocationid', 'dolocationid',
    'avg_temp_c', 'precip_mm', 'wind_speed_km_h',
    'is_art_gallery_p', 'is_museum_p', 'is_college_p', 'is_high_school_p',
    'is_art_gallery_d', 'is_museum_d', 'is_college_d', 'is_high_school_d'
]
TARGET_COL = 'trip_count'
DATE_COL = 'pickup_date'
SERVICES = ['green', 'yellow', 'fhv', 'fhvhv']


def setup_client(scheduler_file=None):
    """
    Initialize Dask client; use local cluster or provided scheduler.
    """
    if scheduler_file:
        return Client(scheduler_file=scheduler_file)
    return Client(LocalCluster(n_workers=2, threads_per_worker=2, memory_limit="4GB"))


def load_service_data(base_path, service, sample_n):
    """
    Load and preprocess service data from partitioned parquet files.
    """
    path = os.path.join(base_path, f"{service}_partitioned", "year=2024")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Partition not found: {path}")
    
    ddf = dd.read_parquet(path, blocksize="64MB", split_row_groups=True)

    # If sampling for quick tests
    if sample_n:
        df = ddf.head(sample_n, compute=True)
        # Ensure all expected columns exist and are numeric
        for col in FEATURE_COLS + [DATE_COL]:
            if col not in df.columns:
                df[col] = 0
        df = df[FEATURE_COLS + [DATE_COL]]
        df[FEATURE_COLS] = df[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")
        df = df.dropna()
        df[TARGET_COL] = 1

        # Aggregate features and target by date and locations
        agg_feats = [c for c in FEATURE_COLS if c not in ['pulocationid', 'dolocationid', DATE_COL]]
        agg_dict = {c: 'mean' for c in agg_feats}
        agg_dict[TARGET_COL] = 'sum'
        df = df.groupby([DATE_COL, 'pulocationid', 'dolocationid']).agg(agg_dict).reset_index()
        return df
    else:
        # Process full dataset lazily with Dask
        for col in FEATURE_COLS + [DATE_COL]:
            if col not in ddf.columns:
                ddf[col] = 0

        ddf = ddf[FEATURE_COLS + [DATE_COL]]
        ddf[FEATURE_COLS] = ddf[FEATURE_COLS].map_partitions(
            lambda df: df.apply(pd.to_numeric, errors="coerce")
        )
        ddf = ddf.dropna()
        ddf[TARGET_COL] = 1

        # Prepare aggregation dictionary
        agg_feats = [c for c in FEATURE_COLS if c not in ['pulocationid', 'dolocationid', DATE_COL]]
        agg_dict = {c: 'mean' for c in agg_feats}
        agg_dict[TARGET_COL] = 'sum'

        # Lazy groupby and compute final result
        ddf = ddf.groupby([DATE_COL, 'pulocationid', 'dolocationid']).agg(agg_dict).reset_index()
        return ddf.compute()


def train_sgd(df):
    """
    Train SGDRegressor with incremental fitting and return RMSE and training time.
    """
    X, y = df[FEATURE_COLS], df[TARGET_COL]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler().fit(X_tr)
    X_tr_s, X_te_s = scaler.transform(X_tr), scaler.transform(X_te)
    model = SGDRegressor(max_iter=1000, tol=1e-3)
    t0 = time.time()
    # Incremental training in batches
    for i in range(0, len(X_tr_s), 1000):
        model.partial_fit(X_tr_s[i:i+1000], y_tr.iloc[i:i+1000])
    duration = time.time() - t0
    rmse = np.sqrt(mean_squared_error(y_te, model.predict(X_te_s)))
    return rmse, duration


def train_lightgbm(ddf, client):
    """
    Train distributed LightGBM model using Dask.
    """
    X, y = ddf[FEATURE_COLS], ddf[TARGET_COL]
    model = DaskLGBMRegressor(n_estimators=100, learning_rate=0.1, client=client)
    t0 = time.time()
    model.fit(X, y)
    duration = time.time() - t0
    # Compute RMSE across all partitions
    rmse = np.sqrt(((y - model.predict(X)) ** 2).mean().compute())
    return rmse, duration


def train_xgboost(ddf, client):
    """
    Train distributed XGBoost model using Dask.
    """
    X, y = ddf[FEATURE_COLS], ddf[TARGET_COL]
    model = DaskXGBRegressor(
        objective='reg:squarederror',
        n_estimators=100,
        learning_rate=0.1
    )
    t0 = time.time()
    model.fit(X, y)
    duration = time.time() - t0
    preds = model.predict(X)
    rmse = np.sqrt(((y - preds) ** 2).mean().compute())
    return rmse, duration


def train_all_models(df, client, label, results):
    """
    Train SGD, LightGBM, and XGBoost, collect metrics, and append to results list.
    """
    print(f"[{label}] Training SGD...")
    rmse_sgd, t_sgd = train_sgd(df)
    print(f"SGD → RMSE: {rmse_sgd:.2f}, Time: {t_sgd:.2f}s")

    print(f"[{label}] Training LightGBM...")
    ddf = dd.from_pandas(df, npartitions=4)
    rmse_lgb, t_lgb = train_lightgbm(ddf, client)
    print(f"LGBM → RMSE: {rmse_lgb:.2f}, Time: {t_lgb:.2f}s")

    print(f"[{label}] Training XGBoost...")
    rmse_xgb, t_xgb = train_xgboost(ddf, client)
    print(f"XGB → RMSE: {rmse_xgb:.2f}, Time: {t_xgb:.2f}s")

    results.append({
        "Service": label,
        "SGD_RMSE": round(rmse_sgd, 2), "SGD_Time": round(t_sgd, 2),
        "LGBM_RMSE": round(rmse_lgb, 2), "LGBM_Time": round(t_lgb, 2),
        "XGB_RMSE": round(rmse_xgb, 2), "XGB_Time": round(t_xgb, 2),
    })


def main():
    """
    Parse arguments, load data for each service, train models, and output summary.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--base-path', required=True, help="Base path for partitioned data")
    parser.add_argument('--sample-n', type=str, default=None, help="Number of rows to sample for testing")
    parser.add_argument('-s', '--scheduler-file', type=str, default=None,
                        help="Path to Dask scheduler.json for distributed cluster")
    parser.add_argument('-o', '--output', type=str, default=None, help="File path to save summary CSV")
    args = parser.parse_args()

    # Convert sample_n to integer or None
    if args.sample_n in ("None", None):
        args.sample_n = None
    else:
        args.sample_n = int(args.sample_n)

    client = setup_client(scheduler_file=args.scheduler_file)
    all_dfs, results = [], []

    for service in SERVICES:
        print(f"Loading: {service.upper()} 2024 data")
        try:
            df = load_service_data(args.base_path, service, args.sample_n)
            print(f"{service.upper()} rows: {df.shape[0]}")
            train_all_models(df, client, service.upper(), results)
            all_dfs.append(df)
        except Exception as e:
            print(f"Failed for {service}: {e}")

    # Combine all service data and train global model if available
    if all_dfs:
        df_global = pd.concat(all_dfs).reset_index(drop=True)
        print(f"Global training on combined data: {df_global.shape[0]} rows")
        train_all_models(df_global, client, "GLOBAL", results)

    client.close()

    # Display summary of results
    print("Summary:")
    summary_df = pd.DataFrame(results)
    print(summary_df.to_string(index=False))

    # Save summary to CSV if output path provided
    if args.output:
        summary_df.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
