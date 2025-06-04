def clean_taxi_data(df, dataset_type="yellow", is_dask=False):
    import pandas as pd
    import dask.dataframe as dd

    # Standardize column names
    df.columns = df.columns.str.lower()

    # Map config based on dataset type
    pickup_col = {
        "yellow": "tpep_pickup_datetime",
        "green": "lpep_pickup_datetime",
        "fhv": "pickup_datetime",
        "fhvhv": "pickup_datetime"
    }[dataset_type]

    dropoff_col = {
        "yellow": "tpep_dropoff_datetime",
        "green": "lpep_dropoff_datetime",
        "fhv": "dropoff_datetime",
        "fhvhv": "dropoff_datetime"
    }[dataset_type]

    # Optional fields
    distance = df.get("trip_distance", None)
    fare = df.get("fare_amount", None) if dataset_type != "fhvhv" else df.get("base_passenger_fare", None)
    tip = df.get("tip_amount", None)
    passengers = df.get("passenger_count", None)
    puloc = df.get("pulocationid", None)
    doloc = df.get("dolocationid", None)

    # Convert timestamps
    df[pickup_col] = dd.to_datetime(df[pickup_col], errors="coerce") if is_dask else pd.to_datetime(df[pickup_col], errors="coerce")
    df[dropoff_col] = dd.to_datetime(df[dropoff_col], errors="coerce") if is_dask else pd.to_datetime(df[dropoff_col], errors="coerce")

    # Compute duration
    duration = df[dropoff_col] - df[pickup_col]
    duration_sec = duration.dt.total_seconds()

    # Define flags
    flags = [
        df[pickup_col] > df[dropoff_col],               # dropoff before pickup
        df[pickup_col].dt.year < 2012,                  # year before 2012
        df[pickup_col] == df[dropoff_col],              # same pickup/dropoff time
        distance == 0 if distance is not None else False,
        fare < 0 if fare is not None else False,
        df[pickup_col].isna(),
        df[dropoff_col].isna(),
        duration_sec > 86400,
        duration_sec <= 0,
        passengers > 6 if passengers is not None else False,
        passengers.isna() if passengers is not None else False,
        tip < 0 if tip is not None else False,
        puloc == doloc if puloc is not None and doloc is not None else False
    ]

    # Clean mask
    clean_mask = ~dd.concat(flags, axis=1).any(axis=1) if is_dask else ~pd.concat(flags, axis=1).any(axis=1)
    return df[clean_mask]


#For Pandas
df = pd.read_parquet("yourfile.parquet")
df_clean = clean_taxi_data(df, dataset_type="green")

#For Dask
import dask.dataframe as dd
df = dd.read_parquet("yourfile.parquet", engine="pyarrow")
df_clean = clean_taxi_data(df, dataset_type="fhvhv", is_dask=True)
