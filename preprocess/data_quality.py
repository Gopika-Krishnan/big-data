def clean_taxi_data(df, dataset_configs, dataset_type="yellow", is_dask=False):
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
    df[pickup_col] > df[dropoff_col], # Dropping before picking up not allowed
    df[pickup_col].dt.year <= dataset_configs[f"{dataset_type}"]["years"][0], # Year has to be after start of period
    df[pickup_col].dt.year >= dataset_configs[f"{dataset_type}"]["years"][-1], # Year has to be before finish of period
    df[pickup_col] == df[dropoff_col], # Dropping and pick up should not be equal
    distance == 0 if distance is not None else pd.Series(False, index=df.index), # Distance equal to 0
    fare < 0 if fare is not None else pd.Series(False, index=df.index), # Negative fare charged not possible
    df[pickup_col].isna(), # Missing data on pickup
    df[dropoff_col].isna(), # Missing data on dropoff
    duration_sec > 86400, # Trip lasting 1 day
    duration_sec <= 0, # Trip lasting nothing or negative values of time.
    passengers > 6 if passengers is not None else pd.Series(False, index=df.index), # More than 6 passengers in 
    passengers.isna() if passengers is not None else pd.Series(False, index=df.index), # Missing passenger info
    tip < 0 if tip is not None else pd.Series(False, index=df.index), # Negative tip
    puloc == doloc if puloc is not None and doloc is not None else pd.Series(False, index=df.index), # Pickup and dropoff equal
    ]

    # Clean mask
    clean_mask = ~dd.concat(flags, axis=1).any(axis=1) if is_dask else ~pd.concat(flags, axis=1).any(axis=1)
    return df[clean_mask]
