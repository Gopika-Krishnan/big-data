import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

BASE_PATH = "/d/hpc/projects/FRI/jo83525/big-data/T5/POI"
PARQUET_PATH = "/d/hpc/projects/FRI/jo83525/big-data/T8/fhvhv_influence.parquet"
plt.style.use("seaborn-v0_8-muted")
con = duckdb.connect(database=':memory:')

def query_service_share(dataset):
    dataset_type = dataset.split('_')[0]
    path = os.path.join(BASE_PATH, dataset, "year=*/part*.parquet")

    if dataset_type == "fhvhv":
        return f"""
            SELECT
                '{dataset_type}' AS service_type,
                pickup_date,
                hvfhs_license_num,
                COUNT(*) AS trip_count
            FROM read_parquet('{path}')
            GROUP BY pickup_date, hvfhs_license_num
        """
    else:
        return f"""
            SELECT
                '{dataset_type}' AS service_type,
                pickup_date,
                NULL AS hvfhs_license_num,
                COUNT(*) AS trip_count
            FROM read_parquet('{path}')
            GROUP BY pickup_date
        """

def run_analysis():
    queries = [
        query_service_share("yellow_partitioned"),
        query_service_share("green_partitioned"),
        query_service_share("fhvhv_partitioned")
    ]
    union_query = " UNION ALL ".join(queries)
    con.execute(f"CREATE OR REPLACE TEMP TABLE temp_table AS {union_query}")

    con.execute("""
        CREATE OR REPLACE TABLE result AS
        SELECT 
            pickup_date,
            service_type,
            COALESCE(hvfhs_license_num, '') AS hvfhs_license_num,
            SUM(trip_count) AS trips
        FROM temp_table
        GROUP BY pickup_date, service_type, hvfhs_license_num
        ORDER BY pickup_date
    """)

    con.execute(f"COPY result TO '{PARQUET_PATH}' (FORMAT PARQUET)")
    print(f"✅ Saved competitor-augmented results to {PARQUET_PATH}")

if __name__ == "__main__":
    # run_analysis()
    # Load the enriched trip counts
    df = duckdb.sql(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
    print("Available columns:", df.columns.tolist())

    # Parse date
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    df = df.sort_values("pickup_date")

    # Pivot to wide format: one column per service_type or company
    df_wide = df.pivot_table(
        index="pickup_date",
        columns=["service_type", "hvfhs_license_num"],
        values="trips",
        aggfunc="sum"
    ).fillna(0)

    # Flatten columns: use license_num for FHVHVs
    df_wide.columns = [
        col[0] if col[0] in {"yellow", "green"} else col[1]
        for col in df_wide.columns
    ]

    # Rename operators
    fhvhv_mapping = {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft"
    }
    df_wide = df_wide.rename(columns=fhvhv_mapping)

    # Add fhvhv total
    df_wide["fhvhv_total"] = df_wide[["Juno", "Uber", "Via", "Lyft"]].sum(axis=1)

    # Compute relative shares
    df_wide["total"] = df_wide[["yellow", "green", "fhvhv_total"]].sum(axis=1)
    for col in ["yellow", "green", "Juno", "Uber", "Via", "Lyft", "fhvhv_total"]:
        df_wide[f"{col}_share"] = df_wide[col] / df_wide["total"]

    # 1 Absolute Trips per Day
    plt.figure(figsize=(14, 6))
    for col in ["yellow", "green", "Juno", "Uber", "Via", "Lyft"]:
        plt.plot(df_wide.index, df_wide[col], label=col, linewidth=1)
    plt.title("Absolute Daily Trips by Service Type")
    plt.ylabel("Number of Trips")
    plt.xlabel("Date")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("absolute_trips_by_type.png", dpi=300)
    plt.close()

    # 2 Relative Share per Day
    plt.figure(figsize=(14, 6))
    for col in ["yellow_share", "green_share", "Juno_share", "Uber_share", "Via_share", "Lyft_share"]:
        label = col.replace("_share", "")
        plt.plot(df_wide.index, df_wide[col], label=label, linewidth=1)
    plt.title("Relative Share of Trips by Service Type")
    plt.ylabel("Proportion of Total Trips")
    plt.xlabel("Date")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("relative_share_by_type.png", dpi=300)
    plt.close()

    # 3 Monthly Stacked Shares
    df_wide["month"] = df_wide.index.to_period("M").to_timestamp()
    monthly = df_wide.groupby("month")[
        ["yellow", "green", "Juno", "Uber", "Via", "Lyft"]
    ].sum()
    monthly["total"] = monthly.sum(axis=1)
    for col in ["yellow", "green", "Juno", "Uber", "Via", "Lyft"]:
        monthly[f"{col}_share"] = monthly[col] / monthly["total"]

    share_cols = [f"{col}_share" for col in ["yellow", "green", "Juno", "Uber", "Via", "Lyft"]]
    ax = monthly[share_cols].plot(
        kind="bar",
        stacked=True,
        figsize=(15, 6),
        colormap="tab20c",
        width=0.9
    )
    ax.set_title("Monthly Relative Share of Trips by Service")
    ax.set_ylabel("Proportion")
    ax.set_xlabel("Month")
    ax.set_xticks(range(0, len(monthly), 6))
    ax.set_xticklabels(
        [d.strftime("%Y-%m") for d in monthly.index[::6]],
        rotation=45,
        ha='right'
    )
    ax.legend(["Yellow", "Green", "Juno", "Uber", "Via", "Lyft"])
    plt.tight_layout()
    plt.savefig("monthly_share_stacked.png", dpi=300)
    plt.close()

    # 4 Yearly Total Trip Volume
    df_wide["year"] = df_wide.index.year
    yearly = df_wide.groupby("year")[
        ["yellow", "green", "Juno", "Uber", "Via", "Lyft"]
    ].sum()
    yearly.plot(kind="bar", figsize=(12, 6))
    plt.title("Total Trips per Year by Service")
    plt.ylabel("Trips")
    plt.xlabel("Year")
    plt.tight_layout()
    plt.savefig("yearly_trips_comparison.png", dpi=300)
    plt.close()

    # 5 COVID Period Highlight
    plt.figure(figsize=(14, 6))
    mask = df_wide.index >= "2017-01-01"
    df_filtered = df_wide.loc[mask]
    plt.plot(df_filtered.index, df_filtered["fhvhv_total_share"], label="FHVHV Total Share", color="purple")
    plt.axvspan(pd.Timestamp("2020-03-15"), pd.Timestamp("2021-06-15"), color="lightgrey", alpha=0.5, label="COVID Period")
    plt.axvline(pd.Timestamp("2020-03-15"), color="red", linestyle="--", label="March 2020: COVID Lockdown")
    plt.axvline(pd.Timestamp("2021-06-15"), color="green", linestyle="--", label="June 2021: Reopening")
    plt.title("FHVHV Market Share Over Time with COVID Events")
    plt.ylabel("FHVHV Share")
    plt.xlabel("Date")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("fhvhv_share_covid_highlight.png", dpi=300)
    plt.close()

    print("All plots with individual company breakdown saved.")
