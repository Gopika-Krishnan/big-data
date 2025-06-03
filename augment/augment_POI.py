import os
import glob
import pandas as pd
import geopandas as gpd
from pathlib import Path
import dask.dataframe as dd
import pyarrow.parquet as pq


def load_parquets_safely(base_path):
    # look for every .parquet under base_path, at any depth
    pattern = os.path.join(base_path, "**", "*.parquet")
    parquet_files = glob.glob(pattern, recursive=True)
    print(f"Found {len(parquet_files)} parquet files total.")

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {pattern!r}")

    valid_files = []
    bad_files   = []

    for f in parquet_files:
        if os.path.getsize(f) == 0:
            bad_files.append(f)
            continue
        try:
            pq.ParquetFile(f)  # validate
            valid_files.append(f)
        except Exception:
            bad_files.append(f)

    print(f"Valid parquet files: {len(valid_files)}")
    if bad_files:
        print(f"Skipped {len(bad_files)} bad/empty files:")
        for bf in bad_files:
            print("  ", bf)

    # Now we’re guaranteed non‐empty valid_files
    return dd.read_parquet(valid_files, engine="pyarrow")

# 1a) Read taxi zones once (keep in a projected CRS for spatial ops)
taxi_zones = (
    gpd.read_file("DB/taxi_zones/taxi_zones.shp")
       .to_crs(epsg=3857)
       .loc[:, ["LocationID", "geometry"]]
)
print("Taxi Zones Read")

# 1b) Helper to load any CSV of points and tag them
def load_poi_zones(csv_path: Path, tag: str,
                   geom_col="the_geom",
                   lat_col="Latitude", lon_col="Longitude",
                   sep=";") -> pd.DataFrame:
    """
    Reads csv_path and returns a DataFrame of unique LocationIDs
    with a boolean column `tag` == True indicating presence of a POI.
    """
    df = pd.read_csv(csv_path, sep=sep, on_bad_lines="skip")

    # 1) if there's a WKT point column, extract lon/lat from it
    if geom_col in df.columns:
        coords = df[geom_col].str.extract(
            r'POINT\s*\(\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s*\)'
        ).astype(float)
        df[["Longitude", "Latitude"]] = coords

    # 2) ensure we now have Longitude & Latitude
    if not {"Longitude", "Latitude"}.issubset(df.columns):
        raise ValueError(f"No {geom_col} or lat/lon cols found in {csv_path}")

    # 3) build a GeoDataFrame, project it, and spatial‐join
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326"
    ).to_crs(taxi_zones.crs)

    joined = gpd.sjoin(
        gdf[["geometry"]], taxi_zones,
        how="inner", predicate="within"
    )

    # 4) dedupe and flag
    unique_zones = pd.DataFrame({
        "LocationID": joined["LocationID"].unique(),
        tag: True
    })
    return unique_zones

# 1c) Build one DataFrame with flags for each POI category
zones_art   = load_poi_zones(Path("DB/ART_GALLERY_20250410.csv"), "is_art_gallery")
zones_muse  = load_poi_zones(Path("DB/MUSEUM_20250410.csv"),     "is_museum")
zones_coll  = load_poi_zones(Path("DB/COLLEGE_UNIVERSITY_20250413.csv"), "is_college")
zones_hs    = load_poi_zones(Path("DB/2017_DOE_High_School_Directory_20250410.csv"),
                             "is_high_school", sep=";")

# Merge them into one small lookup table
poi_lookup = (
    zones_art
      .merge(zones_muse,  on="LocationID", how="outer")
      .merge(zones_coll,  on="LocationID", how="outer")
      .merge(zones_hs,    on="LocationID", how="outer")
      .fillna(False)
      .astype({"is_art_gallery": bool,
               "is_museum":      bool,
               "is_college":     bool,
               "is_high_school": bool})
)
print("Created POI look_up_table")

# Read your main Parquet (already has holiday/weather)
ddf = load_parquets_safely("/d/hpc/projects/FRI/bigdata/students/gk40784/bigdata_hw5_output/partitioned_with_holidayweather")
print("Loaded parquets safely")
# insert this:
print("Columns in ddf:", list(ddf.columns))

art_zones  = set(poi_lookup.loc[poi_lookup.is_art_gallery,  "LocationID"])
muse_zones = set(poi_lookup.loc[poi_lookup.is_museum,       "LocationID"])
coll_zones = set(poi_lookup.loc[poi_lookup.is_college,      "LocationID"])
hs_zones   = set(poi_lookup.loc[poi_lookup.is_high_school,  "LocationID"])

# 2) define your partition function
def add_poi_flags(pdf):
    pdf["is_art_gallery_p"] = pdf["pulocationid"].isin(art_zones)
    pdf["is_museum_p"]      = pdf["pulocationid"].isin(muse_zones)
    pdf["is_college_p"]     = pdf["pulocationid"].isin(coll_zones)
    pdf["is_high_school_p"] = pdf["pulocationid"].isin(hs_zones)

    pdf["is_art_gallery_d"] = pdf["dolocationid"].isin(art_zones)
    pdf["is_museum_d"]      = pdf["dolocationid"].isin(muse_zones)
    pdf["is_college_d"]     = pdf["dolocationid"].isin(coll_zones)
    pdf["is_high_school_d"] = pdf["dolocationid"].isin(hs_zones)
    return pdf

# 3) build a proper meta DataFrame by copying and then adding empty columns
meta = ddf._meta.copy()
for col in [
    "is_art_gallery_p", "is_museum_p", "is_college_p", "is_high_school_p",
    "is_art_gallery_d", "is_museum_d", "is_college_d", "is_high_school_d"
]:
    # note: use pandas boolean dtype or plain bool depending on downstream needs
    meta[col] = pd.Series(dtype="bool")

# 4) apply without any global shuffle
ddf = ddf.map_partitions(add_poi_flags, meta=meta)

# Finally, write out
ddf.to_parquet(
    "bigdata_hw5_output/partitioned_augmented",
    engine="pyarrow",
    compression="snappy",
    write_index=False,
    row_group_size=100000,
    partition_on=["year"]
)
print("Parquet saved to: bigdata_hw5_output/partitioned_augmented")