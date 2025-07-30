import os
import glob
import pandas as pd
import geopandas as gpd
from pathlib import Path
import dask.dataframe as dd
import pyarrow.parquet as pq
from dask.dataframe import read_parquet

def load_parquets_safely(base_path):
    pattern = os.path.join(base_path, "", "*.parquet")
    parquet_files = glob.glob(pattern, recursive=True)
    print(f"Found {len(parquet_files)} parquet files total.")

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {pattern!r}")

    valid_files, bad_files = [], []
    for f in parquet_files:
        if os.path.getsize(f) == 0:
            bad_files.append(f)
            continue
        try:
            pq.ParquetFile(f)
            valid_files.append(f)
        except Exception:
            bad_files.append(f)

    print(f"Valid parquet files: {len(valid_files)}")
    if bad_files:
        print(f"Skipped {len(bad_files)} bad/empty files:")
        for bf in bad_files:
            print("  ", bf)

    return dd.read_parquet(valid_files, engine="pyarrow")

# === Use correct taxi zones path
taxi_zones = (
    gpd.read_file("/d/hpc/projects/FRI/jo83525/big-data/augment/taxi_zones.shp")
       .to_crs(epsg=3857)
       .loc[:, ["LocationID", "geometry"]]
)
print("Taxi Zones Read")

def load_poi_zones(csv_path: Path, tag: str,
                   geom_col="the_geom",
                   lat_col="Latitude", lon_col="Longitude",
                   sep=";") -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=sep, on_bad_lines="skip")
    if geom_col in df.columns:
        coords = df[geom_col].str.extract(
            r'POINT\s*\(\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s*\)'
        ).astype(float)
        df[["Longitude", "Latitude"]] = coords

    if not {"Longitude", "Latitude"}.issubset(df.columns):
        raise ValueError(f"No {geom_col} or lat/lon cols found in {csv_path}")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326"
    ).to_crs(taxi_zones.crs)

    joined = gpd.sjoin(gdf[["geometry"]], taxi_zones, how="inner", predicate="within")
    return pd.DataFrame({"LocationID": joined["LocationID"].unique(), tag: True})

zones_art  = load_poi_zones(Path("/d/hpc/projects/FRI/jo83525/big-data/augment/ART_GALLERY_20250410.csv"), "is_art_gallery")
zones_muse = load_poi_zones(Path("/d/hpc/projects/FRI/jo83525/big-data/augment/MUSEUM_20250410.csv"), "is_museum")
zones_coll = load_poi_zones(Path("/d/hpc/projects/FRI/jo83525/big-data/augment/COLLEGE_UNIVERSITY_20250413.csv"), "is_college")
zones_hs   = load_poi_zones(Path("/d/hpc/projects/FRI/jo83525/big-data/augment/2017_DOE_High_School_Directory_20250410.csv"), "is_high_school", sep=";")

poi_lookup = (
    zones_art
      .merge(zones_muse, on="LocationID", how="outer")
      .merge(zones_coll, on="LocationID", how="outer")
      .merge(zones_hs, on="LocationID", how="outer")
      .fillna(False)
      .infer_objects(copy=False)
      .astype({
          "is_art_gallery": bool,
          "is_museum": bool,
          "is_college": bool,
          "is_high_school": bool
      })
)
print("Created POI lookup table")

art_zones  = set(poi_lookup.loc[poi_lookup.is_art_gallery, "LocationID"])
muse_zones = set(poi_lookup.loc[poi_lookup.is_museum, "LocationID"])
coll_zones = set(poi_lookup.loc[poi_lookup.is_college, "LocationID"])
hs_zones   = set(poi_lookup.loc[poi_lookup.is_high_school, "LocationID"])

def add_poi_flags(pdf):
    pdf["is_art_gallery_p"] = pdf["pulocationid"].isin(art_zones)
    pdf["is_museum_p"] = pdf["pulocationid"].isin(muse_zones)
    pdf["is_college_p"] = pdf["pulocationid"].isin(coll_zones)
    pdf["is_high_school_p"] = pdf["pulocationid"].isin(hs_zones)

    pdf["is_art_gallery_d"] = pdf["dolocationid"].isin(art_zones)
    pdf["is_museum_d"] = pdf["dolocationid"].isin(muse_zones)
    pdf["is_college_d"] = pdf["dolocationid"].isin(coll_zones)
    pdf["is_high_school_d"] = pdf["dolocationid"].isin(hs_zones)
    return pdf

# === Parquet path
# Get all available years
# ========== Main loop over datasets ==========
DATASETS = ["yellow_partitioned", "green_partitioned", "fhv_partitioned", "fhvhv_partitioned"]
BASE_INPUT = "/d/hpc/projects/FRI/jo83525/big-data/T5/holweather"
BASE_OUTPUT = "/d/hpc/projects/FRI/jo83525/big-data/T5/POI"

for dataset_type in DATASETS:
    input_path = Path(BASE_INPUT) / dataset_type
    output_path = Path(BASE_OUTPUT) / dataset_type
    output_path.mkdir(parents=True, exist_ok=True)

    all_years = sorted([p.name for p in input_path.glob("year=*") if p.is_dir()])
    print(f"\n📂 {dataset_type}: Found {len(all_years)} years → {all_years}")

    for yfolder in all_years:
        year = yfolder.split("=")[-1]
        in_dir = input_path / yfolder
        out_dir = output_path / yfolder
        out_dir.mkdir(exist_ok=True)

        print(f"🗓️  Processing {dataset_type} year={year}")

        ddf = read_parquet(str(in_dir), engine="pyarrow")

        if "sr_flag" in ddf.columns:
            ddf["sr_flag"] = ddf["sr_flag"].astype("string")

        ddf = ddf.map_partitions(add_poi_flags)

        ddf.to_parquet(
            str(out_dir),
            engine="pyarrow",
            compression="snappy",
            write_index=False,
            row_group_size=100_000
        )

        print(f"✅ Saved to {out_dir}")