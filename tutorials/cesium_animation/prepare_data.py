"""
Convert parquet source datasets to compact JSON/GeoJSON for Cesium animation.

Shots: compact JSON point arrays (subsampled to 50K).
All H3 layers (3-7): polygon GeoJSON with H3 cell boundaries.
H3 level 7 has many features but at ~2km hexagons they're visually impactful
as polygons, so we keep polygon format for all H3 levels.

Usage:
    python prepare_data.py
"""

import json
import os
from pathlib import Path

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from gedih3.gedidriver import GEDIShot

DATA_DIR = Path(__file__).resolve().parent / "data"

# Max height for normalization (meters)
RH98_MAX = 40.0
# Number of orbits to sample for shot display
N_ORBITS = 20

# Layer definitions
LAYERS = [
    ("shots_maryland",      "shots_points.json", True),
    ("h3_07_maryland",      "h3_07.geojson",     False),
    ("h3_06_midatlantic",   "h3_06.geojson",     False),
    ("h3_05_eastern",       "h3_05.geojson",     False),
    ("h3_04_central",       "h3_04.geojson",     False),
    ("h3_03_conus",         "h3_03.geojson",     False),
]


def load_parquet_dir(src_dir):
    """Load all parquet files from a directory into a single GeoDataFrame."""
    files = sorted(f for f in os.listdir(src_dir) if f.endswith(".parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {src_dir}")
    gdfs = []
    for f in files:
        gdf = gpd.read_parquet(os.path.join(src_dir, f))
        # Preserve index column (e.g. h3_07) that may contain H3 cell IDs
        if gdf.index.name and gdf.index.name.startswith("h3_") and gdf.index.name not in gdf.columns:
            gdf = gdf.reset_index()
        gdfs.append(gdf)
    return pd.concat(gdfs, ignore_index=True)


def normalize_rh98(values):
    return np.clip(values / RH98_MAX, 0, 1)


def find_rh98_column(df):
    candidates = [c for c in df.columns if "rh_098" in c and "count" not in c]
    if not candidates:
        raise ValueError(f"No rh_098 column found. Columns: {list(df.columns)}")
    return candidates[0]


def find_h3_column(df):
    for col in df.columns:
        if col.startswith("h3_") and col != "h3_03":
            return col
    # fallback to h3_03 if it's the only one
    if "h3_03" in df.columns:
        return "h3_03"
    raise ValueError(f"No h3_XX column found. Columns: {list(df.columns)}")


def h3_to_polygon(h3_index):
    boundary = h3.cell_to_boundary(h3_index)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def find_shot_number_column(df):
    candidates = [c for c in df.columns if c.startswith("shot_number")]
    if not candidates:
        raise ValueError(f"No shot_number column found. Columns: {list(df.columns)}")
    return candidates[0]


def process_shots(src_dir, out_path):
    """Convert shots to compact [[lon, lat, normalizedValue], ...] JSON.

    Selects all shots from N_ORBITS randomly chosen orbits to show
    GEDI's orbital ground-track pattern instead of random scatter.
    """
    gdf = load_parquet_dir(src_dir)
    rh98_col = find_rh98_column(gdf)
    gdf = gdf[gdf[rh98_col] >= 0].copy()
    print(f"  Loaded {len(gdf)} shots (rh98 col: {rh98_col})")

    # Sample by orbit to show ground-track pattern
    shot_col = find_shot_number_column(gdf)
    gs = GEDIShot(gdf[shot_col].values)
    gdf["_orbit"] = gs.orbit
    unique_orbits = gdf["_orbit"].unique()
    rng = np.random.RandomState(42)
    n_pick = min(N_ORBITS, len(unique_orbits))
    selected_orbits = rng.choice(unique_orbits, size=n_pick, replace=False)
    gdf = gdf[gdf["_orbit"].isin(selected_orbits)].copy()
    gdf.drop(columns=["_orbit"], inplace=True)
    print(f"  Selected {n_pick} orbits ({len(gdf)} shots) from {len(unique_orbits)} available")

    lons = gdf.geometry.x.values
    lats = gdf.geometry.y.values
    norms = normalize_rh98(gdf[rh98_col].values)

    points = [
        [round(float(lon), 5), round(float(lat), 5), round(float(nv), 4)]
        for lon, lat, nv in zip(lons, lats, norms)
    ]
    with open(out_path, "w") as f:
        json.dump(points, f)
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"  Wrote {len(points)} points -> {out_path.name} ({size_mb:.1f} MB)")


def process_h3(src_dir, out_path):
    """Convert H3 aggregated data to GeoJSON with hex polygon geometries."""
    df = load_parquet_dir(src_dir)
    rh98_col = find_rh98_column(df)
    h3_col = find_h3_column(df)
    df = df[df[rh98_col] >= 0].copy()
    print(f"  Loaded {len(df)} hexagons (rh98: {rh98_col}, h3: {h3_col})")

    # Build H3 polygon geometries
    print(f"  Generating H3 polygon geometries...")
    geometries = df[h3_col].apply(h3_to_polygon)
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")

    norms = normalize_rh98(gdf[rh98_col].values)
    rh98_vals = gdf[rh98_col].values

    features = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        geom = row.geometry.__geo_interface__
        if geom["type"] == "Polygon":
            geom["coordinates"] = [
                [[round(c, 6) for c in pt] for pt in ring]
                for ring in geom["coordinates"]
            ]
        props = {
            "rh98": round(float(rh98_vals[i]), 2),
            "height_color": round(float(norms[i]), 4),
            "extrusion": round(float(rh98_vals[i]) * 100, 0),
        }
        features.append({"type": "Feature", "properties": props, "geometry": geom})

    geojson = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w") as f:
        json.dump(geojson, f)
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"  Wrote {len(features)} polygons -> {out_path.name} ({size_mb:.1f} MB)")


def main():
    print("Converting parquet datasets to Cesium-ready formats")
    print(f"Data directory: {DATA_DIR}\n")

    for subdir, filename, is_shots in LAYERS:
        src = DATA_DIR / subdir
        out = DATA_DIR / filename
        print(f"Processing: {subdir} -> {filename}")
        if not src.exists():
            print(f"  SKIP: {src} not found\n")
            continue
        if is_shots:
            process_shots(src, out)
        else:
            process_h3(src, out)
        print()

    print("Done! Output files are in:", DATA_DIR)


if __name__ == "__main__":
    main()
