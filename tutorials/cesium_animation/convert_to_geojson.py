#!/usr/bin/env python3
"""Convert gedih3 simplified datasets to GeoJSON for CesiumJS animation.

Loads each dataset produced by generate_data.sh, adds H3 polygon geometries
for aggregated levels, and exports single GeoJSON files per layer.
Shots are subsampled if needed to keep file sizes browser-friendly (<50MB).
"""

import json
import sys
from pathlib import Path

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

# Resolve paths relative to this script
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"

# Maximum number of shot points to keep browser responsive
MAX_SHOTS = 50_000

# rh98 range for color normalization (meters)
RH98_MIN = 0.0
RH98_MAX = 40.0

# Dataset configurations: (subdirectory, output_filename, is_shots)
DATASETS = [
    ("shots_maryland", "shots_maryland.geojson", True),
    ("h3_09_maryland", "h3_09.geojson", False),
    ("h3_08_midatlantic", "h3_08.geojson", False),
    ("h3_07_eastern", "h3_07.geojson", False),
    ("h3_05_central", "h3_05.geojson", False),
    ("h3_03_conus", "h3_03.geojson", False),
]


def normalize_rh98(values: pd.Series) -> pd.Series:
    """Normalize rh98 values to 0-1 range for color mapping."""
    return ((values - RH98_MIN) / (RH98_MAX - RH98_MIN)).clip(0, 1)


def find_rh98_column(df: pd.DataFrame) -> str:
    """Find the rh98 column name (may have _mean suffix from aggregation)."""
    candidates = [c for c in df.columns if "rh_098" in c and "count" not in c]
    if not candidates:
        raise ValueError(f"No rh_098 column found. Columns: {list(df.columns)}")
    return candidates[0]


def h3_to_polygon(h3_index: str) -> Polygon:
    """Convert an H3 cell index to a Shapely polygon."""
    boundary = h3.cell_to_boundary(h3_index)
    # h3 returns (lat, lng) pairs; Shapely needs (lng, lat)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])  # close the ring
    return Polygon(coords)


def find_h3_column(df: pd.DataFrame) -> str:
    """Find the H3 index column in the DataFrame."""
    for col in df.columns:
        if col.startswith("h3_"):
            return col
    raise ValueError(f"No h3_XX column found. Columns: {list(df.columns)}")


def load_simplified_dataset(path: Path) -> gpd.GeoDataFrame:
    """Load a gedih3 simplified dataset directory as GeoDataFrame."""
    parquet_files = sorted(path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files in {path}")
    # Use gpd.read_parquet to correctly deserialize WKB geometry from geoparquet
    gdfs = [gpd.read_parquet(f) for f in parquet_files]
    gdf = pd.concat(gdfs, ignore_index=True)
    if not isinstance(gdf, gpd.GeoDataFrame) and "geometry" in gdf.columns:
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
    return gdf


def process_shots(path: Path, output: Path):
    """Process individual GEDI shots: load, subsample, add color, export."""
    print(f"  Loading shots from {path}")
    gdf = load_simplified_dataset(path)
    rh98_col = find_rh98_column(gdf)
    print(f"  rh98 column: {rh98_col}, total shots: {len(gdf)}")

    # Filter out invalid/negative values
    gdf = gdf[gdf[rh98_col] >= 0].copy()

    # Subsample if too many shots (spatial stratified sampling)
    if len(gdf) > MAX_SHOTS:
        print(f"  Subsampling from {len(gdf)} to {MAX_SHOTS} shots")
        gdf = gdf.sample(n=MAX_SHOTS, random_state=42)

    # Add normalized height for color mapping
    gdf["rh98"] = gdf[rh98_col].round(2)
    gdf["height_color"] = normalize_rh98(gdf[rh98_col]).round(4)

    # Keep only essential columns
    gdf = gdf[["geometry", "rh98", "height_color"]].copy()
    gdf.to_file(output, driver="GeoJSON")
    size_mb = output.stat().st_size / 1e6
    print(f"  Exported {len(gdf)} shots -> {output.name} ({size_mb:.1f} MB)")


def process_h3_hexagons(path: Path, output: Path):
    """Process H3 aggregated data: load, add hex geometries, export."""
    print(f"  Loading aggregated data from {path}")
    df = load_simplified_dataset(path)
    rh98_col = find_rh98_column(df)
    h3_col = find_h3_column(df)
    print(f"  rh98 column: {rh98_col}, h3 column: {h3_col}, hexagons: {len(df)}")

    # Filter out invalid values
    df = df[df[rh98_col] >= 0].copy()

    # Create hex polygon geometries
    print(f"  Generating H3 polygon geometries...")
    geometries = df[h3_col].apply(h3_to_polygon)
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")

    # Add normalized height for color mapping and extrusion
    gdf["rh98"] = gdf[rh98_col].round(2)
    gdf["height_color"] = normalize_rh98(gdf[rh98_col]).round(4)
    # Extrusion height in meters (scale for visual effect)
    gdf["extrusion"] = (gdf[rh98_col] * 100).round(0)  # 1m tree = 100m extrusion

    # Keep only essential columns
    gdf = gdf[["geometry", h3_col, "rh98", "height_color", "extrusion"]].copy()
    gdf.to_file(output, driver="GeoJSON")
    size_mb = output.stat().st_size / 1e6
    print(f"  Exported {len(gdf)} hexagons -> {output.name} ({size_mb:.1f} MB)")


def main():
    print("Converting gedih3 datasets to GeoJSON for CesiumJS animation")
    print(f"Data directory: {DATA_DIR}")
    print()

    for subdir, filename, is_shots in DATASETS:
        src = DATA_DIR / subdir
        dst = DATA_DIR / filename
        print(f"Processing: {subdir} -> {filename}")
        if not src.exists():
            print(f"  SKIPPED: {src} not found")
            continue
        try:
            if is_shots:
                process_shots(src, dst)
            else:
                process_h3_hexagons(src, dst)
        except Exception as e:
            print(f"  ERROR: {e}")
        print()

    print("Done! GeoJSON files are in:", DATA_DIR)


if __name__ == "__main__":
    main()
