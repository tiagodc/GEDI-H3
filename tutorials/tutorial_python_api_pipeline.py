#!/usr/bin/env python
"""
gedih3 Python API Tutorial: Complete GEDI Data Processing Pipeline
==================================================================

This tutorial demonstrates the complete workflow for processing GEDI satellite
LiDAR data using the gedih3 Python API. This is ideal for:

- Jupyter notebook workflows
- Custom analysis scripts
- Integration into larger pipelines
- Programmatic access to GEDI data

By the end, you will have:
1. Downloaded GEDI data from NASA's DAAC
2. Built an H3-indexed database for fast spatial queries
3. Loaded and queried GEDI shots with spatial/quality filters
4. Aggregated data to coarser resolutions (H3 hexagons or EGI squares)
5. Created GeoTIFF raster maps

Prerequisites:
- conda environment with gedih3 installed (see environment.yml)
- NASA Earthdata account credentials in ~/.netrc
- ~10GB disk space for example data

Author: gedih3 team

To run this tutorial:
    python tutorial_python_api_pipeline.py

Or copy cells into a Jupyter notebook.
"""

# =============================================================================
# Setup and Imports
# =============================================================================

import os
import sys
import json
from pathlib import Path

# Core data science libraries
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

# Visualization (optional)
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Dask for distributed computing
from dask.distributed import Client, progress

# gedih3 imports
import gedih3.gh3driver as gh3
from gedih3.daac import gedi_download, GEDIAccessor
from gedih3.gh3builder import build_h3db
from gedih3.gedidriver import gedi_vars_expand, GEDIFile
from gedih3 import egi
from gedih3 import raster
from gedih3.config import GEDI_PRODUCTS

print("gedih3 Tutorial - Python API Pipeline")
print("=" * 50)
print(f"gedih3 imported successfully")

# =============================================================================
# Configuration
# =============================================================================

# Study area (Amazon rainforest example)
# Bounding box: [west, south, east, north]
STUDY_AREA = [-51, 0, -50, 1]

# Temporal range
DATE_START = "2020-01-01"
DATE_END = "2021-12-31"

# Output directories
BASE_DIR = Path("/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/") / "gedih3_tutorial_python"
SOC_DIR = BASE_DIR / "soc_data"
H3_DIR = BASE_DIR / "h3_database"
OUTPUT_DIR = BASE_DIR / "output"

# Create directories
for d in [SOC_DIR, H3_DIR, OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# H3 resolution settings
H3_RESOLUTION = 12  # Index level (~9m edge, matches GEDI footprint)
H3_PARTITION = 5    # Partition level for this tutorial (smaller files)

print(f"\nStudy Area: {STUDY_AREA}")
print(f"Date Range: {DATE_START} to {DATE_END}")
print(f"Output Directory: {BASE_DIR}")

# =============================================================================
# Step 0: Explore GEDI Products and Variables
# =============================================================================

print("\n" + "=" * 50)
print("Step 0: Exploring GEDI Products and Variables")
print("=" * 50)

# Available GEDI products
print("\nAvailable GEDI Products:")
for product, info in GEDI_PRODUCTS.items():
    print(f"  {product}: {info.get('description', 'N/A')}")

# Expand 'default' variables to see what's included
print("\nDefault L2A variables:")
l2a_vars = gedi_vars_expand({'L2A': ['default']})
print(f"  {l2a_vars['L2A'][:10]}...")  # First 10

print("\nDefault L4A variables:")
l4a_vars = gedi_vars_expand({'L4A': ['default']})
print(f"  {l4a_vars['L4A'][:10]}...")

# EGI resolution levels
print("\nEGI Resolution Levels:")
for level in [5, 6, 7, 8]:
    res_m = egi.get_resolution(level)
    print(f"  Level {level}: ~{res_m:.0f}m pixels")

# =============================================================================
# Step 1: Download GEDI Data
# =============================================================================

print("\n" + "=" * 50)
print("Step 1: Downloading GEDI Data")
print("=" * 50)

# Define which products and variables to download
product_vars = {
    'L2A': ['default'],  # Height metrics, quality flags
    'L4A': ['agbd'],     # Aboveground biomass density
}

print(f"\nProducts to download: {list(product_vars.keys())}")
print(f"Spatial filter: {STUDY_AREA}")
print(f"Temporal filter: {DATE_START} to {DATE_END}")

# --- Method 1: Using gedi_download function (recommended) ---
print("\nDownloading data (this may take several minutes)...")

try:
    downloaded_files = gedi_download(
        product_vars=product_vars,
        odir=str(SOC_DIR),
        spatial=STUDY_AREA,
        temporal=(DATE_START, DATE_END),
        resume=True,        # Skip already downloaded files
        n_jobs=4,           # Parallel downloads
        to_list=False       # Save to disk
    )
    print(f"Download complete! Files in: {SOC_DIR}")
except Exception as e:
    print(f"Download error (may need NASA credentials): {e}")
    print("Continuing with any existing files...")

# List downloaded files
h5_files = list(SOC_DIR.rglob("*.h5"))
print(f"\nDownloaded HDF5 files: {len(h5_files)}")
for f in h5_files[:5]:
    print(f"  {f.name}")

# --- Alternative: Using GEDIAccessor for more control ---
print("\n--- Alternative: Using GEDIAccessor class ---")

try:
    accessor = GEDIAccessor(
        authenticate=True,
        spatial=STUDY_AREA,
        temporal=(DATE_START, DATE_END)
    )

    # Search for granules
    granules = accessor.search_data('L2A')
    print(f"Found {len(granules)} L2A granules in region")
except Exception as e:
    print(f"GEDIAccessor example skipped: {e}")

# =============================================================================
# Step 2: Build H3-Indexed Database
# =============================================================================

print("\n" + "=" * 50)
print("Step 2: Building H3-Indexed Database")
print("=" * 50)

# Check if we have data to build from
if len(h5_files) == 0:
    print("No HDF5 files found. Skipping build step.")
    print("(You can still use existing databases)")
else:
    print(f"\nBuilding H3 database with:")
    print(f"  Index resolution: {H3_RESOLUTION} (~9m hexagons)")
    print(f"  Partition level: {H3_PARTITION}")
    print(f"  Source: {SOC_DIR}")
    print(f"  Output: {H3_DIR}")

    # Start Dask client for distributed processing
    print("\nStarting Dask cluster...")

    with Client(n_workers=4, threads_per_worker=1, memory_limit='4GB') as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        # Build the H3 database
        h3_files = build_h3db(
            product_vars=product_vars,
            res=H3_RESOLUTION,
            part=H3_PARTITION,
            spatial=STUDY_AREA,
            soc_source=str(SOC_DIR),
            h3_dir=str(H3_DIR)
        )

        if h3_files:
            print(f"\nDatabase built! {len(h3_files)} partition files created.")

# Read database metadata
log_file = H3_DIR / "gedih3_build_log.json"
if log_file.exists():
    with open(log_file) as f:
        db_meta = json.load(f)

    print("\nDatabase Metadata:")
    print(f"  Status: {db_meta.get('status')}")
    print(f"  H3 Resolution: {db_meta.get('h3_resolution_level')}")
    print(f"  H3 Partition: {db_meta.get('h3_partition_level')}")
    print(f"  Columns: {len(db_meta.get('h3_columns', []))} variables")
    print(f"  Partitions: {len(db_meta.get('h3_partition_ids', []))} tiles")

# =============================================================================
# Step 3: Load and Query Data
# =============================================================================

print("\n" + "=" * 50)
print("Step 3: Loading and Querying Data")
print("=" * 50)

# Check if database exists
if not log_file.exists():
    print("No database found. Using example data loading code.")
    print("(Run with real data to see results)")
else:
    # Set database path
    gh3.gh3_set_db_path(str(H3_DIR))

    # Start Dask client
    with Client(n_workers=4, threads_per_worker=1, memory_limit='4GB') as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        # --- Basic Load ---
        print("\n--- Basic data loading ---")

        ddf = gh3.gh3_load(
            columns=['agbd_l4a', 'quality_flag_l2a', 'rh_098_l2a'],
            gh3_dir=str(H3_DIR)
        )

        print(f"Loaded Dask DataFrame:")
        print(f"  Partitions: {ddf.npartitions}")
        print(f"  Columns: {ddf.columns.tolist()}")

        # --- Load with Spatial Filter ---
        print("\n--- Loading with spatial filter ---")

        ddf_region = gh3.gh3_load(
            columns=['agbd_l4a', 'lat_lowestmode', 'lon_lowestmode'],
            region=STUDY_AREA,  # Clip to study area
            gh3_dir=str(H3_DIR)
        )

        print(f"Filtered to region: {ddf_region.npartitions} partitions")

        # --- Load with Quality Filter ---
        print("\n--- Loading with quality filter ---")

        ddf_quality = gh3.gh3_load(
            columns=['agbd_l4a', 'quality_flag_l2a'],
            region=STUDY_AREA,
            query='quality_flag_l2a == 1',  # High-quality shots only
            gh3_dir=str(H3_DIR)
        )

        print(f"Quality-filtered: {ddf_quality.npartitions} partitions")

        # --- Preview Data ---
        print("\n--- Data Preview ---")
        sample = ddf_quality.head(10)
        print(sample)

        # --- Basic Statistics ---
        print("\n--- Basic Statistics ---")
        if 'agbd_l4a' in ddf_quality.columns:
            stats = ddf_quality['agbd_l4a'].describe().compute()
            print(f"AGBD Statistics:\n{stats}")

# =============================================================================
# Step 4: Aggregate Data
# =============================================================================

print("\n" + "=" * 50)
print("Step 4: Aggregating Data")
print("=" * 50)

if log_file.exists():
    with Client(n_workers=4, threads_per_worker=1, memory_limit='4GB') as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        # Load data
        ddf = gh3.gh3_load(
            columns=['agbd_l4a', 'rh_098_l2a', 'quality_flag_l2a',
                     'lat_lowestmode', 'lon_lowestmode'],
            query='quality_flag_l2a == 1',
            gh3_dir=str(H3_DIR)
        )

        # --- Option A: H3 Hexagonal Aggregation ---
        print("\n--- H3 Aggregation (to level 6) ---")

        h3_agg = gh3.gh3_aggregate(
            ddf,
            target_res=6,           # Aggregate to H3 level 6 (~36 km²)
            agg='mean',             # Mean of all shots in hexagon
            columns=['agbd_l4a', 'rh_098_l2a'],  # Columns to aggregate
            add_geometry=True       # Add hexagon polygons
        )

        h3_result = h3_agg.compute()
        print(f"H3 Aggregated: {len(h3_result)} hexagons")
        print(h3_result.head())

        # Save H3 aggregated data
        h3_output = OUTPUT_DIR / "h3_aggregated.parquet"
        h3_result.to_parquet(h3_output)
        print(f"Saved to: {h3_output}")

        # --- Option B: EGI Square Pixel Aggregation ---
        print("\n--- EGI Aggregation (to level 6, ~1km pixels) ---")

        egi_agg = gh3.egi_aggregate(
            ddf,
            target_level=6,         # EGI level 6 (~1km)
            agg='mean',
            columns=['agbd_l4a', 'rh_098_l2a'],
            add_geometry=True
        )

        egi_result = egi_agg.compute()
        print(f"EGI Aggregated: {len(egi_result)} pixels")
        print(egi_result.head())

        # Save EGI aggregated data
        egi_output = OUTPUT_DIR / "egi_aggregated.parquet"
        egi_result.to_parquet(egi_output)
        print(f"Saved to: {egi_output}")

        # --- Multiple Aggregation Functions ---
        print("\n--- Multiple aggregation functions ---")

        multi_agg = gh3.gh3_aggregate(
            ddf,
            target_res=6,
            agg=['mean', 'std', 'count'],  # Multiple functions
            columns=['agbd_l4a'],
            add_geometry=True
        )

        multi_result = multi_agg.compute()
        print(f"Multi-agg columns: {multi_result.columns.tolist()}")
else:
    print("No database found. Skipping aggregation examples.")

# =============================================================================
# Step 5: Rasterize to GeoTIFF
# =============================================================================

print("\n" + "=" * 50)
print("Step 5: Rasterizing to GeoTIFF")
print("=" * 50)

# Check for aggregated data
egi_parquet = OUTPUT_DIR / "egi_aggregated.parquet"
h3_parquet = OUTPUT_DIR / "h3_aggregated.parquet"

if egi_parquet.exists():
    print("\n--- EGI Rasterization ---")

    # Load aggregated data
    egi_gdf = gpd.read_parquet(egi_parquet)
    print(f"Loaded {len(egi_gdf)} EGI pixels")

    # Rasterize to xarray
    print("Rasterizing...")
    xras = egi.geodf_to_raster(egi_gdf, columns=['agbd_l4a_mean'])

    print(f"Raster shape: {xras.dims}")
    print(f"CRS: {xras.rio.crs}")

    # Save to GeoTIFF
    output_tif = OUTPUT_DIR / "agbd_egi_raster.tif"
    egi.export_raster(xras, str(output_tif), compress='LZW')
    print(f"Saved to: {output_tif}")

    # Quick visualization (if matplotlib available)
    if HAS_MATPLOTLIB and 'agbd_l4a_mean' in xras.data_vars:
        print("\nCreating visualization...")

        fig, ax = plt.subplots(figsize=(10, 8))
        xras['agbd_l4a_mean'].plot(ax=ax, cmap='viridis')
        ax.set_title('GEDI Aboveground Biomass Density (Mg/ha)')

        plot_file = OUTPUT_DIR / "agbd_preview.png"
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Preview saved to: {plot_file}")

if h3_parquet.exists():
    print("\n--- H3 Rasterization ---")

    # Load H3 aggregated data
    h3_gdf = gpd.read_parquet(h3_parquet)
    print(f"Loaded {len(h3_gdf)} H3 hexagons")

    # Rasterize H3 hexagons
    print("Rasterizing H3 hexagons...")
    xras_h3 = raster.h3_to_raster(h3_gdf, columns=['agbd_l4a_mean'])

    # Save
    h3_tif = OUTPUT_DIR / "agbd_h3_raster.tif"
    raster.export_raster(xras_h3, str(h3_tif), compress='LZW')
    print(f"Saved to: {h3_tif}")

if not egi_parquet.exists() and not h3_parquet.exists():
    print("No aggregated data found. Run aggregation step first.")

# =============================================================================
# Bonus: Time-Series Analysis
# =============================================================================

print("\n" + "=" * 50)
print("Bonus: Time-Series Analysis")
print("=" * 50)

# TIP: For simpler time-series workflows, use the CLI:
#   gh3_aggregate -d <db> -egi 6 -ti 1 -tu years -t0 ... -t1 ... -o ts_dir/
#   gh3_rasterize -d ts_dir/ -m -o rasters/

if log_file.exists():
    print("\nGenerating time-series rasters (annual)...")

    with Client(n_workers=4, threads_per_worker=1) as client:
        ddf = gh3.gh3_load(
            columns=['agbd_l4a', 'datetime', 'quality_flag_l2a',
                     'lat_lowestmode', 'lon_lowestmode'],
            query='quality_flag_l2a == 1',
            gh3_dir=str(H3_DIR)
        )

        # Generate time windows
        from gedih3.raster.timeseries import generate_time_windows

        timeseries_dir = OUTPUT_DIR / "timeseries"
        timeseries_dir.mkdir(exist_ok=True)

        for t0, t1, suffix in generate_time_windows(
            DATE_START, DATE_END, 1, 'years'
        ):
            print(f"\nProcessing: {suffix}")

            # Filter by time
            time_query = f"datetime >= '{t0}' and datetime < '{t1}'"

            try:
                time_ddf = ddf.query(time_query)
                n_rows = time_ddf.map_partitions(len).compute().sum()

                if n_rows == 0:
                    print(f"  No data for {suffix}")
                    continue

                print(f"  {n_rows} shots")

                # Aggregate and rasterize
                agg_gdf = gh3.egi_aggregate(
                    time_ddf,
                    target_level=6,
                    agg='mean',
                    add_geometry=True
                ).compute()

                if len(agg_gdf) > 0:
                    xras = egi.geodf_to_raster(agg_gdf)
                    output_file = timeseries_dir / f"agbd_{suffix}.tif"
                    egi.export_raster(xras, str(output_file))
                    print(f"  Saved: {output_file}")

            except Exception as e:
                print(f"  Error: {e}")
else:
    print("No database found. Skipping time-series example.")

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 50)
print("Tutorial Complete!")
print("=" * 50)

print(f"""
Output Summary:
  - Downloaded HDF5 files: {SOC_DIR}
  - H3 database: {H3_DIR}
  - Aggregated data: {OUTPUT_DIR}

Files Created:
  - egi_aggregated.parquet: EGI-indexed aggregated data
  - h3_aggregated.parquet: H3-indexed aggregated data
  - agbd_egi_raster.tif: GeoTIFF from EGI pixels
  - agbd_h3_raster.tif: GeoTIFF from H3 hexagons
  - timeseries/: Annual raster time-series

Next Steps:
  1. Open .tif files in QGIS/ArcGIS for visualization
  2. Load .parquet files in pandas/geopandas for analysis
  3. Modify this script for your own study area

For more information:
  - See CLAUDE.md for API documentation
  - Run gh3_<tool> --help for CLI options
  - Visit https://github.com/your-repo/gedih3
""")

# =============================================================================
# Quick Reference: Common Operations
# =============================================================================

"""
Quick Reference - Common gedih3 Operations
==========================================

# 1. Load data from H3 database
import gedih3.gh3driver as gh3

ddf = gh3.gh3_load(
    columns=['agbd_l4a', 'rh_098_l2a'],
    region=[-51, 0, -50, 1],           # Bounding box
    query='quality_flag_l2a == 1',     # Quality filter
    gh3_dir='/path/to/database'
)

# 2. Load simplified dataset (from gh3_extract or gh3_aggregate output)
gdf = gh3.gh3_load_dataset('/path/to/extracted/')  # Eager load
ddf = gh3.gh3_load_dataset_lazy('/path/to/aggregated/')  # Lazy Dask load

# 3. Aggregate to H3 hexagons
agg_df = gh3.gh3_aggregate(
    ddf,
    target_res=6,                       # H3 level 6
    agg='mean',                         # Aggregation function
    add_geometry=True
)

# 4. Aggregate to EGI square pixels
from gedih3 import egi

egi_df = gh3.egi_aggregate(
    ddf,
    target_level=6,                     # EGI level 6 (~1km)
    agg='mean',
    add_geometry=True
)

# 5. Rasterize EGI data
xras = egi.geodf_to_raster(egi_df.compute())
egi.export_raster(xras, 'output.tif', compress='LZW')

# 6. Rasterize H3 data
from gedih3 import raster

xras = raster.h3_to_raster(h3_df.compute())
raster.export_raster(xras, 'output.tif')

# 7. Read database metadata
part_level = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir='/path/to/db')
res_level = gh3.gh3_read_meta('h3_resolution_level', gh3_root_dir='/path/to/db')
columns = gh3.gh3_read_meta('h3_columns', gh3_root_dir='/path/to/db')
"""

if __name__ == '__main__':
    print("\nTutorial executed successfully!")
