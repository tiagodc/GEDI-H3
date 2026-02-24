# Quick Start

This guide covers the core workflow in 5 steps: download → build → extract → aggregate → rasterize.

## Step 1: Download GEDI data

```bash
gh3_download \
  -r "-51,0,-50,1" \
  -l2a default \
  -l4a default \
  -N 4
```

Downloaded files are organized as `<download_dir>/year/doy/*.h5`.

## Step 2: Build the H3 database

```bash
gh3_build \
  -r "-51,0,-50,1" \
  -l2a default \
  -l4a default \
  -h3r 12 \
  -h3p 3
```

This creates an H3-partitioned parquet database in `GH3_DEFAULT_H3_DIR`.

## Step 3: Extract a simplified dataset

```bash
gh3_extract \
  -d /path/to/h3_database \
  -r region.shp \
  -l2a rh \
  -l4a agbd \
  -o output/
```

Output is flat parquet files named by H3 partition cell.

## Step 4: Aggregate

```bash
# H3 aggregation to level 6 (~36 km²)
gh3_aggregate \
  -d /path/to/h3_database \
  -h3 6 \
  -o output/

# EGI aggregation to level 6 (~1 km)
gh3_aggregate \
  -d /path/to/h3_database \
  -egi 6 \
  -a mean \
  -o output/
```

## Step 5: Rasterize

```bash
gh3_rasterize \
  -d output/ \
  -o rasters/ \
  --compress LZW
```

## Python API

```python
import gedih3.gh3driver as gh3
from gedih3 import raster

# Load data
ddf = gh3.gh3_load(
    source='/path/to/h3_database',
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',
)

# Aggregate
agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')

# Rasterize
gdf = agg_df.compute()
xras = raster.h3_to_raster(gdf, columns=['agbd_l4a_mean'])
raster.export_raster(xras, 'agbd.tif', compress='LZW')
```

## EGI workflow (no shuffle)

```python
import gedih3.gh3driver as gh3

# Load directly into EGI partitions from H3 database
ddf = gh3.egi_load(
    source='/path/to/h3_database',
    columns=['agbd_l4a'],
    index_level=1,       # ~1 m fine index
    partition_level=12,  # ~160 km tiles
)

# Aggregate to ~1 km
agg_df = gh3.egi_aggregate(ddf, target_level=6, agg='mean')
```
