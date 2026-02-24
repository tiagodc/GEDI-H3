# EGI — EASE Grid Index

The `egi` sub-module provides square-pixel spatial indexing for GEDI data using the EASE-Grid 2.0 projection (EPSG:6933). It is the preferred indexing system for outputs that need to align with GEDI L4B gridded products.

---

## Overview

EGI encodes every EPSG:6933 pixel at a given resolution into a single `uint64` hash. This makes spatial indexing, aggregation, and rasterization both efficient and lossless — no coordinate lookup tables needed.

**Why use EGI instead of H3?**

| | H3 (Hexagonal) | EGI (Square) |
|---|---|---|
| Cell shape | Hexagon | Square |
| CRS | WGS84 (EPSG:4326) | EASE-Grid 2.0 (EPSG:6933) |
| L4B alignment | Approximate | Native — no resampling |
| Raster export | Needs reprojection | Direct pixel-perfect output |
| Hierarchy | Levels 0–15 | Levels 1–12 |

**Key properties**:
- 12 resolution levels from ~1 m (level 1) to ~160 km (level 12)
- Level 6 (~1 km) is the native GEDI L4B baseline resolution
- Lower level number = finer resolution (opposite of H3)
- All EGI data stays in EPSG:6933; output GeoDataFrames are reprojected to EPSG:4326

---

## Resolution Levels

| Level | Pixel Size | Description |
|-------|------------|-------------|
| 1 | ~1 m | Finest resolution |
| 2 | ~5 m | Very high resolution |
| 3 | ~25 m | GEDI footprint size |
| 4 | ~100 m | NISAR compatible |
| 5 | ~200 m | BIOMASS compatible |
| **6** | **~1 km** | **GEDI L4B native (baseline)** |
| 7 | ~2 km | GEDI threshold |
| 8 | ~10 km | GEDI wall-to-wall |
| 9 | ~20 km | Regional |
| 10 | ~40 km | Continental |
| 11 | ~80 km | Large scale |
| 12 | ~160 km | Coarsest; default partition level |

Access exact resolution values programmatically:
```python
from gedih3.egi import RESOLUTIONS, get_resolution

print(RESOLUTIONS)          # {1: 1.000895, 2: 5.004478, ..., 12: 160143.204...}
print(get_resolution(6))    # 1000.895023...
```

---

## Hash Encoding

Each EGI pixel is encoded as a `uint64` value with the following structure:

```
hash = level * 1e18 + px_outer * 1e15 + py_outer * 1e12 + px_inner * 1e6 + py_inner
```

| Component | Bits used | Range | Description |
|-----------|-----------|-------|-------------|
| `level` | digits 19–20 | 1–12 | Resolution level |
| `px_outer` | digits 16–18 | 0–215 | Outer tile X index |
| `py_outer` | digits 13–15 | 0–90 | Outer tile Y index |
| `px_inner` | digits 7–12 | varies | Inner pixel X within tile |
| `py_inner` | digits 1–6 | varies | Inner pixel Y within tile |

**Important**: Always use `np.uint64` when storing or manipulating EGI hashes. Python `int` and `np.int64` cannot represent the full range.

```python
from gedih3.egi import to_hash, from_hash, get_level, validate_hash

# Encode EPSG:6933 coordinates to EGI hash
h = to_hash(x=500000.0, y=-1000000.0, level=6)   # np.uint64

# Decode hash back to coordinates
x, y, level = from_hash(h)

# Extract level from hash
level = get_level(h)   # 6

# Validate a hash value
is_valid = validate_hash(h)
```

---

## Module Structure

| File | Purpose |
|------|---------|
| `config.py` | Constants (`RESOLUTIONS`, `LIMITS`, `EGI_CRS`), `egi_col_name()`, `validate_level()` |
| `core.py` | Hash encoding (`to_hash`, `from_hash`, `hasher`), hierarchy (`to_parent`, `get_children`) |
| `spatial.py` | Geometry (`pixel_shape`, `pixel_coordinate`, `aoi_tiles`, `egi_h3_intersection`) |
| `dataframe.py` | DataFrame operations (`egi_dataframe`, `egi_to_parent`, `egi_aggregate`) |
| `raster.py` | Rasterization (`geodf_to_raster`, `rasterize_partition`, `export_raster`) |

---

## Quick Start

### 1. Index a DataFrame

```python
import gedih3.egi as egi

# Add EGI index column to a GeoDataFrame with Point geometry (EPSG:4326)
gdf = egi.egi_dataframe(shots_gdf, level=6)
# Adds column 'egi06' (np.uint64 hash)

# Coarsen to parent level
gdf_coarse = egi.egi_to_parent(gdf, parent_level=8)
# Adds column 'egi08'
```

### 2. Aggregate spatially

```python
# Compute mean of all numeric columns per EGI pixel
agg_gdf = egi.egi_aggregate(gdf, mapper='mean')
# Returns GeoDataFrame with one row per unique EGI pixel, geometry = pixel polygon
```

### 3. Rasterize

```python
# Rasterize aggregated data to xarray DataArray (EPSG:6933)
xr_data = egi.geodf_to_raster(agg_gdf, columns=['agbd_mean'])

# Export to GeoTIFF
xr_data.rio.to_raster("output.tif")

# Or use the export helper
egi.export_raster(xr_data, "output.tif")
```

### 4. Pixel geometry

```python
# Get polygon for a specific EGI pixel
poly = egi.pixel_shape(h)        # Polygon in EPSG:6933

# Get center coordinate
x, y = egi.pixel_coordinate(h)   # EPSG:6933 meters

# Get all EGI tiles covering a region
tiles = egi.aoi_tiles(region_gdf)  # GeoDataFrame of EGI tiles
```

### 5. Hash hierarchy

```python
from gedih3.egi import to_parent, get_children, pixels_per_tile

# Move to coarser level
parent_hash = to_parent(h, parent_level=8)

# Get child hashes at finer level
children = get_children(h, children_level=7)

# Count pixels in a tile
n = pixels_per_tile(h)   # How many level-N pixels fit in this tile
```

---

## Integration with gedih3

### CLI — Extract with EGI indexing

```bash
# Extract shots indexed at EGI level 1 (~1m), partitioned by level 12 (~160km)
gh3_extract -d /path/to/database -r region.shp -l4a agbd -egi 1 -o output/

# Explicit index:partition syntax
gh3_extract -d /path/to/database -l4a agbd -egi 1:12 -o output/
```

### CLI — Aggregate to EGI level

```bash
# Aggregate to ~1km resolution EGI pixels
gh3_aggregate -d /path/to/database -egi 6 -a mean -o aggregated/

# With explicit partition level and rasterization
gh3_aggregate -d /path/to/database -egi 6:10 -a mean -R -o aggregated/
```

### Python API — Direct H3→EGI loading (no shuffle)

```python
import gedih3.gh3driver as gh3

# Load H3 database directly into EGI-partitioned Dask DataFrame
# Internally uses egi_h3_intersection() + bbox-filtered parquet reads
ddf = gh3.egi_load(
    source='/path/to/h3_database',
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',
    index_level=1,       # EGI index resolution
    partition_level=12,  # EGI tile partition size (default)
)

# Aggregate loaded data to coarser EGI level
agg_df = gh3.egi_aggregate(ddf, target_level=6, agg='mean')

# Export as simplified dataset
gh3.gh3_export(agg_df, output='aggregated/', fmt='parquet')
```

---

## CRS Notes

- **Input coordinates**: `egi_dataframe()` accepts input in any CRS via GeoDataFrame `geometry` column; internally converts to EPSG:6933
- **Hash encoding**: Always EPSG:6933 projected coordinates (meters)
- **Output GeoDataFrames**: Geometry column uses EPSG:4326 by default (re-projected for compatibility)
- **Raster output**: Native EPSG:6933 — do not reproject before writing to maintain L4B alignment
- **Coordinate limits**: EASE-Grid 2.0 extends to ±17,367,530 m East-West and ±7,314,540 m North-South

```python
from gedih3.egi import LIMITS, EGI_CRS_STRING

print(EGI_CRS_STRING)   # "EPSG:6933"
print(LIMITS)           # {'lat_s': ..., 'lat_n': ..., 'lon_w': ..., 'lon_e': ...}
```
