# gedih3

A Python library for automated downloading and processing of NASA's Global Ecosystem Dynamics Investigation (GEDI) LiDAR data, focused on spatially indexed data through the H3 hexagonal system and highly efficient tools for distributed processing of GEDI footprint data at any scale.

GEDI produces billions of LiDAR footprints distributed across thousands of HDF5 granules. **gedih3** transforms this data into a spatially-indexed GeoParquet database that enables fast spatial and temporal queries, multi-resolution aggregation, raster export, and integration with external datasets, all from the command line or Python API.

---

## Why gedih3?

Working with GEDI data at scale is hard: granules are organized by orbit (not geography), files are large HDF5 containers requiring specialized libraries, and spatial queries over billions of footprints require large computing times even for small regions. Manual workflows can break down quickly.

**gedih3** solves this with:

- **Complete pipeline**: Download from NASA DAAC, build a spatial database, query/aggregate, and export geospatial vector/raster files -- all in one package
- **Spatial indexing**: H3 hexagons (Uber's system) for flexible resolution queries + EGI square pixels (EASE-Grid 2.0) for GEDI L4B compatibility and quick image generation
- **Scale via Dask**: Distributed computing handles billion-row datasets across HPC clusters or in your homelab
- **Interoperable outputs**: Parquet is a highly efficient column oriented file format with ample support in R, QGIS, Python and many other tools. Support to multiple vector/table formats are also available for broad compatibility
- **Ancillary data fusion**: Sample external rasters and join vector polygons at the GEDI footprint level
- **NASA Earthdata integration**: Authentication, search, download, and S3 streaming with automatic retry through the [earthaccess API](https://earthaccess.readthedocs.io/en/stable/)

---

## Key Features

- Generation of analysis ready datasets suitable for users with or without familiarity to GEDI data, providing minimal and complete presets of GEDI data useful for both beginner and advanced users; and customizable data filtering and optional automatic selection of high quality data observations
- Multiple ways to interact with the GEDI-H3 databases: command oine interface (CLI), Python API and DuckDB SQL queries
- H3 hexagonal spatial indexing (levels 0-15) for efficient spatial queries
- Optional EGI square pixel indexing outputs based on the [EASE-Grid 2.0](https://nsidc.org/data/user-resources/help-center/guide-ease-grids) (EPSG:6933) equal-area projection system
- Aggregation of GEDI information at multiple resolutions with customizable functions and time-series support
- Ancillary data tools: raster sampling (`gh3_from_img`) and vector spatial join (`gh3_from_polygon`) to match external data sources to GEDI footprints
- Dask distributed processing for large datasets
- Simplified flat Parquet output format for external tool compatibility and support of several other table and geospatial data formats (feather, geopackage, hdf5, shapefile etc.) 
- NASA Earthdata access with resume/retry logic and on-the-fly file subsetting for efficient network usage and minimal disk allocation  
- Quality filtering, temporal windowing, and support for all GEDI footprint products (L1B, L2A, L2B, L4A, L4C)

---

## Quick Start

Installing through [conda](https://docs.conda.io/projects/conda/en/stable/) is recommended.

```bash
# Install
git clone https://github.com/tiagodc/GEDI-H3.git
cd GEDI-H3

conda env create -f environment.yml
conda activate gedih3

# 1. Build a H3-indexed database with minimal directly from the cloud
  # - l2a: canopy height metrics
  # - l4a: aboveground biomass estimates
gh3_build -r="-51,0,-50,1" -l2a minimal -l4a minimal -s3

# 2. See whcih variables are available in the built database
gh3_list_variables

# 3. Extract filtered data for a spatial subset
gh3_extract -r="-51,0,-50,1" -l agbd_l4a rh_098_l2a --quality -o extracted/

# 4. Aggregate to coarser resolution (~1km)
gh3_list_resolutions
gh3_aggregate -d extracted -h3 8 -a "['mean','std','count']" -l rh_098_l2a agbd_l4a -o aggregated/

# 5. Export as GeoTIFF
gh3_rasterize -d aggregated -o rasterized --compress ZSTD
```

---

## CLI Tools

| Tool | Purpose |
|------|---------|
| `gh3_download` | Download GEDI data from the NASA DAACs |
| `gh3_build` | Build H3-indexed Parquet database from GEDI HDF5 files |
| `gh3_extract` | Extract filtered data with spatial/temporal constraints |
| `gh3_aggregate` | Aggregate to coarser H3/EGI resolution levels |
| `gh3_rasterize` | Convert aggregated/extracted datasets to GeoTIFF |
| `gh3_update` | Add/merge variables to existing datasets |
| `gh3_from_img` | Sample external raster values at GEDI shot locations |
| `gh3_from_polygon` | Spatial join vector polygon attributes to GEDI shots |
| `gh3_list_variables` | List available GEDI variables with grep filtering support |
| `gh3_list_resolutions` | Display H3 and EGI resolution level tables |
| `gh3_read_schema` | Inspect Parquet, GeoPackage, or HDF5 file schemas |

### Common Flags

```
-r, --region       Spatial filter: vector file, bbox "W,S,E,N", or ISO3 country code
-t0, -t1           Temporal filters (YYYY-MM-DD)
-l2a, -l4a, ...    Product variables (use 'default', 'minimal', or explicit list of variables)
-N, -T, -M         Number of Dask workers, threads, memory per worker
-v / -vv / -Q      Verbosity: INFO / DEBUG / quiet
-egi INDEX[:PART]  Use EGI spatial indexing instead of H3 (e.g., -egi 6 or -egi 6:12)
```

---

## Python API

```python
import gedih3.gh3driver as gh3

# Load H3-indexed data with spatial filter
ddf = gh3.gh3_load(
    source='/path/to/database',  # preferred; gh3_dir= is deprecated
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',
    query='quality_flag_l2a == 1',
)

# Aggregate to coarser H3 level
agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')

# Load H3 database directly into EGI partitions (no shuffle)
ddf = gh3.egi_load(source='/path/to/database', columns=['agbd_l4a'], index_level=1)
agg_df = gh3.egi_aggregate(ddf, target_level=6, agg='mean')

# Load simplified dataset (output of gh3_extract or gh3_aggregate)
gdf = gh3.gh3_load_dataset('/path/to/extracted/')

# EGI indexing (from a DataFrame already in memory)
import gedih3.egi as egi
egi_df = egi.egi_dataframe(shots_df, level=6)

# Rasterization
from gedih3 import raster
xras = raster.h3_to_raster(h3_gdf, columns=['agbd_mean'])
raster.export_raster(xras, "output.tif", compress='LZW')

# Ancillary data: sample raster at shot locations
from gedih3.imgutils import from_image
ddf = from_image('/path/to/dem.tif', data_source='/path/to/database', region='region.shp')
```

---

## Architecture

```mermaid
---
config:
  theme: dark
  layout: dagre
---
flowchart TB
    n1["☁️ DAAC"] --> n2["🌐 Network"]
    n2 --> n17["⬇️ gh3_download"]
    n17 --> n4["🪣 SOC"]
    n4 --> C["⚙️ gh3_build"]
    C --> n3["💽 Local H3<br>database"]
    n3 --> D["gh3_extract"] & n10["gh3_list_variables"] & n11["gh3_list_resolutions"]
    D --> SHOTS["📂 Shots dataset"]
    SHOTS --> n6["gh3_aggregate"] & RAST["gh3_rasterize"] & n13["gh3_read_schema"]
    n6 --> AGG["📊 Aggregated dataset"]
    RAST --> TIF["🗺️ GeoTIFF"]
    AGG --> RAST & n13
    SHOTS <--> n12["gh3_update"] & n8["🖼️ gh3_from_img"] & n9["🌐 gh3_from_polygon"]
    n15["External Raster"] -.-> n8
    n16["External Vector"] -.-> n9
    n1@{ shape: db}
    n2@{ shape: com-link}
    n4@{ shape: das}
    n3@{ shape: disk}
    SHOTS@{ shape: das}
    AGG@{ shape: das}
    TIF@{ shape: das}
    n15@{ shape: das}
    n16@{ shape: das}
    n4:::fade
    n15:::fade
    n16:::fade
    classDef fade stroke:#757575,color:#757575
    linkStyle 19 stroke:#757575,fill:none
    linkStyle 20 stroke:#757575,fill:none
```

**Output Formats**:
- **H3 Database**: Hive-partitioned Parquet optimized for repeated queries (`gh3_build`)
- **Simplified Dataset**: Flat Parquet files with `gedih3_dataset.json` metadata, designed for external tools (`gh3_extract`, `gh3_aggregate`, `gh3_from_img`, `gh3_from_polygon`)
- **GeoTIFF**: Raster output with compression, tiling, and BIGTIFF support (`gh3_rasterize`)

---

## GEDI Products Supported

| Product | Description |
|---------|-------------|
| L1B | Geolocated waveforms |
| L2A | Elevation and height metrics (RH percentiles) |
| L2B | Canopy cover and vertical profiles |
| L4A | Footprint-level aboveground biomass (AGBD) |
| L4C | Footprint-level structural complexity (WSCI) |

---

## Spatial Indexing

### H3 (Hexagonal Hierarchical Index)

Uber's H3 system for hexagonal spatial partitioning. Used as the primary index for the database.

| Level | Avg. Hex Area | Typical Use |
|-------|---------------|-------------|
| 0 | 4,250,547 km2 | Global |
| 3 | 12,393 km2 | Partition level |
| 6 | 36.13 km2 | Regional analysis |
| 9 | 0.105 km2 | Local analysis |
| 12 | 307 m2 | Index level (default) |
| 15 | 0.90 m2 | Maximum resolution |

### EGI (EASE Grid Index)

Square pixel indexing on EASE-Grid 2.0 (EPSG:6933) for compatibility with GEDI L4B gridded products.

| Level | Pixel Size | Typical Use |
|-------|------------|-------------|
| 1 | ~1 m | Finest resolution |
| 3 | ~25 m | GEDI footprint |
| 6 | ~1 km | GEDI L4B baseline |
| 8 | ~10 km | Wall-to-wall |
| 12 | ~160 km | Partition level (coarsest) |

> **Note**: Lower EGI level = finer resolution (opposite to H3).

---

## Configuration

Configuration priority (highest to lowest):
1. Command-line arguments
2. Environment variables (`GH3_DEFAULT_*`)
3. `~/.gedih3.env` file
4. Package defaults

```bash
# Environment variables
export GH3_DEFAULT_DOWNLOAD_DIR=/path/to/data
export GH3_DEFAULT_H3_DIR=/path/to/h3_database
export GH3_DEFAULT_SOC_DIR=/path/to/hdf5_files
export GH3_DEFAULT_TMP_DIR=/path/to/tmp
```

---

## Tutorials

See the `tutorials/` directory:
- `tutorial_cli_pipeline.sh` -- End-to-end CLI workflow
- `tutorial_python_api_pipeline.py` -- Python API examples

---

## Requirements

- **Python** >= 3.13 (tested with 3.10+; update `pyproject.toml` for older versions)
- **NASA Earthdata account** (for downloading GEDI data)
- **Key dependencies**: dask, geopandas, h3, pyarrow, h5py, rioxarray, earthaccess

See `pyproject.toml` for the full dependency list.

---

## License

TBD
