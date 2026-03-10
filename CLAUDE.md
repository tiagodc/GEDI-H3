# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Setup
- use the /gpfs/data1/vclgp/decontot/environments/gh3_dev conda environment when present
- this project is a refactoring of /gpfs/data1/vclgp/decontot/repos/gedi_tools with extended and enhanced functionality and revisited software design and architecture

## AI Agent Operating Principles (Non-Negotiable)

- **Correctness over cleverness**: Prefer boring, readable solutions that are easy to maintain.
- **Smallest change that works**: Minimize blast radius; don't refactor adjacent code unless it meaningfully reduces risk or complexity.
- **Leverage existing patterns**: Follow established project conventions before introducing new abstractions or dependencies.
- **Prove it works**: "Seems right" is not done. Validate with tests/build/lint and/or a reliable manual repro.
- **Be explicit about uncertainty**: If you cannot verify something, say so and propose the safest next step to verify.
- **DRY and reuse**: Always check for existing utilities before writing new code. Reuse functions across modules.

---

## Project Overview

**gedih3** is a Python library for accessing NASA's GEDI (Global Ecosystem Dynamics Investigation) satellite LiDAR data with H3 and EGI spatial indexing. It handles downloading GEDI products from NASA's DAACs, building spatially-indexed parquet databases for efficient queries, extracting/aggregating data, and producing raster outputs.

### Key Features
- **H3 Hexagonal Indexing**: Uber's H3 system for efficient spatial queries
- **EGI Square Pixel Indexing**: EASE Grid Index (EPSG:6933) for GEDI L4B compatibility
- **Rasterization**: H3/EGI to GeoTIFF conversion with time-series support
- **Dask Integration**: Distributed processing for large datasets
- **NASA Earthdata Access**: Direct download or S3 streaming via earthaccess

## Development Setup

```bash
conda env create -f environment.yml
conda activate gedih3
pip install -e .
```

Configuration via `~/.gedih3.env` or environment variables:
- `GH3_DEFAULT_DOWNLOAD_DIR` - Base directory for all data
- `GH3_DEFAULT_TMP_DIR` - Temporary files
- `GH3_DEFAULT_SOC_DIR` - Downloaded GEDI HDF5 files (SOC format)
- `GH3_DEFAULT_H3_DIR` - H3-indexed parquet database

Configuration priority: CLI args > env vars > `~/.gedih3.env` > `config.py` defaults.

## CLI Tools

Twelve command-line tools are installed as entry points (+ `gh3_build_ducklake` experimental):

### Core Workflow Tools

```bash
# Download GEDI data from NASA DAAC
gh3_download -r "W,S,E,N" -l2a default -l4a default -N 8

# Build H3 database from downloaded HDF5 files
gh3_build -r "W,S,E,N" -l2a default -l4a default -h3r 12 -h3p 3

# Extract data from H3 database with filters
gh3_extract -d /path/to/database -r region.shp -l2a rh -l4a agbd -q -o output/

# Extract with EGI indexing (index at level 1 ~1m, partition by level 12 ~160km)
gh3_extract -d /path/to/database -r region.shp -l4a agbd -egi 1 -o output/
gh3_extract -d /path/to/database -r region.shp -l4a agbd -egi 1:12 -o output/  # Explicit index:partition

# Aggregate H3 database data (supports EGI with -egi flag)
gh3_aggregate -d /path/to/database -h3 6 -o output/  # H3 aggregation
gh3_aggregate -d /path/to/database -egi 6 -a mean -o output/  # EGI aggregation (partition at level 12)
gh3_aggregate -d /path/to/database -egi 6:10 -a mean -o output/  # Explicit aggregation:partition levels
gh3_aggregate -d /path/to/database -egi 6 -a mean -R -o output/  # With rasterization

# Rasterize pre-aggregated datasets to GeoTIFF
# NOTE: gh3_rasterize requires a dataset from gh3_aggregate or gh3_extract
gh3_rasterize -d /path/to/aggregated_dataset -o output/ --compress LZW  # Tiled output
gh3_rasterize -d /path/to/aggregated_dataset -m -o output.tif  # Merged output
gh3_rasterize -d /path/to/aggregated_dataset -l agbd_l4a -o output/  # Select variables
```

### Utility Tools

```bash
# List available GEDI variables
gh3_list_variables -l2a -l4a
gh3_list_variables -g "agbd"  # grep filter

# Display H3/EGI resolution levels
gh3_list_resolutions
gh3_list_resolutions -egi  # EGI levels

# Inspect file schemas
gh3_read_schema /path/to/file.parquet
gh3_read_schema /path/to/file.h5
```

### Ancillary Data Tools

```bash
# Sample raster pixel values at GEDI shot locations
gh3_from_img -i /path/to/dem.tif -d /path/to/database -r region.shp -o output/

# Sample tile directory with band selection and window operations
gh3_from_img -i /path/to/tiles/ -if tif -B 0 2 -w 131 -d /path/to/database -o output/

# Join polygon attributes to GEDI shots
gh3_from_polygon -i ecoregions.shp -c ECO_NAME BIOME_NAME -d /path/to/database -o output/

# Join with column prefix and inner join (drop unmatched shots)
gh3_from_polygon -i landcover.gpkg -x lc_ --dropna -d /path/to/database -o output/
```

### Common CLI Flags

| Flag | Description |
|------|-------------|
| `-r, --region` | Spatial filter: vector file, bbox as "W,S,E,N", or ISO3 country code |
| `-t0, -t1` | Temporal filters (YYYY-MM-DD) |
| `-l1b, -l2a, -l2b, -l4a, -l4c` | Product variables (use `default`, `minimal`, or list) |
| `-N, -T, -M, -P` | Dask: workers, threads, memory per worker, dashboard port |
| `-s, --dask-scheduler` | Connect to existing Dask scheduler |
| `-v, -vv` | Verbosity levels (INFO, DEBUG) |
| `-Q, --quiet` | Suppress output except errors |
| `-egi INDEX[:PART]` | Use EGI indexing (e.g., `-egi 1` or `-egi 1:12` for index:partition levels) |
| `-R, --rasterize` | Also export rasters after aggregation (gh3_aggregate only) |
| `-i, --image` / `-i, --input` | Ancillary data source: raster file/dir or vector file |
| `-w, --window` | Window operations for raster sampling (3-digit BZO format) |
| `-B, --bands` | Select specific raster bands by 0-based index |
| `-x, --prefix` | Column name prefix for polygon attributes (avoids conflicts) |
| `-c, --columns` | Polygon attribute columns to include |
| `-p, --predicate` | Spatial join predicate: `within` or `intersects` |

## Architecture

### Data Flow
1. **Download**: `daac.py` → `earthaccess` → GEDI HDF5 files in SOC directory structure (`year/doy/`)
2. **Build**: `gh3builder.py` reads HDF5 → H3 indexes → partitions by H3 cell → parquet files with metadata JSON
3. **Query**: `gh3driver.py` loads partitioned parquet via Dask with spatial/temporal filtering
4. **Extract/Aggregate**: Filter and aggregate data → simplified flat parquet files for external use
5. **Rasterize**: Convert to GeoTIFF with time-series support

### Output Formats

**H3 Database (internal)** — created by `gh3_build`, hive-partitioned structure:
```
h3_database/
├── h3_03=abc123/data.parquet
└── gedih3_build_log.json
```

**Simplified Dataset (user-friendly)** — created by `gh3_extract` and `gh3_aggregate`, flat parquet files:
```
output/
├── abc123.parquet
└── gedih3_dataset.json
```
Simplified datasets work with any parquet reader (R, QGIS, Python) and can be chained as input to other tools.

### Module Structure

```
src/gedih3/
├── config.py             # GEDI product definitions, default paths
├── daac.py               # NASA Earthdata access with retry logic
├── gedidriver.py         # HDF5 reading, GEDIFile/GEDIShot classes
├── gh3builder.py         # H3 database building
├── gh3driver.py          # H3 database queries, EGI/raster integration
├── h3utils.py            # H3 cell operations
├── imgutils.py           # Raster image sampling at shot locations
├── vecutils.py           # Vector polygon spatial join
├── sqlutils.py           # DuckDB utilities (experimental)
├── cliutils.py           # CLI shared utilities (args, logging, data loading)
├── utils.py              # File I/O, transaction safety utilities
├── exceptions.py         # Structured exception hierarchy (26 types)
├── validation.py         # Parameter validation functions
├── logging_config.py     # Logging configuration
├── logger.py             # Build/download loggers
├── egi/                  # EGI (EASE Grid Index) module
│   ├── config.py         # EGI constants, resolution table
│   ├── core.py           # Hash encoding/decoding
│   ├── spatial.py        # Geometry operations
│   ├── dataframe.py      # DataFrame operations
│   └── raster.py         # EGI rasterization
├── raster/               # Rasterization module
│   ├── config.py         # GeoTIFF defaults
│   ├── h3_raster.py      # H3 to raster conversion
│   ├── timeseries.py     # Time-series generation
│   └── export.py         # Batch export utilities
└── cli/                  # CLI entry points
    ├── gh3_build.py
    ├── gh3_download.py
    ├── gh3_extract.py
    ├── gh3_aggregate.py
    ├── gh3_rasterize.py
    ├── gh3_update.py
    ├── gh3_from_img.py
    ├── gh3_from_polygon.py
    ├── gh3_list_variables.py
    ├── gh3_list_resolutions.py
    ├── gh3_read_schema.py
    └── gh3_build_ducklake.py  # experimental
```

### Key Classes

| Class | Module | Purpose |
|-------|--------|---------|
| `GEDIFile` | gedidriver.py | Parses GEDI filename (orbit, granule, track, version) |
| `GEDIShot` | gedidriver.py | Decodes shot_number to extract beam, orbit, track |
| `GEDIAccessor` | daac.py | NASA Earthdata authentication and data search |
| `TimeSeriesRasterizer` | raster/timeseries.py | Time-series raster generation |
| `AtomicFileWriter` | utils.py | Atomic file writes with rollback |
| `H3BuildLogger` | logger.py | Tracks build progress and resume state |

### Exception Hierarchy

```
GediError (base)
├── GediNetworkError
│   ├── GediDownloadError
│   ├── GediAuthenticationError
│   └── GediS3AccessError
├── GediValidationError
│   ├── H3ValidationError
│   ├── EGIValidationError
│   ├── GediProductError
│   └── GediVariableError
├── GediFileError
│   ├── GediHDF5Error
│   ├── GediParquetError
│   ├── GediCorruptedFileError
│   └── GediTransactionError
├── GediDatabaseError
│   ├── GediDatabaseNotFoundError
│   ├── GediDatabaseCorruptedError
│   └── GediMergeError
├── GediSpatialError
├── GediTemporalError
└── GediProcessingError
    ├── GediAggregationError
    ├── GediRasterizationError
    ├── GediImageSamplingError
    └── GediSpatialJoinError
```

### GEDI Products Supported

| Product | Description |
|---------|-------------|
| L1B | Geolocated waveforms |
| L2A | Elevation and height metrics (RH percentiles) |
| L2B | Canopy cover and vertical profiles |
| L4A | Footprint-level aboveground biomass (AGBD) |
| L4C | Footprint-level structural complexity (WSCI) |

## Key Patterns

- **Dask everywhere**: All heavy operations use Dask DataFrames/Bags for distributed processing
- **H3 partitioning**: Data partitioned by H3 cells (configurable via `-h3p` for partition, `-h3r` for index; levels stored in metadata)
- **EGI alignment**: Square pixels aligned to EASE-Grid 2.0 (EPSG:6933) for L4B compatibility
- **Direct EGI loading (no shuffle)**: `egi_load()` pre-computes EGI↔H3 intersection and reads tiles directly — no `set_index()` shuffle needed
- **Parquet + JSON metadata**: Each H3 partition has a `.parquet` file; database root has `gedih3_build_log.json`
- **Unified `source=` API**: `gh3_load()` and `egi_load()` accept `source=` as the primary path parameter
- **Variable expansion**: CLI accepts `default`, `minimal`, `*`, or explicit variable lists/files
- **Spatial filtering**: Supports vector files, bounding boxes, or ISO3 country codes
- **S3 ETL mode**: `gh3_build --s3` / `gh3_download --s3` stream from NASA S3 without persistent local download
- **Retry logic**: Network operations use exponential backoff (3 attempts, 1-60s wait)
- **Atomic writes**: File operations use `AtomicFileWriter` for transaction safety
- **Structured exceptions**: Catch specific `GediError` subclasses for targeted error handling
- **DRY CLI utilities**: Shared argument builders and setup functions in `cliutils.py`
- **Ancillary data fusion**: External raster sampling (`imgutils.py`) and vector spatial join (`vecutils.py`) at shot level with worker-level caching

## CLI Shared Utilities (`cliutils.py`)

Before writing new CLI code, check `cliutils.py` for existing builders and helpers:

```python
from gedih3.cliutils import (
    add_dask_args, add_verbosity_args, add_product_args, add_storage_args,  # arg builders
    setup_logging, print_banner, print_success, configure_database_path,     # setup
    cli_exception_handler,                                                    # error handling
    parse_egi_levels,                                                         # -egi 6 → (6, 12)
    load_data_from_source, get_numeric_columns, h3_col_name,                 # data loading
    is_internal_column, filter_data_columns, get_rasterizable_columns,       # column filtering
)
```

**Internal column patterns** (auto-filtered): `h3_XX`, `egiXX`, `_egi_x`, `_egi_y`, `shot_number*`.

**Dask warning suppression**: `setup_logging()` suppresses noisy Dask warnings at INFO/ERROR levels.

## Performance Optimizations

- **`from_map=True`** (default in `gh3_load()`): bypasses `_metadata` file, loads partitions directly via `dask.dataframe.from_map()` — critical for databases with thousands of partitions.
- **`map_partitions` aggregation**: `gh3_aggregate` / `egi_aggregate` avoid shuffling by processing each partition independently.
- **EGI coordinate priority**: uses `geometry` column (Point) first, falls back to product-suffixed coordinate columns (e.g., `lon_lowestmode_l2a`).

## EGI Resolution Levels

| Level | Pixel Size | Description |
|-------|------------|-------------|
| 1 | ~1 m | Finest resolution |
| 2 | ~5 m | |
| 3 | ~25 m | GEDI footprint |
| 4 | ~100 m | NISAR compatible |
| 5 | ~200 m | BIOMASS compatible |
| 6 | ~1 km | GEDI L4B baseline |
| 7 | ~2 km | GEDI threshold |
| 8 | ~10 km | GEDI wall-to-wall |
| 9 | ~20 km | |
| 10 | ~40 km | |
| 11 | ~80 km | |
| 12 | ~160 km | Partition level (coarsest) |

## H3 Resolution Levels

| Level | Avg. Hex Area | Description |
|-------|---------------|-------------|
| 0 | 4,250,547 km² | Global |
| 3 | 12,393 km² | Typical partition level |
| 6 | 36.13 km² | Regional analysis |
| 9 | 0.105 km² | Local analysis |
| 12 | 307 m² | Typical index level |
| 15 | 0.90 m² | Maximum resolution |

## Dependencies

Key dependencies (see `pyproject.toml` for full list):
- `earthaccess >= 0.14.0` - NASA Earthdata access
- `h3 >= 4.3.0`, `h3pandas >= 0.3.0` - H3 indexing
- `dask >= 2025.5.1`, `dask-geopandas >= 0.5.0` - Distributed processing
- `geopandas >= 1.1.1`, `shapely >= 2.0.0` - Geospatial operations
- `pyarrow >= 20.0.0` - Parquet I/O
- `h5py >= 3.14.0` - HDF5 reading
- `rioxarray >= 0.19.0`, `geocube >= 0.7.1` - Rasterization
- `tenacity >= 8.2.0` - Retry logic

Python `>=3.12` required.

## Testing

```bash
# Unit tests (fast, no network required)
pytest tests/ -m "not integration and not slow"

# Integration tests (requires NASA Earthdata credentials)
pytest tests/ -m integration

# Full test suite
pytest tests/ -v
```

## Known Limitations

- **Memory**: PyArrow unmanaged memory accumulates during large Parquet operations. Use `--dask-config dask-config-aggressive-memory.yaml` for production workloads.
- **Coordinate systems**: H3 uses EPSG:4326; EGI uses EPSG:6933. Output GeoDataFrames are in EPSG:4326 unless noted. EGI rasters stay in EPSG:6933.
- **Large datasets**: Use `from_map=True` (default) and `map_partitions` aggregation (default). Process by time period or region to manage memory.

## Claude Code Sub-Agents

Four specialized sub-agents in `.claude/agents/`:

| Agent | Use For |
|-------|---------|
| `core-pipeline` | Download, build, extract, aggregate workflows |
| `raster-egi` | Spatial indexing, rasterization, CRS transforms |
| `cli-deployment` | CLI tools, configuration, deployment |
| `testing-qa` | Test coverage, validation, benchmarking, **DRY/redundancy audits** |

Use `testing-qa` to audit for duplicate code before adding new utilities. Check `cliutils.py`, `utils.py`, `validation.py` first.
