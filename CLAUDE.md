# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
# Create conda environment
conda env create -f environment.yml
conda activate gedih3

# Install package in editable mode
pip install -e .
```

Configuration paths can be set via `~/.gedih3.env` or environment variables:
- `GH3_DEFAULT_DOWNLOAD_DIR` - Base directory for all data
- `GH3_DEFAULT_TMP_DIR` - Temporary files
- `GH3_DEFAULT_SOC_DIR` - Downloaded GEDI HDF5 files (SOC format)
- `GH3_DEFAULT_H3_DIR` - H3-indexed parquet database

## CLI Tools

Eight command-line tools are installed as entry points:

### Core Workflow Tools

```bash
# Download GEDI data from NASA DAAC
gh3_download -r "W,S,E,N" -l2a default -l4a default -N 8

# Build H3 database from downloaded HDF5 files
gh3_build -r "W,S,E,N" -l2a default -l4a default -h3r 12 -h3p 3

# Extract data from H3 database with filters
gh3_extract -d /path/to/database -r region.shp -l2a rh -l4a agbd -q -o output/

# Aggregate H3 database data (supports EGI with -egi flag)
gh3_aggregate -d /path/to/database -o output/
gh3_aggregate -d /path/to/database -egi 6 -a mean -o output/  # EGI aggregation

# Rasterize H3/EGI data to GeoTIFF
gh3_rasterize -d /path/to/database -o output.tif -m --compress LZW
gh3_rasterize -d /path/to/database -egi 6 -o output/ -l agbd_l4a
gh3_rasterize -d /path/to/database -o output/ -t0 2020-01-01 -t1 2023-01-01 -ti 1 -tu years
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

### Common CLI Flags

| Flag | Description |
|------|-------------|
| `-r, --region` | Spatial filter: vector file, bbox as "W,S,E,N", or ISO3 country code |
| `-d0, -d1` | Temporal filters (YYYY-MM-DD) |
| `-l1b, -l2a, -l2b, -l4a, -l4c` | Product variables (use `default`, `minimal`, or list) |
| `-N, -T, -M, -P` | Dask: workers, threads, memory per worker, dashboard port |
| `-s, --dask-scheduler` | Connect to existing Dask scheduler |
| `-v, -vv` | Verbosity levels (INFO, DEBUG) |
| `-Q, --quiet` | Suppress output except errors |
| `-egi LEVEL` | Use EGI indexing instead of H3 (levels 1-12) |

## Architecture

### Data Flow
1. **Download**: `daac.py` → `earthaccess` → GEDI HDF5 files in SOC directory structure (`year/doy/`)
2. **Build**: `gh3builder.py` reads HDF5 → H3 indexes → partitions by H3 cell → parquet files with metadata JSON
3. **Query**: `gh3driver.py` loads partitioned parquet via Dask with spatial/temporal filtering
4. **Extract/Aggregate**: Filter and aggregate data → simplified flat parquet files for external use
5. **Rasterize**: Convert to GeoTIFF with time-series support

### Output Formats

The package distinguishes between two types of output formats:

#### H3 Database (Internal Format)
Created by `gh3_build`, this is a complex hive-partitioned structure optimized for repeated queries:
```
h3_database/
├── h3_03=abc123/
│   └── data.parquet
├── h3_03=def456/
│   └── data.parquet
└── gedih3_build_log.json
```

#### Simplified Dataset (User-Friendly Format)
Created by `gh3_extract` and `gh3_aggregate`, these are flat parquet/geoparquet files designed for use with external tools (R, QGIS, custom Python, etc.):
```
output/
├── abc123.parquet
├── def456.parquet
├── ghi789.parquet
└── gedih3_dataset.json
```

Benefits of simplified format:
- **Easy to use**: Simple flat files, no hive directory structure
- **Portable**: Works with any tool that reads parquet (R, Python, QGIS, etc.)
- **Named by partition**: Files named by H3/EGI partition ID for easy identification
- **Single metadata file**: One `gedih3_dataset.json` describes the whole dataset
- **Chainable**: Can be used as input for other gedih3 tools (e.g., `gh3_rasterize`)

### Module Structure

```
src/gedih3/
├── __init__.py           # Package metadata
├── config.py             # GEDI product definitions, default paths
├── daac.py               # NASA Earthdata access with retry logic
├── gedidriver.py         # HDF5 reading, GEDIFile/GEDIShot classes
├── gh3builder.py         # H3 database building
├── gh3driver.py          # H3 database queries, EGI/raster integration
├── h3utils.py            # H3 cell operations
├── cliutils.py           # CLI shared utilities (args, logging, data loading)
├── utils.py              # File I/O, transaction safety utilities
├── exceptions.py         # Structured exception hierarchy
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
    ├── gh3_list_variables.py
    ├── gh3_list_resolutions.py
    └── gh3_read_schema.py
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
└── GediProcessingError
    ├── GediAggregationError
    └── GediRasterizationError
```

### GEDI Products Supported

| Product | Description |
|---------|-------------|
| L1B | Geolocated waveforms |
| L2A | Elevation and height metrics (RH percentiles) |
| L2B | Canopy cover and vertical profiles |
| L4A | Footprint-level aboveground biomass (AGBD) |
| L4C | Footprint-level structural complexity (WSCI) |

## Python API Examples

### Basic Data Access

```python
import gedih3.gh3driver as gh3

# Load H3-indexed data with spatial filter
ddf = gh3.gh3_load(
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',  # or bbox or ISO3
    query='quality_flag_l2a == 1',
    gh3_dir='/path/to/database'
)

# Aggregate to coarser H3 level
agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')
```

### Loading Simplified Datasets

```python
import gedih3.gh3driver as gh3

# Load simplified dataset created by gh3_extract or gh3_aggregate
gdf = gh3.gh3_load_dataset('/path/to/extracted/')

# Load specific columns only
gdf = gh3.gh3_load_dataset('/path/to/aggregated/', columns=['agbd_l4a_mean', 'geometry'])

# Load lazily as Dask DataFrame for large datasets
ddf = gh3.gh3_load_dataset_lazy('/path/to/dataset/')
```

### EGI (EASE Grid Index)

```python
import gedih3.egi as egi

# Add EGI index to GEDI shots
egi_df = egi.egi_dataframe(shots_df, level=6)  # ~1km resolution

# Aggregate to coarser level
agg_df = egi.egi_aggregate(egi_df, mapper='mean')

# Rasterize for GIS output
raster = egi.geodf_to_raster(agg_df, columns=['agbd_mean'])
raster.rio.to_raster("output.tif")
```

### Rasterization

```python
from gedih3 import raster

# H3 to raster conversion
xras = raster.h3_to_raster(h3_gdf, columns=['agbd_mean'])
raster.export_raster(xras, "output.tif", compress='LZW')

# Time-series rasterization
for t0, t1, suffix in raster.generate_time_windows('2020-01-01', '2023-01-01', 1, 'years'):
    time_data = gdf[(gdf['datetime'] >= t0) & (gdf['datetime'] < t1)]
    xras = raster.h3_to_raster(time_data)
    raster.export_raster(xras, f"output_{suffix}.tif")

# High-level time-series rasterizer
ts = raster.TimeSeriesRasterizer(gdf, time_col='datetime', target_level=6)
for xras, suffix in ts.generate('2020-01-01', '2023-01-01', 1, 'years'):
    xras.rio.to_raster(f"output_{suffix}.tif")
```

### Download with Retry

```python
from gedih3.daac import gedi_download

# Download with automatic retry on failures
paths = gedi_download(
    product_vars={'L2A': ['default'], 'L4A': ['agbd']},
    odir='/path/to/output',
    spatial=[-50, 0, -49, 1],
    temporal=('2020-01-01', '2020-12-31'),
    resume=True,
    max_attempts=3  # retry up to 3 times
)
```

### Validation

```python
from gedih3.validation import validate_h3_params, validate_egi_level

# Validate H3 parameters (raises H3ValidationError if invalid)
res, part = validate_h3_params(res=12, part=3)

# Validate EGI level (raises EGIValidationError if invalid)
level = validate_egi_level(6)
```

## Testing

```bash
# Run test script
python tests/run_tests.py
```

## Key Patterns

- **Dask everywhere**: All heavy operations use Dask DataFrames/Bags for distributed processing
- **H3 partitioning**: Data partitioned by H3 cells (configurable via `-h3p` for partition, `-h3r` for index; levels stored in metadata)
- **EGI alignment**: Square pixels aligned to EASE-Grid 2.0 (EPSG:6933) for L4B compatibility
- **Parquet + JSON metadata**: Each H3 partition has a `.parquet` file and `.metadata.json` sidecar
- **Variable expansion**: CLI accepts `default`, `minimal`, `*`, or explicit variable lists/files
- **Spatial filtering**: Supports vector files, bounding boxes, or ISO3 country codes
- **Retry logic**: Network operations use exponential backoff (3 attempts, 1-60s wait)
- **Atomic writes**: File operations use `AtomicFileWriter` for transaction safety
- **Structured exceptions**: Catch specific `GediError` subclasses for targeted error handling
- **DRY CLI utilities**: Shared argument builders and setup functions in `cliutils.py`

## CLI Shared Utilities

The `cliutils.py` module provides shared utilities for CLI tools to avoid code duplication:

### Argument Builders

```python
from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args

# Add standard Dask arguments (-N, -T, -M, -P, -s, --dask-config)
add_dask_args(parser)

# Add verbosity arguments (-v, -vv, -Q)
add_verbosity_args(parser)

# Add GEDI product arguments (-l1b, -l2a, -l2b, -l4a, -l4c)
add_product_args(parser)
```

### Setup Functions

```python
from gedih3.cliutils import setup_logging, print_banner, print_success, configure_database_path

# Configure logging based on args and get logger
logger = setup_logging(args, __name__)

# Print tool banner
print_banner("GEDI Tool Name", logger=logger)

# Print success message with banner formatting
print_success("Operation complete", logger=logger)

# Configure and validate database path
configure_database_path(args, logger=logger)
```

### Data Loading

```python
from gedih3.cliutils import load_data_from_source, get_numeric_columns, h3_col_name

# Auto-detect and load from H3 database, simplified dataset, or parquet directory
ddf = load_data_from_source(database_path, columns, region, query, logger)

# Get numeric columns for aggregation
numeric_cols = get_numeric_columns(ddf)

# Get H3 column name for a level
col = h3_col_name(6)  # Returns 'h3_06'
```

## Performance Optimizations

### Efficient Data Loading (`from_map=True`)

The `gh3_load()` function uses `from_map=True` by default, which provides significant performance benefits for large databases with thousands of partition files:

- **Bypasses `_metadata` file**: No need to read/write large metadata files
- **Direct partition loading**: Uses `dask.dataframe.from_map()` to load each H3 partition directory directly
- **Explicit divisions**: Sets divisions from partition IDs, enabling efficient data access
- **Memory efficient**: Avoids loading partition metadata into memory

```python
# Default behavior (efficient for large databases)
ddf = gh3.gh3_load(columns=['agbd_l4a'], gh3_dir='/path/to/database')

# Legacy behavior (if needed for backwards compatibility)
ddf = gh3.gh3_load(columns=['agbd_l4a'], gh3_dir='/path/to/database', from_map=False)
```

### Aggregation with `map_partitions`

Aggregation functions (`gh3_aggregate`, `egi_aggregate`) use `map_partitions` instead of `groupby().apply()` for better performance:

- **No shuffling**: Each partition is processed independently without data movement
- **Partition-aligned**: When loaded with `from_map=True`, each partition contains data from a single H3 cell
- **Efficient memory usage**: Processes one partition at a time instead of grouping across partitions

This optimization is transparent to the API - the same function calls work as before, but with better performance.

### EGI Coordinate Handling

EGI functions prioritize using Point geometry from GeoDataFrames over coordinate columns:

1. **Primary method**: Extract coordinates from `geometry` column (Point geometries)
2. **Fallback**: Search for coordinate columns with product suffixes (e.g., `lon_lowestmode_l2a`)

This handles the case where H3 database columns have product suffixes automatically.

## EGI Resolution Levels

| Level | Resolution | Description |
|-------|------------|-------------|
| 1 | ~160 km | Continental scale |
| 2 | ~80 km | Regional scale |
| 3 | ~40 km | Sub-regional |
| 4 | ~20 km | Large area |
| 5 | ~10 km | Medium area |
| 6 | ~5 km | GEDI L4B native |
| 7 | ~2.5 km | High resolution |
| 8 | ~1.25 km | Very high resolution |
| 9 | ~625 m | Ultra high resolution |
| 10 | ~312 m | Fine scale |
| 11 | ~156 m | Very fine scale |
| 12 | ~78 m | Maximum resolution |

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
