## Setup
- use the gh3_dev conda environment when present

## AI Agent Operating Principles (Non-Negotiable)

- **Correctness over cleverness**: Prefer boring, readable solutions that are easy to maintain.
- **Smallest change that works**: Minimize blast radius; don't refactor adjacent code unless it meaningfully reduces risk or complexity.
- **Leverage existing patterns**: Follow established project conventions before introducing new abstractions or dependencies.
- **Prove it works**: "Seems right" is not done. Validate with tests/build/lint and/or a reliable manual repro.
- **Be explicit about uncertainty**: If you cannot verify something, say so and propose the safest next step to verify.
- **DRY and reuse**: Always check for existing utilities before writing new code. Reuse functions across modules.

---

## Project Overview

**gedih3** is a Python library for accessing NASA's GEDI (Global Ecosystem Dynamics Investigation) satellite LiDAR data with H3 and EGI spatial indexing. It handles downloading GEDI products from NASA's DAACs, building spatially-indexed parquet databases for efficient queries, extracting/aggregating data, and producing raster outputs. It's core function relies on generating analysis rady data with minimal user expertise required on both GEDI data and programming skills. 

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

Command-line tools are installed as entry points under src/gedih3/cli.

### Core Workflow Tools

```bash
# Download GEDI data from NASA DAAC
gh3_download -r="W,S,E,N" -l2a default -l4a default -N 8

# Build H3 database from downloaded HDF5 files
gh3_build -r="W,S,E,N" -l2a default -l4a default -h3r 12 -h3p 3

# Extract data from H3 database with filters
gh3_extract -r region.shp -l2a rh_098 -l4a agbd -o output/

# Extract with EGI indexing (index at level 1 ~1m, partition by level 12 ~160km)
gh3_extract -r region.shp -l4a agbd -egi -o output/
gh3_extract -r region.shp -l4a agbd -egi 1:12 -o output/  # Explicit index:partition

# Aggregate H3 database data (supports EGI with -egi flag)
gh3_aggregate -h3 6 -o output/  # H3 aggregation
gh3_aggregate -egi 6 -a mean -o output/  # EGI aggregation (partition at level 12)
gh3_aggregate -egi 6:10 -a mean -o output/  # Explicit aggregation:partition levels
gh3_aggregate -egi 6 -a mean -R -o output/  # With rasterization

# Rasterize pre-aggregated datasets to GeoTIFF
# NOTE: gh3_rasterize requires a dataset from gh3_aggregate or gh3_extract
gh3_rasterize -d /path/to/aggregated_dataset -o output/ --compress LZW  # Tiled output
gh3_rasterize -d /path/to/aggregated_dataset -m -o output.tif  # Merged output
gh3_rasterize -d /path/to/aggregated_dataset -l agbd_l4a -o output/  # Select variables
```

### Utility Tools

```bash
# Display H3/EGI resolution levels
gh3_list_resolutions
gh3_list_resolutions -egi  # EGI levels

# Inspect schemas and browse variables
gh3_read_schema                    # default H3 database
gh3_read_schema --grep "agbd"      # grep filter
gh3_read_schema -p L2A             # filter by product
gh3_read_schema /path/to/file.parquet
gh3_read_schema /path/to/file.h5
```

### Database Doctor

```bash
# Audit DB health (read-only, default = all DB diagnoses)
gh3_doctor -i /path/to/db

# Subsets / aliases: db (default), soc, all, or comma-separated names
gh3_doctor -i /db --check backfill,parquet_health
gh3_doctor -i /db --check all

# Apply safe remedies (no destructive changes; corrupt files are reported only)
gh3_doctor -i /db --fix
gh3_doctor -i /db --fix backfill --s3       # backfill from NASA S3 ETL temp

# Decorate report with NASA upstream availability + recommendations
gh3_doctor -i /db --online                  # adds gh3_download / gh3_build commands

# Machine-readable
gh3_doctor -i /db --report report.json
```

Diagnoses: `backfill` (NaN gaps in product columns), `orphans` (leftover .tmp +
empty dirs), `log_state` (stuck flags + log↔disk drift), `metadata` (partition
JSON + manifest), `parquet_health` (corrupt files + duplicate shots + schema
drift), `soc_health` (invalid HDF5 + download log drift). Exit codes: 0 clean,
1 findings remain, 2 errors during fix.

### Ancillary Data Tools

```bash
# Sample raster pixel values at GEDI shot locations
gh3_from_img -i /path/to/dem.tif -d /path/to/dataset -r region.shp -o output/

# Sample tile directory with band selection and window operations
gh3_from_img -i /path/to/tiles/ -B 0 2 -w 131 -d /path/to/database -o output/

# Join polygon attributes to GEDI shots
gh3_from_polygon -i ecoregions.shp -c ECO_NAME BIOME_NAME -d /path/to/dataset -o output/

# Join with column prefix and inner join (drop unmatched shots)
gh3_from_polygon -i landcover.gpkg -x lc_ --dropna -d /path/to/databaset -o output/
```

### Data Flow
1. **Download**: `daac.py` → `earthaccess` → GEDI HDF5 files in SOC directory structure (`year/doy/`)
2. **Build**: `gh3builder.py` reads HDF5 → H3 indexes → partitions by H3 cell → parquet files with metadata JSON
3. **Query**: `gh3driver.py` loads partitioned parquet via Dask with spatial/temporal filtering
4. **Extract/Aggregate**: Filter and aggregate data → simplified flat parquet files for external use
5. **Rasterize**: Convert to GeoTIFF with time-series support

### Key Classes

| Class | Module | Purpose |
|-------|--------|---------|
| `GEDIFile` | gedidriver.py | Parses GEDI filename (orbit, granule, track, version) |
| `GEDIShot` | gedidriver.py | Decodes shot_number to extract beam, orbit, track |
| `GEDIAccessor` | daac.py | NASA Earthdata authentication and data search |
| `TimeSeriesRasterizer` | raster/timeseries.py | Time-series raster generation |
| `AtomicFileWriter` | utils.py | Atomic file writes with rollback |
| `H3BuildLogger` | logger.py | Tracks build progress and resume state |

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

## Testing

```bash
# Unit tests (fast, no network required)
pytest tests/ -m "not integration and not slow"

# Integration tests (requires NASA Earthdata credentials)
pytest tests/ -m integration

# Full test suite
pytest tests/ -v
```

## Claude Code Sub-Agents

Four specialized sub-agents in `.claude/agents/`:

| Agent | Use For |
|-------|---------|
| `core-pipeline` | Download, build, extract, aggregate workflows |
| `raster-egi` | Spatial indexing, rasterization, CRS transforms |
| `cli-deployment` | CLI tools, configuration, deployment |
| `testing-qa` | Test coverage, validation, benchmarking, **DRY/redundancy audits** |

Use `testing-qa` to audit for duplicate code before adding new utilities. Check `cliutils.py`, `utils.py`, `validation.py` first.
