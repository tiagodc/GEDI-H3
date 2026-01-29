# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gedih3** is a Python library for accessing NASA's GEDI (Global Ecosystem Dynamics Investigation) satellite LiDAR data with H3 spatial indexing. It handles downloading GEDI products from NASA's DAACs, building H3-indexed parquet databases for efficient spatial queries, and extracting/aggregating data.

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

Four command-line tools are installed as entry points:

```bash
# Download GEDI data from NASA DAAC
gh3_download -r "W,S,E,N" -l2a default -l4a default -N 8

# Build H3 database from downloaded HDF5 files
gh3_build -r "W,S,E,N" -l2a default -l4a default -h3r 12 -h3p 3

# Extract data from H3 database with filters
gh3_extract -d /path/to/database -r region.shp -l2a rh -l4a agbd -q -o output/

# Aggregate H3 database data
gh3_aggregate -d /path/to/database -o output/
```

Common CLI flags:
- `-r, --region` - Spatial filter: vector file, bbox as "W,S,E,N", or ISO3 country code
- `-d0, -d1` - Temporal filters (YYYY-MM-DD)
- `-l1b, -l2a, -l2b, -l4a, -l4c` - Product variables (use `default`, `minimal`, or list)
- `-N, -T, -M, -P` - Dask: workers, threads, memory per worker, dashboard port
- `-s, --dask-scheduler` - Connect to existing Dask scheduler

## Architecture

### Data Flow
1. **Download**: `daac.py` → `earthaccess` → GEDI HDF5 files in SOC directory structure (`year/doy/`)
2. **Build**: `gh3builder.py` reads HDF5 → H3 indexes → partitions by H3 cell → parquet files with metadata JSON
3. **Query**: `gh3driver.py` loads partitioned parquet via Dask with spatial/temporal filtering

### Core Modules

| Module | Purpose |
|--------|---------|
| `config.py` | GEDI product definitions (DOIs, versions, variables), default paths |
| `daac.py` | `GEDIAccessor` class, earthaccess authentication, granule search/download |
| `gedidriver.py` | `GEDIFile`/`GEDIShot` classes, HDF5 reading, Dask dataframe creation |
| `gh3builder.py` | `build_h3db()` orchestrates H3 indexing, partitioning, merging |
| `gh3driver.py` | `gh3_load()` for querying H3 database, metadata functions |
| `h3utils.py` | H3 cell operations, geometry intersection, dataframe indexing |
| `cliutils.py` | Argument parsing, region/variable collection for CLI tools |

### Key Classes

- **`GEDIFile`**: Parses GEDI filename convention (orbit, granule, track, version, etc.)
- **`GEDIShot`**: Decodes shot_number to extract beam, orbit, track information
- **`GEDIAccessor`**: Main interface for earthaccess authentication and data search

### GEDI Products Supported

| Product | Description |
|---------|-------------|
| L1B | Geolocated waveforms |
| L2A | Elevation and height metrics (RH percentiles) |
| L2B | Canopy cover and vertical profiles |
| L4A | Footprint-level aboveground biomass (AGBD) |
| L4C | Footprint-level structural complexity (WSCI) |

## Testing

```bash
# Run test script
python tests/run_tests.py
```

## Key Patterns

- **Dask everywhere**: All heavy operations use Dask DataFrames/Bags for distributed processing
- **H3 partitioning**: Data partitioned by H3 cells (default: res 3 for partitions, res 12 for indexing)
- **Parquet + JSON metadata**: Each H3 partition has a `.parquet` file and `.metadata.json` sidecar
- **Variable expansion**: CLI accepts `default`, `minimal`, `*`, or explicit variable lists/files
- **Spatial filtering**: Supports vector files, bounding boxes, or ISO3 country codes
