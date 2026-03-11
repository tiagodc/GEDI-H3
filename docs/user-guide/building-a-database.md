# Building a Database

The H3 database is the foundation of every gedih3 workflow. Every other tool — `gh3_extract`, `gh3_aggregate`, `gh3_rasterize`, and the ancillary data tools — reads from this database. Getting the build right means faster queries, less disk usage, and analysis-ready data from the start.

This page explains what `gh3_build` does, what it produces, how to control the process, and how to make good choices for your specific use case.

> **Use the CLI.** `gh3_build` is best invoked from the command line. A Python API exists for advanced programmatic use, but the CLI handles all configuration, logging, resume logic, and Dask setup automatically. If in doubt, use the CLI.

---

## What `gh3_build` Does

Raw GEDI data arrives as thousands of large HDF5 files organized by acquisition time (year/day-of-year). Each file contains up to 8 laser beams, and each beam holds hundreds of variables for millions of shots. Reading this data requires specialized tools, knowledge of the file structure, and significant engineering effort just to get a simple spatial query working.

`gh3_build` performs a one-time transformation:

1. **Reads** the raw HDF5 files, navigating the beam/variable hierarchy automatically
2. **Selects** the variables you requested, expanding presets like `default` or `minimal` to exact variable lists
3. **Assigns** each shot an H3 spatial index at a fine resolution (default: level 12, ~22 m — roughly the GEDI footprint size)
4. **Groups** shots into spatial partitions at a coarser H3 resolution (default: level 3, ~12,000 km²)
5. **Writes** column-oriented GeoParquet files — one per spatial partition per year — along with a build log that records exactly what was built and how

The result is a spatially-indexed database where regional queries skip irrelevant partitions entirely, making subsequent operations fast regardless of dataset size.

---

## The Output Database Structure

```
~/gedih3_db/h3/
├── h3_03=8031fffffffffff/
│   ├── 8031fffffffffff.metadata.json
│   ├── year=2019/
│   │   ├── 8031fffffffffff.2019.0.parquet
│   │   └── 8031fffffffffff.2019.0.metadata.json
│   ├── year=2020/
│   │   ├── 8031fffffffffff.2020.0.parquet
│   │   └── 8031fffffffffff.2020.0.metadata.json
│   │   ...
├── h3_03=8033fffffffffff/
│   ├── 8033fffffffffff.metadata.json
│   ├── year=2019/
│   │   ...
│   ...
├── gedih3_build_log.json
└── _manifest.txt
```

The database uses **nested hive partitioning** — first by H3 level-3 cell, then by year. Each top-level directory corresponds to a spatial partition; inside, data is further split into yearly sub-partitions. This two-level scheme caps the maximum file size while making it straightforward to add new data — appending a new year never touches existing files.

Each year sub-directory contains a `.parquet` data file and a companion `.metadata.json`. A cell-level metadata file sits at the H3 partition root. At the database root, `gedih3_build_log.json` records the full build configuration — products, variables, region, temporal range, and H3 resolution settings — and `_manifest.txt` lists all partition paths.

::::{tip} The database can live anywhere — not just on local disk. Once built, all downstream tools (`gh3_extract`, `gh3_aggregate`, `gh3_rasterize`, `gh3_from_img`, `gh3_from_polygon`) can read the database transparently from **S3**, **HTTP/HTTPS**, **SFTP/SSH**, or **FTP** via [fsspec](https://filesystem-spec.readthedocs.io/). Just point `-d` at a remote URI:

```bash
# Public S3 bucket
gh3_extract -d s3://my-bucket/h3_database/ --s3-anon -r region.shp -o output/

# SFTP server
gh3_aggregate -d sftp://server.example.com/data/h3_database/ --ssh-key ~/.ssh/id_rsa -egi 6 -o output/

# local HTTP server
gh3_extract -d http://192.169.0.33/data/h3_database/ -l4a agbd -l2a rh_098 -o output/

```

See {ref}`Remote Storage Credentials <remote-storage-credentials>` for the full list of credential flags.
::::

### From nested HDF5 to flat rows

In a raw GEDI HDF5 file, you navigate a tree: `file → BEAM0101 → agbd → array`. Each beam is a separate group. Variables have no consistent names across products. To access even a single variable, you need `h5py` and intimate knowledge of the file structure.

In the gedih3 database, every shot is a **row** and every variable is a **column**. The beam structure is dissolved — shots from all 8 beams are unified into a single flat table. The file can be read with any tool that handles Parquet: pandas, R's `arrow`, DuckDB, QGIS, or any dataframe library.

```python
# Read a single year partition directly with pandas — no gedih3 required
import pandas as pd
df = pd.read_parquet('~/gedih3_db/h3/h3_03=8031fffffffffff/year=2020/8031fffffffffff.2020.0.parquet')
```

This interoperability is intentional. The database is your data asset — it does not lock you in to any particular processing stack.

---

## Column Naming: Product Suffixes

Every variable extracted from a GEDI product is stored with a product suffix appended to its HDF5 name:

| HDF5 name | GEDI product | gedih3 column |
|-----------|--------------|---------------|
| `agbd` | L4A | `agbd_l4a` |
| `agbd_se` | L4A | `agbd_se_l4a` |
| `quality_flag` | L2A | `quality_flag_l2a` |
| `l4_quality_flag` | L4A | `l4_quality_flag_l4a` |
| `rh[98]` | L2A | `rh_098_l2a` |
| `rh[50]` | L2A | `rh_050_l2a` |
| `rh` (array, 101 elements) | L2A | `rh_000_l2a` … `rh_100_l2a` (101 columns) |
| `cover` | L2B | `cover_l2b` |
| `fhd_normal` | L2B | `fhd_normal_l2b` |
| `cover_z` (array, 30 elements) | L2B | `cover_z_000_l2b` … `cover_z_029_l2b` (30 columns) |
| `rxwaveform` (array, variable size bins) | L1B | `rxwaveform_0000_l1b` … `rxwaveform_1419_l1b` (1420 columns) |
| `wsci` | L4C | `wsci_l4c` |

The suffix is always the lowercase product code: `_l1b`, `_l2a`, `_l2b`, `_l4a`, `_l4c`.

**Why the suffix?** The same variable name can appear in multiple GEDI products. For example, `sensitivity` exists in both L2A and L4A with subtly different meanings. `quality_flag` exists in L2A, L2B, L4A, and L4C. When building a database with multiple products, suffixing makes every column unambiguous and prevents silent collisions.

**Array variable expansion:** Array variables in GEDI HDF5 files are expanded into individual named columns using zero-padded indices. The index width depends on the variable type: 3 digits for 2d arrays (`rh`, `cover_z`, `pavd_z`) and 4 digits for waveforms (`rxwaveform`).

**L1B waveforms and large databases:** Including `rxwaveform` expands a single variable into 1420 columns. For a database covering millions of shots this produces a table with billions of waveform cells — significant disk usage and severely degraded query performance. L1B waveform data is **strongly discouraged for any area larger than a small study site**. If you need raw waveforms, build a separate small-area database explicitly for that purpose.

---

## Built-In Columns (Not From HDF5 Products)

In addition to the product variables you select, gedih3 adds a set of columns automatically during the build process. These are **not** suffixed and are present in every database regardless of which products were built.

| Column | How it is created | Why it matters |
|--------|------------------|----------------|
| `shot_number` | Copied from the HDF5 `shot_number` field (same value across all products for the same shot) | The universal shot identifier. Links rows across products and datasets. Used internally to join multi-product tables and to recover provenance. |
| `geometry` | Computed from `lat_lowestmode` and `lon_lowestmode` (L2A) | Point geometry (WGS-84/EPSG:4326) for every shot. Required for all spatial operations, GIS output, and spatial joins with vector data. |
| `datetime` | Converted from `delta_time` (seconds since 2018-01-01 J2000 epoch) | Human-readable UTC timestamp. Used for temporal filtering in `gh3_extract` and time-series analysis. Avoids the need to decode the raw epoch offset yourself. |
| `h3_12` (or `h3_XX` at your chosen index level) | Computed from `geometry` at the configured index resolution | The primary spatial join key in the H3 system. All aggregations use this column to assign shots to coarser H3 cells. Stored as a string H3 cell ID. |
| `root_file` | The filename of the source HDF5 granule | Provenance. Tells you which raw file each shot came from. Useful for debugging, reproducibility, and tracking down anomalous values. |

> **L2A is always required.** The `geometry` and `datetime` columns depend on variables from L2A (`lat_lowestmode`, `lon_lowestmode`, `delta_time`). Even if you only want L4A biomass data, gedih3 reads the L2A essentials automatically. You do not need to explicitly request them — this is handled internally.

---

## Selecting Variables: The `-l` Flags

> **Variable subsetting is the most impactful build-time decision.** Raw GEDI HDF5 files are large (~1–3 GB each), and variables you include become permanent columns in the database. More columns = larger files, slower queries, longer builds. Two rules of thumb:
> - **Always use `minimal`, `default` or an explicit variable list.** Never use `all`/`*` unless you have a specific reason — L2A `all` alone exceeds 300 variables, many of them diagnostic outputs with limited research value.
> - **Never include L1B waveforms for large areas.** `rxwaveform` expands to 1420 columns per shot. For small sites it is acceptable; for regional or global builds it is impractical.

Variable selection is the most important build-time decision. The variables you build with are the only variables available for all subsequent extractions and aggregations. You can add variables later, but you cannot remove them without rebuilding.

### Product flags

Each GEDI footprint product has its own flag:

| Flag | Product |
|------|---------|
| `-l1b` | L1B (raw waveforms) |
| `-l2a` | L2A (canopy height, ground elevation) |
| `-l2b` | L2B (canopy cover, vertical structure) |
| `-l4a` | L4A (aboveground biomass density) |
| `-l4c` | L4C (structural complexity) |

At minimum, you must specify at least one product flag. You can combine as many as needed.

### Variable selection keywords

After each product flag, pass one of the following:

**`minimal`** — the smallest usable set. Geolocation, timestamp, quality flag, and the primary headline variable for each product. Use this when disk space is very limited or you only need one or few metrics.

```bash
gh3_build -r "-51,0,-50,1" -l2a minimal -l4a minimal
```

**`default`** — the recommended science-ready set. An expert-curated selection covering all variables needed for common research workflows, including uncertainty estimates, alternative algorithm outputs, and land-cover ancillary data. This is a good choice for most projects.

```bash
gh3_build -r "-51,0,-50,1" -l2a default -l4a default
```

**Explicit variable names** — list specific HDF5 variable names after the flag. Consult the data dictionaries for each GEDI product for reference.

```bash
gh3_build -r "-51,0,-50,1" -l2a rh elev_lowestmode quality_flag -l4a agbd agbd_se l4_quality_flag
```

**`all` or `*` or bare flag** — every variable in the product. Produces very large databases. Use with caution and only if you have a specific reason to need the full variable set.

```bash
gh3_build -r "-51,0,-50,1" -l2a "*"  # all L2A variables
```

**A text file** — one HDF5 variable name per line. Useful for reproducible builds with large custom variable lists.

```bash
gh3_build -r "-51,0,-50,1" -l2a /path/to/my_variables.txt -l4a default
```

> See [Variable Presets Reference](../concepts/variable-presets.md) for the exact variable list in each preset for GEDI different products and data versions.

---

## H3 Resolution Settings

Two flags control the spatial resolution of the database:

**`-h3r INDEX_LEVEL`** (default: `12`) — the H3 resolution at which each shot is indexed. Each shot gets assigned to the H3 cell at this level that contains its coordinates. Level 12 cells are ~307 m² — approximately the size of a GEDI footprint, so each footprint's center coordinate falls in its own cell.

**`-h3p PARTITION_LEVEL`** (default: `3`) — the H3 resolution used to partition files on disk. Level 3 cells are ~12,393 km². All shots within the same level-3 cell are stored in the same partition file. This controls the granularity of spatial skipping during queries.

```bash
# Default: index at level 12, partition at level 3
gh3_build -r "-51,0,-50,1" -l4a default

# Custom: index at level 10, partition at level 5
gh3_build -r "-51,0,-50,1" -l4a default -h3r 10 -h3p 5
```

**When to change the defaults:**

- **Smaller region**: Increase `-h3p` (e.g., to 5 or 6) to get more files with lower size each. With the default level-3 partitions, a small region of interest may produce too few partitions with concentrated data.
- **Very coarse analysis**: Lower `-h3r` if you only plan to aggregate to resolutions coarser than level 10, to reduce index column cardinality.
- **Sub-footprint indexing**: Raise `-h3r` above 12 only if you need the finest possible spatial granularity for shot-level EGI mapping.

The defaults are well-calibrated for regional-to-global analyses. If you are unsure, leave them unchanged.

> See [H3 Indexing](../concepts/h3-indexing.md) for a full explanation of the resolution system and the partition/index dual-level design.

---

## Source Modes: Where Does the Data Come From?

`gh3_build` supports three modes for sourcing raw GEDI HDF5 data:

### Mode 1: Local HDF5 files (default)

The default mode reads HDF5 files that you have already downloaded to disk. As long as the original names of the GEDI files are preserved, no specific file organization standard is required - as long as all GEDI HDF5 files are in the same directory. The file structure layout adopted by `gh3_download` is the SOC (Science Operation Center) directory structure: `soc/LXXX/year/doy/*.h5`.

```bash
# Download first
gh3_download -r "-51,0,-50,1" -l2a default -l4a default

# Then build
gh3_build -r "-51,0,-50,1" -l2a default -l4a default
```

**Best for**: workstations and HPC clusters with fast local or network-attached storage, or when you plan to build multiple databases from the same raw files. The raw HDF5 files remain on disk after the build and can be re-used.

### Mode 2: Embedded download (`-dl`)

With `-dl`, `gh3_build` automatically calls `gh3_download` as a first step before building. This is a convenience shortcut — it does not change the build behavior, only the workflow.

```bash
# Download and build in one command
gh3_build -r "-51,0,-50,1" -l2a default -l4a default -dl
```

**Best for**: good network connections where you want a single command to handle the full pipeline. The raw HDF5 files are kept on disk after the build.

### Mode 3: S3 ETL (`-s3`)

With `-s3`, `gh3_build` streams GEDI data directly from NASA's S3 bucket into a temporary location and converts it to Parquet without writing the full HDF5 files to persistent storage. Each granule is streamed, processed, and discarded.

```bash
# Stream from NASA S3 — no HDF5 files on disk
gh3_build -r "-51,0,-50,1" -l2a default -l4a default -s3
```

**Best for**: environments with slow or expensive local disk, cloud computing instances, or any situation where you want to avoid storing the raw HDF5 files (~1–3 GB per granule). S3 mode requires a good network connection to NASA's servers. 

> **Download requires Earthdata credentials for all modes.** If you have not already authenticated, run `python -c "import earthaccess; earthaccess.login()"` and follow the prompts. Credentials are stored in `~/.netrc`.

---

## Subsetting Strategies

GEDI covers the entire globe from 51.6°S to 51.6°N. Building a global database is possible but can require hundreds of gigabytes of disk and many hours of compute time. **Subsetting at build time** is the most effective way to keep resource usage proportional to your actual needs.

There are three independent subsetting axes:

### 1. Spatial subsetting (`-r`)

Pass a region specification to restrict the build to shots within your area of interest. Only granules that intersect the region are downloaded/processed, and only shots belonging to H3 hexagons intersecting the region are written to the database.

```bash
# Bounding box: W,S,E,N (degrees)
gh3_build -r "-60,-10,-40,5" -l2a default -l4a default

# Vector file (any format readable by GeoPandas: Shapefile, GeoPackage, GeoJSON, ...)
gh3_build -r /path/to/country_boundary.shp -l2a default -l4a default

# ISO3 country code (polygon fetched automatically)
gh3_build -r USA -l2a default -l4a default
```

Spatial subsetting is almost always worthwhile unless you genuinely need global coverage. Even a small bounding box can reduce data volume by orders of magnitude.

### 2. Temporal subsetting (`-t0` / `-t1`)

Pass start and end dates to restrict the build to a specific time window. Granules outside the window are skipped.

```bash
# Build only data from 2020
gh3_build -r "-51,0,-50,1" -l2a default -l4a default -t0 2020-01-01 -t1 2020-12-31

# Build from launch through end of 2021
gh3_build -r "-51,0,-50,1" -l2a default -l4a default -t1 2021-12-31
```

GEDI has been collecting data since April 2019. Without temporal subsetting, the build will include all available data — which grows every periodically.

### 3. Variable subsetting (`-lXX minimal` vs `default` vs explicit)

The number of variables you store has a direct effect on database size and build time. As a rough guide:

| Preset / variables | Approximate disk per billion shots |
|--------------------|-------------------------------------|
| `minimal` (any product) | 10- GB |
| `default` (any product) | 10+ GB |
| `all` (any product) | 100+ GB |
| `rxwaveform` (L1B, 1420 cols) | 1000+ GB — avoid for large areas |

If you are not sure which variables you will need, **`default` is a good starting point** — it covers the variables needed for the vast majority of research workflows. You can always add variables later (see Resume and Updates below) without rebuilding from scratch.

### Practical scenarios

**Small region study (e.g., a national park or watershed):**
```bash
gh3_build -r study_area.shp -l2a default -l4a default
```

**Country-level analysis with disk constraints:**
```bash
gh3_build -r COL -l2a minimal -l4a minimal
```

**Multi-year time series:**
```bash
gh3_build -r "-80,-20,20,20" -l2a default -l4a default -t0 2019-01-01 -t1 2023-12-31
```

**Global build on a cloud instance with limited disk:**
```bash
gh3_build -l2a minimal -l4a minimal -s3 -N 16 -M 8GB
```

---

## Resume and Updates

### Resuming an interrupted build

If a build is interrupted (power loss, time limit, Ctrl-C), simply re-run the exact same command. `gh3_build` tracks which HDF5 granules have been successfully processed and skips them on the next run. No data is lost.

```bash
# Interrupted — re-run the same command to resume
gh3_build -r "-51,0,-50,1" -l2a default -l4a default
```

### Adding variables to an existing database

You can add new variables to an existing database without re-reading all the HDF5 files. `gh3_build` detects that the database already exists and performs a variable-only update — reading only the new columns and appending them to the existing Parquet files.

```bash
# Add biomass uncertainty columns to an existing database
gh3_build -l4a agbd_se agbd_pi_lower agbd_pi_upper
```

### Expanding the spatial or temporal coverage

Re-run with a wider region or date range. `gh3_build` performs a safe two-phase update: it first processes the new shots (spatial/temporal expansion), then handles any variable additions in a second phase. Existing data is not touched.

```bash
# Original build covered a small area; extend to a larger region
gh3_build -r wider_region.shp -l2a default -l4a default
```

---

## Dask Configuration and Performance

`gh3_build` uses Dask for parallel processing. The defaults work on a laptop with a few cores, but performance scales dramatically with available resources.

### Key flags

| Flag | Default | Description |
|------|---------|-------------|
| `-N` | auto | Number of Dask workers |
| `-T` | 1 | Threads per worker |
| `-M` | auto | Memory per worker in GB |
| `-P` | — | Dask dashboard port |
| `-s` | — | Connect to an existing Dask scheduler |

### Recommendations

**Laptop or workstation (8–32 cores):**
```bash
gh3_build -r ... -l4a default -N 8 -T 2 -M 8
```

**HPC cluster (SLURM etc.) — connect to an existing scheduler:**
```bash
gh3_build -r ... -l4a default -s tcp://scheduler-host:8786
```

**Very large datasets with memory issues:**
```bash
gh3_build -r ... -l4a default -N 4 -M 16
```

---

## Verifying the Result

After building, use `gh3_read_schema` to inspect the database schema — it lists every column name and its data type:

```bash
gh3_read_schema ~/gedih3_db/h3/
```

This reads a parquet file from the database and prints its column schema. Check the output before running `gh3_extract` or `gh3_aggregate` to confirm the database contains the variables you need. You can also point it at any individual file:

```bash
gh3_read_schema ~/gedih3_db/h3/h3_03=8031fffffffffff/year=2020/8031fffffffffff.2020.0.parquet
```

---

## Quick Reference

```bash
# Minimal example — small area, minimal variables
gh3_build -r "-51,0,-50,1" -l2a minimal -l4a minimal

# Recommended — default variables with download embedded
gh3_build -r study_area.shp -l2a default -l4a default -dl

# S3 mode — no persistent HDF5 files
gh3_build -r COL -l2a default -l4a default -s3

# Multi-product with temporal filter
gh3_build -r "-51,0,-50,1" -l2a default -l2b default -l4a default \
          -t0 2020-01-01 -t1 2022-12-31

# Add a missing variable to an existing database
gh3_build -l4a agbd_se agbd_pi_lower agbd_pi_upper

# HPC cluster build
gh3_build -r large_region.shp -l2a default -l4a default \
          -s3 -N 32 -M 16GB

# See all options
gh3_build --help
```
