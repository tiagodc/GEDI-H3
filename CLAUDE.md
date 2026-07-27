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

## Software Design Priorities

Every change in this codebase is evaluated against four non-negotiable pillars. They were articulated explicitly during the v0.8.x continental-build hardening (Apr–May 2026) and now apply uniformly across build, download, and query tools.

1. **Scalability — high CPU, low driver bottleneck.** Push work to workers; never serialize through the driver. No driver-side O(N) GPFS scans (use a manifest sentinel or `client.map` listing instead). No driver-side inflight throttle — let the dask scheduler distribute. Stream `as_completed` instead of `bag.persist + compute` for long-running phases.
2. **Low-memory plateau — use as much CPU as possible with as little RAM as possible.** Per-task `gc.collect()` + Arrow pool release + glibc `malloc_trim` (`data/dask-worker-trim.py`). Cap pyarrow scanner readahead (`batch_readahead=1`, `fragment_readahead=1`). `pre_buffer=True` for I/O coalescing on shared GPFS. Per-file iter (not `ds.dataset` scanner) for merges. Per-worker memory must plateau, not climb, regardless of build duration.
3. **Atomic & resumable I/O — safety as a first-class concern.** Every output write goes through `AtomicFileWriter` (`.tmp` + `os.replace`, cleanup on exception). Tolerate corrupt destinations / inputs and re-merge or skip+log instead of failing the whole job. Resume via append-only progress files + stable filename conventions (granule ID embedded in fragment basename) and HDF5/parquet header validity checks (`h5_is_valid`).
4. **A-priori knowledge over runtime detection — knowing how the data should look, or its bounds, a priori saves real I/O.** Use shipped per-product variable manifests (`gedi_vars_static`), sidecar metadata (`gh3_read_meta` for `h3_columns` / `h3_columns_dtypes` / `h3_partition_level` / `h3_partition_ids`), and H3/EGI cell math (`h3_partition_bbox`) instead of reading HDF5 headers, scanning columns, or computing bounds when the answer is knowable for free.

When in doubt, ask: *"Am I doing work the structure of the data already answers?"* If yes, replace the work with a lookup.

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

**Dependency invariant**: `pip install gedih3` must work with zero system
libraries — every native dependency ships wheels that vendor its own GDAL /
GEOS / PROJ / HDF5. Declare every imported module in `pyproject.toml`; never
rely on a transitive edge. `tests/test_dependencies.py` walks the source AST
and fails the build otherwise. `osgeo` (GDAL bindings) is the single permitted
optional import — it has no PyPI wheels — and every call site must guard it
with `try`/`except ImportError` plus a working fallback.

The conda recipe is **no longer in this repo** — it lives in
`conda-forge/gedih3-feedstock` (see `recipe/README.md`). A new dependency, or a
new `[project.scripts]` entry point, must be mirrored there by hand; the
autotick bot only bumps version and sha256. `test_recipe_matches_pyproject`
skips now that `recipe/meta.yaml` is gone, so **CI does not catch this drift.**

Configuration via `~/.gedih3.env` or environment variables:
- `GH3_DEFAULT_DOWNLOAD_DIR` - Base directory for all data
- `GH3_DEFAULT_TMP_DIR` - Temporary files
- `GH3_DEFAULT_SOC_DIR` - Downloaded GEDI HDF5 files (SOC format)
- `GH3_DEFAULT_H3_DIR` - H3-indexed parquet database

Configuration priority: CLI args > env vars > `~/.gedih3.env` > `config.py` defaults.

**Operator tuning env vars** (build-time only; safe to leave unset):
- `GH3_WRITE_STREAMING` - default on; toggles streaming partition writer vs legacy ddf.to_parquet path
- `GH3_LOG_PROGRESS` - default off; re-enables the 60s `Streaming write: N/M done` INFO line in `gh3_build.log` for detached / tail-followed workflows (terminal tqdm postfix is always on)
- `GH3_MANIFEST_REFRESH_EVERY` - default 1000; how often (in merged partitions) `_merge_and_finalize` re-writes the database `_manifest.txt` so consumers reading mid-merge see fresh state
- `ARROW_DEFAULT_MEMORY_POOL=system` + `MALLOC_TRIM_THRESHOLD_=0` - per-worker env required for the low-memory plateau (set externally via your cluster launcher, not by gedih3)

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

### BBox Index

```bash
# Build the _bbox_index.parquet root sidecar (footer-only scan; minutes even
# on a continental DB). Query tools then skip partition files a region/EGI
# tile provably cannot touch. gh3_build (re)creates it automatically after
# every successful build (--no-bbox-index to skip); this CLI retrofits
# pre-existing DBs and restores it after gh3_doctor --fix. Auto-invalidated
# at merge entry and ignored whenever the build log is newer than it
# (fail-safe: absence only costs speed).
gh3_bbox_index -d /path/to/db -N 16
```

### Database Doctor

```bash
# Audit DB health (read-only; default = the `db` alias group, NOT every diagnosis)
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

Alias groups (`src/gedih3/doctor/runner.py`): `db` (the default) =
`backfill, orphans, log_state, metadata, parquet_health, geoparquet_bbox`;
`soc` = `soc_health`; `all` = every registered diagnosis. **`soc_health` and
`tmp_partitions_health` are not in `db`** — ask for them by name or use `all`.

Diagnoses: `backfill` (NaN gaps in product columns), `orphans` (leftover .tmp +
empty dirs), `log_state` (stuck flags + log↔disk drift), `metadata` (partition
JSON + manifest), `parquet_health` (corrupt files + duplicate shots + schema
drift), `geoparquet_bbox` (GeoParquet bbox metadata coverage),
`soc_health` (invalid HDF5 + download log drift),
`tmp_partitions_health` (post-build forensics on `tmp/partitions/`:
`_merge_failures/` sentinels + `_granule_failures.jsonl` summaries +
progress↔manifest drift; `--fix` calls `preclean_merge_failures` and refuses
to act while a `gh3_build` is live). Exit codes: 0 clean, 1 findings remain,
2 errors during fix.

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
- **Parquet + JSON metadata**: Each H3 partition has a `.parquet` file; database root has `gedih3_build_log.json` (carries `h3_columns`, `h3_columns_dtypes`, `h3_partition_level`, `h3_partition_ids`)
- **Database manifest sentinel** (`_manifest.txt`): one relative path per line at the database root; `smart_glob` reads it before falling back to a recursive walk. Keeps `gh3_load()` cheap on million-partition databases over HTTP/S3/GPFS.
- **R2 manifest invariant** (producer-driven refresh): every code path that mutates a SOC or H3 DB tree refreshes the corresponding manifest before returning — `gh3_download`, `gh3_build --download`, `s3_etl_subset`, `gh3_build -i` (opportunistic write at exit when its in-memory file list is available), `gh3_doctor --fix soc_health`/`--fix orphans`/`--fix metadata`. Long merges now also refresh **incrementally** every `GH3_MANIFEST_REFRESH_EVERY` (default 1000) successful merges via `_derive_merged_output_paths(_merge_progress.txt, h3_dir) → generate_manifest(files=…)` — pure in-memory derive + one atomic file write, no tree walk — so consumers reading mid-build see partial-but-fresh state instead of stale data from the previous build. Consumers trust the manifest blindly; the only consumer-side check is a constant-time `mtime(manifest) >= mtime(root)` smoke test in `_read_manifest` that emits a loud ERROR pointing at the relevant `gh3_doctor --fix` remedy when a producer crashed or an external population (NASA delivery, manual rsync) bypassed the gh3 toolchain.
- **SOC manifest sentinel** (`_soc_manifest.txt`): the download-side parallel of `_manifest.txt`. **Write-only** as of v0.10.20 — `soc_file_tree` and the doctor `soc_health` diagnosis no longer read it, because external population paths (manual rsync, NASA delivery) bypass the producer-driven refresh and a stale manifest silently narrows every scan. Producers still write it (`SOCDownloadLogger.set_post_download_info`, `gh3_build` exit, `download_soc`) as an informational sidecar for external tooling; readers always fan a `walk_soc_parallel` year/doy scan across the registered dask Client.
- **Unified `source=` API**: `gh3_load()` and `egi_load()` accept `source=` as the primary path parameter
- **Variable expansion**: CLI accepts `default`, `minimal`, `*`, or explicit variable lists/files
- **Static product variable manifests**: `data/GEDI*_DATASETS_*.txt` ship the canonical variable list per `(product, version)`. `gedi_vars_static(product, version)` is the cached, free lookup; prefer it over `gedi_vars_from_h5` whenever the file under inspection is a NASA release file. Files that may have been previously subset (compact HDF5 from S3 ETL) still need `gedi_vars_from_h5` — the static manifest would over-count them.
- **Cached H3 schema dtypes** (`h3_columns_dtypes` in the build log): `gh3_load()` builds its Dask `_meta` from the cache (zero parquet I/O) and falls back to sampling `h3_dirs[0]` only when the field is missing (legacy DBs).
- **Distributed doctor diagnoses**: every diagnosis that scans every partition (`parquet_health`, `backfill`, `geoparquet_bbox`, `metadata`, `orphans`, `soc_health`) ships per-partition work to dask workers via `gedih3.doctor.parallel.parallel_map`. There is no serial fallback — `parallel_map` raises `GediError` when no Client is registered. O(1) emptiness checks (`partition_is_empty`, `year_dir_is_empty`) replace the legacy recursive globs.
- **Spatial filtering**: Supports vector files, bounding boxes, or ISO3 country codes
- **S3 ETL mode**: `gh3_build --s3` / `gh3_download --s3` stream from NASA S3 without persistent local download
- **Retry logic**: Network operations use exponential backoff (3 attempts, 1-60s wait)
- **Atomic writes everywhere**: file operations route through `AtomicFileWriter` (`utils.py`). Build merges, JSON metadata writes, and extract/aggregate single-file outputs (`_write_dataframe`, `_write_egi_file` for parquet/feather/csv/txt/h5) all use `.tmp` + `os.replace`. Geo-vector formats (geojson/gpkg/shp) bypass the wrap because they depend on file-extension driver inference and shapefile emits multiple sidecars.
- **HDF5 validity gate**: `h5_is_valid(path)` (cheap header open) is the resume-safety check on downloads — a truncated `.h5` left by a SIGKILL must not be silently consumed by the build phase.
- **Merge-failure recovery loop** (v0.10+): when `_merge_and_finalize` hits a known-bad fragment class (0-byte parquet, missing magic bytes, truncated thrift footer — see `_RECOVERABLE_FRAGMENT_ERROR_MARKERS`), it (a) writes an atomic per-failure sentinel `tmp/partitions/_merge_failures/<h3_cell>__year=Y.fail`, (b) parses the affected granules from fragment basenames via `_FRAGMENT_BASENAME_RE` and appends them to `_merge_failed_granules.jsonl`. On the NEXT resume, `_merge_and_finalize`'s entry-time `preclean_merge_failures` unlinks the named-bad fragments + `.tmp` siblings, the CLI's `apply_merge_failures_to_logger` flips the affected granules `INDEXED → MERGE_FAILED` so Stage 1 re-extracts them. Closes the silent-data-loss path where a worker SIGKILL leaves a 0-byte parquet that the next merge would either fail on or silently produce empty output for.
- **Stage 1 failure telemetry** (v0.10+): `_write_one_granule_beam`'s `KeyError` catch site sets `stats['failure'] = _classify_load_h5_failure(exc, soc_dict)`, distinguishing `missing_var` (NASA-side schema variance — e.g. orbits O20752–O20767 of L2A lack `l2a_quality_flag_rel3_a10`) from generic `other`. Driver appends each to `tmp/partitions/_granule_failures.jsonl` (single-writer, append-only) so post-build consumers resolve `(orbit,granule,track) → failure cause` without log-grep. End-of-build advisory in `cli/gh3_build.py` groups by `(kind, product, var)` and prints a recovery recipe per class.
- **H3 levels are immutable across resumes**: `-h3r` / `-h3p` argparse defaults are `None` (not 12 / 3); fresh-build fallbacks live in the logger. `H3BuildLogger.__init__` raises `GediValidationError` if a user-passed `res`/`part` differs from the existing log's value (mirrors the `gedi_version` mismatch check). Naked resume on a non-default DB is safe — the logger loads from the log when the args are `None`.
- **Merge-failure log line format** (v0.10+): `[WARNING] Merge failed for <h3_cell>/<year>: <ErrorType>: <message> [file=<fragment_path>]`. The `[file=...]` suffix is attached inside `parquet_merge_files` by `_iter_batches_with_path`, which wraps `pq.ParquetFile` open AND `iter_batches` so truncated-body failures (raised mid-stream) also self-identify their source.
- **Build-log progress messages**: tqdm's `set_postfix` is the canonical liveness indicator during partition-write + merge; the 60-second `Streaming write: N/M done` INFO line is OFF by default (v0.10+). Set `GH3_LOG_PROGRESS=1` to re-enable for detached / tail-followed log workflows. Per-failure WARN + end-of-phase ERROR summary lines remain unconditional — those are actionable, not progress noise.
- **Worker memory hygiene**: `data/dask-worker-trim.py` preload (per-task `gc.collect` + Arrow pool release + glibc `malloc_trim`) is applied externally via `dask worker --preload …` or `DASK_CONFIG=…/dask-config-massive-build.yaml`. Treat as production setup, not a per-tool concern.
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
- **`_manifest.txt` sentinel**: short-circuits recursive globs over multi-million-file H3 databases on every resume / load. O(N) → O(1) on the GPFS metadata server. (`_soc_manifest.txt` is write-only — see the manifest section above; SOC enumeration uses `walk_soc_parallel` instead.)
- **`gedi_vars_static` cache**: replaces a remote HDF5 metadata round-trip (~50 ms hot, more cold) with a free local lookup of the per-product manifest shipped in `data/`. Hot path on S3 ETL.
- **`h3_columns_dtypes` cache**: `gh3_load()` builds its Dask `_meta` from the build-log dtypes dict, eliminating the ~50 ms – 1 s parquet sample read previously paid by every load — compounds across chained query tools (extract → aggregate → rasterize).
- **Per-file pyarrow iter for merges**: `parquet_merge_files` opens one file at a time with `pre_buffer=True` and capped scanner readahead instead of `ds.dataset` over the whole partition. Keeps per-merge memory bounded (~1 GB plateau on continental-scale builds, vs. >15 GB before v0.8.22).
- **Streaming stats during merge**: `parquet_merge_files` captures shot/date stats inline so `h3_write_metadata` can skip a 1.5–2 GB post-merge re-read.
- **H3 partition bbox from cell math**: `h3_partition_bbox()` derives the bounding box from the H3 cell ID (no geometry-column scan).
- **Granule ID from fragment basename**: build merge reconciles via regex on `O{orbit}_G{granule}_T{track}.{beam}.parquet` instead of opening parquet files.
- **Parallel reconcile Pass A** (v0.10+): `_reconcile_granules_from_disk` Pass A no longer does two driver-side recursive `glob.glob` calls over the finalized DB tree. It sources partition dirs from the `_manifest.txt` sentinel (or `os.scandir(h3_dir)` for legacy DBs), then dispatches per-partition metadata reads across workers via `parallel_map(_scan_partition_meta_granules, …)` when a client is registered. At continental scale this turns minutes of serial GPFS metadata work into seconds.
- **Post-merge in-memory derivation** (v0.10+): `_merge_and_finalize`'s tail no longer calls `glob.glob('h3_*/*/*.parquet')` or `glob.glob('h3_*/')`. The final `h3_files` + `h3_subdirs` lists come from `_derive_merged_output_paths(_merge_progress.txt, h3_dir)` — pure in-memory transform via the deterministic `h3_merge_files` naming contract (`<tmp>/h3_<p>=X/year=Y` → `<h3_dir>/h3_<p>=X/year=Y/X.Y.0.parquet`). Zero GPFS metadata ops.
- **Preventative 0-byte source-fragment drop** (v0.10+): `h3_merge_files` stats each `*.parquet` in `in_dir` and unlinks any 0-byte file before passing to `parquet_merge_files`. One `stat` per fragment, effectively free (the file open hits the same metadata). Catches the SIGKILL-leftover class B before it breaks the merge.
- **Variable-only update: inverted granule fan-out, scatter-free driver**: `_build_add_variables` reads each granule h5 **once** and fans its shots to every owning cell, then merges per-`(cell, year)` into the base parquet — `_var_fan_granule` followed by `_var_merge_cell_year`. The granule→cell relationship comes free from the metadata granule lists. The legacy per-`(cell, year)` sharding re-read every granule once per cell that listed it: a 39.75× redundancy measured on the production tree (2.64M reads for 66.5k unique granules), and h5 reads are ~93% of the runtime. Shots are routed by matching `shot_number` against the existing base parquets — never by recomputing H3 from the new product's coordinates. The driver follows the fresh-build streaming-driver's **"no `client.scatter`"** rule (see `tests/test_write_streaming.py::test_streaming_driver_completes_end_to_end`): per-task args are inlined, keeping payloads tiny and avoiding broadcast waits. Atomicity: `parquet_join_columns` writes via `.join.tmp` + `os.replace` and filters columns already present in the base file, so re-running against a partially-updated year file never duplicates columns.
- **S3 ETL vs DAAC mode selection** (`gh3_download`, May 2026 single-granule L2A bench, clean home link, earthaccess 0.18 / stock `BackgroundBlockCache` + 16 MB blocks):
  - DAAC (full granule download + local subset): **3:04** wall, +323 MB RSS, 2.32 GB on the wire.
  - S3 ETL, *broad* subset (minimal w/ `rh`, ~558 MB output): **2:49** wall — barely faster than DAAC.
  - S3 ETL, *narrow* subset (8 essentials, no `rh`, ~39 MB output): **1:50** wall, +0 MB RSS — 1.67× faster than DAAC and ~50× less data on the wire.
  - **Decision rule**: use `-s3` when the subset is narrow (< ~10% of the granule) **or** bandwidth is constrained; use plain DAAC for broad subsets on fast links. `rh` is the cost driver in L2A — its presence in the subset flips the recommendation.
  - **Block-size tuning is a trap**: increasing `block_size` from 16 MB to 32 MB regressed both subsets (+17 s on broad, +32 s and +521 MB RAM on narrow). Stock earthaccess defaults are already tuned correctly. Speculative-prefetch (`BackgroundBlockCache` default) overlaps I/O with h5py compute on the same TCP session and is doing real work even though it looks wrong on paper for HDF5's jumpy reads. Switching to plain `BlockCache` regressed wall time by 2-3×.
  - **The real S3 ETL lever is kerchunk sidecars**, not knob-twiddling — collapses the per-granule b-tree-walk RTT cost (hundreds of small GETs) to one batched async fetch. Significant code change, but the only realistic path to making S3 ETL the unconditional default.

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

## Reusable utilities (DRY anchors)

Before writing new helper code, check whether one of these covers your case. Every change shipped recently has reused these rather than inventing new abstractions:

| Helper | Module | Purpose |
|---|---|---|
| `AtomicFileWriter` | `utils.py` | Context-managed `.tmp` + `os.replace` with cleanup-on-exception. Use for *every* output write that should not leave partial files on crash. |
| `generate_manifest` / `_read_manifest` | `utils.py` | Maintain / read the `_manifest.txt` sentinel for any directory of files. |
| `smart_glob` | `utils.py` | Manifest-aware glob; falls back to filesystem walk. Use anywhere the code currently does `glob.glob(..., recursive=True)`. |
| `smart_open` / `smart_join` / `smart_exists` / `smart_isdir` | `utils.py` | Local + remote (HTTP/S3) path-agnostic I/O primitives. |
| `smart_database_exists(path)` | `utils.py` | Existence check for a database/dataset **root**. Probes the root sidecars (`gedih3_dataset.json` / `gedih3_build_log.json` / `_manifest.txt`) with a HEAD instead of `exists()` on a bucket/prefix, which object stores answer with a LIST. Use at every CLI "does the database exist" gate. |
| `smart_isfile(path)` | `utils.py` | File-vs-dataset-dir discrimination. Extension-based for remote paths (`NON_LOCAL_PREFIXES` never lists); `os.path.isfile` locally. Use instead of `smart_isdir` when the question is "single file or dataset directory?". |
| `NON_LOCAL_PREFIXES` | `utils.py` | Canonical prefix tuple for "not a local filesystem path": fsspec schemes + `gs://` + GDAL `/vsi*` virtual file systems. Shared by `resolve_path_args` and `resolve_raster_source`. Distinct from `is_remote_path`, which is the narrower fsspec-readable set. |
| `_select_dataset_files(data_files, region, dataset_path=)` | `gh3driver.py` | Region → dataset file subset, for BOTH index types (H3 via `intersect_h3_geometries` ring-1; EGI via partition-square intersection with a densified, buffered ROI). Candidates come from the filenames; pruning only engages when `_dataset_prune_is_safe` proves the names BOUND their files (sidecar partition level strictly coarser than index level, all IDs at that level, no files renamed since `partition_ids` was written). Applied in both branches of `_load_dataset`, so every query CLI inherits it. Fails safe: unprovable/unparseable → all files; empty intersection → keep one file for schema/meta. |
| `region_to_geometry(spatial)` | `utils.py` | The one region normalizer (path / `"W,S,E,N"` / `[W,S,E,N]` / GeoDataFrame / GeoSeries / shapely → EPSG:4326 geometry). Use in ANY new spatial-selection path so H3 and EGI cannot diverge. |
| `smart_open_columnar(path)` / `read_parquet_coalesced(source, …, geo=)` | `utils.py` | The remote-parquet read pair. Open with fsspec's read-ahead cache OFF, read with pyarrow `pre_buffer=True` (non-geo routed through `pq.read_table` — pandas' reader does not coalesce). 359 MB → 18 MB of server reads for a one-column projection of a 1.8 GB partition. Use for ANY remote parquet read; local reads keep the plain readers. |
| `json_read_cached(path)` | `utils.py` | Memoized `json_read` for read-only metadata sidecars (build log / dataset meta). The build log is 21 MB on a continental DB and was fetched 13× per `gh3_aggregate` over HTTP. Local paths revalidate on `(mtime_ns, size, inode)`; remote paths are held for the process. Use `json_read` for read-modify-write. |
| `endpoint_from_s3_urls(args)` | `cliutils.py` | Parses `s3://host:port/bucket/...` CLI args into an endpoint + plain `s3://bucket/...` path (bucket names cannot contain `:`). Called from `setup_storage`; `--s3-endpoint` still wins. |
| `parquet_merge_files` | `utils.py` | Streaming per-file merge with bounded memory, GeoParquet bbox, and inline stats capture. |
| `parquet_schema_add_bbox` | `utils.py` | Embed GeoParquet `bbox` metadata into a finalized parquet (used when the writer doesn't auto-embed, e.g. raw pyarrow merge paths). |
| `read_parquet_schema` | `utils.py` | Footer-only schema read, returns DataFrame of `column` + `dtype`. |
| `h5_is_valid` | `utils.py` | Cheap HDF5 header open; the canonical "is this file readable" check on resume. |
| `h3_partition_bbox` | `utils.py` | H3 cell bbox from the cell ID — no geometry scan. |
| `object_series(items)` | `utils.py` | Build an object-dtype `pd.Series` of array-likes (xarray Datasets, ndarrays). `pd.Series([...])` — even with `dtype=object` — coerces through `np.asarray`, which xarray Datasets reject; this assigns positionally instead. Use for ANY `map_partitions` result carrying non-scalars. |
| `build_vrt` / `build_vrt_xml` / `build_vrt_safe` | `raster/export.py` | VRT mosaic writers. `build_vrt` prefers `osgeo.gdal` and falls back to `build_vrt_xml` (rasterio-only, verified pixel-identical to `gdal.BuildVRT`); the fallback refuses rotated or mixed-CRS inputs rather than emit a wrong mosaic. Use `build_vrt_safe` on any path where the `.tif` tiles are the deliverable — it downgrades mosaic failure to a WARNING. `osgeo` is the package's ONLY optional import; keep it that way (see `tests/test_dependencies.py`). |
| `h3_expand_ring(cells, ring=1, valid=None)` | `h3utils.py` | The shared overhang-safety primitive: grid_disk ring expansion of a cell set, optionally restricted to a valid set (existing partitions). Use for ANY ROI→partition selection — exact polygon intersection alone silently misses boundary shots stored in neighbor partitions (child overhang ≈ 0.18 × edge). Consumers: `intersect_h3_geometries`, `egi_h3_intersection`, `geoseries_to_filter`. |
| `gh3_build_bbox_index(source)` / `_load_bbox_index(gh3_dir)` | `gh3driver.py` | The data-bbox index pair: builder materializes the true data envelope of every partition year-file from existing row-group statistics (footer-only) into the `_bbox_index.parquet` root sidecar; loader returns `{rel_key: (lon0,lat0,lon1,lat1)}` (cached, fail-safe `None` when absent). Consumers skip files a region/EGI tile provably cannot touch. Staleness impossible by construction: `_merge_and_finalize` deletes it at merge ENTRY, `gh3_doctor --fix` drops it after applied remedies, and the loader ignores any index older than the build log. |
| `_bbox_index_key(path)` / `_bbox_disjoint(b, bbox)` | `gh3driver.py` | Index key normalizer (last 3 path segments) and the inclusive-edge disjointness test. Use these — never re-derive keys or overlap logic inline. |
| `gh3_read_meta(var)` | `gh3driver.py` | Read fields from the build-log sidecar (`h3_columns`, `h3_columns_dtypes`, `h3_partition_level`, `h3_partition_ids`). Use this before reaching for a parquet open. |
| `gedi_vars_static(product, version)` | `gedidriver.py` | Cached per-product variable list from shipped manifests in `data/`. Prefer over `gedi_vars_from_h5` for NASA release files. |
| `gedi_vars_from_h5` | `gedidriver.py` | HDF5 BEAM-tree walk. Use *only* when the file may have been previously subset (compact HDF5 from S3 ETL or `gedi_subset`); otherwise use `gedi_vars_static`. |
| `manifest_check_scope(h3_logger, product_vars)` | `cli/gh3_build.py` | Regime-aware gate for `validate_soc_files`. Returns the subset of `product_vars` to validate against the shipped manifest. Empty for granules-only or explicit-list resumes (log is the contract); non-empty only for fresh builds or `default` re-requests. Apply this gate before any call to `validate_soc_files` on a resume path. |
| `_meta_from_dtype_dict` | `gh3driver.py` | Build a Dask `_meta` (Geo)DataFrame from cached dtypes — no parquet I/O. Falls back to `None` on complex types. |
| `parallel_map` | `parallel.py` | Always-parallel map of a worker fn across items via the registered dask Client + `as_completed` streaming + optional `batch_size` for >10k fan-outs. Package-wide parallelism primitive — use instead of writing new `client.map` glue. Raises `GediError` when no Client is registered (no serial fallback). Re-exported from `doctor/parallel.py` for backward compat. |
| `walk_soc_parallel` / `walk_h3db_parallel` / `walk_flat_parallel` | `parallel.py` | Year/doy-parallel, h3-partition-parallel, and flat-dir walkers that replace serial recursive globs over multi-million-file trees. Used by `write_soc_manifest`, `generate_manifest`, and the no-`--download` `gh3_build -i` path. Always parallel; fail-loud on worker exceptions (no partial manifest). |
| `check_manifest_freshness` | `parallel.py` | Constant-time mtime smoke test wired into `_read_manifest`. Logs a loud ERROR (or raises) when the manifest is older than its root dir — the producer-crash / external-population guard for R2. |
| `partition_is_empty` / `list_year_dirs` / `year_dir_is_empty` | `doctor/parallel.py` | O(1) `os.scandir`-based checks that replace `glob.glob('**/*.parquet', recursive=True)`. |
| `dask-worker-trim.py` preload | `data/` (external) | Per-task `gc.collect` + Arrow pool release + `malloc_trim`. Wire via `dask worker --preload` or `DASK_CONFIG`, not from CLIs. |
| `_iter_batches_with_path(batch_iter, path)` | `utils.py` | Wraps a pyarrow `iter_batches` generator and re-raises any exception with `[file=<path>]` appended — so truncated-body parquets that fail mid-stream self-identify. Use whenever you stream-merge per-fragment and need the source path in the error chain. |
| `_derive_merged_output_paths(merge_progress_file, h3_dir)` | `gh3builder.py` | Pure in-memory transform of `_merge_progress.txt` lines into the deterministic final parquet paths via the `h3_merge_files` naming contract. Use instead of `glob.glob('h3_*/*/*.parquet')` for any post-merge listing — zero GPFS metadata ops. |
| `_scan_partition_meta_granules(partition_dir, *, meta_filename)` | `gh3builder.py` | Worker-pickleable parser of granule IDs from PARTITION_META JSONs under one h3 partition. The unit dispatched by reconcile Pass A. |
| `preclean_merge_failures(tmp_dir)` / `apply_merge_failures_to_logger(h3_logger, tmp_dir)` | `gh3builder.py` | The merge-failure recovery loop. Preclean reads `_merge_failures/*.fail` sentinels (and `_merge_failed_granules.jsonl`), unlinks named-bad fragments + `.tmp` siblings, drops the sentinels. Apply flips affected granules `INDEXED → MERGE_FAILED` so Stage 1 re-extracts. Idempotent; both run from `_merge_and_finalize` and `cli/gh3_build.py` finalize. |
| `_classify_load_h5_failure(exc, soc_dict)` / `_append_granule_failure` / `_read_granule_failures(tmp_dir)` | `gh3builder.py` | Stage 1 failure telemetry. Classifier turns a `KeyError` / generic exception into `{'kind': 'missing_var'|'other', 'var', 'product', …}`. Append-only JSONL sidecar at `tmp/partitions/_granule_failures.jsonl`. Read it post-build for the recovery advisory or downstream forensics. |
| `_emit_merge_failure_sentinel(tmp_dir, partition_dir, exc)` / `_scan_merge_failure_sentinels(tmp_dir)` | `gh3builder.py` | Atomic per-merge-failure sentinel under `tmp/partitions/_merge_failures/<encoded>.fail`. Mirrors the `_complete/` sentinel pattern — file existence is the signal, no append-to-shared-file torn-line risk. |
| `explicit_vars_missing_in_sample(product_vars, default_products, sample_dict)` | `cli/gh3_build.py` | Pre-flight typo check. For each product with an explicit (non-`default`) variable list, opens one sample HDF5 via `gedi_vars_from_h5` and reports missing names. Wildcard patterns matching nothing surface as a single error string. Used by `_validate_existing_h5` Stage 2 to exit with code 2 before a multi-hour build hits a runtime `KeyError`. |
