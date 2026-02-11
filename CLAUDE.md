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

## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for any non-trivial task (3+ steps, multi-file change, architectural decision, production-impacting behavior).
- Include verification steps in the plan (not as an afterthought).
- If new information invalidates the plan: **stop**, update the plan, then continue.
- Write a crisp spec first when requirements are ambiguous (inputs/outputs, edge cases, success criteria).

### 2. Subagent Strategy (Parallelize Intelligently)
- Use subagents to keep the main context clean and to parallelize:
  - repo exploration, pattern discovery, test failure triage, dependency research, risk review.
- Give each subagent **one focused objective** and a concrete deliverable:
  - "Find where X is implemented and list files + key functions" beats "look around."
- Merge subagent outputs into a short, actionable synthesis before coding.

### 3. Incremental Delivery (Reduce Risk)
- Prefer **thin vertical slices** over big-bang changes.
- Land work in small, verifiable increments:
  - implement → test → verify → then expand.
- When feasible, keep changes behind:
  - feature flags, config switches, or safe defaults.

### 4. Self-Improvement Loop
- After any user correction or a discovered mistake:
  - add a new entry to `tasks/lessons.md` capturing:
    - the failure mode, the detection signal, and a prevention rule.
- Review `tasks/lessons.md` at session start and before major refactors.

### 5. Verification Before "Done"
- Never mark complete without evidence:
  - tests, lint/typecheck, build, logs, or a deterministic manual repro.
- Compare behavior baseline vs changed behavior when relevant.
- Ask: "Would a staff engineer approve this diff and the verification story?"

### 6. Demand Elegance (Balanced)
- For non-trivial changes, pause and ask:
  - "Is there a simpler structure with fewer moving parts?"
- If the fix is hacky, rewrite it the elegant way **if** it does not expand scope materially.
- Do not over-engineer simple fixes; keep momentum and clarity.

### 7. Autonomous Bug Fixing (With Guardrails)
- When given a bug report:
  - reproduce → isolate root cause → fix → add regression coverage → verify.
- Do not offload debugging work to the user unless truly blocked.
- If blocked, ask for **one** missing detail with a recommended default and explain what changes based on the answer.

---

## Task Management (File-Based, Auditable)

1. **Plan First**
   - Write a checklist to `tasks/todo.md` for any non-trivial work.
   - Include "Verify" tasks explicitly (lint/tests/build/manual checks).
2. **Define Success**
   - Add acceptance criteria (what must be true when done).
3. **Track Progress**
   - Mark items complete as you go; keep one "in progress" item at a time.
4. **Checkpoint Notes**
   - Capture discoveries, decisions, and constraints as you learn them.
5. **Document Results**
   - Add a short "Results" section: what changed, where, how verified.
6. **Capture Lessons**
   - Update `tasks/lessons.md` after corrections or postmortems.

---

## Communication Guidelines

### 1. Be Concise, High-Signal
- Lead with outcome and impact, not process.
- Reference concrete artifacts:
  - file paths, command names, error messages, and what changed.
- Avoid dumping large logs; summarize and point to where evidence lives.

### 2. Ask Questions Only When Blocked
When you must ask:
- Ask **exactly one** targeted question.
- Provide a recommended default.
- State what would change depending on the answer.

### 3. State Assumptions and Constraints
- If you inferred requirements, list them briefly.
- If you could not run verification, say why and how to verify.

### 4. Show the Verification Story
- Always include:
  - what you ran (tests/lint/build), and the outcome.
- If you didn't run something, give a minimal command list the user can run.

### 5. Avoid "Busywork Updates"
- Don't narrate every step.
- Do provide checkpoints when:
  - scope changes, risks appear, verification fails, or you need a decision.

---

## Context Management Strategies

### 1. Read Before Write
- Before editing:
  - locate the authoritative source of truth (existing module/pattern/tests).
- Prefer small, local reads (targeted files) over scanning the whole repo.

### 2. Keep a Working Memory
- Maintain a short running "Working Notes" section in `tasks/todo.md`:
  - key constraints, invariants, decisions, and discovered pitfalls.
- When context gets large:
  - compress into a brief summary and discard raw noise.

### 3. Minimize Cognitive Load in Code
- Prefer explicit names and direct control flow.
- Avoid clever meta-programming unless the project already uses it.
- Leave code easier to read than you found it.

### 4. Control Scope Creep
- If a change reveals deeper issues:
  - fix only what is necessary for correctness/safety.
  - log follow-ups as TODOs/issues rather than expanding the current task.

---

## Error Handling and Recovery Patterns

### 1. "Stop-the-Line" Rule
If anything unexpected happens (test failures, build errors, behavior regressions):
- stop adding features
- preserve evidence (error output, repro steps)
- return to diagnosis and re-plan

### 2. Triage Checklist (Use in Order)
1. **Reproduce** reliably (test, script, or minimal steps).
2. **Localize** the failure (which layer: UI, API, DB, network, build tooling).
3. **Reduce** to a minimal failing case (smaller input, fewer steps).
4. **Fix** root cause (not symptoms).
5. **Guard** with regression coverage (test or invariant checks).
6. **Verify** end-to-end for the original report.

### 3. Safe Fallbacks (When Under Time Pressure)
- Prefer "safe default + warning" over partial behavior.
- Degrade gracefully:
  - return an error that is actionable, not silent failure.
- Avoid broad refactors as "fixes."

### 4. Rollback Strategy (When Risk Is High)
- Keep changes reversible:
  - feature flag, config gating, or isolated commits.
- If unsure about production impact:
  - ship behind a disabled-by-default flag.

### 5. Instrumentation as a Tool (Not a Crutch)
- Add logging/metrics only when they:
  - materially reduce debugging time, or prevent recurrence.
- Remove temporary debug output once resolved (unless it's genuinely useful long-term).

---

## Engineering Best Practices

### 1. API / Interface Discipline
- Design boundaries around stable interfaces:
  - functions, modules, components, route handlers.
- Prefer adding optional parameters over duplicating code paths.
- Keep error semantics consistent (throw vs return error vs empty result).

### 2. Testing Strategy
- Add the smallest test that would have caught the bug.
- Prefer:
  - unit tests for pure logic,
  - integration tests for DB/network boundaries,
  - E2E only for critical user flows.
- Avoid brittle tests tied to incidental implementation details.

### 3. Type Safety and Invariants
- Avoid suppressions (`any`, ignores) unless the project explicitly permits and you have no alternative.
- Encode invariants where they belong:
  - validation at boundaries, not scattered checks.

### 4. Dependency Discipline
- Do not add new dependencies unless:
  - the existing stack cannot solve it cleanly, and the benefit is clear.
- Prefer standard library / existing utilities.

### 5. Security and Privacy
- Never introduce secret material into code, logs, or chat output.
- Treat user input as untrusted:
  - validate, sanitize, and constrain.
- Prefer least privilege (especially for DB access and server-side actions).

### 6. Performance (Pragmatic)
- Avoid premature optimization.
- Do fix:
  - obvious N+1 patterns, accidental unbounded loops, repeated heavy computation.
- Measure when in doubt; don't guess.

### 7. DRY and Code Reuse
- **Before writing new code**: Search for existing utilities in `cliutils.py`, `utils.py`, `validation.py`, and `egi/` modules.
- **Common utilities belong in shared modules**: If functionality is needed in 2+ places, extract to `cliutils.py` or `utils.py`.
- **Use existing patterns**: Check how similar operations are done elsewhere in the codebase.
- **Avoid copy-paste**: If you find yourself copying code, refactor into a shared function.

---

## Git and Change Hygiene

- Keep commits atomic and describable; avoid "misc fixes" bundles.
- Don't rewrite history unless explicitly requested.
- Don't mix formatting-only changes with behavioral changes unless the repo standard requires it.
- Treat generated files carefully:
  - only commit them if the project expects it.

---

## Definition of Done (DoD)

A task is done when:
- Behavior matches acceptance criteria.
- Tests/lint/typecheck/build (as relevant) pass or you have a documented reason they were not run.
- Risky changes have a rollback/flag strategy (when applicable).
- The code follows existing conventions and is readable.
- A short verification story exists: "what changed + how we know it works."

---

## Templates

### Plan Template (Paste into `tasks/todo.md`)
```markdown
- [ ] Restate goal + acceptance criteria
- [ ] Locate existing implementation / patterns
- [ ] Design: minimal approach + key decisions
- [ ] Implement smallest safe slice
- [ ] Add/adjust tests
- [ ] Run verification (lint/tests/build/manual repro)
- [ ] Summarize changes + verification story
- [ ] Record lessons (if any)
```

### Bugfix Template (Use for Reports)
```markdown
- Repro steps:
- Expected vs actual:
- Root cause:
- Fix:
- Regression coverage:
- Verification performed:
- Risk/rollback notes:
```

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
| `-egi INDEX[:PART]` | Use EGI indexing (e.g., `-egi 1` or `-egi 1:12` for index:partition levels) |
| `-R, --rasterize` | Also export rasters after aggregation (gh3_aggregate only) |

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
# Also suppresses Dask warnings unless in DEBUG mode (-vv)
logger = setup_logging(args, __name__)

# Print tool banner
print_banner("GEDI Tool Name", logger=logger)

# Print success message with banner formatting
print_success("Operation complete", logger=logger)

# Configure and validate database path
configure_database_path(args, logger=logger)
```

**Dask Warning Suppression**: `setup_logging()` automatically suppresses noisy Dask/distributed warnings when not in DEBUG mode (`-vv`). Warnings about "Sending large graph", "Consider loading the data", and shuffle warnings are hidden at INFO and ERROR levels.

### Data Loading

```python
from gedih3.cliutils import load_data_from_source, get_numeric_columns, h3_col_name

# Auto-detect and load from H3 database, simplified dataset, or parquet directory
ddf = load_data_from_source(database_path, columns, region, query, logger)

# Get numeric columns for aggregation (auto-excludes internal columns)
numeric_cols = get_numeric_columns(ddf)

# Get H3 column name for a level
col = h3_col_name(6)  # Returns 'h3_06'
```

### Column Filtering

Internal/partition columns are automatically filtered out from data operations:

```python
from gedih3.cliutils import is_internal_column, filter_data_columns, get_rasterizable_columns

# Check if a column is internal (h3_XX, egiXX, _egi_x, _egi_y, shot_number)
is_internal_column('h3_03')  # True
is_internal_column('agbd_l4a')  # False

# Filter out internal columns from a list
data_cols = filter_data_columns(['h3_03', 'agbd_l4a', 'rh_098_l2a'])
# Returns: ['agbd_l4a', 'rh_098_l2a']

# Get columns suitable for rasterization (numeric, non-internal)
raster_cols = get_rasterizable_columns(ddf)
```

**Internal Column Patterns**:
- `h3_XX` - H3 partition columns (e.g., `h3_03`, `h3_06`)
- `egiXX` - EGI index columns (e.g., `egi06`, `egi12`)
- `_egi_x`, `_egi_y` - Internal EGI coordinate columns
- `shot_number*` - Shot identifier columns

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

## Known Limitations

### Memory Management with Large Datasets

PyArrow's unmanaged memory can accumulate during large Parquet operations. For production workloads with many partition files, use the aggressive memory configuration:

```bash
# Use aggressive memory management config
gh3_build --dask-config dask-config-aggressive-memory.yaml -r "W,S,E,N" ...
```

This configuration enables:
- Automatic worker restarts every 15 minutes (prevents memory accumulation)
- Lower memory thresholds (target: 10%, pause: 80%)
- Smaller chunk sizes (64 MiB)

### Python Version Requirement

The current `pyproject.toml` specifies `>=3.13`. For HPC environments with older Python:
- The codebase has been tested with Python 3.10+
- Update `pyproject.toml` if needed for your environment
- No 3.13-specific features are used

### Coordinate Systems

- **H3 indexing**: Uses WGS84 (EPSG:4326) internally
- **EGI indexing**: Uses EASE-Grid 2.0 (EPSG:6933) for GEDI L4B compatibility
- All output GeoDataFrames are in EPSG:4326 unless otherwise specified
- EGI rasters maintain EPSG:6933 for native L4B alignment

### Large Dataset Considerations

For billion-row datasets:
- Use `from_map=True` (default) for efficient partition loading
- Enable `map_partitions` aggregation (default) to avoid shuffling
- Consider processing by time period or spatial region to manage memory
- Monitor Dask dashboard for worker memory usage

## Development Notes

### DEBUG Mode in CLI Tools

CLI tools contain DEBUG blocks for development testing. These are disabled by default (`DEBUG=False`). For development:

```python
# At top of CLI tool file
DEBUG=True  # Enable to use hardcoded test paths
```

**Warning**: DEBUG blocks contain site-specific paths (`/gpfs/...`). Do not commit with `DEBUG=True`.

### Running Tests

```bash
# Unit tests (fast, no network required)
pytest tests/ -m "not integration and not slow"

# Integration tests (requires NASA Earthdata credentials)
pytest tests/ -m integration

# Full test suite
pytest tests/ -v

# Run specific test file
pytest tests/test_egi_comprehensive.py -v
```

### Environment Setup

```bash
# Create conda environment
conda env create -f environment.yml
conda activate gedih3

# Install in editable mode for development
pip install -e .

# Configure NASA credentials (required for downloads)
python -c "import earthaccess; earthaccess.login()"
```

### Configuration Priority

Configuration is loaded in this priority order:
1. Command-line arguments (highest priority)
2. Environment variables (`GH3_DEFAULT_*`)
3. `~/.gedih3.env` file
4. Package defaults in `config.py` (lowest priority)

## Troubleshooting

### Common Issues

**"Sending large graph" warnings**
```python
# Suppressed automatically in INFO mode (-v)
# For DEBUG mode (-vv), increase threshold:
dask.config.set({'distributed.admin.large-graph-warning-threshold': '500MB'})
```

**KeyError with internal columns (h3_XX, egiXX)**
```python
# Use column filtering utilities
from gedih3.cliutils import filter_data_columns, get_rasterizable_columns

# Filter out internal columns from a list
data_cols = filter_data_columns(['h3_03', 'agbd_l4a', 'rh_098_l2a'])
# Returns: ['agbd_l4a', 'rh_098_l2a']

# Get numeric columns suitable for aggregation/rasterization
numeric_cols = get_rasterizable_columns(ddf)
```

**Out of memory with large databases**
```bash
# Use aggressive memory config
gh3_build --dask-config dask-config-aggressive-memory.yaml ...

# Or reduce workers and increase per-worker memory
gh3_build -N 4 -M 16GB ...
```

**NASA Earthdata authentication errors**
```bash
# Verify credentials are valid
python -c "import earthaccess; earthaccess.login()"

# Check ~/.netrc file exists and has correct format:
# machine urs.earthdata.nasa.gov
#     login YOUR_USERNAME
#     password YOUR_PASSWORD
```

**Empty results from spatial filter**
```python
# Verify region format
# Bbox format: "W,S,E,N" (West,South,East,North)
# Example: "-51,0,-50,1" for a 1x1 degree area in Brazil

# Check if database has data in that region
gh3_read_schema /path/to/database/gedih3_build_log.json
```

**Parquet schema mismatch errors**
```bash
# Inspect schema of existing files
gh3_read_schema /path/to/file.parquet

# Check database build log for expected schema
gh3_read_schema /path/to/database/gedih3_build_log.json
```

## Claude Code Sub-Agents

Five specialized sub-agents are configured in `.claude/agents/` for domain-specific work:

| Agent | File | Use For |
|-------|------|---------|
| `core-pipeline` | `.claude/agents/core-pipeline.md` | Download, build, extract, aggregate workflows |
| `raster-egi` | `.claude/agents/raster-egi.md` | Spatial indexing, rasterization, CRS transforms |
| `cli-deployment` | `.claude/agents/cli-deployment.md` | CLI tools, configuration, deployment |
| `testing-qa` | `.claude/agents/testing-qa.md` | Test coverage, validation, benchmarking, **DRY/redundancy audits** |

### DRY and Redundancy Checking

The `testing-qa` agent is responsible for checking code redundancy and DRYness:
- **Audit for duplicate code**: Before implementing new features, check if similar functionality exists
- **Identify refactoring opportunities**: Find code that could be extracted to shared utilities
- **Enforce reuse patterns**: Ensure new code uses existing utilities from `cliutils.py`, `utils.py`, `validation.py`
- **Cross-module consistency**: Verify similar operations across modules use the same underlying functions

**Invoke for DRY audits**:
```
> Use the testing-qa agent to check for code redundancy in the EGI module
> Have testing-qa audit cliutils.py for functions that should be reused elsewhere
> Ask testing-qa to identify duplicate logic across CLI tools
```

### How to Use Sub-Agents

**Automatic delegation**: Claude auto-delegates based on task description:
```
> Implement H3 database building with resume
  → Delegates to core-pipeline agent

> Add time-series rasterization for EGI
  → Delegates to raster-egi agent
```

**Explicit invocation**:
```
> Use the core-pipeline agent to debug this Dask memory issue
> Have the raster-egi agent review the CRS transform logic
> Ask the testing-qa agent to write integration tests
```

**Chaining agents**:
```
> First, use core-pipeline agent to implement the feature.
> Then use testing-qa agent to write tests for it.
> Finally, have cli-deployment agent create a CLI tool.
```

**Parallel research**:
```
> In parallel:
> - Have core-pipeline agent investigate the memory issue
> - Have testing-qa agent check which tests are failing
```

### List Available Agents
```bash
claude /agents
```
