# Testing Principles and Safety Criteria

This document defines the safety, correctness, and robustness requirements for the gedih3 test suite. It serves as a reference for writing new tests, auditing existing coverage, and validating that the package produces trustworthy data across versions, updates, and operational scenarios.

gedih3 is intended for long-term maintenance. Databases built with any version of the package must remain readable and usable. All core functionality (CLI + Python API) must never corrupt data and must produce consistent, correct outputs.

---

## 1. Build Safety (`gh3_build`)

The build step is the most critical component. It creates new H3 databases and updates existing ones. The following invariants must always hold:

### 1.1 Resume without corruption
- A stopped or failed build must be resumable without re-downloading or re-parsing all HDF5 files.
- Granules already indexed (status=INDEXED) must be skipped on resume.
- Granules with status=PENDING or FAILED must be retried.
- The build log (`gedih3_build_log.json`) must never be left in a corrupted state. All JSON writes use `AtomicFileWriter` (write to temp file, then `os.replace`).

### 1.2 Safe updates on all scopes
A GH3 database can be updated by adding variables, expanding the spatial extent, expanding the temporal range, or any combination of these. Each update must:
- Preserve all existing data (no row loss, no column loss).
- Update metadata (build log, partition metadata) to reflect exactly what exists in the database.
- Be resumable if cancelled or failed mid-update.
- Record each update in `update_history` with action type and timestamp.

### 1.3 Valid data only
- No GEDI shots may be silently filtered out during build.
- No column in any partition may contain only NaN values. The `check_nan_only_columns()` utility warns at build time and export time if this occurs.
- Every `shot_number` must be unique across all partitions. The `parquet_merge_files(check_shots=True)` deduplication is the enforcement mechanism.

### 1.4 GEDI version isolation
- A GH3 database must contain data from exactly one GEDI version.
- Attempting to update a v2 database with v3 data (or vice versa) must raise `GediValidationError` with an actionable message explaining that separate databases are required.
- Different versions may coexist in the same SOC directory, but each GH3 database must be version-homogeneous.

---

## 2. Downstream Tool Safety

Extract (`gh3_extract`), aggregate (`gh3_aggregate`), and rasterize (`gh3_rasterize`) must also produce correct output:

### 2.1 No NaN-only columns
Exported or aggregated datasets must not contain columns that are entirely NaN. The export path checks for this and emits warnings.

### 2.2 No duplicate shots
Every `shot_number` in an extracted dataset must be unique.

### 2.3 Correct level auto-detection
- Tools must auto-detect H3 index/partition levels from the source database metadata (`gedih3_build_log.json` or `gedih3_dataset.json`).
- Output files must be named using the correct partition-level H3 cell IDs (or EGI defaults) unless levels are explicitly overridden by the user.
- Every shot in a file named after a given H3 partition cell must be a child of that cell at the index resolution.

### 2.4 Lazy loading
GEDI data is often too large for in-memory pipelines. All tools must load and export data lazily (via Dask) and never load everything into memory at once before exporting, unless in merge mode.
- `gh3_load(lazy=True)` must return a Dask GeoDataFrame.
- `gh3_load(lazy=False)` must return a pandas GeoDataFrame.

---

## 3. Metadata Accuracy

The build log and partition metadata must always reflect the actual state of the data on disk:

- `h3_partition_ids` must match the set of `h3_03=*` directories present.
- `h3_columns` must match the actual parquet schema (excluding transient columns like `datetime` which may be computed on read).
- `date_range` must encompass the actual temporal extent of the data.
- `granules` must list every granule with its correct status.
- `products` must list every product with its variables.

---

## 4. Error Handling

Errors and edge cases must produce clear, actionable messages that tell the user what went wrong and how to fix it. Specifically:

- Version mismatch: explain that separate databases are needed.
- Missing database path: point to the expected path.
- Missing SOC files: explain the download step.
- Invalid parameters: cite the valid range (e.g., H3 resolution 0-15, EGI level 1-12).

All errors use structured `GediError` subclasses for targeted `try/except` handling.

---

## 5. Integration Test Strategy

### 5.1 Test region and scope
- Use the 1-degree square `[-51, 0, -50, 1]` (Brazil, Amazon) as the standard test region. It is small enough to build quickly but large enough to contain multiple H3 level-3 partitions.
- Integration tests use `--s3` mode (stream from NASA S3) to be self-contained and not depend on pre-downloaded local files.
- NASA Earthdata credentials are required. Tests skip gracefully if credentials are unavailable.

### 5.2 Incremental correctness
- Start with a minimal variable subset (`-l2a minimal -l4a agbd`) and incrementally add variables on update tests.
- A database built at once (all dates) must match the content of a database built incrementally (same area + variables, but dates added in separate updates). Same shot set, same columns, same total rows.
- A build without time constraints (`-d0`/`-d1` omitted), followed by a re-run, must detect "already up to date" and not re-process data.

### 5.3 Full pipeline coverage
Integration tests should cover the complete workflow:
1. Build from scratch (fresh database)
2. Idempotent rebuild (no-op when up to date)
3. Variable-only update (schema expands, row count unchanged)
4. Temporal expansion (more rows, same columns)
5. Spatial expansion (new partitions appear)
6. Version mismatch rejection
7. Extract from built database (no duplicates, no NaN-only columns, correct file names)
8. Aggregate (row count reduced, no NaN-only columns)
9. Rasterize (valid GeoTIFF output)

---

## 6. Test Organization

### 6.1 Markers
Tests use two custom pytest markers defined in `pyproject.toml`:
- `@pytest.mark.integration` — requires external resources (NASA creds, S3 access, or a pre-built tutorial database).
- `@pytest.mark.slow` — takes more than ~30 seconds (e.g., full pipeline builds, 500-point EGI randomized tests).

### 6.2 Running tests

```bash
# Unit tests only (fast, no external resources)
pytest tests/ -m "not integration and not slow" -v

# Integration tests (requires NASA Earthdata credentials)
pytest tests/ -m integration -v

# Full suite including slow tests
pytest tests/ -v
```

### 6.3 Test file inventory

| File | Scope | What it covers |
|------|-------|----------------|
| `conftest.py` | Shared fixtures | `tmp_dir`, `sample_gdf`, `sample_ddf`, `mini_h3_database`, `mini_extracted_dataset`, and helper functions `make_gedi_parquet`, `make_partition_dir`, `make_build_log` |
| `test_validation.py` | Unit | Parameter validation: H3 resolution, EGI level, products, files, coordinates, bounding boxes |
| `test_exceptions.py` | Unit | Exception hierarchy, inheritance, custom attributes, retry logic |
| `test_cliutils.py` | Unit | CLI utilities: column filtering, naming, coordinate helpers, argument parsing, EGI level parsing |
| `test_egi_core.py` | Unit | EGI hash functions: `to_hash`, `from_hash`, `hasher`, `to_parent`, `pixels_per_tile`, config utilities |
| `test_egi_comprehensive.py` | Unit (slow) | EGI coordinate-to-hash validation with 500 random points: point-polygon intersection, hash roundtrip, outer tile consistency, inner coordinate range, tile boundary consistency |
| `test_imgutils.py` | Unit + Integration | Raster sampling at GEDI shot locations: synthetic rasters (unit), real GEDI + NASA DEM (integration) |
| `test_vecutils.py` | Unit | Vector polygon spatial join: synthetic shapefiles, column prefixing, CRS handling |
| `test_build_safety.py` | Unit + Integration | Build safety: atomic writes, shot dedup, skip column, resume logic, build log history, variable updates, idempotent rebuilds |
| `test_data_integrity.py` | Unit | Data integrity: version mismatch detection, NaN-only column detection, duplicate shots, build log metadata accuracy, atomic JSON writes, index auto-detection, file naming, lazy loading types, error messages |
| `test_merge_build_logs.py` | Unit | Build log merging: metadata aggregation, column union, incompatible version/resolution rejection |
| `test_pipeline_integration.py` | Integration | Full pipeline via CLI: extract, aggregate, rasterize, ancillary tools, S3 build from scratch, variable updates, incremental-equals-full comparison |
| `test_python_api_pipeline.py` | Unit + Integration | Python API pipeline: config, gedidriver, H3 utils, EGI, raster modules, full download-to-raster workflow |
| `test_cli_pipeline.py` | Unit + Integration | CLI argument parsing (`--help` for all tools), CLI pipeline: download, build, extract, aggregate, rasterize via subprocess |
| `test_s3_benchmark.py` | Integration | Benchmark: direct download vs S3 subset performance |

### 6.4 Shared test infrastructure
All synthetic database helpers live in `conftest.py` to avoid duplication. Test modules import them via `from conftest import make_gedi_parquet, make_partition_dir, make_build_log`. The `tmp_dir` fixture is defined once in `conftest.py` and auto-discovered by all test modules.

---

## 7. Production Safeguards Enforced by Tests

The following production-code mechanisms are verified by the test suite:

| Safeguard | Location | Tested in |
|-----------|----------|-----------|
| Atomic JSON writes | `utils.py:json_write()` via `AtomicFileWriter` | `test_data_integrity.py::TestAtomicJsonWrite` |
| Shot deduplication | `utils.py:parquet_merge_files(check_shots=True)` | `test_build_safety.py::TestParquetMergeAtomicWrite`, `test_data_integrity.py::TestDuplicateShots` |
| GEDI version check | `logger.py:H3BuildLogger.__init__()` | `test_data_integrity.py::TestVersionMismatch` |
| NaN-only column warning | `utils.py:check_nan_only_columns()` | `test_data_integrity.py::TestNanOnlyColumns` |
| Resume from FAILED/INTERRUPTED | `logger.py:H3BuildLogger` granule status tracking | `test_build_safety.py::TestResumeFromFailed` |
| Variable update skip | `gh3builder.py:_add_variables_to_year_file()` | `test_build_safety.py::TestAddVariablesResume` |
| Build log merge | `gh3builder.py:merge_build_logs()` | `test_merge_build_logs.py::TestMergeBuildLogs` |
| H3/EGI level auto-detection | `cliutils.py:get_dataset_index_info()` | `test_data_integrity.py::TestGetDatasetIndexInfo` |
| Structured exceptions | `exceptions.py` | `test_exceptions.py` |
| Parameter validation | `validation.py` | `test_validation.py` |

---

## 8. When Adding New Features

When adding new functionality to gedih3, ensure tests cover:

1. **Happy path** — the feature works as documented.
2. **Idempotency** — running the same operation twice produces the same result without side effects.
3. **Resume** — if the operation can fail mid-way, verify it can resume correctly.
4. **Metadata consistency** — any change to data on disk must be reflected in the build log or dataset metadata.
5. **No data loss** — updates must not lose existing rows, columns, or partitions.
6. **No NaN pollution** — new columns must contain real data, not just NaN.
7. **Unique shots** — `shot_number` uniqueness must be preserved.
8. **Error messages** — invalid inputs must produce clear, actionable errors using the appropriate `GediError` subclass.
