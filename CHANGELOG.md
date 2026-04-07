# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.4.0] - 2026-04-07

### Added
- Targeted quality flag selection per product and version for `--quality` in extract/aggregate: applies only the primary flag for each selected product (e.g. `l4_quality_flag` for L4A, `l2a_quality_flag_rel3` for L2A v3)
- Quality flag auto-injection during build and download: each product's primary quality flag is always included in the database regardless of the variable list specified

### Fixed
- Stale test for invalid window operation spec (extended ops 0-8 error message)

## [0.3.2] - 2026-04-07

### Fixed
- Bug fix for check exported files tasks
- Apply schema to metadata on custom aggregation

## [0.3.1] - 2026-04-07

### Added
- `range` (max − min) window operation for raster sampling (`gh3_from_img`)

## [0.3.0] - 2026-04-07

### Added
- Window operations `std`, `min`, `max`, and `count` for raster sampling (`gh3_from_img`)

### Changed
- Cap Dask worker CPU count to 20
- Revised documentation

## [0.2.0] - 2026-04-01

### Added
- Beam type selection option for filtering GEDI data by beam type
- Short beam type selector flag for convenient beam filtering

### Changed
- Updated tool comparisons documentation
- New landing page for documentation
- Added funding acknowledgements

## [0.1.6] - 2026-04-01

### Changed
- Internal variables are now kept by default during extract/aggregate operations
- Version tracker updated to include `recipe/meta.yaml` in bump-version skill

### Fixed
- Safe merging no longer drops the H3 index when concatenating partitions

## [0.1.5] - 2026-03-31

### Fixed
- Fixed `os.path.join` producing backslash-corrupted URLs on Windows for all remote paths (HTTP, S3, FTP)
- Added `smart_join` utility that uses forward slashes for remote URLs across all CLI tools and library functions
- Fixed Dask workers failing to authenticate against remote HTTP/S3 servers (storage credentials were not propagated to worker processes)
- Fixed `h3_12` index being dropped when reading multi-file H3 partitions from remote servers (`pd.concat` with `ignore_index=True`)
- Fixed lint failure from missing `smart_join` import in `list_dataset_files`

## [0.1.4] - 2026-03-27

### Fixed
- Fixed `gh3_build` early exit blocking detection of new GEDI granules when re-run with identical parameters
- Local mode now scans the SOC directory for untracked HDF5 files before deciding the database is up-to-date
- S3 and download modes bypass the early exit to query CMR for newly released data
- `gh3_build --download` no longer skips the NASA download when parameters haven't changed

## [0.1.3] - 2026-03-26

### Changed
- Moved GDAL from runtime dependencies to optional extras (`pip install gedih3[gdal]`) to prevent pip install failures
- Pinned all previously unpinned dependencies with minimum versions
- Added `gedih3.data` to setuptools packages list (fixes build warning)

### Added
- Conda-forge recipe skeleton (`recipe/meta.yaml`) for future publication

## [0.1.2] - 2026-03-26

### Fixed
- Fixed Windows file-locking bug in `parquet_join_columns`, `parquet_append_rows`, and `parquet_append_columns` where open `ParquetFile` handles prevented `os.replace` from completing
- Variable-only updates (`gh3_build -l4c wsci` on existing database) now correctly merge new columns into partition parquet files on all platforms
- Added warning when variable update completes but no partition files were modified
- Orphaned `.join.tmp` files are now cleaned up on failure instead of left on disk

### Added
- Comprehensive test suite: `test_data_integrity.py` (data safety, correctness, error messages), `test_pipeline_integration.py` (S3 end-to-end workflows), `TESTING.md` (testing principles)
- Unit tests for `parquet_join_columns` (basic join, index preservation, partial match)
- Shared test fixtures in `conftest.py` for synthetic H3 databases and persistent test output

## [0.1.1] - 2026-03-20

### Changed
- Enriched `GEDIFile` parser with `product_code`, `year`, `doy`, `mission_week`, and `suffix` attributes
- Removed deprecated `min_vars`, `default_vars_file`, and `GEDI_L2A_ESSENTIALS` from config

### Fixed
- Fixed `pyproject.toml` dependencies incorrectly nested under `[project.urls]`
- Fixed docs CI intersphinx inventory failure tolerance
- Removed license classifier superseded by PEP 639 license expression

## [0.1.0] - 2026-03-19

### Added
- Complete CLI toolchain: download, build, extract, aggregate, rasterize (11 tools)
- H3 hexagonal spatial indexing with configurable index and partition levels
- EGI square-pixel indexing (EASE-Grid 2.0, EPSG:6933) for L4B compatibility
- Dask-distributed processing for large datasets
- NASA Earthdata integration via earthaccess (download and S3 streaming)
- Expert-curated variable presets (`minimal`, `default`) for all GEDI products
- Pre-configured quality filtering
- GeoTIFF export with compression, tiling, and time-series support
- DuckDB integration for spatial SQL queries
- Ancillary data fusion: raster sampling (`gh3_from_img`) and vector joins (`gh3_from_polygon`)
- Custom aggregation functions via Python API
- Support for GEDI products: L1B, L2A, L2B, L4A, L4C
- Sphinx documentation site
