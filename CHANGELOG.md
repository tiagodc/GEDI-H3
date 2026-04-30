# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.7.4] - 2026-04-29

### Changed
- `validate_soc_files` now reads the per-product per-version variable manifests that already ship in `src/gedih3/data/GEDI*_DATASETS_*.txt` instead of opening every HDF5 file to enumerate datasets. Drops a multi-minute scan to ~8 ms while preserving the typo-catching contract. Removes the dask bag, `futures_of`/`as_completed` plumbing, and `Validating SOC files` tqdm bar that v0.7.2 added.

### Fixed
- `soc_file_tree` no longer crashes (`AttributeError: Can only use .str accessor with string values!`) when the glob returns zero matches; returns an empty result early.

## [0.7.3] - 2026-04-29

### Added
- `--exclude PATTERN` flag in `gh3_build` (repeatable, fnmatch-style) and matching `exclude=` kwarg on `soc_file_tree`, `validate_soc_files`, and `build_h3db`. Drops files whose basename matches any pattern at the discovery step, so internal/non-release HDF5 variants (e.g. `*_SGS.h5`) can be skipped while keeping the variants you do want (e.g. `*_7algs.h5`). Default behaviour unchanged when the flag is omitted.

### Fixed
- `validate_soc_files` no longer aborts the whole validation pass on a single corrupt or truncated HDF5 file. The per-file worker (`check_soc_file_vars`) now wraps the HDF5 read in `try/except`, logs a warning naming the file, and returns no products for it instead of raising — mirrors the corruption tolerance applied to `dask_h5_merged` in commit `91bb04f`.

## [0.7.2] - 2026-04-29

### Fixed
- `gh3_build` startup phases between "using only existing data" and the partition build no longer silent: added "Validating product variables..." and "Listing SOC files..." log lines, swapped the per-file `dask.delayed` validation for a batched dask-bag with a `Validating SOC files: N/M [batch]` tqdm bar, and wrapped the granule-metadata-pivot loop with a `Parsing granule metadata` tqdm bar. Eliminates multi-minute silence on directories with hundreds of thousands of HDF5 files.

## [0.7.1] - 2026-04-29

### Fixed
- `gh3_build` SOC-check phase: replace `dask.distributed.progress()` (silent in non-TTY/SSH/log-redirected sessions) with a `tqdm` + `as_completed` bar over the bag's futures, so the "Checking SOC files" stage now shows reliable batch-level progress in every terminal context.

## [0.7.0] - 2026-04-29

### Added
- Progress bars for previously-silent CLI long-running loops: `gh3_doctor` partition scans (backfill, parquet_health, metadata, orphans, soc_health), `gh3_update` per-file partition loops, the `gh3_aggregate` time-series window loop, and `gh3builder` parquet metadata finalization. New shared `cliutils.progress_iter()` helper wraps `tqdm` with `logging_redirect_tqdm` so log lines no longer clobber the bar.

### Fixed
- `gh3_build`: tolerate faulty SOC files and isolate version handling
- CI: blank lines around bullet list in `parquet_fill_columns` docstring

## [0.6.0] - 2026-04-25

### Added
- `gh3_doctor`: new CLI tool that audits and (optionally) heals a gedih3 database. Six diagnoses cover NaN-fill backfill, leftover temp/empty-dir cleanup, stuck recovery flags, log↔disk partition reconciliation, partition-meta and dataset-manifest health, parquet corruption / duplicate `shot_number` / cross-partition schema drift, and SOC HDF5 health. `--check` is read-only by default; `--fix` applies safe remedies. `--s3` lets backfill fetch missing source files via NASA S3 ETL temp. `--online` queries NASA CMR and emits concrete `gh3_download` / `gh3_doctor --fix backfill` / `gh3_build` recommendations classified by gap type.
- `H3BuildLogger` gains an additive per-granule per-product status map (`granule.products`) plus `get_granule_product_status`, `get_product_gaps`, and `mark_granule_product` helpers. Lazy in-memory upgrade for legacy logs; on-disk format only changes after the next `save_log()`. `set_post_build_info` derives per-product status from existing partition meta `columns` — no edits to `gh3builder.py`, `gh3_build.py`, or `gh3_download.py`.
- New `gedih3.doctor` package houses the runner, registry, shared inspectors, streaming `parquet_fill_columns` (combine_first variant of `parquet_join_columns`) and `parquet_dedup_partition`, the CMR upstream check, and one module per diagnosis.

## [0.5.5] - 2026-04-24

### Added
- `gedi_download`: new `granule_names` parameter forwards a filename list to the CMR search (`.h5` suffix is stripped since CMR's `readable_granule_name` matches the stem), so callers that already know which granules to fetch no longer need to enumerate the full release

### Fixed
- `gedi_download` `on_granule_complete` callback now receives the file path in `granule_info_dict['path']` (target SOC path on PENDING, actual downloaded path on DOWNLOADED, None on FAILED) — previously callers had no way to reach the file they just landed
- `GEDIAccessor.search_data` DOI-fallback warning now names every dropped filter (`short_name` and `version`) and the DOI being used, instead of silently broadening across versions

## [0.5.4] - 2026-04-20

### Changed
- `gh3_aggregate`: more flexible callable handling in `gh3driver` aggregation pipeline

## [0.5.3] - 2026-04-15

### Fixed
- `gh3_build_ducklake`: interactive `input()` confirmation was silently bypassed when invoking via the installed CLI entry point; replaced with standard argparse CLI (`-d`/`--database`, `-t`/`--tmpdir`, `-v`/`-Q`) matching all other tools

### Changed
- `gh3_build_ducklake` now uses `setup_logging`, `print_banner`, `print_success`, and `cli_exception_handler` from `cliutils`, and `get_file_list()` is parameterised to accept a custom database path

## [0.5.2] - 2026-04-15

### Fixed
- Quality filtering with `-l/--list` flag now infers products from variable name suffixes (e.g. `wsci_l4c` → L4C) and applies only the minimal flags from `_PRODUCT_QUALITY_FLAGS`, instead of brute-force applying every `quality_flag` column in the dataset

## [0.5.1] - 2026-04-11

### Fixed
- Ruff lint: moved `_PRODUCT_QUALITY_FLAGS` to top-level import in `gh3builder.py` to fix F823 (local variable referenced before assignment caused by inline re-import shadowing module-level binding)
- Ruff lint: corrected two malformed `# noqa` directives to use explicit rule codes (`B018`, `F401`)

## [0.5.0] - 2026-04-11

### Changed
- Revised `default` variable presets for all products and GEDI versions (L1B, L2A, L2B, L4A, L4C): updated variable selections to match current HDF5 file contents, corrected variable counts, and aligned minimal/default presets with `config.py`
- Updated documentation to reflect new variable lists in `docs/concepts/variable-presets.md`

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
