# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

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
