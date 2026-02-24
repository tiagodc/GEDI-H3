# gedih3

**gedih3** is a Python library for accessing NASA's GEDI (Global Ecosystem Dynamics Investigation) satellite LiDAR data with H3 and EGI spatial indexing.

## Key Features

- **H3 Hexagonal Indexing** — Uber's H3 system for efficient spatial queries
- **EGI Square Pixel Indexing** — EASE Grid Index (EPSG:6933) for GEDI L4B compatibility
- **Rasterization** — H3/EGI to GeoTIFF with time-series support
- **Dask Integration** — Distributed processing for large datasets
- **NASA Earthdata Access** — Direct download or S3 streaming via earthaccess
- **Ancillary Data Fusion** — Sample rasters and join polygon attributes at shot level

## Quick Example

```python
import gedih3

# Load H3-indexed data with spatial filter
ddf = gedih3.gh3_load(
    source='/path/to/h3_database',
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',
    query='quality_flag_l2a == 1',
)

# Aggregate to H3 level 6 (~36 km²)
agg_df = gedih3.gh3_aggregate(ddf, target_res=6, agg='mean')
agg_df.compute().head()
```

## Data Flow

```
Download (gh3_download)
    ↓
Build H3 database (gh3_build)
    ↓
Extract / Aggregate (gh3_extract, gh3_aggregate)
    ↓
Rasterize (gh3_rasterize)
```

## Navigation

- [**Getting Started**](getting-started/installation.md) — Installation, configuration, and a 5-minute walkthrough
- [**CLI Reference**](cli-reference.md) — All 11 command-line tools with examples
- [**Data Formats**](data-formats.md) — H3 database structure vs. simplified datasets
- [**API Reference**](autoapi/index) — Auto-generated from source docstrings

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting-started/installation
getting-started/quickstart
getting-started/configuration
```

```{toctree}
:maxdepth: 2
:caption: Reference

cli-reference
data-formats
```

```{toctree}
:maxdepth: 1
:caption: API Reference

autoapi/index
```
