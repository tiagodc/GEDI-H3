# gedih3

**Turn billions of NASA GEDI LiDAR footprints into analysis-ready spatial datasets.**

[GEDI](https://gedi.umd.edu/) (Global Ecosystem Dynamics Investigation) is NASA's premier spaceborne LiDAR mission measuring forest structure globally — but its raw data is a collection of thousands of large HDF5 files organized by satellite orbit, not by geography. Spatial queries, quality filtering, and format conversion require specialized expertise and significant engineering effort.

**gedih3** handles all of this. It transforms raw GEDI data into a spatially-indexed GeoParquet database with expert-curated variable presets and pre-configured quality filtering, and provides a complete toolchain for querying, aggregating, and exporting this data in formats compatible with R, Python, QGIS, and any other modern tool.

> **Suggested image (hero)**: A raster map of aboveground biomass (AGBD) or canopy height over a tropical forest region, generated from gedih3 output. A strong visual result immediately communicates what the package produces.

---

## What is GEDI-H3?

gedih3 is built on four components that together make billion-shot GEDI analysis tractable:

| Component | Role |
|-----------|------|
| **GEDI** | NASA ISS-mounted LiDAR: global forest height, biomass, and canopy structure at ~25 m footprints |
| **H3** | Uber's hexagonal spatial indexing system — the primary database index enabling fast regional queries |
| **Dask** | Distributed Python computing — scales from a laptop to an HPC cluster without changing your code |
| **earthaccess** | NASA's official library for Earthdata authentication, search, and download |

---

## Key Features

- **Expert-curated presets** — `minimal` and `default` variable sets for each GEDI product, ready to use immediately
- **Pre-configured quality filtering** — scientifically-validated filters applied with a single flag
- **Complete CLI pipeline** — 11 tools from download to GeoTIFF export
- **Full Python API** — chain operations in memory; pass custom Python functions as aggregators
- **H3 spatial indexing** — fast regional queries over billions of shots
- **Dask-distributed** — works on laptops, workstations, and HPC clusters
- **NASA Earthdata integration** — authenticated downloads with retry logic and S3 streaming

---

## Five-Minute Example

```bash
# Download → Build → Extract → Aggregate → Rasterize
gh3_download  -r "-51,0,-50,1" -l2a minimal -l4a minimal
gh3_build     -r "-51,0,-50,1" -l2a minimal -l4a minimal
gh3_extract   -q -l agbd_l4a rh_098_l2a -o extracted/
gh3_aggregate -d extracted/ -h3 6 -a mean -o aggregated/
gh3_rasterize -d aggregated/ -o rasters/ --compress LZW
```

Or in Python, without saving intermediate files:

```python
import gedih3.gh3driver as gh3
from gedih3 import raster

ddf = gh3.gh3_load(source='~/gedi_data/h3/', columns=['agbd_l4a', 'rh_098_l2a'])
agg = gh3.gh3_aggregate(ddf, target_res=6, agg='mean').compute()
raster.export_raster(raster.h3_to_raster(agg), 'agbd_mean.tif')
```

> Run any CLI tool with `--help` for the full list of options.

---

## Navigate the Docs

- [**Getting Started**](getting-started/index.md) — Installation and a step-by-step walkthrough
- [**Concepts**](concepts/index.md) — What is GEDI? How does H3 indexing work? When to use EGI?
- [**Core Functionality**](reference/index.md) — CLI reference, Python API guide, and data format specifications
- [**API Reference**](autoapi/index) — Auto-generated documentation from source code

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

getting-started/index
```

```{toctree}
:maxdepth: 2
:caption: Concepts
:hidden:

concepts/index
```

```{toctree}
:maxdepth: 2
:caption: Core Functionality
:hidden:

reference/index
```

```{toctree}
:maxdepth: 1
:caption: API Reference
:hidden:

autoapi/index
```
