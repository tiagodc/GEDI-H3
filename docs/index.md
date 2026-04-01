<img src="_static/gh3_logo.png" alt="gedih3 Logo" style="width: 50%; background: transparent;" />

**Turn billions of NASA GEDI LiDAR footprints into analysis-ready spatial datasets.**

[GEDI](https://gedi.umd.edu/) (Global Ecosystem Dynamics Investigation) is NASA's premier spaceborne LiDAR mission measuring forest structure globally — but its raw data is a collection of thousands of large HDF5 files organized by satellite orbit, not by geography. Spatial queries, quality filtering, and format conversion require specialized expertise and significant engineering effort.

**gedih3** handles all of this. It transforms raw GEDI data into a spatially-indexed GeoParquet database with curated variable presets and pre-configured quality filtering functionality, and provides a complete toolchain for querying, aggregating, and exporting GEDI data in formats compatible with R, Python, QGIS, and any other modern tool.

```{raw} html
<video autoplay loop muted playsinline style="width: 100%; border-radius: 8px;">
  <source src="_static/zooming.mp4" type="video/mp4">
</video>
```

---

## What is GEDI-H3?

gedih3 is built on four components that together make billion-shot GEDI analysis tractable:

| Component | Role |
|-----------|------|
| [**GEDI**](https://gedi.umd.edu/) | NASA ISS-mounted LiDAR: near-global forest height, biomass, and canopy structure at ~25 m footprints |
| [**H3**](https://h3geo.org/) | Uber's hexagonal spatial indexing system — the primary database index enabling fast spatial queries |
| [**Dask**](https://www.dask.org/) | Distributed Python computing — scales from a laptop to an HPC cluster without changing your code |
| [**earthaccess**](https://earthaccess.readthedocs.io/en/latest/) | NASA's official library for Earthdata authentication, search, and download |

---

## Key Features

- **Expert-curated presets** — `minimal` and `default` variable sets for each GEDI product, ready to use immediately
- **Pre-configured quality filtering** — default filters applied with a single flag
- **Complete CLI pipeline** — tools from download to GeoTIFF export
- **Full Python API** — chain operations in memory and customize your spatial analyses
- **H3 spatial indexing** — fast regional queries over billions of shots
- **Dask-distributed** — works on laptops, workstations, and HPC clusters
- **NASA Earthdata integration** — authenticated downloads with retry logic and S3 streaming

---

## Five-Minute Example

The core data pipeline for working with GEDI data in **gedih3** typically involves 5 steps form the command line interface (CLI):

```{raw} html
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 150" style="width:100%;max-width:1000px;display:block;margin:1.5em 0">
  <rect width="1000" height="150" fill="#0d1b2e" rx="6"/>
  <!-- Box 1 -->
  <rect x="22" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="109" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_download</text>
  <text x="109" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">NASA DAAC</text>
  <rect x="200" y="62" width="14" height="14" fill="#00e676"/>
  <!-- Box 2 -->
  <rect x="217" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="304" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_build</text>
  <text x="304" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">H3 Database</text>
  <rect x="395" y="62" width="14" height="14" fill="#00e676"/>
  <!-- Box 3 -->
  <rect x="412" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="499" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_extract</text>
  <text x="499" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">Filter &amp; Query</text>
  <rect x="590" y="62" width="14" height="14" fill="#00e676"/>
  <!-- Box 4 -->
  <rect x="607" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="694" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_aggregate</text>
  <text x="694" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">Multi-scale</text>
  <rect x="785" y="62" width="14" height="14" fill="#00e676"/>
  <!-- Box 5 -->
  <rect x="802" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="889" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_rasterize</text>
  <text x="889" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">GeoTIFF</text>
</svg>
```

```bash
# Download → Build → Extract → Aggregate → Rasterize
gh3_download  -r="-51,0,-50,1" -l2a minimal -l4a minimal
gh3_build     -r="-51,0,-50,1" -l2a minimal -l4a minimal
gh3_extract   -l agbd_l4a rh_098_l2a --quality -o extracted/
gh3_aggregate -d extracted/ -h3 8 -a mean -o aggregated/
gh3_rasterize -d aggregated/ -o rasters/ --compress ZSTD
```

:::{tip} Shortcut: steps 3–5 in one command
`gh3_build` can download and subset files directly from the cloud, internalizing the download step.
`gh3_aggregate` can read the H3 database directly — no need to run `gh3_extract` first. Add `-R` and it also rasterizes, collapsing steps 3–5 into one command.

```bash
gh3_aggregate -y -l agbd_l4a rh_098_l2a -h3 8 -a mean -R -o output/
```
:::

Or in Python, without saving intermediate files (after building using the CLI):

```python
from gedih3.config import GH3_DEFAULT_H3_DIR
import gedih3.gh3driver as gh3
from gedih3 import raster

ddf = gh3.gh3_load(source=GH3_DEFAULT_H3_DIR, columns=['agbd_l4a', 'rh_098_l2a'])
agg = gh3.gh3_aggregate(ddf, target_res=6, agg=['mean','std','count'])
raster.export_raster(raster.h3_to_raster(agg), 'agbd_mean.tif')
```

> Run any CLI tool with `-h` or `--help` for the full list of options.

---

## Navigate the Docs

- [**Getting Started**](getting-started/index.md) — Installation and a step-by-step walkthrough
- [**Concepts**](concepts/index.md) — What is GEDI? How does H3 indexing work?
- [**Building a Database**](user-guide/building-a-database.md) — The most important step: variable selection, subsetting, source modes, and performance tuning
- [**Core Functionality**](user-guide/index.md) — CLI reference, Python API guide, and data format specifications
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

user-guide/index
```

```{toctree}
:maxdepth: 1
:caption: API Reference
:hidden:

autoapi/index
```

```{toctree}
:maxdepth: 1
:caption: About
:hidden:

about
```
