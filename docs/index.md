<img src="_static/gh3_logo.png" alt="gedih3 Logo" style="width: 50%; background: transparent;" />

# Turn billions of NASA GEDI footprints into analysis-ready datasets.

NASA's [GEDI](https://gedi.umd.edu/) has measured forest height, biomass, and canopy structure across the planet --- billions of individual laser measurements spanning every continent except Antarctica. But the raw data is stored in thousands of complex files organized by orbit, not by location. Getting from "I want a biomass map of my study area" to actually having one requires navigating deeply nested file formats, applying multi-criteria quality filters, and processing terabytes of data.

**gedih3** handles all of that. It transforms raw GEDI data into a spatial database you can query by region, filter for quality with a single flag, aggregate to any scale, and export to GeoTIFF, GeoParquet, or any format your tools can read --- from the command line or Python.

```bash
pip install gedih3
```

...or with conda, recommended for HPC and shared clusters:

```bash
git clone https://github.com/tiagodc/GEDI-H3.git
cd GEDI-H3
conda env create -n gedih3 -f environment.yml 
conda activate gedih3
```

::::{grid} 2
:gutter: 2

:::{grid-item}
```{button-ref} getting-started/installation
:color: primary
:expand:

Get Started
```
:::

:::{grid-item}
```{button-ref} getting-started/quickstart
:color: secondary
:outline:
:expand:

5-Minute Example
```
:::

::::

```{raw} html
<video autoplay loop muted playsinline style="width: 100%; border-radius: 8px; margin-top: 1.5em;">
  <source src="_static/zooming.mp4" type="video/mp4">
</video>
<p style="text-align: center; color: #8ba4b8; font-size: 0.9em; margin-top: 0.5em;">
  GEDI canopy height data aggregated to H3 hexagons at multiple scales, built and exported entirely with gedih3.
</p>
```

---

## Working with GEDI data is harder than it should be

GEDI is one of the most important datasets for understanding forests at a global scale --- but working with it requires solving several hard engineering problems before you can do any science. These problems affect everyone from PhD students writing their first analysis to national forest inventory programs.

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`globe;1.2em` Organized by orbit, not by location
GEDI files are organized by time, not geography. Querying a study area means scanning thousands of orbits --- most containing no relevant data.
:::

:::{grid-item-card} {octicon}`file-code;1.2em` Hundreds of variables, buried in HDF5
Each product has hundreds of variables across 8 laser beams in nested HDF5 files. Knowing which ones matter requires domain expertise most users don't have.
:::

:::{grid-item-card} {octicon}`alert;1.2em` Quality flags easy to miss or misapply
Each product ships with different quality flags. Skipping or misapplying them produces plausible but silently biased results.
:::

:::{grid-item-card} {octicon}`graph;1.2em` Scale: billions of measurements
The full archive is terabytes across thousands of files. Without spatial indexing and distributed processing, simple analyses take hours or fail entirely.
:::

::::

---

## What gedih3 does about it

gedih3 is a Python library and CLI toolchain that handles the entire pipeline from raw NASA data to analysis-ready output.

::::{grid} 1 2 3 3
:gutter: 3

:::{grid-item-card} {octicon}`database;1.2em` Spatial database from day one
:link: concepts/h3-indexing
:link-type: doc

H3-indexed, region-partitioned database built from raw GEDI files. Queries touch only relevant tiles. Builds incrementally; interrupted builds resume automatically.
:::

:::{grid-item-card} {octicon}`list-unordered;1.2em` Expert-curated variable presets
:link: concepts/variable-presets
:link-type: doc

Use `minimal` or `default` presets designed by remote sensing scientists. Output is flat GeoParquet --- readable by pandas, R, QGIS, DuckDB, or any modern tool.
:::

:::{grid-item-card} {octicon}`check-circle;1.2em` Automated quality filtering
:link: user-guide/building-a-database
:link-type: doc

A single `--quality` flag enforces all product-specific flags at once. Add custom pandas filters for beams, sensitivity thresholds, or time ranges with `--query`.
:::

:::{grid-item-card} {octicon}`beaker;1.2em` Flexible aggregation

CLI shorthands (`mean`, `p95`) or any Python callable per hexagon. Partition-local grouping avoids data shuffling --- scales linearly with data size.
:::

:::{grid-item-card} {octicon}`rocket;1.2em` Scales from laptop to cluster

Built on [Dask](https://www.dask.org/). Streams HDF5 beam-by-beam within constrained RAM on a laptop; connects to an existing Dask scheduler on HPC with no code changes.
:::

:::{grid-item-card} {octicon}`tools;1.2em` Complete pipeline
:link: user-guide/cli-reference
:link-type: doc

Download, build, extract, aggregate, fuse with external data, and export to GeoTIFF or GeoParquet --- all from the CLI or Python API. All GEDI products (L1B–L4C).
:::

::::

---

## Get started in 5 minutes

::::::{tab-set}

:::::{tab-item} Just make it work

Three commands --- no configuration, no decisions. This downloads GEDI data for a 1-by-1 degree area in the Amazon, builds a spatial database, applies quality filtering, aggregates to ~5 km hexagons, and exports a GeoTIFF.

```bash
# 1. Install
git clone https://github.com/tiagodc/GEDI-H3.git && cd GEDI-H3
conda env create -f environment.yml && conda activate gedih3

# 2. Build a sample database (downloads data automatically)
gh3_build -r "-51,0,-50,1" -l2a minimal -l4a minimal -dl

# 3. Get a biomass map
gh3_aggregate -y -l agbd_l4a -h3 7 -a mean -R -o my_first_map/
```

Open `my_first_map/*.tif` in QGIS, R, or Python. Done.

:::{note}
You need a free [NASA Earthdata account](https://urs.earthdata.nasa.gov/). On first run, `earthaccess` will prompt you to log in. See [Installation](getting-started/installation.md) for details.
:::

:::::

:::::{tab-item} I want to customize

The pipeline has 5 discrete steps. Each can be configured independently --- choose your region, products, variable sets, time range, aggregation level, and output format.

```{raw} html
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 150" style="width:100%;max-width:1000px;display:block;margin:1.5em 0">
  <rect width="1000" height="150" fill="#0d1b2e" rx="6"/>
  <rect x="22" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="109" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_download</text>
  <text x="109" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">NASA DAAC</text>
  <rect x="200" y="62" width="14" height="14" fill="#00e676"/>
  <rect x="217" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="304" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_build</text>
  <text x="304" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">H3 Database</text>
  <rect x="395" y="62" width="14" height="14" fill="#00e676"/>
  <rect x="412" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="499" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_extract</text>
  <text x="499" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">Filter &amp; Query</text>
  <rect x="590" y="62" width="14" height="14" fill="#00e676"/>
  <rect x="607" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="694" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_aggregate</text>
  <text x="694" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">Multi-scale</text>
  <rect x="785" y="62" width="14" height="14" fill="#00e676"/>
  <rect x="802" y="18" width="175" height="68" fill="#0f2340" stroke="#00e676" stroke-width="2" rx="2"/>
  <text x="889" y="58" text-anchor="middle" fill="#00e676" font-size="16" font-weight="700" font-family="'Courier New',monospace">gh3_rasterize</text>
  <text x="889" y="118" text-anchor="middle" fill="#8ba4b8" font-size="13" font-family="sans-serif">GeoTIFF</text>
</svg>
```

```bash
# Choose your region, products, variable sets, and time range
gh3_download  -r study_area.shp -l2a default -l4a default -l2b minimal -t0 2020-01-01 -t1 2023-12-31
gh3_build     -r study_area.shp -l2a default -l4a default -l2b minimal
gh3_extract   -y -l agbd_l4a rh_098_l2a cover_l2b -r study_area.shp -o extracted/
gh3_aggregate -d extracted/ -h3 6 -a mean std count -o aggregated/
gh3_rasterize -d aggregated/ -o rasters/ --compress ZSTD
```

Or in Python, without saving intermediate files:

```python
import gedih3.gh3driver as gh3
from gedih3 import raster

ddf = gh3.gh3_load(source='~/gedi_data/h3/', columns=['agbd_l4a', 'rh_098_l2a'])
agg = gh3.gh3_aggregate(ddf, target_res=6, agg=['mean', 'std', 'count']).compute()
raster.export_raster(raster.h3_to_raster(agg), 'agbd_mean.tif', compress='LZW')
```

:::{tip}
`gh3_aggregate` can read the H3 database directly and rasterize in one pass with `-R`, collapsing extract + aggregate + rasterize into a single command:

```bash
gh3_aggregate -y -l agbd_l4a rh_098_l2a -h3 6 -a mean -R -o output/
```
:::

See the [Quick Start guide](getting-started/quickstart.md) for a step-by-step walkthrough, or [Building a Database](user-guide/building-a-database.md) for the full configuration reference.

:::::

::::::

---

## Choosing the right tool

gedih3 is not the only way to access GEDI data. Here is an honest look at when it is the best choice --- and when another tool might serve you better.

### gedih3 vs. Google Earth Engine

[Google Earth Engine](https://earthengine.google.com/) hosts GEDI L2A, L2B, and L4A as pre-loaded datasets. It is the most widely used platform for GEDI analysis and an excellent choice for many workflows. Here is where the two tools diverge:

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`check-circle-fill;1.2em` Where gedih3 wins
:class-card: sd-border-success comparison-card

**Fast spatial aggregation** --- index-based grouping, not geometry operations. Any Python callable; GEE is limited to fixed reducers.

**Full variable access** --- 300+ variables per product vs. ~100 in GEE. Includes per-algorithm RH metrics and waveform parameters.

**All GEDI products** --- L1B, L2A, L2B, L4A, and L4C. GEE lacks L1B waveforms and L4C (WSCI).

**Your data, your hardware** --- offline, no compute quotas, reproducible. Scales from laptop to HPC.

**SQL compatible** --- query with DuckDB, join with any dataset, larger-than-memory queries.

**Incremental and resumable** --- add orbits, time periods, or variables without rebuilding. Interrupted builds resume automatically.

**Storage efficient** --- S3 streaming transfers only selected variables. A fraction of the full archive footprint.
:::

:::{grid-item-card} {octicon}`arrow-switch;1.2em` Where GEE may be better
:class-card: sd-border-danger comparison-card

**Zero setup** --- GEDI data pre-loaded. No download, no build, no local storage.

**Quick exploration** --- faster to a first result for common variables.

**Massive ecosystem** --- thousands of existing scripts and community examples.

**No local infrastructure** --- runs in the cloud; no disk space or conda environments.
:::

::::

**In short:** if you need quick access to basic height and biomass variables for exploratory analysis, GEE is hard to beat. If you need the full variable set, fast index-based spatial aggregation with custom functions, L4C data, beam-level control, or reproducible offline pipelines --- gedih3 is the right tool.

:::{dropdown} Other GEDI tools and how they compare

| Capability | gedih3 | rGEDI | gediDB | SlideRule |
|---|:---:|:---:|:---:|:---:|
| Incremental database | Yes | -- | Yes | -- |
| On-the-fly variable subsetting | Yes | -- | Yes | Yes |
| Spatial indexing | Yes | -- | -- | -- |
| Spatial aggregation functions | Yes | -- | -- | -- |
| CLI pipeline | Yes | -- | -- | -- |
| GeoTIFF rasterization | Yes | -- | -- | -- |
| Offline / reproducible | Yes | Yes | Yes | -- |
| All GEDI products (L1B--L4C) | Yes | Partial | Partial | Partial |

**[rGEDI](https://github.com/carlos-alberto-silva/rGEDI)** (R) --- Supports L1B, L2A, L2B with waveform visualization and waveform simulation capability. Best for: R users doing waveform-level analysis on small areas.

**[gediDB](https://github.com/simonbesnard1/gedidb)** (Python) --- Uses TileDB as the storage backend instead of H3-partitioned Parquet. Supports L2A-B and L4A-C. Best for: users who prefer the TileDB ecosystem with strong Python programming skills.

**[SlideRule Earth](https://slideruleearth.io/)** (cloud service) --- On-demand, cloud-based processing of GEDI and ICESat-2 data. Returns subsets with quality filtering but no spatial aggregation or rasterization. Best for: quick, on-demand subsets without local infrastructure.

**Manual workflow** (earthaccess + h5py + geopandas) --- Always an option, but spatial aggregation alone typically requires writing point-in-polygon joins or manual grid binning. gedih3 automates the ~500 lines of boilerplate this requires and replaces geometry operations with instant index-based grouping.

:::

---

## Explore the documentation

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`play;1.2em` Getting Started
:link: getting-started/index
:link-type: doc

Install gedih3, set up NASA credentials, and run your first pipeline.

*Best for: new users*
:::

:::{grid-item-card} {octicon}`light-bulb;1.2em` Understand the Concepts
:link: concepts/index
:link-type: doc

Learn about GEDI data, H3 hexagonal indexing, EGI square-pixel indexing, and variable presets.

*Best for: users who want to understand what happens under the hood*
:::

:::{grid-item-card} {octicon}`gear;1.2em` Build and Analyze
:link: user-guide/index
:link-type: doc

Complete guide to building databases, CLI reference, Python API, and data format specifications.

*Best for: users ready to run their own analyses*
:::

:::{grid-item-card} {octicon}`book;1.2em` API Reference
:link: autoapi/index
:link-type: any

Auto-generated documentation from source code: every function, class, and parameter.

*Best for: developers and advanced Python users*
:::

::::

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
