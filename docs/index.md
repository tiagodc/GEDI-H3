<img src="_static/gh3_logo.png" alt="gedih3 Logo" style="width: 50%; background: transparent;" />

# Turn billions of NASA GEDI footprints into analysis-ready datasets.

NASA's [GEDI](https://gedi.umd.edu/) has measured forest height, biomass, and canopy structure across the planet --- billions of individual laser measurements spanning every continent except Antarctica. But the raw data is stored in thousands of complex files organized by orbit, not by location. Getting from "I want a biomass map of my study area" to actually having one requires navigating deeply nested file formats, applying multi-criteria quality filters, and processing terabytes of data.

**gedih3** handles all of that. It transforms raw GEDI data into a spatial database you can query by region, filter for quality with a single flag, aggregate to any scale, and export to GeoTIFF, GeoParquet, or any format your tools can read --- from the command line or Python.

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
GEDI files are organized by when the International Space Station (ISS) passed overhead, not where. Asking *"show me all the data over Costa Rica"* means downloading and scanning thousands of files --- most of which contain no relevant data.
:::

:::{grid-item-card} {octicon}`file-code;1.2em` Hundreds of variables, and which ones matter?
Each GEDI product contains hundreds of variables across 8 laser beams in deeply nested HDF5 files. Most data tools cannot open them directly --- and even if you can, knowing which variables are relevant for your analysis requires domain expertise that most users don't have.
:::

:::{grid-item-card} {octicon}`alert;1.2em` Quality flags and filters buried in the data
Each GEDI product ships with its own quality flags, and using all of them correctly is easy to overlook. Without enforcing them, results look plausible but carry silent biases. Applying custom filters on top (e.g. beam selection, sensitivity thresholds) adds another layer of complexity.
:::

:::{grid-item-card} {octicon}`graph;1.2em` Scale: billions of measurements
The full GEDI archive stores terabytes of data across thousands of files. Even a single country can involve millions of footprints. Without spatial indexing and distributed processing, simple analyses take hours or fail entirely.
:::

::::

---

## What gedih3 does about it

gedih3 is a Python library and CLI toolchain that handles the entire pipeline from raw NASA data to analysis-ready output.

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`database;1.2em` Spatial database from day one
:link: concepts/h3-indexing
:link-type: doc

gedih3 builds a spatially-indexed database from raw GEDI files using [Uber's H3 hexagonal grid](https://h3geo.org/). Once built, you query by region --- bounding box, shapefile, or country code --- and only relevant partitions are read from disk. A query over Costa Rica touches only tiles that cover Costa Rica; everything else is skipped before any data is loaded.

The database is incremental. New orbits, time periods, or variables merge into an existing database without rebuilding --- gedih3 tracks what has been ingested and processes only what is new. Interrupted builds resume from the last completed step.
:::

:::{grid-item-card} {octicon}`list-unordered;1.2em` Expert-curated variable presets
:link: concepts/variable-presets
:link-type: doc

Instead of figuring out which of the hundreds of GEDI variables matter for your analysis, use `minimal` or `default` presets designed by remote sensing scientists. They select the right variables from each product so you don't have to. The output is flat GeoParquet --- one row per measurement, one column per variable --- readable by pandas, R, QGIS, DuckDB, or any modern tool.
:::

:::{grid-item-card} {octicon}`check-circle;1.2em` Automated quality filtering + custom queries
:link: user-guide/building-a-database
:link-type: doc

Quality flags are included in every database by default. A single `--quality` flag enforces them across all products at once --- no need to remember which flags apply where. Need finer control? Use `--query` to add any custom pandas filter on top (beam type, sensitivity thresholds, time ranges, or any variable in the database).
:::

:::{grid-item-card} {octicon}`beaker;1.2em` Flexible aggregation

From the CLI: `mean`, `std`, `count`, percentile shorthands (`p25`, `p95`), per-column specs, or JSON/text files. From the Python API: pass any callable --- fit a regression model, compute a custom metric, or run any analysis per hexagon. Aggregation uses partition-local grouping with no data shuffle across workers, so it scales linearly with data size.
:::

:::{grid-item-card} {octicon}`rocket;1.2em` Scales from laptop to cluster

Built on [Dask](https://www.dask.org/), gedih3 auto-detects available resources and distributes work accordingly. On a laptop, it streams HDF5 data beam-by-beam without loading entire files into memory --- the build process works within constrained RAM. On an HPC cluster, it can use an existing Dask scheduler with no code changes. NASA credentials are propagated to worker processes automatically.
:::

:::{grid-item-card} {octicon}`tools;1.2em` Complete pipeline --- from download to analysis ready datasets
:link: user-guide/cli-reference
:link-type: doc
:columns: 12

several command-line tools and a full Python API cover every step: download from NASA, build the database, extract and filter, aggregate to any spatial scale, fuse with external rasters or vector data, and export to GeoTIFF, GeoParquet, or any format your tools can read. Downloads subset HDF5 files on the fly, keeping only the variables you requested. S3 streaming mode uses range requests to transfer only selected variables --- up to 10--50x less data than downloading full GEDI granules. Supports all major GEDI products (L1B, L2A, L2B, L4A, L4C).
:::

::::

### Build once, iterate fast

The download and build steps run once and may take time depending on your network, system resources, and region of interest. But once the database exists, everything downstream is fast. Extract, aggregate, and rasterize read only the partitions they need and process them without shuffling data between parallel workers. Changing your aggregation resolution, variable selection, quality filters, or output format takes seconds to minutes --- not hours. This makes gedih3 well-suited for iterative exploration and experimentation.

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

**Fast spatial aggregation** --- H3 indexing resolves spatial containment by arithmetic, not geometry operations. Pick a resolution and a reduction function --- grouping is instant. Any Python callable from the API; GEE is limited to fixed reducers.

**Full variable access** --- 300+ variables per product vs. ~101 in GEE. Per-algorithm RH metrics, waveform parameters, and geolocation details that GEE does not expose.

**All GEDI products** --- L1B, L2A, L2B, L4A, and L4C. GEE lacks L1B waveforms and L4C (WSCI).

**Your data, your hardware** --- offline databases, no compute quotas, fully reproducible. Scales from laptop to HPC cluster.

**DuckDB/SQL compatible** --- query your GEDI database with SQL, join with any dataset, larger-than-memory queries.

**Incremental and resumable** --- add new time periods, regions, or variables to an existing database without starting over. gedih3 tracks every ingested granule and processes only what is new. Interrupted builds resume automatically.

**Network/storage efficient** --- S3 streaming mode transfers only selected variables via range requests. Post-download subsetting trims already-fetched files to the variables you need. The storage required is a fraction of the full archive.
:::

:::{grid-item-card} {octicon}`arrow-switch;1.2em` Where GEE may be better
:class-card: sd-border-danger comparison-card

**Zero setup** --- GEE has pre-loaded GEDI data. No download step, no build step, no local storage needed.

**Quick exploration** --- for simple queries on common variables (canopy height, biomass), GEE is faster to a first result.

**Massive ecosystem** --- thousands of existing scripts, tutorials, and community examples. If your workflow already lives in GEE, adding GEDI is straightforward.

**No local infrastructure** --- everything runs in the cloud. No disk space, no conda environments, no dependency management.
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
