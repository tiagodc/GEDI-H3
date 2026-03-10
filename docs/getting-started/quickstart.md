# Quick Start

This guide covers the core workflow in five steps: download → build → extract → aggregate → rasterize.

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

> **Tip**: Run any tool with `--help` for the full list of options.
> ```bash
> gh3_build --help
> ```

---

## Step 1: Download GEDI Data

```bash
gh3_download -r "-51,0,-50,1" -l2a minimal -l4a minimal
```

Downloads GEDI L2A and L4A granules covering the bounding box (W,S,E,N). Files are saved to `~/gedi_data/soc/` by default.

Use `default` instead of `minimal` for a broader variable set, or pass explicit variable names.

---

## Step 2: Build the H3 Database

```bash
gh3_build -r "-51,0,-50,1" -l2a minimal -l4a minimal
```

Converts downloaded HDF5 files into an H3-indexed Parquet database at `~/gedi_data/h3/`. This is a one-time step; the database can be queried and re-used without rebuilding.

> The database you build here determines which variables, region, and time period are available to all downstream tools. For a full guide to variable selection, subsetting strategies, source modes, and performance tuning, see [**Building a Database**](../user-guide/building-a-database.md).

---

## Step 3: Browse Variables

```bash
gh3_list_variables         # list all available variables
gh3_list_variables -g agbd # filter with a keyword
```

---

## Step 4: Extract a Dataset

```bash
gh3_extract -y -l agbd_l4a rh_098_l2a -o extracted/
```

The `-y` flag applies pre-configured quality filtering. Output is a set of flat Parquet files in `extracted/`, readable with pandas, R, QGIS, or DuckDB.

To filter spatially or temporally:

```bash
gh3_extract -y -r region.shp -t0 2020-01-01 -t1 2022-12-31 \
            -l agbd_l4a rh_098_l2a -o extracted/
```

---

## Step 5: Aggregate

```bash
gh3_aggregate -d extracted/ -h3 7 -a mean -o aggregated/
```

Aggregates GEDI shots to H3 level 7 hexagons (~5 km²). Use `gh3_list_resolutions` to see all available levels.

---

(shortcut-all-in-one)=
## Shortcut: All-in-One

`gh3_aggregate` can read the H3 database directly (no prior `gh3_extract` needed) and produce rasters in the same call with `-R`, collapsing steps 3–5 into one command:

```bash
gh3_aggregate -y -l agbd_l4a rh_098_l2a -h3 7 -a mean -R -o output/
```

This writes GeoTIFFs to `output/` in a single pass. Use this when you don't need to export intermediate datasets.

--- 

:::{note} 
Aggregating from H3 hexagons to raster files will resample hexagon polygons to pixels using nearest neighbor interpolation.
For exact pixel matches consider using the [**EGI indexing**](../concepts/egi-indexing.md) system. 
:::

---

## Step 6: Export as GeoTIFF

```bash
gh3_rasterize -d aggregated/ -o rasters/ --compress LZW
```

Produces one GeoTIFF per variable per partition, tiled for efficient access.


---

## Python Equivalent

The same workflow without saving intermediate files:

```python
import gedih3.gh3driver as gh3
from gedih3 import raster

# Load from the H3 database
ddf = gh3.gh3_load(
    source='~/gedi_data/h3/',
    columns=['agbd_l4a', 'rh_098_l2a'],
    query='quality_flag_l2a == 1 and agbd_l4a > 0',
)

# Aggregate to H3 level 6
agg = gh3.gh3_aggregate(ddf, target_res=6, agg=['mean', 'std', 'count']).compute()

# Export to GeoTIFF
xras = raster.h3_to_raster(agg)
raster.export_raster(xras, 'agbd_mean.tif', compress='LZW')
```

For advanced Python API usage (custom aggregation functions, ancillary data integration, and more), see [Python API](../user-guide/python-api.md).

---

## Next Steps

- [Configuration](configuration.md) — customize storage paths and Dask settings (optional)
- [CLI Reference](../user-guide/cli-reference.md) — all 11 tools with full flag documentation
- [Python API](../user-guide/python-api.md) — custom aggregation functions and in-memory workflows
- [Concepts: GEDI Data](../concepts/gedi-data.md) — understand what you're working with
- [Concepts: EGI Indexing](../concepts/egi-indexing.md) — advanced square-pixel indexing for L4B compatibility
