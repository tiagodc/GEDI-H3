# Quick Start

This guide covers the core workflow in five steps: download → build → extract → aggregate → rasterize.

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
gh3_aggregate -d extracted/ -h3 6 -a mean -o aggregated/
```

Aggregates GEDI shots to H3 level 6 hexagons (~36 km²). Use `gh3_list_resolutions` to see all available levels.

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
- [CLI Reference](../cli-reference.md) — all 11 tools with full flag documentation
- [Python API](../user-guide/python-api.md) — custom aggregation functions and in-memory workflows
- [Concepts: GEDI Data](../concepts/gedi-data.md) — understand what you're working with
- [Concepts: EGI Indexing](../concepts/egi-indexing.md) — advanced square-pixel indexing for L4B compatibility
