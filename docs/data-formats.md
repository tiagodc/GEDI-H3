# Data Formats

gedih3 produces and consumes several data formats throughout the pipeline. This page describes each format, its structure, and when to use it.

---

## H3 Database (Internal Format)

Created by `gh3_build`. Optimized for repeated queries with Dask.

```
h3_database/
├── h3_03=abc123/
│   └── data.parquet
├── h3_03=def456/
│   └── data.parquet
└── gedih3_build_log.json
```

- **Hive-partitioned** by H3 cell at the partition level (default: level 3, ~12,000 km²)
- Each directory corresponds to one H3 partition tile; its contents can be read independently
- `gedih3_build_log.json` records build metadata (products, variables, region, resolution levels)
- **Not designed for direct use with external tools** — use `gh3_extract` to produce user-friendly flat files

### Build Log Keys

| Key | Description |
|-----|-------------|
| `h3_index_level` | Fine H3 resolution used for shot-level indexing |
| `h3_partition_level` | Coarse H3 resolution used for partitioning (directory names) |
| `products` | GEDI products included |
| `columns` | Column schema |
| `region` | Spatial extent |

### H3 Dual-Level Structure

The H3 database uses two H3 resolution levels simultaneously:

- **Partition level** (default: 3) — determines the directory structure. A query for a specific region only reads tiles that overlap that region.
- **Index level** (default: 12) — the H3 cell ID assigned to each individual GEDI shot, stored as a column in every parquet file.

> **Parent/child caveat**: H3 parent hexagons are not perfectly geometrically inclusive of their children. When aggregating across resolution levels, `gh3_aggregate` uses `h3.cell_to_parent()` which assigns each shot to its closest parent, which is consistent and fast but not a strict geometric containment. See [H3 Indexing](concepts/h3-indexing.md) for details.

---

## Simplified Dataset (User-Friendly Format)

Created by `gh3_extract` and `gh3_aggregate`. Flat Parquet files for use with any tool.

```
output/
├── abc123.parquet
├── def456.parquet
├── ghi789.parquet
└── gedih3_dataset.json
```

- Files named by H3 or EGI partition ID
- `gedih3_dataset.json` describes the whole dataset (index type, columns, aggregation, etc.)
- Readable with **pandas, R, QGIS, DuckDB**, and any other Parquet-compatible tool
- Used as input for `gh3_rasterize`, `gh3_from_img`, `gh3_from_polygon`, `gh3_update`

```python
# Read with pandas
import pandas as pd
df = pd.read_parquet('/path/to/output/abc123.parquet')

# Load all files with gedih3
import gedih3.gh3driver as gh3
gdf = gh3.gh3_load_dataset('/path/to/output/')
```

### Dataset Metadata (`gedih3_dataset.json`)

| Key | Description |
|-----|-------------|
| `index_type` | `"h3"` or `"egi"` |
| `index_level` | Spatial resolution level |
| `partition_level` | Partition tile size |
| `columns` | Data columns included |
| `agg` | Aggregation method (if from `gh3_aggregate`) |

---

## GeoTIFF (Raster Output)

Created by `gh3_rasterize` or the `-R` flag in `gh3_aggregate`. Standard GeoTIFF files compatible with GDAL, QGIS, R (`terra`), Python (`rioxarray`), and virtually any GIS tool.

```bash
# Tiled output (one file per partition)
gh3_rasterize -d aggregated/ -o rasters/ --compress LZW

# Single merged raster
gh3_rasterize -d aggregated/ -m -o output.tif --compress LZW

# Select specific variables
gh3_rasterize -d aggregated/ -l agbd_l4a_mean -o rasters/
```

Key properties:
- **Tiled by default** — output is split by spatial partition for efficient access
- **Compression support** — `LZW`, `DEFLATE`, `ZSTD`, `NONE`
- **BIGTIFF support** — for files exceeding 4 GB
- **Time-series naming** — when produced from time-windowed data, files are named with a temporal suffix

```python
# Load GeoTIFF output in Python
import rioxarray
xds = rioxarray.open_rasterio('agbd_mean.tif')
xds.plot()
```

---

## Other Supported Formats

`gh3_export` (Python API) and `gh3_extract` support additional output formats beyond Parquet:

| Format | Extension | Notes |
|--------|-----------|-------|
| GeoParquet | `.parquet` | Default; includes geometry for spatial tools |
| Feather | `.feather` | Fast in-memory format; no geometry |
| GeoPackage | `.gpkg` | OGC standard vector format; QGIS native |
| HDF5 | `.h5` | For compatibility with scientific workflows |
| Shapefile | `.shp` | Legacy vector format; column name length limited |
| CSV | `.csv` | Tabular export; no geometry |

---

## Parquet Schema

Each simplified dataset Parquet file contains:

- **Index column**: `h3_XX` (H3 cell ID, string) or `egiXX` (EGI hash, uint64)
- **Data columns**: product variables (e.g., `agbd_l4a`, `rh_098_l2a`)
- **Geometry** (optional): `geometry` column (WKB Point geometries in EPSG:4326)
- **Metadata**: stored in Parquet file metadata (accessible via `pyarrow`)

### Inspecting Files

```bash
# Inspect schema from CLI
gh3_read_schema /path/to/output/abc123.parquet
gh3_read_schema /path/to/database/gedih3_build_log.json
```

---

## Choosing Between H3 and EGI

| Consideration | H3 | EGI |
|--------------|----|----|
| Grid shape | Hexagonal | Square |
| Coordinate system | WGS84 (EPSG:4326) | EASE-Grid 2.0 (EPSG:6933) |
| Rasterization | Requires hex-to-pixel conversion | Direct 1:1 mapping |
| GEDI L4B compatible | No | Yes |
| Parent/child nesting | Approximate (see above) | Exact |
| Default in gedih3 | Yes | No |

EGI is the right choice when you need alignment with GEDI L4B gridded products or when producing global pixel-grid datasets for interoperability with raster-native workflows. For general analysis and exploratory work, H3 is simpler and faster. See [EGI Indexing](concepts/egi-indexing.md) for a detailed comparison.
