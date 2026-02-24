# Data Formats

gedih3 distinguishes two output formats: the internal **H3 database** and the user-friendly **simplified dataset**.

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

- Hive-partitioned by H3 cell at the partition level
- `gedih3_build_log.json` contains build metadata (index/partition levels, products, columns)
- Not designed for direct use with external tools — use `gh3_extract` first

### Build Log Keys

| Key | Description |
|-----|-------------|
| `h3_index_level` | Fine H3 resolution used for indexing |
| `h3_partition_level` | Coarse H3 resolution used for partitioning |
| `products` | GEDI products included |
| `columns` | Column schema |
| `region` | Spatial extent |

---

## Simplified Dataset (User-Friendly Format)

Created by `gh3_extract` and `gh3_aggregate`. Flat parquet files for use with any tool.

```
output/
├── abc123.parquet
├── def456.parquet
├── ghi789.parquet
└── gedih3_dataset.json
```

- Files named by H3 or EGI partition ID
- `gedih3_dataset.json` describes the whole dataset (index type, columns, etc.)
- Readable with pandas, R, QGIS, DuckDB, and any other parquet-compatible tool
- Used as input for `gh3_rasterize`, `gh3_from_img`, `gh3_from_polygon`

### Dataset Metadata (`gedih3_dataset.json`)

| Key | Description |
|-----|-------------|
| `index_type` | `"h3"` or `"egi"` |
| `index_level` | Spatial resolution level |
| `partition_level` | Partition tile size |
| `columns` | Data columns included |
| `agg` | Aggregation method (if from `gh3_aggregate`) |

---

## EGI vs H3 Indexing

| Feature | H3 | EGI |
|---------|----|----|
| Grid shape | Hexagonal | Square |
| CRS | EPSG:4326 | EPSG:6933 |
| Resolution spec | Higher level = finer | Lower level = finer |
| GEDI L4B compatible | No | Yes |
| Best use | General analysis | Global pixel grids |

### H3 Resolution Levels

| Level | Avg. Hex Area | Typical Use |
|-------|---------------|-------------|
| 0 | 4,250,547 km² | Global |
| 3 | 12,393 km² | Partition level (default) |
| 6 | 36 km² | Regional analysis |
| 9 | 0.105 km² | Local analysis |
| 12 | 307 m² | Index level (default) |

### EGI Resolution Levels

| Level | Pixel Size | Typical Use |
|-------|------------|-------------|
| 1 | ~1 m | Finest; matches GEDI footprint |
| 3 | ~25 m | GEDI footprint level |
| 6 | ~1 km | GEDI L4B baseline |
| 8 | ~10 km | Wall-to-wall |
| 12 | ~160 km | Partition tiles (default) |

---

## Parquet Schema

Each simplified dataset parquet file contains:

- **Index column**: `h3_XX` (H3 cell ID) or `egiXX` (EGI hash, uint64)
- **Data columns**: product variables (e.g., `agbd_l4a`, `rh_098_l2a`)
- **Geometry** (optional): `geometry` column (WKB Point geometries in EPSG:4326)
- **Metadata**: Stored in parquet file metadata

### Inspecting Files

```bash
# Inspect schema
gh3_read_schema /path/to/output/abc123.parquet

# Read with pandas
import pandas as pd
df = pd.read_parquet('/path/to/output/abc123.parquet')

# Load all files with gedih3
import gedih3.gh3driver as gh3
gdf = gh3.gh3_load_dataset('/path/to/output/')
```
