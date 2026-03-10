# CLI Reference

gedih3 installs 11 command-line tools. All tools support `-v` (INFO) and `-vv` (DEBUG) verbosity, and `-Q` for quiet mode.

> **Tip**: Every tool supports `--help` (`-h`) for a complete list of flags and examples:
> ```bash
> gh3_build --help
> gh3_aggregate --help
> gh3_extract --help
> ```

---

## Core Workflow Tools

### `gh3_download`

Download GEDI data from NASA DAAC.

```bash
gh3_download -r "W,S,E,N" -l2a default -l4a default -N 8
gh3_download -r region.shp -l4a agbd -t0 2020-01-01 -t1 2021-01-01
gh3_download --s3  # Stream from NASA S3 without local download
```

| Flag | Description |
|------|-------------|
| `-r, --region` | Spatial filter: bbox, vector file, or ISO3 code |
| `-t0, -t1` | Start/end date (YYYY-MM-DD) |
| `-l1b, -l2a, -l2b, -l4a, -l4c` | Products to download (`default`, `minimal`, or list) |
| `--gedi-version` | GEDI data version (default: latest) |
| `--resume` | Skip already-downloaded files |
| `--s3` | S3 streaming mode |

---

### `gh3_build`

Build H3 parquet database from downloaded HDF5 files.

```bash
gh3_build -r "W,S,E,N" -l2a default -l4a default -h3r 12 -h3p 3
gh3_build -r region.shp -l4a agbd --resume
gh3_build --s3 -r region.shp -l4a agbd  # Build directly from S3
```

| Flag | Description |
|------|-------------|
| `-h3r` | H3 index resolution (default: 12, ~25 m²) |
| `-h3p` | H3 partition resolution (default: 3, ~12,393 km²) |
| `--resume` | Skip already-built partitions |
| `-i` | Input SOC directory (default: `GH3_DEFAULT_SOC_DIR`) |
| `-d` | Output H3 database directory |

---

### `gh3_extract`

Extract data from H3 database into simplified flat parquet files.

```bash
gh3_extract -d /path/to/database -r region.shp -l2a rh -l4a agbd -y -o output/
```

| Flag | Description |
|------|-------------|
| `-d` | H3 database path |
| `-r` | Spatial filter |
| `-d0, -d1` | Temporal filter |
| `-l*` | Product variables |
| `-y, --quality` | Apply pre-configured quality filters |
| `-q, --query` | Pandas-style filter string |
| `-g` | Include geometry |
| `-o` | Output directory |

#### EGI variant

For square-pixel indexing instead of H3 — see [EGI Indexing](../concepts/egi-indexing.md).

```bash
gh3_extract -d /path/to/database -egi 6 -o output/       # ~1 km EGI index
gh3_extract -d /path/to/database -egi 6:10 -o output/    # explicit index:partition
```

| Flag | Description |
|------|-------------|
| `-egi INDEX[:PART]` | EGI index level and optional partition level |

---

### `gh3_aggregate`

Aggregate data to a coarser spatial resolution.

```bash
gh3_aggregate -d /path/to/database -h3 6 -o output/
gh3_aggregate -d /path/to/database -h3 6 -a "['mean','std','count']" -o output/
```

| Flag | Description |
|------|-------------|
| `-h3 LEVEL` | Aggregate to H3 level |
| `-a` | Aggregation function: `mean`, `sum`, `median`, `std`, `count` |
| `-R, --rasterize` | Also export rasters after aggregation |
| `-o` | Output directory |

#### EGI variant

```bash
gh3_aggregate -d /path/to/database -egi 6 -a mean -o output/        # ~1 km
gh3_aggregate -d /path/to/database -egi 6:10 -a mean -o output/     # explicit partition
gh3_aggregate -d /path/to/database -egi 6 -a mean -R -o output/     # aggregate + rasterize
```

| Flag | Description |
|------|-------------|
| `-egi INDEX[:PART]` | EGI aggregation level and optional partition level |

---

### `gh3_rasterize`

Convert pre-aggregated dataset to GeoTIFF rasters.

```bash
gh3_rasterize -d /path/to/aggregated/ -o output/ --compress LZW  # tiled output
gh3_rasterize -d /path/to/aggregated/ -m -o output.tif           # merged GeoTIFF
gh3_rasterize -d /path/to/aggregated/ -l agbd_l4a -o output/     # select variables
```

| Flag | Description |
|------|-------------|
| `-d` | Dataset from `gh3_aggregate` or `gh3_extract` |
| `-l` | Variable(s) to rasterize |
| `-m` | Merge all tiles into a single GeoTIFF |
| `--compress` | Compression: `LZW`, `DEFLATE`, `ZSTD`, `NONE` |
| `-o` | Output path (directory or `.tif` when `-m`) |

---

## Ancillary Data Tools

### `gh3_from_img`

Sample raster pixel values at GEDI shot locations.

```bash
# Single raster
gh3_from_img -i /path/to/dem.tif -d /path/to/database -r region.shp -o output/

# Tile directory with band selection and window operations
gh3_from_img -i /path/to/tiles/ -if tif -B 0 2 -w 131 -d /path/to/database -o output/

# Custom band names, quality filter, include geometry
gh3_from_img -i /path/to/raster.vrt -b elevation slope -d /path/to/database -y -g -o output/
```

| Flag | Description |
|------|-------------|
| `-i` | Raster file, VRT, or tile directory |
| `-if` | Tile file extension (default: `tif`) |
| `-B` | Band indices to sample (0-based) |
| `-b` | Custom band names |
| `-w` | Window operations (3-digit BZO format) |
| `-y, --fillna` | Fill NoData value |
| `-g` | Include geometry in output |

**Window spec format** (`-w BZO`):
- `B` = band index (0-based)
- `Z` = window size (odd, 1–9)
- `O` = operation: `0`=sum, `1`=mean, `2`=median, `3`=mode

---

### `gh3_from_polygon`

Join polygon attributes to GEDI shots via spatial join.

```bash
gh3_from_polygon -i ecoregions.shp -c ECO_NAME BIOME_NAME -d /path/to/database -o output/
gh3_from_polygon -i landcover.gpkg -x lc_ --dropna -d /path/to/database -o output/
gh3_from_polygon -i boundaries.shp -p intersects -d /path/to/database -o output/
```

| Flag | Description |
|------|-------------|
| `-i` | Polygon vector file (shapefile, GPKG, GeoJSON) |
| `-c` | Columns to include from polygon file |
| `-x, --prefix` | Column name prefix (avoids conflicts) |
| `-p` | Spatial predicate: `within` (default) or `intersects` |
| `--dropna` | Drop shots not matched to any polygon |
| `-g` | Include geometry in output |

---

## Utility Tools

### `gh3_list_variables`

List available GEDI variables by product.

```bash
gh3_list_variables -p L2A
gh3_list_variables -g agbd     # grep filter
```

| Flag | Description |
|------|-------------|
| `-d` | H3 database path (optional) |
| `-p` | Filter by product (e.g., `L2A`, `L4A`) |
| `-g` | Grep filter: show only variables matching keyword |

---

### `gh3_list_resolutions`

Display H3 and EGI resolution levels with pixel sizes.

```bash
gh3_list_resolutions        # H3 levels
gh3_list_resolutions -egi   # EGI levels
```

---

### `gh3_read_schema`

Inspect parquet or HDF5 file schemas.

```bash
gh3_read_schema /path/to/file.parquet
gh3_read_schema /path/to/file.h5
gh3_read_schema /path/to/database/gedih3_build_log.json
```

---

## Common Flags

| Flag | Description |
|------|-------------|
| `-r, --region` | Spatial filter: vector file, bbox `"W,S,E,N"`, or ISO3 code |
| `-d0, -d1` | Temporal filters (YYYY-MM-DD) |
| `-l1b, -l2a, -l2b, -l4a, -l4c` | Product variables |
| `-N, -T, -M, -P` | Dask workers, threads, memory, dashboard port |
| `-s` | Connect to existing Dask scheduler |
| `-v, -vv` | Verbosity: INFO, DEBUG |
| `-Q` | Quiet mode (errors only) |
| `-egi INDEX[:PART]` | EGI indexing |
| `-R` | Rasterize after aggregation (`gh3_aggregate` only) |
