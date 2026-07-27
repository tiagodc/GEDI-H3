---
paths:
  - "src/gedih3/egi/**/*.py"
  - "src/gedih3/h3utils.py"
  - "src/gedih3/raster/**/*.py"
  - "src/gedih3/imgutils.py"
  - "src/gedih3/vecutils.py"
---

# Spatial indexing, rasterization, and ancillary fusion

Two index systems coexist: H3 hexagons (the storage layout) and EGI square pixels (the
analysis layout, aligned to EASE-Grid 2.0 so outputs stack with GEDI L4B).

## Coordinate systems — do not mix these up

- **H3 is always WGS84 (EPSG:4326).**
- **EGI is always EASE-Grid 2.0 (EPSG:6933).**
- Output GeoDataFrames default to EPSG:4326.
- **EGI rasters stay in EPSG:6933** — reprojecting them to 4326 breaks L4B alignment,
  which is the entire reason EGI exists.

## EGI levels run the opposite way to H3

Lower EGI level = **finer** resolution (level 1 ≈ 1 m, level 12 ≈ 160 km, the partition
level). Higher H3 resolution = finer. This inversion is the most common source of
off-by-a-lot bugs here.

`src/gedih3/egi/config.py` is the source of truth for the level table; `gh3_list_resolutions
-egi` prints it, and `docs/concepts/egi-indexing.md` publishes it. Do not copy the table
into another file — the copy that used to live in the agent definitions had drifted out of
agreement with `config.py`.

Anchors worth knowing without looking: level 3 ≈ 25 m is the GEDI footprint, level 6 ≈ 1 km
is the GEDI L4B baseline, level 12 ≈ 160 km is the partition level.

## EGI hash layout (uint64)

```
hash = level * 1e18 + px_outer * 1e15 + py_outer * 1e12 + px_inner * 1e6 + py_inner
```

Encode/decode lives in `egi/core.py`; geometry operations (`pixel_shape`,
`pixel_coordinate`, `egi_h3_intersection`) in `egi/spatial.py`; DataFrame-level indexing,
`egi_to_parent` / `egi_to_parent_vectorized`, and `egi_aggregate` in `egi/dataframe.py`.
Prefer the vectorized parent conversion for arrays.

## Ring-1 overhang

`egi_h3_intersection` goes through `h3_expand_ring` for the same reason
`intersect_h3_geometries` and `geoseries_to_filter` do: H3 child cells overhang their
parent boundary by roughly 0.18 × edge, so an exact polygon intersection silently misses
boundary shots stored in neighbour partitions. Any new ROI → cell-set path must expand by
one ring, restricted to the set of partitions that actually exist.

## Direct EGI load (no shuffle)

```
_prepare_egi_loading()      compute the EGI↔H3 intersection up front
_load_egi_tile_from_h3()    read the H3 parquet files for one tile, with bbox filter
egi_load()                  public API: H3 DB → EGI-partitioned Dask DataFrame
egi_aggregate()             coarsen an already-EGI-partitioned frame, still no shuffle
```

One Dask partition per EGI tile, so no `set_index()` is needed. Preserve that property.

**Coordinate priority** when deriving EGI cells: use the `geometry` column (Point) first,
then fall back to product-suffixed coordinate columns (e.g. `lon_lowestmode_l2a`).

## Rasterization

`h3_to_raster` / `geodf_to_raster` produce the arrays; `export_raster` writes GeoTIFF;
`generate_time_windows` drives time-series output (see `TimeSeriesRasterizer`).

**`gh3_rasterize(data, output, ...)` is the one entry point for anything larger than a
single frame** — a dataset directory or a Dask frame, either index type, tiled or merged.
Both `gh3_rasterize` and `gh3_aggregate -R` delegate to it. Do not re-derive the
index-type → rasterizer mapping in a CLI; that duplication is what it was written to
remove.

**One partition = one outer tile = one file.** An EGI partition nests in exactly one
level-12 tile by construction (`egi_export_part`, `_prepare_egi_loading`), so
`egi.rasterize_partition` returns a Series of length 0 or 1. A multi-tile partition is
upstream corruption: it is reported with a WARNING and the majority tile is rasterized —
never silently split into several files. Callers that legitimately hold a multi-tile
frame (an arbitrary-ROI aggregate, e.g. `TimeSeriesRasterizer`) must split explicitly via
`egi.split_by_outer_tile` first.

Mosaics go through **`build_vrt_safe`**, never `gdal.BuildVRT` directly. `build_vrt`
prefers `osgeo.gdal` and falls back to `build_vrt_xml`, a rasterio-only writer verified
pixel-identical to GDAL's; the fallback refuses rotated or mixed-CRS inputs rather than
emit a wrong mosaic. `build_vrt_safe` downgrades mosaic failure to a WARNING, which is
correct wherever the `.tif` tiles are the real deliverable.

`osgeo` is this package's **only** optional import and must stay that way — it has no PyPI
wheels. Guard every call site with `try`/`except ImportError` plus a working fallback;
`tests/test_dependencies.py` enforces it.

## Ancillary fusion

`imgutils.py` samples external rasters at shot locations; `vecutils.py` does vector
spatial joins. Both cache at worker level — a tile or layer opened once per worker, not
once per partition. Keep new sampling paths on that pattern; the naive version reopens the
source per task and dominates runtime.
