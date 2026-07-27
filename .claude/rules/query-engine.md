---
paths:
  - "src/gedih3/gh3driver.py"
  - "src/gedih3/sqlutils.py"
  - "src/gedih3/cli/gh3_extract.py"
  - "src/gedih3/cli/gh3_aggregate.py"
  - "src/gedih3/cli/gh3_rasterize.py"
  - "src/gedih3/cli/gh3_build_ducklake.py"
---

# Query and load path

`gh3_load` / `egi_load` are the two entry points; everything downstream (extract,
aggregate, rasterize, ancillary fusion) reads through them. The design goal is that a
region-scoped query never reads a byte it cannot need.

## Public API surface

`source=` is the path parameter on every loader. `configure_storage` /
`get_storage_options` (exported from `__init__.py`) hold remote credentials.
`resolve_s3_source` (`utils.py`) parses `s3://host:port/bucket/...` into an endpoint plus
a plain `s3://bucket/...` path — bucket names cannot contain `:`, which is what makes the
parse unambiguous. Every public entry point taking a `source=` must call it;
`endpoint_from_s3_urls` (`cliutils.py`) is the CLI-side half.

`gh3_select_partitions(source, region=None)` is the canonical, overhang-safe way to
determine which partition files a region touches, and is what external consumers should
call rather than re-deriving the intersection.

## Read-narrowing, in the order it applies

1. **Partition selection** — `_select_dataset_files` maps region → file subset for both
   index types (H3 via ring-1 expanded intersection, EGI via partition-square intersection
   with a densified, buffered ROI). Candidates come from the *filenames*; pruning only
   engages when `_dataset_prune_is_safe` proves the names bound their files (sidecar
   partition level strictly coarser than index level, all IDs at that level, no renames
   since `partition_ids` was written). Fails safe: unprovable or unparseable → all files;
   empty intersection → keep one file so schema and metadata still resolve.
2. **Data-bbox index** — `_bbox_index.parquet` at the DB root maps each partition
   year-file to its true data envelope, materialized from existing row-group statistics
   (footer-only). `_load_bbox_index` returns `{rel_key: (lon0, lat0, lon1, lat1)}`, cached,
   and fail-safe `None` when absent — absence only costs speed. Use `_bbox_index_key`
   (last three path segments) and `_bbox_disjoint` (inclusive-edge test); never re-derive
   keys or overlap logic inline. Pass the index as a second `from_map` iterable, never as
   a broadcast kwarg.
3. **Predicate pushdown** — see below.

**Bbox-index staleness is impossible by construction**, and must stay that way:
`invalidate_bbox_index` is called at merge *entry* in `_merge_and_finalize` and after
applied `gh3_doctor --fix` remedies, and the loader ignores any index older than the build
log. If you add a path that mutates the DB, call `invalidate_bbox_index`.

## `filters=`

`gh3_load` and `egi_load` both accept `filters=`: a conjunctive list of
`(column, op, value)` tuples, a DNF list-of-lists, or a pyarrow `Expression`. It is pushed
straight into each per-year parquet read, so row groups that cannot match are never
decompressed, and it is ANDed with the region/tile bbox predicate at
`_read_parquet_bbox(..., extra_filters=)` when both apply.

Two contract points that are easy to get wrong:

- Predicate columns do **not** have to be listed in `columns`.
- A file whose schema lacks a predicate column **raises**, rather than silently returning
  unfiltered rows. That strictness is deliberate — silent unfiltered output is worse than
  a crash.

`query=` is different: it filters in pandas *after* the read, so those rows do enter
worker memory. Prefer `filters=`.

## `from_map`

`from_map=True` is the default and the only maintained path. **`from_map=False` is
deprecated** — it selects the original `dask_geopandas.read_parquet` branch, which reads
`_metadata`, gets no region bbox pushdown and no `_bbox_index` file skipping, and is
slower on every axis. The argument and the branch are slated for removal. Do not build on
it or extend it.

## Metadata without I/O

`gh3_load` builds its Dask `_meta` from the `h3_columns_dtypes` cache in the build log via
`_meta_from_dtype_dict`, falling back to sampling a partition only for legacy DBs that
lack the field. This eliminates a parquet sample read on every load, which compounds
across chained tools (extract → aggregate → rasterize). Read build-log fields through
`gh3_read_meta` and the cached `json_read_cached`, not a fresh `json_read` — the log runs
to tens of MB on a continental database.

## Aggregation

`gh3_aggregate` / `egi_aggregate` use `map_partitions` so each partition is processed
independently — no shuffle. `egi_load` pre-computes the EGI↔H3 intersection and reads
tiles directly, so it needs no `set_index()` either. Keep it that way; a shuffle at this
scale is not recoverable.

Stay lazy. Query paths return Dask objects unless the caller explicitly asked to compute;
never materialize a whole database to satisfy an intermediate step.

## DuckLake / DuckDB (`sqlutils.py`)

`gh3_build_ducklake` builds a DuckLake catalog over an H3 database. `init_duckdb` sets up
the connection and extensions; `attach_ducklake_db` attaches the catalog;
`geoseries_to_cells` and `geoseries_to_filter` translate a geometry into an H3 cell set
and a SQL predicate (the latter goes through `h3_expand_ring`, so it inherits the ring-1
overhang guarantee); `duck_to_gdf` and `gdf_to_duck` bridge to GeoPandas.

The `duckdb` pin in `pyproject.toml` is narrow on purpose — the DuckLake catalog format is
version-locked, and widening the pin silently breaks existing catalogs. Read the inline
comment there before touching it.
