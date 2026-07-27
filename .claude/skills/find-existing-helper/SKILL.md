---
name: find-existing-helper
description: Index of gedih3's reusable helpers — atomic writes, manifest-aware globbing, remote/S3 path and parquet primitives, region and geometry normalization, H3/EGI cell math, cached metadata lookups, dask fan-out, merge and failure-recovery utilities, DuckLake bridges. Use BEFORE writing any new helper function, and when deciding whether a capability already exists in utils.py, cliutils.py, parallel.py, h3utils.py, sqlutils.py, gh3driver.py or gh3builder.py.
allowed-tools: Read, Grep, Glob
---

# Existing helpers

Every helper below has a numpydoc docstring — read it rather than trusting this one-line
summary. The purpose of this index is **discoverability**: knowing a thing exists so you
don't build a second one. The second column is the module; it is machine-checked by
`tests/test_agent_docs.py`, so keep it exact.

## Files and atomicity

| Helper | Module | Use for |
|---|---|---|
| `AtomicFileWriter` | `utils.py` | Every output write. `.tmp` + `os.replace`, cleanup on exception. |
| `h5_is_valid` | `utils.py` | "Is this HDF5 readable" — cheap header open, the resume-safety gate. |
| `parquet_merge_files` | `utils.py` | Streaming per-file merge with bounded memory, GeoParquet bbox, inline stats capture. |
| `parquet_schema_add_bbox` | `utils.py` | Embed GeoParquet `bbox` metadata into a finalized parquet. |
| `parquet_join_columns` | `utils.py` | Add columns to an existing parquet via `.join.tmp` + `os.replace`, skipping columns already present. |
| `read_parquet_schema` | `utils.py` | Footer-only schema read → DataFrame of `column` + `dtype`. |
| `_iter_batches_with_path` | `utils.py` | Wrap a pyarrow `iter_batches` so mid-stream failures re-raise with `[file=…]`. |

## Paths, remote storage, and columnar reads

| Helper | Module | Use for |
|---|---|---|
| `smart_open` / `smart_join` / `smart_exists` / `smart_isdir` | `utils.py` | Path-agnostic local + HTTP/S3 primitives. |
| `smart_database_exists` | `utils.py` | "Does this database root exist" — HEADs the root sidecars instead of LISTing a prefix. |
| `smart_isfile` | `utils.py` | Single file vs. dataset directory. Extension-based for remote paths. |
| `smart_glob` | `utils.py` | Manifest-aware glob. Use anywhere you would write `glob.glob(..., recursive=True)`. |
| `smart_open_columnar` / `read_parquet_coalesced` | `utils.py` | The remote-parquet read pair: fsspec read-ahead off, pyarrow `pre_buffer=True`. |
| `json_read_cached` | `utils.py` | Memoized read of a read-only metadata sidecar. Use plain `json_read` for read-modify-write. |
| `resolve_s3_source` | `utils.py` | Python-API parse of `s3://host:port/bucket/...` → endpoint + plain path. |
| `NON_LOCAL_PREFIXES` | `utils.py` | Canonical "not a local path" tuple: fsspec schemes + `gs://` + GDAL `/vsi*`. |
| `configure_storage` / `get_storage_options` | `__init__.py` | Top-level remote-credential API. |
| `endpoint_from_s3_urls` | `cliutils.py` | CLI-side half of the `s3://host:port` parse. |

## Manifests

| Helper | Module | Use for |
|---|---|---|
| `generate_manifest` / `_read_manifest` | `utils.py` | Maintain / read the `_manifest.txt` sentinel. |
| `check_manifest_freshness` | `parallel.py` | Constant-time mtime smoke test; the producer-crash guard. |
| `write_soc_manifest` | `gedidriver.py` | Write the SOC-side sidecar. **Write-only by design — do not add a reader.** |

## Parallelism

| Helper | Module | Use for |
|---|---|---|
| `parallel_map` | `parallel.py` | The package-wide fan-out primitive. Raises when no Client is registered. |
| `walk_soc_parallel` / `walk_h3db_parallel` / `walk_flat_parallel` | `parallel.py` | Parallel tree walks that replace serial recursive globs. |
| `partition_is_empty` / `list_year_dirs` / `year_dir_is_empty` | `doctor/parallel.py` | O(1) `os.scandir` emptiness checks. |
| `progress_iter` | `cliutils.py` | Consistent progress reporting over an iterable. |

## Spatial

| Helper | Module | Use for |
|---|---|---|
| `region_to_geometry` | `utils.py` | The one region normalizer → EPSG:4326 geometry. |
| `h3_partition_bbox` | `utils.py` | Partition bbox from the H3 cell ID — no geometry scan. |
| `h3_expand_ring` | `h3utils.py` | Ring expansion of a cell set. The overhang-safety primitive. |
| `gh3_select_partitions` | `gh3driver.py` | Public, overhang-safe region → partition-file selection. |
| `_select_dataset_files` | `gh3driver.py` | Internal region → dataset-file subset, both index types. |
| `gh3_build_bbox_index` / `_load_bbox_index` / `invalidate_bbox_index` | `gh3driver.py` | The data-bbox index trio. Call `invalidate_bbox_index` from any new DB-mutating path. |
| `_bbox_index_key` / `_bbox_disjoint` | `gh3driver.py` | Index key normalizer and inclusive-edge disjointness test. |
| `_read_parquet_bbox` | `gh3driver.py` | The read that ANDs a caller predicate with the bbox predicate (`extra_filters=`). |

## Metadata lookups that avoid I/O

| Helper | Module | Use for |
|---|---|---|
| `gh3_read_meta` | `gh3driver.py` | Build-log fields: `h3_columns`, `h3_columns_dtypes`, `h3_partition_level`, `h3_partition_ids`. |
| `_meta_from_dtype_dict` | `gh3driver.py` | Dask `_meta` from cached dtypes — no parquet read. |
| `gedi_vars_static` | `gedidriver.py` | Cached per-product variable list from the manifests shipped in `src/gedih3/data/`. |
| `gedi_vars_from_h5` | `gedidriver.py` | HDF5 BEAM-tree walk. Only for files that may have been subset already. |

## Build internals

| Helper | Module | Use for |
|---|---|---|
| `_derive_merged_output_paths` | `gh3builder.py` | `_merge_progress.txt` → final parquet paths, in memory. |
| `_scan_partition_meta_granules` | `gh3builder.py` | Worker-pickleable granule-ID parser for one partition. |
| `preclean_merge_failures` / `apply_merge_failures_to_logger` | `gh3builder.py` | The merge-failure recovery loop. Idempotent. |
| `_emit_merge_failure_sentinel` / `_scan_merge_failure_sentinels` | `gh3builder.py` | Atomic per-failure sentinels under `_merge_failures/`. |
| `_classify_load_h5_failure` / `_append_granule_failure` / `_read_granule_failures` | `gh3builder.py` | Stage 1 failure telemetry. |
| `manifest_check_scope` | `cli/gh3_build.py` | Regime-aware gate before `validate_soc_files` on a resume. |
| `explicit_vars_missing_in_sample` | `cli/gh3_build.py` | Pre-flight typo check against a sample HDF5. |

## Raster, DataFrame, and SQL

| Helper | Module | Use for |
|---|---|---|
| `gh3_rasterize` | `gh3driver.py` | Dataset dir **or** (Dask) frame -> GeoTIFF tiles + VRT, or a merged file. Index-type dispatch, column wildcards and H3 partition-level derivation all live here; both raster CLIs delegate to it. Never re-pick a rasterize_func in a CLI. |
| `gh3_to_raster` | `gh3driver.py` | One in-memory frame -> `xr.Dataset`, dispatching H3 (EPSG:4326) vs EGI (EPSG:6933). |
| `split_by_outer_tile` | `egi/raster.py` | Split a multi-tile EGI frame into one sub-frame per level-12 tile. For callers that legitimately hold many tiles (arbitrary-ROI aggregates) — NOT for partitions, which nest in one tile by construction. |
| `build_vrt` / `build_vrt_xml` / `build_vrt_safe` | `raster/export.py` | VRT mosaics. Use `build_vrt_safe` when the tiles are the deliverable. |
| `object_series` | `utils.py` | Object-dtype Series of array-likes (xarray Datasets, ndarrays). |
| `init_duckdb` / `attach_ducklake_db` | `sqlutils.py` | DuckDB connection + DuckLake catalog attach. |
| `geoseries_to_cells` / `geoseries_to_filter` | `sqlutils.py` | Geometry → H3 cell set / SQL predicate (ring-1 safe). |
| `duck_to_gdf` / `gdf_to_duck` | `sqlutils.py` | DuckDB ↔ GeoPandas bridge. |

## Auditing for duplication

Before adding a helper, check whether one of these already covers it:

```bash
grep -rn "def <candidate_name>" src/gedih3/                 # exact name
grep -rn "os.replace\|\.tmp" src/gedih3/ --include=*.py     # atomic-write reimplementations
grep -rn "glob.glob" src/gedih3/ --include=*.py             # should be smart_glob
grep -rn "client.map" src/gedih3/ --include=*.py            # should be parallel_map
grep -rn "to_crs\|EPSG:6933" src/gedih3/ --include=*.py     # CRS handling that may duplicate egi/
```

If the logic appears in two places, extract it into `utils.py` (library-wide) or
`cliutils.py` (CLI-only) rather than adding a third. Keep CLI modules thin — analysis logic
belongs in the library so the Python API and the CLI cannot diverge.
