---
paths:
  - "src/gedih3/**/*.py"
---

# Package source conventions

Cross-cutting rules for anything under `src/gedih3/`. Subsystem detail lives in the
sibling rules (`build-pipeline`, `query-engine`, `spatial-indexing`, `cli`, `doctor`),
which load when you open the files they cover.

## Before writing a helper

Invoke the `find-existing-helper` skill. This package has a large, deliberately shared
utility layer, and reimplementing part of it is the most common defect in changes here.
The helpers below exist because the obvious alternative is *wrong*, not merely
duplicative — reach for them by reflex.

| Instead of | Use | Because |
|---|---|---|
| `open(path, 'w')` on any output | `AtomicFileWriter` (`utils.py`) | `.tmp` + `os.replace` + cleanup on exception. A crash must never leave a partial output. Only geo-vector formats (geojson/gpkg/shp) are exempt — they need extension-based driver inference and shapefiles emit sidecars. |
| `glob.glob(..., recursive=True)` | `smart_glob` (`utils.py`) | Reads the `_manifest.txt` sentinel first; falls back to a walk. O(N)→O(1) on the metadata server for million-file trees. |
| `os.path.exists(db_root)` | `smart_database_exists` (`utils.py`) | Object stores answer `exists()` on a prefix with a LIST. This HEADs the root sidecars instead. |
| `os.path.isfile` on a possibly-remote path | `smart_isfile` (`utils.py`) | Remote prefixes never list; discrimination is extension-based there. |
| ad-hoc `str.startswith('s3://')` | `NON_LOCAL_PREFIXES` (`utils.py`) | Canonical "not a local path" tuple: fsspec schemes + `gs://` + GDAL `/vsi*`. Distinct from `is_remote_path`, which is the narrower fsspec-readable set. |
| bespoke bbox/CRS parsing of a region argument | `region_to_geometry` (`utils.py`) | The one normalizer for path / `"W,S,E,N"` / list / GeoDataFrame / GeoSeries / shapely → EPSG:4326. Use it in any new spatial-selection path so H3 and EGI cannot diverge. |
| exact polygon ∩ partition cells | `h3_expand_ring` (`h3utils.py`) or `gh3_select_partitions` (`gh3driver.py`) | See "Ring-1 overhang" below. |
| `pd.read_parquet(remote_path)` | `smart_open_columnar` + `read_parquet_coalesced` (`utils.py`) | fsspec read-ahead off, pyarrow `pre_buffer=True`. 359 MB → 18 MB of server reads for a one-column projection of a 1.8 GB partition. Pandas' reader does not coalesce. Local reads keep the plain readers. |
| `json_read` on a read-only sidecar | `json_read_cached` (`utils.py`) | The build log is tens of MB on a continental DB and was fetched 13× per `gh3_aggregate`. Use plain `json_read` only for read-modify-write. |
| `pd.Series([...], dtype=object)` of array-likes | `object_series` (`utils.py`) | `pd.Series` coerces through `np.asarray`, which xarray Datasets reject. Needed for any `map_partitions` result carrying non-scalars. |
| new `client.map` glue | `parallel_map` (`parallel.py`) | Streaming `as_completed` + optional batching for >10k fan-outs. |
| `glob('**/*.parquet')` to test emptiness | `partition_is_empty` / `year_dir_is_empty` (`doctor/parallel.py`) | O(1) `os.scandir`. |
| `gdal.BuildVRT` | `build_vrt_safe` (`raster/export.py`) | Prefers `osgeo`, falls back to a rasterio-only writer, and downgrades mosaic failure to a WARNING when the `.tif` tiles are the real deliverable. |
| opening a file to learn its schema/bounds | `gh3_read_meta` (`gh3driver.py`), `gedi_vars_static` (`gedidriver.py`), `h3_partition_bbox` (`utils.py`) | See "A-priori over detection" below. |

## Invariants

**Ring-1 overhang.** H3 partitions do not geometrically contain all of their shots —
child cells overhang the parent boundary by roughly 0.18 × edge. Any ROI → partition
selection must expand by one ring (`h3_expand_ring`, or the public
`gh3_select_partitions`). Exact polygon intersection alone *looks* correct and silently
drops boundary shots stored in neighbour partitions. This failure produces wrong
numbers, not an error.

**`_soc_manifest.txt` is write-only by design.** `_read_soc_manifest` still exists in
`gedidriver.py` and has **zero callers** — it is a decoy. Do not wire it back in.
External population paths (manual rsync, a NASA delivery) bypass the producer-driven
refresh, and a stale manifest silently narrows every scan. SOC enumeration always fans a
`walk_soc_parallel` year/doy scan across the registered Client. The H3-side
`_manifest.txt` *is* read — the asymmetry is deliberate.

**Producer-driven manifest refresh.** Every code path that mutates a SOC or H3 tree
refreshes the corresponding manifest before returning. Consumers trust it blindly; the
only consumer-side check is the constant-time `check_manifest_freshness` mtime smoke test
wired into `_read_manifest`, which logs a loud ERROR pointing at the relevant
`gh3_doctor --fix` remedy.

**`parallel_map` has no serial fallback.** It raises `GediError` when no dask Client is
registered. Do not write call sites that assume graceful degradation to a loop.

**A-priori over detection.** If the answer is knowable from structure, look it up instead
of doing I/O: shipped per-product variable manifests (`gedi_vars_static`), sidecar
metadata (`gh3_read_meta` for `h3_columns`, `h3_columns_dtypes`, `h3_partition_level`,
`h3_partition_ids`), H3 cell math (`h3_partition_bbox`). The question to ask of any new
code path: *am I doing work the structure of the data already answers?*

## Conventions

- **Exceptions**: raise a specific `GediError` subclass from `exceptions.py`, never a
  bare `Exception`. Callers catch by subclass for targeted recovery.
- **Docstrings**: numpydoc, on every public function. They are the reference — this
  package's docstrings carry the sharp edges, so state a contract there rather than in a
  markdown file that will drift from it.
- **Line length**: 120. `ruff check src/` must pass.
- **`osgeo` is the only permitted optional import** — it has no PyPI wheels. Every call
  site needs `try`/`except ImportError` plus a working fallback, and the module must be
  in `OPTIONAL_MODULES` in `tests/test_dependencies.py`. Every other imported module must
  be declared in `pyproject.toml`; the AST walk in that test fails the build otherwise.
