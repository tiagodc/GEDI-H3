# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.8.21] - 2026-05-06

### Changed
- **`h3_write_metadata` no longer re-reads the merged file.** The end-of-merge step previously did `pd.read_parquet(h3_file, columns=['shot_number','root_file_l2a','datetime'])`, which materialized ~1.5–2 GB peak per dense partition (3 cols × ~30M rows for tropical h3_03 cells) and produced a redundant GPFS read of the just-written file. Now the four needed quantities (shot count, shot min/max, datetime min/max, set of unique granule filenames) are accumulated **online** during `parquet_merge_files`'s batch loop using `pyarrow.compute.min/max/unique` — zero allocation per batch beyond a small set of unique strings (~10–100 KB even for the worst-case partition). `parquet_merge_files` returns a stats dict; `h3_merge_files` passes it to `h3_write_metadata` via a new optional `stats=` argument.
- `h3_write_metadata(h3_file, stats=None)` falls back to the old `pd.read_parquet` path when stats are not provided or any required field is missing — preserves backward compatibility for any caller that still uses the read-back form (no current callers do).

### Performance
- Per-merge peak memory drops by **~1.5–2 GB** for dense partitions (depends on shot count). High-water-mark RSS on long-lived workers stops climbing to that ceiling.
- Per-merge GPFS I/O: eliminates the read-back of three full columns of the merged file (~hundreds of MB). On contended GPFS this also reduces metadata-server pressure since the final-file footer/data reads are gone.

### Tests
- 70 tests still pass. Test fixtures use parquets without `shot_number`/`root_file_l2a`/`datetime` columns; `parquet_merge_files` now returns a stats dict where those fields are `None`, callers ignore the return value, no behavior change.
- Added smoke test verifying the fast path (stats-from-merge) and the read-back path produce **identical metadata** (l2a_version, h3_partition, year, shot_count, shot_range, date_range, granules).

## [0.8.20] - 2026-05-06

### Changed
- **Bbox derived from H3 partition geometry — no data scan during merge.** `parquet_merge_files` now accepts a `bbox` parameter; the streaming-bbox computation that read every fragment's geometry column is gone from the merge hot path. `gh3builder.h3_merge_files` parses the H3 cell ID from the partition directory name and computes a buffered bbox via the new `h3_partition_bbox(cell_id, parent_res)` helper — microseconds per merge, no GPFS reads. On a 400-fragment partition this saves ~10–60 s per merge depending on partition size, and removes a non-trivial chunk of metadata-server pressure under heavy worker concurrency.
- **Buffer formula based on empirical icosahedral distortion.** Verified by exhaustive enumeration across H3 resolution pairs: max child overhang asymptotes to ~14% of parent edge length, regardless of how deep the children are (once depth gap ≥ 5 levels). Default `edge_fraction=0.18` (14% × 1.2 safety). Buffer applied with cosine correction at the parent's most poleward vertex so the same scalar in degrees is safe for both lat and lon directions.

### Added
- `utils.h3_partition_bbox(h3_cell_id, parent_res, edge_fraction=0.18)` — public helper returning the EPSG:4326 bbox of an H3 cell padded for safe descendant containment.
- `utils.parse_h3_partition_dirname('h3_03=830e4afffffffff')` → `('830e4afffffffff', 3)` — parser used by `h3_merge_files` to recover the H3 cell from the partition directory name.

### Trade-off
- Output bbox is a guaranteed-valid upper bound but not tight. For an `h3_03` partition (~69 km edge) the buffer expands the bbox by ~9% per side. Predicate pushdown still skips non-overlapping queries correctly; queries near a partition's edge may scan the file when actual data doesn't fall in the query region. Cost: one extra parquet scan per false-positive partition.
- `_streaming_bbox` is preserved for `parquet_backfill_bbox` (the doctor's per-file rewrite path) where the H3 cell is unknown.

## [0.8.19] - 2026-05-05

### Changed
- **Single dataset, no Python footer loop in the merge hot path.** `parquet_merge_files` now takes the schema from the first file (one `pq.read_schema`), constructs a single `ds.dataset(flist, schema=...)` (provided schema → no per-file footer scan at construction; pyarrow trusts our schema and reads footers lazily during scan with pipelined async I/O in C++), and uses that dataset's scanner for both bbox computation and the merge stream. Per-merge metadata-server contact drops from ~2N (schema + bbox footer reads) to whatever pyarrow's pipelined reader does internally — a meaningful reduction at high worker counts on shared GPFS where metadata-server contention is the bottleneck.
- **Bbox via streaming geometry column** (`_streaming_bbox` is now the single bbox path). Constructs the same dataset, runs scanner with `columns=['geometry']`, decodes WKB → bounds via vectorized `shapely.from_wkb` + `shapely.bounds` per batch, accumulates online. Memory bounded by `batch_size=1_000_000` regardless of partition size — safe for 30 M-shot tropical partitions even at 128 concurrent workers.
- **Column-order drift handled in C++.** The dataset scanner casts each batch to the provided schema during the scan, replacing the per-batch Python reconciler from 0.8.17–0.8.18. Faster (no Python loop per batch) and same correctness on the 12/201 partition-years that had column-order drift in the wild.

### Removed
- `_merged_bbox(flist)` helper — folded into `_streaming_bbox` (single path).
- `_merged_bbox_and_schema(flist)` helper — schema is now first-file-only, bbox is streaming-only.
- `_make_batch_reconciler(file_schema, target_schema)` — superseded by the dataset's C++ cast.

## [0.8.18] - 2026-05-05

### Changed
- **Per-file batch reconciler factory.** Replaced the per-batch `_reconcile_batch_to_schema(batch, schema)` call with `_make_batch_reconciler(file_schema, target_schema)`, called once per file at the top of the per-file iteration loop. The schema-comparison loop (~2,540 Python ops over ~1,270 columns) ran on every batch in v0.8.17 — ~250 K interpreter ops per merge wasted on the fast path where every batch from a given file has the same schema. The factory now does that work once per file: returns `None` when the file's schema matches the writer's field-for-field (every batch passes through unchanged, true zero-copy), or returns a closure with a pre-computed `name → column-index` mapping that handles drift in O(num_columns) per batch via direct integer lookup instead of `name in batch_names` set-membership checks.

## [0.8.17] - 2026-05-05

### Fixed
- **`parquet_merge_files` schema-mismatch crash on continental builds.** v0.8.16 dropped the `ds.dataset(flist)` schema unification on the assumption that fragments in a partition-year share an identical column schema. False in practice — `dask_geopandas.to_parquet` produces fragments whose columns can appear in different ORDER (verified on 201 random partition-years: 12 had column-order drift, 0 had column-set drift). With per-file iteration and a positional `pa.Table.from_batches([batch], schema=schema)`, the order mismatch raised `ArrowInvalid('Schema at index 0 was different…')` mid-merge. ~37% of tested partitions failed before the build was halted.

### Changed
- **One footer pass per merge.** Replaced two passes (`ds.dataset()` schema unification + `_merged_bbox()` bbox extraction) with a single helper `_merged_bbox_and_schema(flist)` that reads each fragment's footer once and returns `(unified_schema, merged_bbox)` together. The unified schema is a column-name union (each name appears once, type from the first file that declares it). Net cost: same as v0.8.16 (one footer per file), but correct.
- New helper `_reconcile_batch_to_schema(batch, schema)` reorders each batch's columns to match the writer schema by NAME — null-fills missing columns, drops extras, casts types. Fast path: when the batch already matches field-for-field, returns it unchanged (zero-copy). Solves both column-order drift (the common case observed in the wild) and column-set drift (defensive coverage).
- Per-file streaming via `pq.ParquetFile.iter_batches()` is preserved — `ds.dataset()` is no longer constructed in the merge hot path.

## [0.8.16] - 2026-05-05

### Changed
- **Drop `ds.dataset(flist)` from the merge hot path.** `parquet_merge_files` no longer constructs a pyarrow dataset over the input fragments — that constructor reads every fragment's footer to build a unified schema, which on a continental partition-year (~400 fragments on shared GPFS) cost ~25–30 s of redundant I/O per merge. Replaced with: schema from `pq.read_schema(flist[0])` (single footer), and a per-file `pq.ParquetFile(f).iter_batches(...)` loop instead of `dataset.scanner(...).to_batches()`. The implicit assumption — fragments in a partition-year share an identical column schema — is true by construction in gh3_build (all fragments come from one `dask_geopandas.to_parquet` call).
- Renamed `_streaming_bbox_from_dataset` → `_streaming_bbox`. Same semantics, but iterates per-file via `pq.ParquetFile.iter_batches(columns=['geometry'])` instead of `ds.dataset(flist).scanner(...)`. Avoids the same redundant footer-scan in the slow-path bbox fallback.
- `parquet_backfill_bbox` (single-file rewrite) likewise dropped the `ds.dataset([path])` wrapper for direct `pq.ParquetFile` iteration.

## [0.8.15] - 2026-05-05

### Added
- **`gh3_doctor` diagnosis `geoparquet_bbox`** — detects partition parquets whose GeoParquet `geo` schema metadata is missing or has no usable `columns.<primary>.bbox`, and backfills the bbox in place. Native pyarrow validation (no new runtime deps); evaluated against `geoparquet-pydantic`, GDAL `validate_geoparquet.py`, and `geoparquet-io` and chose to keep the check focused on the spec field that actually breaks predicate pushdown rather than pulling in a heavier validator.
- **`utils.parquet_backfill_bbox(path)`** — rewrites a single parquet file in place to add/refresh its bbox metadata. Atomic via `<path>.bbox.tmp` + `os.replace`. Idempotent: returns `'ok'` for files that already have a valid bbox, `'rewritten'` for files actually modified, `'no_geometry'` for non-spatial sidecars. Raises `ValueError` if the file lacks the entire `geo` metadata key (would need a full re-merge from source — not in-place fixable).
- Findings the new diagnosis can produce:
  - `missing_geo` (no `geo` schema metadata) — reported only, recommends full re-merge.
  - `missing_bbox` (`geo` exists, no bbox field) — auto-fixed by backfill.
  - `invalid_bbox` (bbox present but malformed/non-finite) — auto-fixed by backfill.
  - `no_geometry` (no `geometry` column) — reported only.
  - `unreadable` (parquet schema unreadable) — reported only.
- Wired into the `db` alias group, so `gh3_doctor -i /db --check db` and `gh3_doctor -i /db --fix db` pick it up automatically.
- New tests `test_geoparquet_bbox_passes_when_bbox_present` and `test_geoparquet_bbox_detects_and_backfills_missing_bbox` covering both the green-path and detect+fix+re-check round-trip.

## [0.8.14] - 2026-05-05

### Changed
- **`parquet_merge_files` now always embeds a valid GeoParquet bbox.** Removed the `bbox_threshold` knob and the `GH3_MERGE_BBOX_THRESHOLD` env var. Every merged partition with a `geometry` column now writes a spec-valid GeoParquet `columns.geometry.bbox` in its footer, so downstream readers (geopandas, gh3_load, dask_geopandas) get correct predicate pushdown and the file passes geoparquet validation. Memory cost is bounded:
  - **Fast path** — every input fragment already has a footer-level GeoParquet bbox (true for fragments written by `dask_geopandas.to_parquet`, which is always the case in gh3_build): bbox is the elementwise union of those, no data is read.
  - **Slow path** — any input lacks a footer bbox: stream just the geometry column for those files in batches, decode WKB via vectorized `shapely.from_wkb` + `shapely.bounds`, reduce online. Bounded by `batch_size=100_000` (≈10–20 MB per batch), no full geometry materialization.
- New helpers in `utils.py`: `_bbox_from_geo_metadata(path)` (footer read) and `_streaming_bbox_from_dataset(flist)` (slow-path fallback). Both are used internally by `_merged_bbox(flist)`.

## [0.8.13] - 2026-05-05

### Fixed
- **Dask dashboard merge progress bar fills monotonically.** Removed the per-completion `future.release()` in `_merge_and_finalize`'s `as_completed` loop. Releasing finished futures told the scheduler to drop the task records, which made the dashboard's task panel drain (both numerator and denominator shrinking together) instead of filling. Without `release()`, the scheduler retains finished task records for the duration of the merge phase (~tens of MB total), and the dashboard shows `X done / 47k total` filling as expected. Terminal tqdm bar behavior is unchanged.

## [0.8.12] - 2026-05-05

### Changed
- **Parallelize tmp partition listing in `_merge_and_finalize`.** Replaced the driver-side `glob.glob('h3_*/year=*/')` (a serial two-level walk costing ~50k GPFS readdirs sequentially on a continental build) with `os.scandir` on the outer `h3_*` level + `client.map(_list_year_subdirs, h3_dirs)` on the inner `year=*` level. Cumulative readdir latency that previously delayed the first task submit by minutes is now distributed across all workers. Falls back to in-process scan when no client is available (preserves behavior for unit tests / non-Dask invocations).

## [0.8.11] - 2026-05-05

### Removed
- **Driver-side inflight throttle on the merge phase.** `_merge_and_finalize` no longer reads `n_workers` from `client.scheduler_info().get('workers')` and uses it as a static `max_inflight` cap with a manual `priming + 1-for-1 refill` loop. That counter was unreliable — `scheduler_info()` returns a subset of the cluster (5 vs the 48 actually connected, observed via `c.processing()`) — so `max_inflight` got pinned at the under-counted value at startup and never grew, throttling throughput to a fraction of cluster capacity for the entire merge phase. Also removes the `GH3_MERGE_MAX_INFLIGHT` env knob it gated.
- The companion `_submit_one()` helper, the `pending` deque, and the `inflight=` postfix on the progress bar.

### Changed
- `_merge_and_finalize` now submits every remaining merge to the scheduler at once via `client.map(h3_merge_files, remaining_dirs, ...)` and consumes results via `as_completed`. Work distribution is delegated to the dask scheduler, which sees the full worker set.

## [0.8.10] - 2026-05-05

### Fixed
- **Pre-merge driver-side scan bottleneck.** `_merge_and_finalize` no longer filters empty tmp partition dirs on the driver via per-dir `os.listdir`, and no longer globs the entire final database for stale `.merge.tmp` files. On a continental build (~9.7k tmp partitions × N years on shared GPFS) those two driver-side serial scans took >40 min to complete with the cluster 100% idle — no work was submitted until the scans finished. Empty-dir handling is now delegated to the worker (the existing `if len(files) == 0: return` short-circuit in `h3_merge_files`), and stale-tmp cleanup is scoped to the per-partition `odir` inside `h3_merge_files` (one `listdir` per merge instead of a global glob). First-task submit is now bounded by the single `glob('h3_*/year=*/')` call.

## [0.8.9] - 2026-05-05

### Added
- **Resume shortcut for merge-only resumes.** When a previous run finished the extract phase and was killed during merge, gh3_build now skips the (often very expensive) reconcile + extract pipeline and jumps straight to `_merge_and_finalize`. Detection is two-layered: (L1, canonical) the on-disk build-log status is `MERGING` — set by `build_h3db` immediately before merge starts; (L2, fallback for builds started before this code shipped) `<tmp>/partitions/_merge_progress.txt` exists with at least one merged-partition entry — non-empty progress file proves merge already started, which proves extract finished. The shortcut helper `_detect_merge_resume_signal(h3_logger, parquet_dir)` is exposed for testing. Contract: between crash and resume, the SOC tree is treated as frozen on this path; new HDF5s added between runs require finishing the current build first. PENDING entries left in `granule_info` after a merge-resume are corrected on the next non-shortcut run (or by `gh3_doctor`).

### Changed
- `_merge_and_finalize` now skips empty tmp partition dirs (`h3_*/year=*/` with no `*.parquet` content) instead of attempting a zero-fragment merge. A previous run can leave such scaffolding behind after `rm_src=True` drained an already-merged partition. Logged as a count.
- `h3_merge_files` now validates that the existing destination parquet is readable before treating it as input for a delta-merge or short-circuiting via the disk-canonical "newer than sources" skip. A corrupt destination (e.g. left over from a crash mid-write) is now detected via a header check (`pq.ParquetFile(...).metadata`); the corrupt file is logged at WARNING level, discarded, and the tmp fragments are merged fresh into a `.merge.tmp` then atomically renamed to overwrite the bad dest. Previously a corrupt dest aborted the entire merge phase.
- `gh3_doctor`'s `log_state` diagnosis now detects **granule status drift**: non-`INDEXED` entries in `granule_info` whose `(orbit, granule, track)` triple is already present in a finalized partition's metadata JSON. This is the expected side effect of the merge-only resume shortcut, which skips reconcile and leaves stale `PENDING` statuses behind. Fix path flips drifted entries to `INDEXED` and persists the log (preserving the current top-level status, e.g. `COMPLETED`).

### Tests
- New `tests/test_merge_resume.py`: unit coverage for `_detect_merge_resume_signal` (L1, L2, both present, no signal), empty tmp dir skip in `_merge_and_finalize` (small integration with a 2-worker LocalCluster), and corrupt-dest fallback in `h3_merge_files`.
- New test `test_log_state_detects_and_fixes_granule_status_drift` covering the new doctor diagnosis.

## [0.8.8] - 2026-05-03

### Removed
- `_RECONCILE_FRAGMENT_THREADS` env var and the in-process `ThreadPoolExecutor` inside `_process_h3_partition` (added in 0.8.6 / 0.8.7). On a real continental run on shared GPFS, every worker spawning N threads multiplied cluster-wide concurrent metadata requests by `nworkers × N`, overwhelming the metadata server before per-task throughput could catch up — at N=16 host load avg hit ~800, at N=4 it stayed at ~270 with throughput of ~4 tasks/min (worse than the unthreaded 0.8.5). Lesson: on a shared metadata-server-limited filesystem, all reconcile parallelism must come from `client.map` over h3 partitions; thread pools inside Dask tasks do not help and actively hurt by saturating GPFS in ways the dask scheduler can't see.

### Added
- Filename fast-path in `_process_h3_partition`: when a fragment basename matches the v0.8.0+ convention `O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}.parquet` (written by `_create_h3_dataframe` since v0.8.0), the granule ID is parsed from the basename — microseconds per file, no parquet I/O, no GPFS metadata round-trip. Falls back to the existing parquet-column-statistics read for legacy `part.NNN.parquet` names. After this change, reconcile on a fully v0.8.0+ tmp tree is essentially free regardless of fragment count.

## [0.8.7] - 2026-05-03

### Fixed
- `_RECONCILE_FRAGMENT_THREADS` default lowered from 16 to 4 in response to a real continental run that drove host load average to ~800 and overwhelmed the shared GPFS metadata server. The 0.8.6 default was chosen from a single-process microbenchmark that saturated at ~8 threads in *one* Python process; the multiplicative effect across 64 cluster workers (16 × 64 = 1024 concurrent metadata requests) was not properly accounted for. At N=4 the cluster-wide concurrency is 256 — still ~4× faster than serial per-file reads on cold GPFS, but well within what the metadata server handles cleanly. Now also tunable per-deployment via the `GH3_RECONCILE_FRAGMENT_THREADS` environment variable (read at module import time on the worker, so set it before launching `dask worker`).

## [0.8.6] - 2026-05-03

### Changed
- `_reconcile_granules_from_disk` (gh3builder.py): final-form simplification. After ground-truth measurement on the target dataset (90 ms cold/serial parquet metadata read on GPFS, 5 ms with `ThreadPoolExecutor(16)` inside one Python process), the prior 0.8.5 architecture (one Dask task per `h3_*` partition, serial parquet reads inside) was projected by extrapolation to take ~7.7 h best-case and ~39 h observed on a real continental run. Adding a thread pool inside each task drops per-task wall-clock by ~15× on real partitions (266→1.4 s, 3911→27 s, 4789→28 s). Estimated total reconcile time on a 19.8 M-fragment continental build now: 30–90 min, down from 39+ h.
- `_process_h3_partition(h3_dir, n_threads=16)`: now runs a `ThreadPoolExecutor` over the per-fragment parquet metadata reads. `os.stat` results are no longer collected (the file-mtime fingerprint that fed the cache is no longer used). Returns just the granule-ID set. The path list is consumed locally on the worker; only the small set crosses the network.

### Removed
- `_RECONCILE_CACHE_FILENAME` constant + persistent fragment-fingerprint cache (added in 0.8.4) and the two-pass list-then-process design (0.8.5). The cache fingerprint required a serial `os.stat()` over every fragment on the driver — itself a multi-tens-of-minutes operation on the target dataset, which made the cache net-negative on the very builds that motivated its existence. With the new threaded reconcile finishing in 30–90 min end-to-end, the cache savings on relaunch (~30–60 min) no longer justify the ~50 lines of cache I/O + 2nd worker function (`_list_h3_partition`) + 3 test cases for hit/miss/corrupt + the first-run-pays-fingerprint footgun. Reconcile is now a single Pass B with no validation pass and no cache.
- `_list_h3_partition` helper (0.8.5): only existed to feed the now-removed cache validation pass.
- `_iter_h3_op` closure (0.8.5): inlined back into the reconcile body now that there's a single pass.

### Postmortem
- The 0.8.3–0.8.5 sequence was directionally right (move work off the driver, parallelize across the cluster, stream futures through `as_completed`) but the per-task wall-clock estimates that drove the design were 90× off because the cost of `pq.ParquetFile(...).metadata.row_group(0).column(idx).statistics` on cold GPFS was never measured. Without that ground truth, optimizations that were supposed to make reconcile feasible (cache, two-pass listing, per-batch `client.map` granularity) instead piled abstractions on top of a per-task body that was still serial-blocking on file-open round-trips. The fix was to measure first (see `tests/test_resume_reconcile.py::TestProcessH3Partition` for the regression coverage), then add the single optimization that the measurement justified — a thread pool inside each worker — and rip out the speculative cache layer that was adding code without paying for itself.

## [0.8.5] - 2026-05-03

### Changed
- `_reconcile_granules_from_disk` (gh3builder.py): the listing and fingerprint passes are now distributed across the Dask cluster instead of running serially on the driver. Previously the function did `glob.glob(tmp_dir/h3_*/year=*/*.parquet)` (single-threaded driver-side, walks ~46 M GPFS metadata entries at ~50 µs each on a continental build → 50–70 min wall-clock) followed by `_reconcile_cache_fingerprint(frag_files)` which serially `os.stat()`-ed each of the same files (another 30–60 min). On a real continental build this combined for ~90 min of pure driver-side I/O before any cluster work could even start, with 64 workers sitting idle the entire time.
- New `_list_h3_partition(h3_dir)` helper: walks one h3_* tmp partition via `os.scandir` + `DirEntry.stat`, returns `(count, max_mtime)`. No parquet metadata reads. Used for cache validation only.
- New `_process_h3_partition(h3_dir)` helper: walks one h3_* tmp partition end-to-end on the worker (list + fingerprint + parquet metadata reads) and returns `(count, max_mtime, granule_id_set)`. The fragment path list never crosses the network — only the small derived result tuple comes back to the driver. Replaces the previous `_granule_ids_in_fragments` batch helper, which is removed.
- `_reconcile_granules_from_disk` now drives both passes via a unified `client.map(...) + as_completed` loop with a serial fallback when no Dask client is available. Wall-clock for a continental-scale reconcile drops from ~90 min (50 min glob + 30–60 min fingerprint) to ~10–15 min (single distributed pass), with a near-instant cache-hit path on subsequent relaunches.
- Cache key shape unchanged (`{count, max_mtime}`), so caches written by 0.8.4 remain comparable; only the function that *computes* the key has changed.

## [0.8.4] - 2026-05-03

### Added
- `_reconcile_granules_from_disk`: persistent fragment-fingerprint cache at `<tmp_dir>/_reconcile_cache.json`. After a successful disk scan, the function records `(frag_count, max_mtime)` together with the derived granule-ID set; on the next reconcile, if the fingerprint matches, the cached IDs are reused and the per-fragment parquet metadata reads are skipped entirely. Most relaunches of the same build (most common case during multi-day debugging) now finish reconcile in milliseconds. Pass A (the cheap metadata JSON walk under `h3_dir`) still re-runs every time, so finalized-partition changes are always reflected. A corrupt cache file is detected, ignored, and replaced on the next successful scan.

### Changed
- `_reconcile_granules_from_disk`: short-circuit when the build log already shows every granule as `INDEXED`. Returns 0 immediately without touching disk. Eliminates wasted work for builds that have completed stage 1 cleanly but are being relaunched (e.g. for a stage-2 retry).

## [0.8.3] - 2026-05-03

### Fixed
- `_reconcile_granules_from_disk` (gh3builder.py): on multi-million-fragment continental builds the previous implementation submitted ~one task per 64 files via `dask.bag.from_sequence(..., partition_size=64).map(...).persist()`, then collected via `bag.compute()`. With ~19 M fragments this produced ~300 k cluster keys held in worker memory for hours, ~5 k keys per worker. With the cluster otherwise idle waiting on the reconcile, individual workers occasionally locked the GIL during gc / arrow-pool-release / malloc_trim long enough to miss heartbeats; the scheduler force-restarted them after 5–10 min and queued ~5 k tasks for recompute per kill. Three such kills observed in a 2 h window made the reconcile non-converging on continental scale, blocking all subsequent build progress. Replaced the bag pattern with `client.map(_granule_ids_in_fragments, batches, pure=False)` over 4096-file batches plus a streaming `as_completed` consumer that updates the driver-side `indexed_ids` set and immediately releases each future. Cluster-side memory is now bounded to ~64 in-flight result keys regardless of input size, recompute exposure per worker drops from ~5 k tasks to ~75, and per-task overhead is amortized over a much larger work unit. Behavior, return value, and the in-process fallback path (`client is None or len(frag_files) <= 32`) are unchanged.

## [0.8.2] - 2026-05-03

### Fixed
- `dask-config-massive-build.yaml`: dropped the `worker.lifetime` block (4h rolling restart with 20-30m stagger). On a real continental build the AMM key handoff during graceful retirement saturated survivor event loops, heartbeats timed out, the scheduler force-removed retiring workers, and ~21k tasks were marked for recompute as >40% of the cluster died in ~15 minutes. With `MALLOC_TRIM_THRESHOLD_=0`, `ARROW_DEFAULT_MEMORY_POOL=system`, and the per-task gh3-trim preload already in place, RSS is bounded at the source — rolling restart was solving a problem that no longer exists.
- `dask-config-massive-build.yaml`: tightened `admin.tick.limit` from `1h` to `120s` so multi-minute event-loop stalls remain visible in logs (they were the key diagnostic in the retirement-cascade incident).
- `docs/user-guide/building-a-database.md`: rewrote the "unmanaged worker memory" section to recommend the env-vars-plus-preload mitigation as the primary fix instead of rolling restart; removed `--lifetime`/`--lifetime-stagger`/`--lifetime-restart` from the worker recipe; updated the bundled-YAML feature list; strengthened the sizing guidance with an explicit warning that 64 × 25 GB on a 1 TB node is 1.6× over-committed (kernel OOM-kills before Dask's `terminate: 0.95` fires) and that `--memory-limit` should be lowered when raising `--nworkers`.
- `docs/user-guide/building-a-database.md`: replaced incorrect `dask --config-file` example with `DASK_CONFIG` env var (the dask CLI has no `--config-file` flag).

## [0.8.1] - 2026-05-02

### Added
- New "Building Massive Databases" section in `docs/user-guide/building-a-database.md` documenting the external dask scheduler/worker pattern for global / continental builds: when to use it, the unmanaged-memory bottleneck, copy-paste recipes, sizing table by node RAM, cluster verification snippet, recovery procedure, and the bundled YAML config.
- `src/gedih3/data/dask-config-massive-build.yaml`: reference Dask config (memory thresholds, rolling worker lifetime restart, `nanny.pre-spawn-environ` for `MALLOC_TRIM_THRESHOLD_=0` and `ARROW_DEFAULT_MEMORY_POOL=system`). Not auto-loaded; users opt in via `dask scheduler/worker --config-file …` or `dask.config.update(yaml.safe_load(get_package_data_path(…)))`.
- `src/gedih3/data/dask-worker-trim.py`: optional dask worker preload module that registers a `WorkerPlugin` calling `gc.collect()`, `pyarrow.default_memory_pool().release_unused()`, and `libc.malloc_trim(0)` after every task transition. Now ships in the wheel (was `scripts/dask_worker_trim.py`); resolvable via `gedih3.config.get_package_data_path('dask-worker-trim.py')`.

### Changed
- `gh3_doctor --help` epilog now lists every registered diagnosis with its description and fixable status, expands alias groups (`db`, `soc`, `all`) to their concrete members, documents exit codes (0/1/2), and surfaces common knobs (`--orphan-age-hours`, `--soc-dir`, `-s`). Built dynamically from the diagnosis registry so it cannot drift when diagnoses are added.
- `pyproject.toml` `[tool.setuptools.package-data]` extended to include `data/*.yaml` and `data/*.py` so the new bundled assets ship in the wheel.
- `docs/conf.py` adds `autoapi_ignore = ['*/data/*']` so autoapi does not RST-parse the preload module's docstring (its trailing-underscore `MALLOC_TRIM_THRESHOLD_` looked like an RST hyperlink reference to docutils).
- `.gitignore` tightens `dask-*.yaml` to `/dask-*.yaml` so the pattern only matches root-level files, not the new packaged YAML.

### Fixed
- `parquet_merge_files` docstring: insert blank line between the "Memory profile (per call):" header and the bullet list so docutils does not interpret the colon-terminated header as a definition-list trigger. Resolves the docs CI failure introduced in v0.8.0.

### Removed
- Repo-root `dask-config.yaml` and `dask-config-aggressive-memory.yaml`. Both were never referenced by any code path; the aggressive variant had typos (`memory.target: 0.10`) that would have paused workers immediately at 10% memory if anyone had tried to use it.

## [0.8.0] - 2026-05-02

### Added
- Resumability for massive multi-day `gh3_build` runs. Granule status is now reconciled from on-disk fragments at every build start (`_reconcile_granules_from_disk`), so a kill at any point during stage 1 or stage 2 no longer triggers full re-extraction on rerun. Stage 1 fragments are written under stable, granule-derived basenames (`O{orbit}_G{granule}_T{track}.{beam}.parquet`) with `overwrite=False`, so re-extracted granules overwrite their own files in place — no shot duplication across reruns. Combined with the existing `_merge_progress.txt` tracker, the build classifies any restart into one of four states (fully done / merge partial / stage-1 partial / fresh) and resumes accordingly.
- Bounded merge concurrency in `_merge_and_finalize` via sliding-window `as_completed` (default cap = `n_workers`, override with `GH3_MERGE_MAX_INFLIGHT`). Replaces the previous "submit all 9,615 futures upfront" pattern that pipelined too many merges per worker and triggered OOM at large scale.
- Defensive `.merge.tmp` sweep at the start of every merge stage; cleans up half-written final parquet temp files left by a prior crash.
- Disk-canonical merge skip in `h3_merge_files`: when the final parquet exists and is newer than every source fragment, skip re-merging entirely (eliminates a class of "merge already done but tracker out of sync" bugs).
- Streaming-bbox-skip threshold in `parquet_merge_files` (`GH3_MERGE_BBOX_THRESHOLD`, default 50): skip the upfront geometry bbox computation when many fragments are being merged. Bbox is advisory (predicate-pushdown hint) and the upfront `gpd.read_parquet` of all geometries was the dominant per-merge memory transient.
- Tighter row-group accumulator flush in `parquet_merge_files`: flushes *before* appending an overflowing batch so the accumulator never holds more than `rows_per_group` rows at once (was up to ~2× at flush boundaries).
- `scripts/gh3_resume_recovery.py`: one-shot, idempotent recovery script with `--dry-run` that reconciles the build log against on-disk state, cleans stale `.merge.tmp` files, and rebuilds `_merge_progress.txt`. Multiprocessing-pool parallel scan with streaming `os.scandir` walk for GPFS friendliness; deduplicates tmp fragments by basename (~30× speedup for old `part.<i>.parquet` builds).
- `scripts/dask_worker_trim.py`: optional dask worker preload module that registers a `WorkerPlugin` calling `gc.collect()`, `pyarrow.default_memory_pool().release_unused()`, and `libc.malloc_trim(0)` after every task transition. Bounds unmanaged worker memory growth on multi-day jobs.
- 8 new tests in `tests/test_resume_reconcile.py` covering tmp+database reconciliation, idempotency, corrupt-fragment skip, sequential fallback, and the granule-id parser.

### Fixed
- `dask_worker_trim.py`: `dask_setup` is now `async` and awaits `Worker.plugin_add` (the API became a coroutine in modern dask distributed). The previous sync version silently no-op'd, leaving the plugin unregistered.
- `gh3_resume_recovery.py`: dedupe tmp fragments by basename before the parquet metadata read. With `by_beam=True` + `partition_on=[h3, year]`, the same `part.<i>.parquet` index represents the same `(granule, beam)` source partition replicated across many leaf dirs — reading every instance scaled at ~300 files/sec on GPFS for a 17h ETA. Dedup cuts the work to ~587K unique reads instead of 19.5M.

### Changed
- `_write_partitioned` return type is now `bool` (was `List[str]`). The trailing recursive `glob.glob(tmp_dir/**/*.parquet)` walked millions of fragments on global builds just so the caller could check `len() == 0`. Replaced with a single `os.scandir` iteration that returns `True` on the first `h3_*` leaf dir found. Function is private; the only caller (`build_h3db`) was already discarding the list contents.

## [0.7.9] - 2026-04-29

### Fixed
- Removed `rh100` from the V3 L2B default variable list (`GEDI02_B_DATASETS_003.txt`). The dataset was retired in V3 — it is not present anywhere in V3 L2B files, so requesting it via `default` raised `Unable to synchronously open object (object 'rh100' doesn't exist)` and skipped every L2B granule during `gh3_build --gedi-version 3`. Use L2A `rh[100]` or derive from `elev_highestreturn − elev_lowestmode` for V3 canopy-top height.

## [0.7.8] - 2026-04-29

### Removed
- The `Parsing granule metadata` progress bar in `gh3_build`'s granule-registration loop. After 0.7.6 dropped the per-iteration `os.path.getsize` call, the loop runs at ~800k granules/sec — the bar finished in well under a second on a 91k-granule run, so it no longer earns its keep.

## [0.7.7] - 2026-04-29

### Fixed
- "Excluded N HDF5 files matching ..." now appears exactly once per `gh3_build` invocation. Previously the line was emitted on every `soc_file_tree` call (validate, granule registration, internal `build_h3db` listing — two to three duplicates per run). The library is now silent about exclusions; the single user-facing summary lives in `gh3_build` next to the "Building from N existing HDF5 files" line.

## [0.7.6] - 2026-04-29

### Changed
- `gh3_build` granule-registration loop ("Parsing granule metadata") now parses orbit / granule / track directly from the basename instead of constructing a `GEDIFile`. The `GEDIFile` constructor calls `os.path.exists` + `os.path.getsize` on every first file to populate `file_size`, which this loop never reads. On a 73k-granule gpfs tree that adds ~12 minutes of network stat I/O for nothing.

## [0.7.5] - 2026-04-29

### Fixed
- `gh3_build` no longer prints the "Excluded N HDF5 files matching ..." log line twice. The message is now emitted once by `soc_file_tree` (the single source of truth across all callers); the CLI keeps the matching filter so the "Building from N existing HDF5 files" count still reflects the exclusion.

## [0.7.4] - 2026-04-29

### Changed
- `validate_soc_files` now reads the per-product per-version variable manifests that already ship in `src/gedih3/data/GEDI*_DATASETS_*.txt` instead of opening every HDF5 file to enumerate datasets. Drops a multi-minute scan to ~8 ms while preserving the typo-catching contract. Removes the dask bag, `futures_of`/`as_completed` plumbing, and `Validating SOC files` tqdm bar that v0.7.2 added.

### Fixed
- `soc_file_tree` no longer crashes (`AttributeError: Can only use .str accessor with string values!`) when the glob returns zero matches; returns an empty result early.

## [0.7.3] - 2026-04-29

### Added
- `--exclude PATTERN` flag in `gh3_build` (repeatable, fnmatch-style) and matching `exclude=` kwarg on `soc_file_tree`, `validate_soc_files`, and `build_h3db`. Drops files whose basename matches any pattern at the discovery step, so internal/non-release HDF5 variants (e.g. `*_SGS.h5`) can be skipped while keeping the variants you do want (e.g. `*_7algs.h5`). Default behaviour unchanged when the flag is omitted.

### Fixed
- `validate_soc_files` no longer aborts the whole validation pass on a single corrupt or truncated HDF5 file. The per-file worker (`check_soc_file_vars`) now wraps the HDF5 read in `try/except`, logs a warning naming the file, and returns no products for it instead of raising — mirrors the corruption tolerance applied to `dask_h5_merged` in commit `91bb04f`.

## [0.7.2] - 2026-04-29

### Fixed
- `gh3_build` startup phases between "using only existing data" and the partition build no longer silent: added "Validating product variables..." and "Listing SOC files..." log lines, swapped the per-file `dask.delayed` validation for a batched dask-bag with a `Validating SOC files: N/M [batch]` tqdm bar, and wrapped the granule-metadata-pivot loop with a `Parsing granule metadata` tqdm bar. Eliminates multi-minute silence on directories with hundreds of thousands of HDF5 files.

## [0.7.1] - 2026-04-29

### Fixed
- `gh3_build` SOC-check phase: replace `dask.distributed.progress()` (silent in non-TTY/SSH/log-redirected sessions) with a `tqdm` + `as_completed` bar over the bag's futures, so the "Checking SOC files" stage now shows reliable batch-level progress in every terminal context.

## [0.7.0] - 2026-04-29

### Added
- Progress bars for previously-silent CLI long-running loops: `gh3_doctor` partition scans (backfill, parquet_health, metadata, orphans, soc_health), `gh3_update` per-file partition loops, the `gh3_aggregate` time-series window loop, and `gh3builder` parquet metadata finalization. New shared `cliutils.progress_iter()` helper wraps `tqdm` with `logging_redirect_tqdm` so log lines no longer clobber the bar.

### Fixed
- `gh3_build`: tolerate faulty SOC files and isolate version handling
- CI: blank lines around bullet list in `parquet_fill_columns` docstring

## [0.6.0] - 2026-04-25

### Added
- `gh3_doctor`: new CLI tool that audits and (optionally) heals a gedih3 database. Six diagnoses cover NaN-fill backfill, leftover temp/empty-dir cleanup, stuck recovery flags, log↔disk partition reconciliation, partition-meta and dataset-manifest health, parquet corruption / duplicate `shot_number` / cross-partition schema drift, and SOC HDF5 health. `--check` is read-only by default; `--fix` applies safe remedies. `--s3` lets backfill fetch missing source files via NASA S3 ETL temp. `--online` queries NASA CMR and emits concrete `gh3_download` / `gh3_doctor --fix backfill` / `gh3_build` recommendations classified by gap type.
- `H3BuildLogger` gains an additive per-granule per-product status map (`granule.products`) plus `get_granule_product_status`, `get_product_gaps`, and `mark_granule_product` helpers. Lazy in-memory upgrade for legacy logs; on-disk format only changes after the next `save_log()`. `set_post_build_info` derives per-product status from existing partition meta `columns` — no edits to `gh3builder.py`, `gh3_build.py`, or `gh3_download.py`.
- New `gedih3.doctor` package houses the runner, registry, shared inspectors, streaming `parquet_fill_columns` (combine_first variant of `parquet_join_columns`) and `parquet_dedup_partition`, the CMR upstream check, and one module per diagnosis.

## [0.5.5] - 2026-04-24

### Added
- `gedi_download`: new `granule_names` parameter forwards a filename list to the CMR search (`.h5` suffix is stripped since CMR's `readable_granule_name` matches the stem), so callers that already know which granules to fetch no longer need to enumerate the full release

### Fixed
- `gedi_download` `on_granule_complete` callback now receives the file path in `granule_info_dict['path']` (target SOC path on PENDING, actual downloaded path on DOWNLOADED, None on FAILED) — previously callers had no way to reach the file they just landed
- `GEDIAccessor.search_data` DOI-fallback warning now names every dropped filter (`short_name` and `version`) and the DOI being used, instead of silently broadening across versions

## [0.5.4] - 2026-04-20

### Changed
- `gh3_aggregate`: more flexible callable handling in `gh3driver` aggregation pipeline

## [0.5.3] - 2026-04-15

### Fixed
- `gh3_build_ducklake`: interactive `input()` confirmation was silently bypassed when invoking via the installed CLI entry point; replaced with standard argparse CLI (`-d`/`--database`, `-t`/`--tmpdir`, `-v`/`-Q`) matching all other tools

### Changed
- `gh3_build_ducklake` now uses `setup_logging`, `print_banner`, `print_success`, and `cli_exception_handler` from `cliutils`, and `get_file_list()` is parameterised to accept a custom database path

## [0.5.2] - 2026-04-15

### Fixed
- Quality filtering with `-l/--list` flag now infers products from variable name suffixes (e.g. `wsci_l4c` → L4C) and applies only the minimal flags from `_PRODUCT_QUALITY_FLAGS`, instead of brute-force applying every `quality_flag` column in the dataset

## [0.5.1] - 2026-04-11

### Fixed
- Ruff lint: moved `_PRODUCT_QUALITY_FLAGS` to top-level import in `gh3builder.py` to fix F823 (local variable referenced before assignment caused by inline re-import shadowing module-level binding)
- Ruff lint: corrected two malformed `# noqa` directives to use explicit rule codes (`B018`, `F401`)

## [0.5.0] - 2026-04-11

### Changed
- Revised `default` variable presets for all products and GEDI versions (L1B, L2A, L2B, L4A, L4C): updated variable selections to match current HDF5 file contents, corrected variable counts, and aligned minimal/default presets with `config.py`
- Updated documentation to reflect new variable lists in `docs/concepts/variable-presets.md`

## [0.4.0] - 2026-04-07

### Added
- Targeted quality flag selection per product and version for `--quality` in extract/aggregate: applies only the primary flag for each selected product (e.g. `l4_quality_flag` for L4A, `l2a_quality_flag_rel3` for L2A v3)
- Quality flag auto-injection during build and download: each product's primary quality flag is always included in the database regardless of the variable list specified

### Fixed
- Stale test for invalid window operation spec (extended ops 0-8 error message)

## [0.3.2] - 2026-04-07

### Fixed
- Bug fix for check exported files tasks
- Apply schema to metadata on custom aggregation

## [0.3.1] - 2026-04-07

### Added
- `range` (max − min) window operation for raster sampling (`gh3_from_img`)

## [0.3.0] - 2026-04-07

### Added
- Window operations `std`, `min`, `max`, and `count` for raster sampling (`gh3_from_img`)

### Changed
- Cap Dask worker CPU count to 20
- Revised documentation

## [0.2.0] - 2026-04-01

### Added
- Beam type selection option for filtering GEDI data by beam type
- Short beam type selector flag for convenient beam filtering

### Changed
- Updated tool comparisons documentation
- New landing page for documentation
- Added funding acknowledgements

## [0.1.6] - 2026-04-01

### Changed
- Internal variables are now kept by default during extract/aggregate operations
- Version tracker updated to include `recipe/meta.yaml` in bump-version skill

### Fixed
- Safe merging no longer drops the H3 index when concatenating partitions

## [0.1.5] - 2026-03-31

### Fixed
- Fixed `os.path.join` producing backslash-corrupted URLs on Windows for all remote paths (HTTP, S3, FTP)
- Added `smart_join` utility that uses forward slashes for remote URLs across all CLI tools and library functions
- Fixed Dask workers failing to authenticate against remote HTTP/S3 servers (storage credentials were not propagated to worker processes)
- Fixed `h3_12` index being dropped when reading multi-file H3 partitions from remote servers (`pd.concat` with `ignore_index=True`)
- Fixed lint failure from missing `smart_join` import in `list_dataset_files`

## [0.1.4] - 2026-03-27

### Fixed
- Fixed `gh3_build` early exit blocking detection of new GEDI granules when re-run with identical parameters
- Local mode now scans the SOC directory for untracked HDF5 files before deciding the database is up-to-date
- S3 and download modes bypass the early exit to query CMR for newly released data
- `gh3_build --download` no longer skips the NASA download when parameters haven't changed

## [0.1.3] - 2026-03-26

### Changed
- Moved GDAL from runtime dependencies to optional extras (`pip install gedih3[gdal]`) to prevent pip install failures
- Pinned all previously unpinned dependencies with minimum versions
- Added `gedih3.data` to setuptools packages list (fixes build warning)

### Added
- Conda-forge recipe skeleton (`recipe/meta.yaml`) for future publication

## [0.1.2] - 2026-03-26

### Fixed
- Fixed Windows file-locking bug in `parquet_join_columns`, `parquet_append_rows`, and `parquet_append_columns` where open `ParquetFile` handles prevented `os.replace` from completing
- Variable-only updates (`gh3_build -l4c wsci` on existing database) now correctly merge new columns into partition parquet files on all platforms
- Added warning when variable update completes but no partition files were modified
- Orphaned `.join.tmp` files are now cleaned up on failure instead of left on disk

### Added
- Comprehensive test suite: `test_data_integrity.py` (data safety, correctness, error messages), `test_pipeline_integration.py` (S3 end-to-end workflows), `TESTING.md` (testing principles)
- Unit tests for `parquet_join_columns` (basic join, index preservation, partial match)
- Shared test fixtures in `conftest.py` for synthetic H3 databases and persistent test output

## [0.1.1] - 2026-03-20

### Changed
- Enriched `GEDIFile` parser with `product_code`, `year`, `doy`, `mission_week`, and `suffix` attributes
- Removed deprecated `min_vars`, `default_vars_file`, and `GEDI_L2A_ESSENTIALS` from config

### Fixed
- Fixed `pyproject.toml` dependencies incorrectly nested under `[project.urls]`
- Fixed docs CI intersphinx inventory failure tolerance
- Removed license classifier superseded by PEP 639 license expression

## [0.1.0] - 2026-03-19

### Added
- Complete CLI toolchain: download, build, extract, aggregate, rasterize (11 tools)
- H3 hexagonal spatial indexing with configurable index and partition levels
- EGI square-pixel indexing (EASE-Grid 2.0, EPSG:6933) for L4B compatibility
- Dask-distributed processing for large datasets
- NASA Earthdata integration via earthaccess (download and S3 streaming)
- Expert-curated variable presets (`minimal`, `default`) for all GEDI products
- Pre-configured quality filtering
- GeoTIFF export with compression, tiling, and time-series support
- DuckDB integration for spatial SQL queries
- Ancillary data fusion: raster sampling (`gh3_from_img`) and vector joins (`gh3_from_polygon`)
- Custom aggregation functions via Python API
- Support for GEDI products: L1B, L2A, L2B, L4A, L4C
- Sphinx documentation site
