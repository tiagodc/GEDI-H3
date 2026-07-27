---
paths:
  - "src/gedih3/gh3builder.py"
  - "src/gedih3/logger.py"
  - "src/gedih3/daac.py"
  - "src/gedih3/cli/gh3_build.py"
  - "src/gedih3/cli/gh3_download.py"
  - "src/gedih3/cli/gh3_update.py"
---

# Build, download, and merge pipeline

Stage 1 extracts per-granule-beam parquet fragments into `tmp/partitions/`; the merge
phase folds them into `<h3_dir>/h3_<p>=X/year=Y/X.Y.0.parquet`. Everything below exists
because a build runs for hours or days on a shared filesystem and must survive being
killed at any point.

## Non-negotiables

**Scalability — push work to workers.** No driver-side O(N) filesystem scans; use the
manifest sentinel or a `client.map` listing. No driver-side inflight throttle — let the
scheduler distribute. Stream `as_completed` rather than `bag.persist + compute` for
long-running phases.

**Low-memory plateau.** Per-worker memory must plateau, not climb, regardless of build
duration. Mechanisms: per-task `gc.collect()` + Arrow pool release + glibc `malloc_trim`
(via the `src/gedih3/data/dask-worker-trim.py` preload, wired externally through
`dask worker --preload` or `DASK_CONFIG` — not from the CLIs); capped pyarrow scanner
readahead (`batch_readahead=1`, `fragment_readahead=1`); `pre_buffer=True` for I/O
coalescing; per-file iteration rather than a `ds.dataset` scanner for merges.

**No `client.scatter` in the build drivers.** Inline per-task args instead — tiny
payloads, one scheduler dependency per task, no broadcast wait. Scatter has caused hangs
on tunnelled clusters and has a per-key dict-of-futures footgun. Regression test:
`tests/test_write_streaming.py::test_streaming_driver_completes_end_to_end`.

**Resume safety.** `h5_is_valid` (cheap header open) is the gate on downloads — a
truncated `.h5` left by a SIGKILL must never be consumed by the build. Progress is
append-only files plus stable filename conventions (the granule ID is embedded in the
fragment basename, so reconciliation never opens a parquet).

**H3 levels are immutable across resumes.** `-h3r` / `-h3p` argparse defaults are `None`,
not 12 / 3; fresh-build fallbacks live in the logger. `H3BuildLogger.__init__` raises
`GediValidationError` if a user-passed `res`/`part` differs from the existing log's value,
mirroring the `gedi_version` check. A naked resume on a non-default DB is therefore safe.
One GEDI version per database.

## Merge-failure recovery

When `_merge_and_finalize` hits a known-bad fragment class (0-byte parquet, missing magic
bytes, truncated thrift footer — `_RECOVERABLE_FRAGMENT_ERROR_MARKERS`) it writes an
atomic per-failure sentinel under `tmp/partitions/_merge_failures/` and appends the
affected granules — parsed from fragment basenames via `_FRAGMENT_BASENAME_RE` — to
`_merge_failed_granules.jsonl`.

On the next resume, `preclean_merge_failures` unlinks the named-bad fragments and their
`.tmp` siblings and drops the sentinels, then `apply_merge_failures_to_logger` flips those
granules `INDEXED → MERGE_FAILED` so Stage 1 re-extracts them. Both are idempotent and run
from `_merge_and_finalize` and from the CLI finalize path.

This closes the path where a worker SIGKILL leaves a 0-byte parquet that the next merge
would either fail on or silently produce empty output for. `h3_merge_files` also stats
each fragment and unlinks 0-byte files up front — one stat, effectively free, since the
open hits the same metadata.

Failure log lines carry their source: `Merge failed for <cell>/<year>: <Error>: <msg>
[file=<fragment>]`. The suffix is attached by `_iter_batches_with_path`, which wraps both
the `pq.ParquetFile` open and `iter_batches`, so failures raised mid-stream also
self-identify.

## Stage 1 telemetry

`_write_one_granule_beam`'s `KeyError` catch site calls `_classify_load_h5_failure`,
distinguishing `missing_var` (upstream schema variance — some L2A orbits lack
`l2a_quality_flag_rel3_a10`, e.g. O20752–O20767) from generic `other`. The driver appends
each to `tmp/partitions/_granule_failures.jsonl` (single-writer, append-only) so
post-build consumers resolve `(orbit, granule, track) → cause` without grepping the log.
The end-of-build advisory groups by `(kind, product, var)` and prints a recovery recipe
per class.

## Avoiding filesystem work

- `_derive_merged_output_paths` turns `_merge_progress.txt` into the final parquet paths
  by pure in-memory transform, via the deterministic `h3_merge_files` naming contract
  (`<tmp>/h3_<p>=X/year=Y` → `<h3_dir>/h3_<p>=X/year=Y/X.Y.0.parquet`). Use it instead of
  globbing after a merge — zero metadata ops.
- Reconcile Pass A sources partition dirs from `_manifest.txt` (or `os.scandir` for legacy
  DBs) and dispatches `_scan_partition_meta_granules` across workers. At continental scale
  this turns minutes of serial metadata work into seconds.
- `parquet_merge_files` captures shot/date stats inline so `h3_write_metadata` skips a
  multi-GB post-merge re-read, and embeds the GeoParquet bbox.
- Long merges refresh `_manifest.txt` incrementally every `GH3_MANIFEST_REFRESH_EVERY`
  successful merges (in-memory derive + one atomic write, no tree walk), so consumers
  reading mid-build see partial-but-fresh state.

## Variable-only update (`gh3_update`, `_build_add_variables`)

Reads each granule h5 **once** and fans its shots to every owning cell
(`_var_fan_granule`), then merges per-`(cell, year)` into the base parquet
(`_var_merge_cell_year`). The granule→cell relationship is free from the metadata granule
lists. The legacy per-`(cell, year)` sharding re-read every granule once per cell that
listed it — a 39.75× redundancy measured on a production tree (2.64M reads for 66.5k
unique granules), and h5 reads are ~93% of runtime.

Shots are routed by matching `shot_number` against the existing base parquets — **never**
by recomputing H3 from the new product's coordinates. `parquet_join_columns` writes via
`.join.tmp` + `os.replace` and filters columns already present, so re-running against a
partially-updated year file never duplicates columns.

## Backfilling into a completed build

A resume on a `COMPLETED` build takes a merge-only shortcut and will silently no-op.
Delete `tmp/partitions/_merge_progress.txt` first; keep the `_complete/` sentinels. Verify
the resume did real work by checking the `Streaming write: … skipped_by_resume` line.

## Pre-flight validation

- `manifest_check_scope` gates `validate_soc_files`: empty for granules-only or
  explicit-list resumes (the log is the contract), non-empty only for fresh builds or a
  `default` re-request. Apply it before any `validate_soc_files` call on a resume path.
- `explicit_vars_missing_in_sample` opens one sample HDF5 per product with an explicit
  variable list and reports missing names, so a typo exits with code 2 instead of hitting
  a runtime `KeyError` hours in.

## S3 ETL vs DAAC

Use `--s3` when the subset is narrow (under roughly 10% of the granule) or bandwidth is
constrained; use plain DAAC download for broad subsets on a fast link. In L2A, `rh` is the
cost driver — its presence in the subset flips the recommendation. Do not tune
`block_size`: stock earthaccess defaults measured best, and the speculative-prefetch
`BackgroundBlockCache` is doing real work despite looking wrong on paper for HDF5's jumpy
reads. Numbers and methodology are in `docs/user-guide/building-a-database.md`.

## Operator env vars (build-time only, safe to leave unset)

- `GH3_WRITE_STREAMING` — default on; toggles the streaming partition writer vs. the
  legacy `ddf.to_parquet` path.
- `GH3_LOG_PROGRESS` — default off; re-enables the 60-second `Streaming write: N/M done`
  INFO line for detached / tail-followed workflows. tqdm's `set_postfix` is the canonical
  liveness indicator otherwise. Per-failure WARN and end-of-phase ERROR summaries are
  unconditional — those are actionable, not progress noise.
- `GH3_MANIFEST_REFRESH_EVERY` — default 1000; merges between manifest refreshes.
- `ARROW_DEFAULT_MEMORY_POOL=system` + `MALLOC_TRIM_THRESHOLD_=0` — required per worker
  for the low-memory plateau. Set these from your cluster launcher, not from gedih3.
