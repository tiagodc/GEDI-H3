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

**One GEDI release per database, v3 by default.** `GEDI_DEFAULT_VERSION` (`config.py`) is
the only `version=None` fallback; `--gedi-version` applies to every product. Resolution
order: explicit arg > build log `gedi_version` (resume; a contradicting arg raises) >
`resolve_soc_version(soc_dir)` (download log, else first `_V00N` filename) > package default.
Listings pin it (`soc_file_tree` raises on a mix); updates align to `h3_columns_dtypes`.

## Merge-failure recovery

When `_merge_and_finalize` hits a known-bad fragment class (0-byte parquet, missing magic
bytes, truncated thrift footer — `_RECOVERABLE_FRAGMENT_ERROR_MARKERS`) it writes an
atomic per-failure sentinel under `tmp/partitions/_merge_failures/` and appends the
affected granules — parsed from fragment basenames via `_FRAGMENT_BASENAME_RE` — to
`_merge_failed_granules.jsonl`.

The CLI fold (`apply_merge_failures_to_logger`, after each merge and at startup) flags them
`MERGE_FAILED`, any prior status; `set_post_build_info`, reconcile and `log_state` keep it.
`build_h3db` pre-cleans before Stage 1 (bad fragments and their `_complete/` sentinels go),
so Stage 1 redoes exactly those tasks; `_release_merge_failed` then clears the flag.

This closes the path where a SIGKILL leaves a 0-byte parquet for the next merge; the
pre-clean probes in O(1) (`_parquet_tail_ok`), parsing footers only after thrift errors.
`h3_merge_files` skips as "already merged" only on proof (`_dest_holds_fragments`: the
destination's metadata lists every fragment's granule), never on a newer mtime alone.

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
per class. Reads dedupe by key; `_compact_granule_failures` drops recovered tasks per run.

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

## Variable add and product backfill (`_fan_merge_products`)

Both read each granule h5 **once**, fan its shots to every owning cell (`_var_fan_granule`)
and merge per-`(cell, year)` into the base parquet (`_var_merge_cell_year`); the granule→cell
map is free from the metadata granule lists (per-cell re-reads measured 39.75× redundant).
`_build_add_variables` joins new columns (`parquet_join_columns` skips present ones, so a
re-run never duplicates). `_build_fill_products` writes null cells only
(`parquet_fill_columns`) for granules an `--allow-missing-products` build indexed before
their later products existed (`MISSING_SOURCE`); targets come from the build log, never a
data scan. Its resume state is keyed per target set and it never skips "merged" files.

Shots are routed by matching `shot_number` against the existing base parquets — **never**
by recomputing H3 from the new product's coordinates.

## Post-merge tmp cleanup

A stale `_merge_progress.txt` is the L2 merge-resume signal and turns the next update into
a silent no-op, so `_merge_and_finalize` ends with `_cleanup_merged_tmp` (tiers in its
docstring). A new tmp artifact must fit one of its tiers. Trees from older builds: delete
`_merge_progress.txt` by hand before backfilling, then check `skipped_by_resume`.

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
