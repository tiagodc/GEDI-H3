# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

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
