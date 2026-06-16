# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.12.0] - 2026-06-15

### Added
- `gh3_select_partitions(source, region)` — public, overhang-safe way to determine which H3 partitions a region touches when reading the database directly; returns ring-1-expanded partition cell IDs. Exported alongside `intersect_h3_geometries`, `h3_expand_ring`, and `h3_partition_bbox`
- Overhang-padded `bbox` (+ `bbox_note`) in H3 partition metadata sidecars — a selection-safe extent for direct consumers; the existing `h3_geometry` is the exact cell polygon and is *not* selection-safe

### Changed
- Consolidated ROI ring expansion into a shared `h3_expand_ring` primitive in `h3utils`

### Fixed
- Ring-1 expansion in ROI→H3 partition selection closes silent skipping of boundary shots stored under neighbor partitions (child overhang ≈ 0.18 × edge)
- `gh3_update`: EGI partition lookup compared numeric keys against filename strings and never matched
- `parallel_map` tasks marked impure — dask key-caching was returning stale scans
- EGI rasterization: deterministic, loud outer-tile guard + correct multi-tile time-series merge; raise on multi-tile no-hint input instead of majority-guessing
- EGI rasterization: correct pixel widths in eastern edge tiles and the mosaic VRT (previously averaged mismatched edge/non-edge pixel sizes)

### Contributors
- Tiago de Conto, Amelia Holcomb

## [0.11.6] - 2026-06-05

### Fixed
- Cap `python` below 3.14 in `environment.yml`: on Windows, not-yet-reputable cp314 conda-forge binaries (e.g. scikit-learn's `_cd_fast`) can be blocked from loading by Smart App Control; cp313 builds load fine

## [0.11.5] - 2026-06-02

### Fixed
- Install HTTP `(connect, read)` timeouts per-worker in `_download_with_retry`, so download workers spawned directly via `pqdm.processes` (bypassing `gedi_download`) no longer block forever on a dropped CDN stream

## [0.11.4] - 2026-06-02

### Changed
- `sample_raster_at_points` and `from_image` no longer include `relative_pixel_distance` by default; opt in with `pixel_distance=True`

## [0.11.1] - 2026-05-30

### Fixed
- **Variable-add fragment cleanup is now incremental + parallel** (`gh3builder._var_merge_cell_year`). The Stage 2 merge worker deletes each `(cell, year)`'s fragment directory immediately after its successful atomic join — the same `rm_src=True` discipline the fresh-build merge (`h3_merge_files`) uses. Previously every fragment was left for one serial `shutil.rmtree` on the driver at end-of-run; at continental fan-out scale that is ~2.6M files deleted single-threaded (~130/min), which pinned the driver in disk-sleep for ~6h **and** blocked the `COMPLETED` build-log write (`set_post_build_info` + `save_log`) behind it — so the newly-added product columns were not query-exposed (`gh3_load` builds its Dask `_meta` from the not-yet-updated `h3_columns_dtypes` cache) until the throwaway cleanup finished. Fragments now drain distributed across all workers as merges proceed, so the end-of-run sweep is a cheap no-op. The leftover `_var_fan_complete` sentinel dir (66k+ files at scale) is also cleaned in parallel via `parallel_map(_unlink_path)` instead of a serial rmtree. Observed on the first production L4C update (50,652 cell-years, 2.6M fragments): Stage 1 fan + Stage 2 merge were flawless, but the serial cleanup tail ran ~6h; this removes it. Resume-safe — fragments are deleted only after the base atomically carries the columns.

## [0.11.0] - 2026-05-29

### Changed
- **Variable-add updates rewritten as inverted granule fan-out** (`gh3builder._build_add_variables`). The previous per-`(cell, year)` path re-read every granule h5 once per cell that listed it — a DB-wide 39.75× redundancy measured on the production tree (2.64M reads for 66.5k unique granules), and h5 reads are ~93% of runtime (cold GPFS chunk I/O). The new driver reads each unique granule **exactly once** and fans its shots to all owning cells, cutting the dominant phase by ~40×. Four stages, all scatter-free (per the fresh-build streaming-driver contract): (0) driver inverts the metadata granule lists into `{granule: [base year_pf...]}`, skip-checking each `(cell, year)` so a re-run is a fast no-op; (1) `client.map(_var_fan_granule)` reads each granule once (`shots=None` full read — the `[:]` column read is GPFS-optimal) and routes rows to owning cells by `np.isin` against each base's `shot_number` (per-worker `lru_cache`), writing tiny fragments with per-granule `.done` sentinels for resume; (2) `client.map(_var_merge_cell_year)` concatenates a cell-year's fragments and `parquet_join_columns` them into the **base** parquet (rowgroup-wise, atomic) — one self-contained parquet per partition, **no sidecars**, append-only merge-progress for resume; (3) per-cell `h3_merge_metadata` + manifest refresh, fragments cleaned on a fully-clean run.
- Shots are routed to cells by matching `shot_number` against the existing base parquets — never by recomputing H3 from the new product's coordinates (coordinate drift between products would misroute boundary shots and silently NaN the left-join). Variable-add only changes columns, so the merge patches `columns`/`column_dtypes` in the per-year metadata via a footer-only schema read instead of the full `h3_write_metadata` stats recompute.
- Verified end-to-end on a 3-cell production subset: +72 L4C V003 columns, rows preserved, 100% non-null, 0 sidecars, idempotent re-run, resume-safe after deleting merge progress + half the fan sentinels.

### Removed
- Legacy `_add_variables_to_year_file` (the per-`(cell, year)` join worker). Replaced by the Stage 1/2 fan + merge workers.

## [0.10.29] - 2026-05-29

### Fixed
- `gh3builder.h3_merge_metadata`: per-cell `years` list now includes every year present on disk. The function initialized the years set as empty and populated it from `year_metadata[1:]`, silently dropping whichever year `glob.glob` returned first. Pre-existing bug, surfaced by the rolled-back v0.10.26 sidecar Phase 4 which truncated `years` lists across the entire production DB. Other aggregates (`shot_count`, `shot_range`, `date_range`, `granules`) were unaffected because they were seeded from `year_metadata[0]`. Two regression tests added (`test_h3_merge_metadata_includes_all_years`, `test_h3_merge_metadata_single_year`).

## [0.10.28] - 2026-05-29

### Removed
- **Rolled back the sidecar variable-update architecture** introduced in 0.10.26 (commit `2d533d6b`) and patched in 0.10.27 (commit `d6632a5b`). The sidecar layout produced a per-product `<base>.<prod>.sidecar.parquet` next to each `(cell, year)` base file, broke the package's "one parquet per partition" invariant, complicated every downstream tool (loaders, doctor diagnoses, manifests), and made the database opaque to external readers. The experimental period remains recoverable from the `pre-sidecar-rollback` git tag.
- Reinstated the legacy single-file variable-update path: `_add_variables_to_year_file` + `parquet_join_columns` rewrites the base year parquet in place to bolt on new columns. This path is slower but preserves the single-file invariant. A future redesign will address the legacy path's redundant h5 reads and full-base rewrite cost without breaking the layout contract.

## [0.10.27] - 2026-05-28

### Fixed
- `gh3builder.build_h3db`: both variable-only call sites of `_build_add_variables` (the S3 ETL path and the local SOC path) now forward `tmp_dir=tmp_dir`. The CLI's `-t/--tmpdir` arg flowed into `build_h3db`'s `tmp_dir` kwarg but stopped there — the variable-update intermediate fragments (`<tmp_dir>/var_fragments/<granule>__<prod>.parquet`) were always landing at `<h3_dir>/.tmp_var_update/` regardless of the user's requested tmp path. Two-line forward fix; in-flight builds are unaffected (fragments get cleaned up at Phase 5 regardless of location).

## [0.10.26] - 2026-05-28

### Changed
- **Variable-only update path rewritten as per-granule fan-out + per-(cell, year) sidecar merge.** The legacy `_add_variables_to_year_file` worker (per-(cell, year), opened each granule h5 once per cell that listed it ≈ 40× redundancy at continental scale, and rewrote the entire ~1.9 GB base file just to bolt on ~7 small columns) was the dominant cluster-throughput cap (~1.5 tasks/min, 20-29 day ETA on 5-worker continental builds). New 5-phase driver in `gh3builder._build_add_variables`: (1) parallel `_scan_year_file_for_update` — checks both base AND per-product sidecar so mixed layouts resume cleanly; (2) driver inverts to `{granule → [year_pf...]}`; (3a) `_write_granule_var_fragment` opens each granule h5 EXACTLY ONCE and writes a deterministic fragment to `<tmp_dir>/var_fragments/<granule_key>__<prod>.parquet` (resume-safe — existing non-empty fragments are reused); (3b) `_merge_year_sidecar` reads its base `shot_number` set (cheap projection — base file is NEVER rewritten), streams relevant fragments, filters, dedups, writes `<base>.<prod>.sidecar.parquet` atomically, and extends the per-year metadata JSON with sidecar columns + dtypes so `set_post_build_info` picks them up; (4) per-cell metadata aggregation; (5) fragment cleanup. Architecture matches the fresh-build streaming-driver's "scatter-free, inlined-args" contract.
- `gh3driver.gh3_load_hex` now discovers per-product sidecar parquets via the `<base>.<prod>.sidecar.parquet` pattern (regex `_SIDECAR_RE`) and left-joins them onto the base on `shot_number` at read time. Column projection is split between base and sidecars so a query requesting only sidecar columns reads only `shot_number` from the base. Mixed layouts (cells with sidecars + cells with embedded cols from older runs) load correctly.

### Removed
- `gh3builder._add_variables_to_year_file` (legacy per-(cell, year) join worker that rewrote the base year file). Its replacement is the Phase 3a/3b pair (`_write_granule_var_fragment` + `_merge_year_sidecar`).

## [0.10.25] - 2026-05-28

### Fixed
- `gh3builder._add_variables_to_year_file`: pre-reads `year_pf`'s `shot_number` column (cheap projection, ~1–10 MB) and passes it as `shots=` to every `load_h5` call. Previously each granule h5 was read in full (~5M shots × cols), and the subsequent left-join discarded >99% as non-matching — dominant memory consumer on continental cells touched by many granules (live build saw per-worker peaks of ~28 GB). `load_h5(shots=)` was always there: `_get_beams_to_load` derives the beam(s) from `shot_number` encoding and opens only those (often 2–4 of 8 = 2–4× h5 I/O reduction), and `_extract_beam_data` HDF5-indexes only the matching rows for every column. Right-side memory becomes `O(base_shots)` instead of `O(granule × beam × cells_per_granule)` — expected per-worker peak drops to ~1–3 GB for typical cells with proportional wall-time savings.
- `utils.parquet_join_columns`: now takes an explicit `rows_per_group=100_000` (matching `parquet_merge_files`) and passes it to `ParquetWriter.write_table`, so updated year files have bounded, deterministic per-group sizes regardless of the base file's existing row-group shape.

## [0.10.24] - 2026-05-28

### Fixed
- `gh3builder._build_add_variables`: Phase 1 scan result collection now uses `dask.distributed.as_completed(..., with_results=True)` so each task result rides along with its completion notification — one round-trip per batch instead of one per future. The previous per-future `fut.result()` pattern (fine for fresh-build merge tasks which take seconds-to-minutes each) was ceiling-bound at ~86 collections/sec when applied to Phase 1's tiny millisecond-scale scan tasks, drawing out the driver-side drain to ~10 min even though all 50k worker-side scans finished in ~2 min. Matches the established pattern from `s3_etl_subset` (`gh3builder.py:342`). `_scan_year_file_for_update` wrapped in try/except so a corrupt parquet footer or malformed sidecar JSON returns `None` (with a worker-side warning) instead of crashing the `with_results=True` collection loop.

## [0.10.23] - 2026-05-28

### Fixed
- `gh3builder._build_add_variables`: scatter-free driver for the variable-only update path, matching the established lesson in `tests/test_write_streaming.py::test_streaming_driver_completes_end_to_end` ("SCATTER-FREE DRIVER. Inlining is the correct pattern for this cluster topology."). Replaces the interim `client.scatter([all_soc], broadcast=True)[0]` design — even with the single-future wrap, `scatter(broadcast=True)` remains a known hang risk on tunneled / heterogeneous clusters, and the broadcast itself forces every worker to hold the full 73k-entry SOC tree in memory. New 4-phase flow: (1) parallel `_scan_year_file_for_update` returns the granule list per year file (or `None` for already-done / no-sidecar files — naturally resume-aware); (2) driver resolves h5 paths locally via `all_soc[orb_track][prod]` dict lookups; (3) parallel `_add_variables_to_year_file` receives **only the h5 paths it needs** inlined into its args (a few dozen tuples max per task); (4) parallel `h3_merge_metadata` for touched cells. Tiny per-task payloads, one scheduler dep per task, zero broadcast wait. Worker signature changed from `(year_pf, new_product_vars, all_soc, version)` to `(year_pf, h5_specs, new_product_vars, version)` where `h5_specs = [(prod, h5_path, var_list), ...]`. New `_scan_year_file_for_update` worker covers Phase 1.
- `gh3_build`: fixes the second variable-update stall reported in the in-flight L4C build — after the SOC tree was built, `client.scatter(dict, broadcast=True)` returned 73k per-key futures which `client.map` then registered as 3.7B scheduler edges (50k tasks × 73k entries), hanging the graph build for hours with idle workers. This release's scatter-free design avoids the entire scatter dance.

## [0.10.22] - 2026-05-28

### Fixed
- `gh3_build`: the resource-banner log line was labeled "System:" but reported `get_system_resources()` (always the local process), so on `--dask-scheduler` runs it implied the cluster's CPUs/RAM while actually showing only the driver host's. Renamed to "Driver host:" and split the disk readout with a separator so the line reads as "local host info | output disk", with cluster info covered separately one line down by the existing `Dask config` log.

## [0.10.21] - 2026-05-28

### Fixed
- `gh3builder._build_add_variables`: variable-only update path now shards work per-`(cell, year)` (mirroring the fresh-build merge phase) instead of per-cell, and the SOC tree is built ONCE on the driver and broadcast to workers via `client.scatter(broadcast=True)`. The previous per-cell worker called `soc_file_tree` itself; after the SOC manifest read-removal that fans `walk_soc_parallel` back to the same cluster from inside a worker, deadlocking the phase under any workers-equal-threads config (symptom: progress bar stuck after the initial skip-only batch with zero CPU activity on the dashboard). Per-`(cell, year)` granularity also fixes the multi-year resume edge case where the per-partition skip check could inspect only `parquet_files[0]` and silently mask unfinished year files, and bounds per-task memory to one year's `new_vars_df`. A Phase 2 fans `client.map(h3_merge_metadata, touched_cells)` to re-aggregate the per-year sidecars only for cells whose year files changed. `_add_variables_to_partition` removed; `_add_variables_to_year_file` is the new worker.

## [0.10.20] - 2026-05-27

### Fixed
- `gh3_build`: the "Dask config" log line now reads workers/threads/RAM from `client.scheduler_info()` after the Client connects, so runs with `--dask-scheduler` report the actual external cluster instead of the local-cluster CLI defaults. New `cliutils.format_dask_cluster_info()` helper.
- `gedidriver.soc_file_tree` + `doctor.diagnoses.soc_health._enumerate_soc_files`: stop reading `_soc_manifest.txt`. External population paths (manual rsync, NASA delivery) bypass the producer-driven refresh and a stale manifest silently narrows every downstream scan. Both consumers now unconditionally fan a `walk_soc_parallel` year/doy scan across the registered dask Client. Producers still emit the manifest as an informational sidecar; CLAUDE.md updated to describe it as write-only.
- `logger.H3BuildLogger` / `logger.SOCDownloadLogger`: peek the build/download log before expanding `default`/`minimal` variable lists so an absent `--gedi-version` CLI arg adopts the persisted `gedi_version`. Resolves the v2-manifest-on-a-v3-DB failure where `gh3_build -l4c default` against an existing v3 database expanded against the v2 manifest and aborted at validation.
- `gh3builder._expand_variables_only`: wrap the `as_completed` partition-update stream in a tqdm progress bar (`Updating partitions`, with live `updated`/`skipped`/`failed` postfix), matching the existing merge-phase pattern. Replaces the every-20-partitions INFO line so 10k-partition updates don't go silent in the terminal.

## [0.10.19] - 2026-05-27

### Fixed
- `daac.GEDIAccessor.search_data`: querying an unregistered version on a version-pinned ORNL DAAC product (e.g. L4A v3, L4C v3 before release) now logs a warning and returns an empty granule list instead of raising `ValueError`. Mirrors the LPDAAC behavior where an unknown version just returns zero CMR results, so downstream scripts see consistent "no data available" semantics across DAACs.

## [0.10.18] - 2026-05-27

### Fixed
- `gh3driver.egi_aggregate`: callable aggregations no longer crash with `AttributeError: 'DataFrame' object has no attribute 'shot_number'` (or any other non-numeric input column), and the resulting Dask DataFrame now advertises the callable's *actual* output schema instead of echoing input column names. Both EGI aggregate paths (`_egi_aggregate_from_indexed` direct-load and `local_egi_aggregate` shuffle) used to route through `get_aggregatable_columns()` regardless of `agg` type, stripping `shot_number` / quality flags / geometry before the user's callable ran. The shared `_build_agg_meta` helper also returned `meta_cols = cols` for callables — wrong whenever the callable produces stats unrelated to input columns (e.g. validators returning `shot_count`, `*_mean`, `*_p98`). All three sites now mirror the H3 path (`gh3_aggregate_func`): callables / dicts pass every column through, `_build_agg_meta` invokes the callable on an empty `_meta` slice (forwarding agg kwargs) to capture the real output schema, and empty-partition branches invoke the callable directly on an empty frame so Dask's `apply_and_enforce` meta check passes.

## [0.10.17] - 2026-05-26

### Fixed
- `daac.py`: convert ORNL DAAC L4A and L4C `short_name` / `doi` to version-keyed `{version: identifier}` dicts and resolve them per request via a new `config._resolve_identifier` helper. The ORNL short_names encode the release ID (e.g. `GEDI_L4A_AGB_Density_V2_1_2056`), so a v3 request used to silently send a v2.1-pinned short_name to CMR — `gedi_list_versions` now iterates over every registered short_name and `GEDIAccessor.search_data` raises a clear `ValueError` listing available versions when a requested version has no registered identifier (no silent older-version substitution). Future releases need only an extra dict entry to wire up.
- `daac.GEDIAccessor.search_data`: omit the explicit CMR `version=` filter for ORNL DAAC products. The version-pinned short_name uniquely identifies the release, and an extra `version=` filter (e.g. `version='2'` against a `..._V2_1_2056` short_name) only invited a contradiction → 0 results → DOI-fallback warning. LPDAAC products keep their version-agnostic short_names and zero-padded `version='002'`-style filter.
- `tests/test_unit_gedih3.py::test_meta_from_dtype_dict_projection`: align expected column order with the production `year`-before-`part_col` ordering introduced in 0.10.16.

## [0.10.16] - 2026-05-23

### Fixed
- `gh3driver.gh3_load`: Python API now accepts `region='region.shp'` (and bbox-strings / ISO3 codes) — the form advertised in the docstring example — instead of raising a confusing `TypeError: Array should be of object dtype` deep inside `shapely.STRtree.query`. Region is now normalized once at the top of `gh3_load()` via the CLI's `parse_region()`, so every downstream consumer (`intersect_h3_geometries`, the Dask clip path, `_load_dataset`) sees a list / GeoDataFrame / shapely geometry uniformly. Closes the Python/CLI parity gap.
- `gh3driver.gh3_load_hex`: attach the `year` hive partition column from each file's path so the returned partition matches the synthetic Dask `_meta`. Regression from 0.10.15's `daf7d224`, which added `year` to the meta on the assumption that pyarrow reconstructs hive partition columns on read-back. That holds for *directory* reads or `pyarrow.dataset(partitioning='hive')`, but `gh3_load_hex` passes a *list* of files to `pd.read_parquet`, which does not. Every `gh3_load().head()` / `.compute()` previously raised `Missing: ['year']`. Fix reads each parquet file individually and assigns `year` from the `year=YYYY/` path segment; column order in `_meta_from_dtype_dict` re-ordered so `year` precedes `part_col` to match the data path.
- `cli/gh3_from_polygon`, `cli/gh3_from_img`: `--merge` no longer leaves a 0-byte directory at the user's `-o` file path. Both CLIs were unconditionally calling `os.makedirs(args.output, exist_ok=True)` even when `args.output` was a *file* path (merge mode), so the subsequent `gh3_export → atomic_parquet_write` failed with `IsADirectoryError` on `os.replace(tmp_file, dir)`. The `makedirs` call is now scoped to the non-merge resume path that globs inside the output dir; `gh3_export` handles directory creation itself in both modes.
- `imgutils._empty_sampling_result`, `vecutils._empty_join_result`, `vecutils._compute_join_meta`: declare `shot_number` as `uint64` (matching the on-disk dtype) instead of `int64`. The mismatched meta caused Dask to upcast the reconciled column to `float64` to unify int64-meta with uint64-data — and float64's ~15-significant-digit precision silently collapsed thousands of distinct 19-digit GEDI shot IDs onto the same float key. Any downstream `pd.merge(..., on='shot_number')` then produced a Cartesian blowup (e.g. 2.1 M shots → 44 M merged rows on an ES-wide bench) and joined GEDI variables to the wrong samples. **Critical data-correctness fix** for any analysis that joined `gh3_from_img` output to GEDI variables.

## [0.10.15] - 2026-05-23

### Fixed
- `gedidriver._validate_h5_columns`: replaced `list(set(columns))` with `list(dict.fromkeys(columns))` in both the `rxwaveform` and `txwaveform` dependency-injection branches. `set()` orders by Python's per-process hash seed, so the driver-side schema probe and each Stage 1 worker produced the same column set in different orders, breaking the canonical pyarrow schema cast with `"Target schema's field names are not matching the table's field names"` on every L1B granule. Only the waveform branches hit the set() path; L2A/L2B/L4A/L4C builds were never affected.
- `gh3driver._meta_from_dtype_dict`: aligned the cached Dask `_meta` with what `gh3_load_hex` actually returns at compute time. Two corrections: (1) drop the h3 index column (e.g. `h3_12`) from the synthetic columns and apply it as the named index — parquet pandas metadata pins it as the index on every read, so it never appears as a column; (2) always add `year` as `int32` — the build partitions by year (assigned from `datetime` before write) and pyarrow reconstructs it from the path, but `h3_columns_dtypes` is recorded before the partition split so the cache never carries it. Without this, every `gh3_load(...).head()` / `.compute()` raised `Extra: ['year'], Missing: ['h3_12']`.

## [0.10.14] - 2026-05-20

### Fixed
- `daac.py`: inject HTTP timeouts `(60s connect, 300s read)` into `earthaccess.auth.SessionWithHeaderRedirection.request` via a new `_install_request_timeouts()` helper called from `gedi_download` (main process / pqdm) and `_init_earthaccess_worker` (dask workers). Without this, `earthaccess.store._download_file`'s `session.get(url, stream=True)` blocks indefinitely when a CloudFront edge drops a connection without FIN — observed as workers stuck on `CLOSE-WAIT` sockets with `partial_*.h5` files stalled for 9+ hours, making only N-k of N dask workers visibly progress. Read timeout is inter-packet silence (not total wall time), so multi-minute large-granule downloads on slow links remain safe; only dead sockets trip it. Idempotent install with env overrides `GH3_DOWNLOAD_CONNECT_TIMEOUT` / `GH3_DOWNLOAD_READ_TIMEOUT`. `is_retryable_error` now recognizes `requests.exceptions.{Timeout, ConnectionError}` directly so the `_download_with_retry` exponential-backoff loop fires reliably instead of relying on string-pattern matching. Covered by `tests/test_daac_timeouts.py` (9 tests, including an end-to-end real-socket regression against a local stalling TCP server).

## [0.10.13] - 2026-05-19

### Fixed
- `cliutils.setup_logging`: suppress earthaccess 0.18 `DataGranule.size()` FutureWarnings (internal to earthaccess, not actionable here). Programmatic regex filter on the driver + `PYTHONWARNINGS` env propagation so dask worker subprocesses inherit and silence the same warnings (lists `earthaccess.store` + `earthaccess.results` literally because PYTHONWARNINGS escapes regex chars in the module field).
- `daac.py gedi_download` (DAAC mode): replaced `distributed.progress(futures)` with a tqdm bar over `dask_as_completed`, mirroring the s3_etl_subset pattern. The CR-animated dask bar was being mangled into a wall of `[ ] | 0% Completed | Xs` lines whenever stdout/stderr was captured via `tee` / log redirect.
- `daac.py _gedi_subset_one`: demoted per-granule `Subsetting ... with N vars: ...` from WARNING to DEBUG. The tqdm bar is the canonical progress UI; the per-file note remains available with `-vv`.

## [0.10.12] - 2026-05-19

### Changed
- `s3_etl_subset` (`gh3builder.py`): replaced the every-10-files INFO loop with a live tqdm bar over `dask_as_completed` (postfix carries the failure count), and demoted the per-file `[N/M] Subsetting …` line from INFO to DEBUG. Aligns the CLI output with the Stage 1 build pattern (`gh3builder.py:1977`) — one live progress line in the terminal, hundreds fewer noisy entries in `gh3_download.log`. Per-file detail still available via `-vv`.
- `gh3_download --help` + `CLAUDE.md` Performance Optimizations: documented the S3 ETL vs DAAC mode-selection finding from a controlled single-granule L2A bench (May 2026). On a fast home link, DAAC's bulk download wins for broad subsets (e.g. minimal incl. `rh`); S3 ETL only meaningfully wins (1.67×, ~50× less data on the wire) when the subset is narrow (~10% of the granule). Block-size tuning (16 MB → 32 MB) regressed both subsets; stock earthaccess defaults are tuned correctly. No production code changes — the lever for users is mode selection, not knob-twiddling.

## [0.10.11] - 2026-05-19

### Fixed
- `geo_to_umm` (`utils.py`): auto-simplify polygon rings exceeding 200 vertices (iterative `shapely.simplify` with doubling tolerance) before passing to CMR — large boundaries (e.g. state-level GPKGs) were triggering CloudFront HTTP 414 (URI too long) and silently failing every product search in `gh3_download`. Also reproject GeoDataFrame inputs to EPSG:4326 when the CRS differs or is missing. Both events emit a CLI-visible WARNING.

## [0.10.10] - 2026-05-18

### Fixed
- `gh3_build` (`cli/gh3_build.py`): resolve `--output`, `--tmpdir`, `--indir` to absolute paths so ssh-launched dask workers (CWD = `$HOME`) write to the driver's tree instead of their own — was silently scattering Stage 1 fragments across hub `scripts/` and remote `~/` trees on relative-path invocations.
- `_extract_beam_data` (`gedidriver.py`): normalize h5py's two missing-path error shapes (`"object 'X' doesn't exist"` and `"component not found"`) into the single form `_MISSING_VAR_RE` matches. The latter shape (h5py's error when an intermediate GROUP component is absent, not a final dataset) escaped classification — 32% of failures in the last continental build were mis-bucketed as `kind=other, var=null` and missed the end-of-build `missing_var` recovery advisory.
- CLI path arguments: extend abspath coverage to every CLI tool that can dispatch paths to dask workers — `gh3_build_ducklake` (`database`, `tmpdir`), `gh3_download` (`output`), `gh3_rasterize` (`dataset`, `output`), `gh3_read_schema` (`path`), `gh3_update` (`dataset`, `database`, `merge`), `gh3_doctor` (added `--report`). Driver-only file paths (`gh3_aggregate -a` file mode, `gh3_update -l` file-element mode) deliberately not wrapped — they're parsed on the driver and only the parsed contents travel to workers.

### Changed
- `data/GEDI04_C_DATASETS_002.txt`: comment out 11 `wsci_prediction/*_a10` default variables. NASA aliased L4C a10 to a5 in 2025+ V002 releases (byte-identical on `wsci_a10`, ~0.01% rounding-noise diff on PI bounds) — keeping the columns in the default would double-store a5's values with no information gain. L2A/L2B/L4A V002 defaults already had `*_a10` commented out (commit 7b4fa08); this brings L4C in line for cross-product consistency.

## [0.10.9] - 2026-05-16

### Fixed
- `gh3_aggregate` (`cli/gh3_aggregate.py`): dict-form `-a/--aggregate` spec (e.g. `"{'rh_098_l2a':['count','mean']}"`) now folds the dict keys into the load column list. Previously `collect_columns` only honored `-l/-l2a/-l4a` + query columns, so dict keys were never loaded from disk and `local_aggregate`'s `df.groupby(level=0).agg(agg)` raised `KeyError: "Label(s) ['rh_098_l2a'] do not exist"`. Aggregation spec is now parsed before column collection so the fold-in happens once in the shared setup — works for both H3 and EGI indexes.

### Changed
- `egi_load` bbox pushdown (`gh3driver.py`): encoding-aware routing via `_pick_bbox_strategy(sample_file)` + `_read_parquet_bbox` helper, cached per-database in `_BBOX_STRATEGY_CACHE`. WKB-encoded parquets (the v3 default) route to a coord-column pushdown using GEDI L2A `lat_lowestmode_l2a` / `lon_lowestmode_l2a`; point-encoded parquets use `gpd.read_parquet(bbox=...)`; everything else falls back to full read + clip. Kills the full-read fallback that was forcing every worker through a 15–18 GB peak on dense tropical L12 tiles. Production global EGI extract now plateaus at ~7.7 GB RSS per worker.

## [0.10.8] - 2026-05-16

### Fixed
- `_load_egi_tile_from_h3` (`gh3driver.py`): stream H3 partitions one-at-a-time instead of accumulating all of them in a list and `pd.concat`-ing at the end. Each partition is now read → bbox-clipped → query-filtered → EGI-indexed → spillover-filtered → reduced *before* the next partition is touched, so peak per-task memory is bounded to ~one H3 partition's raw size + the (much smaller) reduced output, independent of `len(h3_list)`. Closes a production OOM class: the ring-1 expansion from v0.10.7 raised typical `h3_list` length from 1–2 to ~7, and the old eager pattern peaked at 7× one partition's working set. Dense tropical L12 tiles consequently crashed under the 20 GB worker memory_limit, triggering `KilledWorker` after dask's 6 retries and aborting the entire run — observed on a global EGI extract that completed only 4,522 of ~6,000 tiles (~16% of shots missing). The spillover filter from v0.10.7 now applies per-partition via a new `tile_egi_id=` kwarg; the post-call filter in `load_tile` is removed (single source of truth). Output bit-identical to the pre-streaming code on the 4°×3° Amazon test region (same 20,617,490 H3 / 20,633,375 EGI shot counts, H3 ⊂ EGI strictly, 0/8 random-box mismatches).

## [0.10.7] - 2026-05-16

### Fixed
- `egi_load._load_egi_from_h3_database.load_tile` (`gh3driver.py`): per-task drop of boundary-spillover rows whose `egi_part_col` doesn't match the task's tile id. Closes a silent data-loss class where two neighbor tasks both write to the same canonical filename and the last writer wins. The bbox clip in `_load_egi_tile_from_h3` is inclusive on edges, so shots at the exact L12 boundary land in *both* neighbors' loaded data; `egi_export_part` then split by `egi_part_col` and emitted one file per group, so task A wrote A.parquet + a tiny spillover B.parquet for shots whose true tile is B. Task B did the symmetric thing. The race usually left a tile with millions of legitimate shots as a 1-row file on disk. Production extract observed: 257 of 6,002 output files had exactly 1 row at high latitudes where shot density is highest. Filtering each task's df to `df[egi_part_col] == egi_id` eliminates the race; spillover shots are loaded and written by their rightful neighbor task via that task's own bbox query. Amazon test (4°×3°): H3 ⊂ EGI strictly, 0 random-box mismatches, 0 files <50KB.
- `egi_h3_intersection` (`egi/spatial.py`): now includes ring-1 neighbors of every H3 cell that geometrically intersects an EGI tile. The H3 v3 database stores each shot at `cell_to_parent(latlng_to_cell(..., finer_res), partition_res)`, but partition assignment via geometric overlap uses the partition-level `latlng_to_cell` boundary — and those two functions disagree at H3 cell boundaries. Boundary shots land in a storage cell whose polygon doesn't overlap their true EGI tile, so the unexpanded intersection silently lost ~5% of shots near every H3 partition boundary. One observed L3 partition had 84,240 of 1,843,930 shots (5%) under a storage cell whose polygon didn't intersect their true L12 tile.
- `egi_export_part` (`gh3driver.py`): skip the unconditional `os.makedirs(odir)` in merge-mode (`is_file_path=True`). The old behavior turned the user's merge-mode output file path into a directory before `_write_egi_file` ran, causing the final `os.replace` to fail with `IsADirectoryError` — aborting every merge-mode aggregate after hours of cluster compute. Mirrors `gh3_export_part`'s existing guard.
- `_meta_from_dtype_dict` / `_load_h3_database` / `_load_dataset` (`gh3driver.py`): cascading sidecar bug that made `gh3_aggregate -h3 4` fail with `H3ResMismatchError: Invalid parent resolution 4 for cell 0x83...`. The lazy ddf's meta lacked a named index (`_meta_from_dtype_dict` returned a default RangeIndex), so `_detect_export_params` inferred `index_level=3` from the only h3 column present (the partition column `h3_03`) and wrote that into the simplified dataset's `gedih3_dataset.json`. On every subsequent load, `_load_dataset` saw the parquet's `h3_12` index disagree with the sidecar's claimed `h3_03` and "restored" the (wrong) sidecar index — silently demoting `h3_12 → h3_03` on every partition, after which `h3.cell_to_parent(L3, 4)` was inevitably bound to fail. Fixes: `_meta_from_dtype_dict` now accepts `index_name=` and sets the synthetic meta's index name; `_load_h3_database` passes the database's index level; and `_load_dataset` skips the "restore" branch when the file already has a valid h3_XX/egiXX named index.
- `_write_egi_file` (`gh3driver.py`): removed the broad `except Exception: return ''` that silently swallowed all write failures (the caller `egi_export_part` interpreted `''` as "skip this tile", producing invisible holes). Errors now propagate.
- new helper `utils.atomic_parquet_write(df, opath, *, compression=None, max_attempts=3)`: write to AtomicFileWriter `.tmp` → stream-verify every data page via `iter_batches` inside the same context → retry up to N times on failure. Catches the production-observed transient-IO class where pyarrow successfully closes a parquet file with corrupt internal page bytes (footer intact, body bad). `_write_dataframe` and `_write_egi_file` now route parquet writes through it.

## [0.10.6] - 2026-05-15

### Fixed
- `egi.core.to_hash` (`src/gedih3/egi/core.py`): boundary-shadow cells eliminated at every EGI level. `OUTER_RES` (~160143.203736 m) is not exactly representable in float64, so the two independent ops `x_offset // OUTER_RES` and `x_offset % OUTER_RES // scale` disagreed by one outer tile for coordinates at (or just below) a tile boundary — yielding `px_inner = SCALE_FACTOR` (one past valid range). The out-of-range index propagated through `to_parent` as `SCALE_FACTOR // SCALE_FACTOR = 1`, producing spurious non-zero inner indices at coarser levels. Observed in production: ~9% of L12 partition files in a continental EGI extract were boundary-shadow cells with filenames like `12198027000001000000.parquet` (`px_inner=1`) or `12152080000000000001.parquet` (`py_inner=1`) instead of folding into their proper neighbor. Fix: detect overflow on each axis and carry it into the next outer tile, so the hash always satisfies `0 <= px_inner < SCALE_FACTOR`. Non-boundary inputs unchanged. Verified across all 12 levels (direct `to_hash`) and all fine→coarse `to_parent` rollups (36 combinations): 0 invalid hashes / 100 boundary coords per case. `tests/test_egi_comprehensive.py::_test_tile_boundary` was passing the old behavior by accident (asserted `py_outer == tile_y - 1` at sub-precision eps without checking `py_inner`); rewrote it to assert the actual invariants: inner index in range, outer in {tile_y-1, tile_y}, and the production pipeline path (fine + `to_parent`) always rolls up to a clean coarse hash.

### Changed
- `gh3_build_ducklake` (`cli/gh3_build_ducklake.py`): reverted the bulk-glob and batched-glob CALL forms from v0.10.4 / v0.10.5. The single bulk `CALL ducklake_add_data_files` and the 16-batch hex-prefix sharding both finished faster on paper (3–5× and amortized similar respectively), but DuckLake's metadata-registration path is single-threaded inside the C++ extension ([duckdb/ducklake#404](https://github.com/duckdb/ducklake/issues/404)) and doesn't yield progress within a CALL. The batched form's 16 tqdm ticks were spaced minutes apart with no per-file feedback in between, indistinguishable from a hung process for most of the run. The per-file CALL + tqdm form visibly ticks ~7 files/s, which is the preferred UX even at >10h total. **Trade-off note:** the `hive_partitioning => true` arg the bulk-form added is also reverted — the per-file CALL never passed it, so newly built ducklakes (and the one already built against the v3 DB) will have NULL `h3_XX` / `year` partition columns. Patchable in one line if/when that matters.

## [0.10.5] - 2026-05-15

### Added
- `gh3_build_ducklake` (`cli/gh3_build_ducklake.py`): new `--batches N` flag (default 16; choices 1/2/4/8/16) splits the bulk `ducklake_add_data_files` CALL by leading hex char of the h3 cell id. Each batch commits its own transaction, so tqdm shows N progress ticks and peak memory is bounded to ~final/N instead of the previous unbounded accumulation (a continental ~50k-partition DB previously hit ~64 GB RSS in a single transaction). `batch_prefixes()` pre-filters empty hex groups via `os.scandir` on the database root, since `ducklake_add_data_files` raises on a glob that matches nothing. `--batches 1` keeps the single-glob path for tiny DBs. The single-threaded parquet_metadata read inside the DuckLake extension ([duckdb/ducklake#404](https://github.com/duckdb/ducklake/issues/404)) is still the dominant cost — this change makes it observable and bounded, not faster per file.

### Fixed
- `gh3_extract`, `gh3_aggregate`, `gh3_from_img`, `gh3_from_polygon` + new `cliutils.resolve_output_abs`: `args.output` is now absolutized on the driver right after the banner. The four tools dispatch per-partition writes via `map_partitions` and previously passed `args.output` verbatim. With a remote dask scheduler (workers launched from a different CWD than the user's shell — the common case for `tcp://localhost:8786` tunnels), every worker resolved a relative path against its own CWD; the driver-side `os.makedirs(output)` created the empty dir under the user's CWD while workers silently wrote into their scratch directories on cluster nodes. Symptom seen in production: a multi-hour extract reported progress while the user-visible output stayed empty. The new helper logs `Resolved relative output path: X -> Y` so the absolutization is observable; no-op when the path is already absolute or `None`.

## [0.10.4] - 2026-05-15

### Changed
- `gh3_build_ducklake` (`cli/gh3_build_ducklake.py`): the per-file `CALL ducklake_add_data_files` loop is replaced by a single bulk call against a glob pattern (`h3_XX=*/year=*/*.parquet`). DuckDB 1.4 microbenchmark: per-file ~25 ms vs glob ~0.73 ms per file — ~35× speedup at the SQL layer; on a 70k-partition database this is the >10h vs minutes difference. DuckLake's metadata-registration path is still single-threaded internally ([duckdb/ducklake#404](https://github.com/duckdb/ducklake/issues/404)), but the per-CALL Python↔DuckDB round-trip + transaction overhead disappears. Also drops the year-range enumeration + N `file.exists()` stat-call helper; a single glob hit seeds the schema.

### Fixed
- `gh3_build_ducklake`: the bulk CALL now passes `hive_partitioning => true`, so the `h3_XX` and `year` partition columns are populated from the path. The previous per-file loop did not pass this flag, leaving those columns NULL in the resulting ducklake.

## [0.10.3] - 2026-05-15

### Fixed
- `gh3_build` resume shortcut (`cli/gh3_build.py:_detect_merge_resume_signal`): the merge-only fast path now refuses to fire when **any** granule in the build log is in `MERGE_FAILED` status. Those granules need Stage 1 re-extraction, which only the full reconcile + extract + merge path performs; taking the merge-only shortcut would re-attempt the same merge against the same corrupt fragments and loop forever. The veto runs before the L1/L2 signal checks; legitimate merge-resume cases (all granules INDEXED, status `MERGING` set just before the merge phase) still take the fast path. Reproduced on the continental rebuild: 5 partition-years stuck failing across resumes, each with a 20-min finalize cycle, until the veto routed the next invocation through the full path so Stage 1 could re-write the corrupt fragments.
- `gh3_doctor` fused per-partition scan (`doctor/fused.py:fused_scan_partition`): force-import `gedih3.doctor.diagnoses` at the top of the worker so the registry is populated on every worker. Without this, workers received `fused_scan_partition` by pickle-reference but never imported the diagnosis submodules; the worker's `_SCAN_REGISTRY` stayed empty, every scan returned `None`, and the driver-side fallback fired for every diagnosis (defeating the fusion refactor's 5x dispatch saving).
- CI: `tests/test_doctor_fused.py` no longer imports fixtures from `tests.test_doctor_diagnoses`. `tests/` is not a package (no `__init__.py`) so CI's working directory cannot resolve the import. Inlined the two fixture helpers the new module needs.

## [0.10.2] - 2026-05-14

### Changed
- `gh3_doctor` (`doctor/runner.py`, new `doctor/fused.py`): the 5 per-partition h3db-tree diagnoses (`metadata`, `geoparquet_bbox`, `parquet_health`, `orphans`, `backfill`) now fuse into a single per-partition scan when ≥2 of them are requested at `mode='check'`. Each partition's parquet listing + meta JSON is opened once and the shared state is routed to every enabled scan inside one dask task. Cuts driver-side GPFS metadata round-trips and worker-side file opens ~5×. Single-diagnosis check + every `--fix` path stay on the legacy per-diagnosis dispatch. Graceful fallback: a diagnosis whose fused result is empty (e.g. cluster has stale bytecode predating this refactor) auto-redirects to single-diagnosis dispatch with a loud WARNING.

### Fixed
- `gh3_doctor` CLI: `args.indir`, `args.tmpdir`, `args.soc_dir` are now resolved to absolute paths before partition_dirs reach remote dask workers. Relative `-i database/` used to silently produce 10k false `empty_partition` + `missing_partition_meta` findings because every worker's `os.scandir` raised OSError on the wrong CWD and the doctor helpers swallowed that as "empty / missing".
- `gh3_doctor` no longer assumes a default `<indir>/.tmp` for the tmp directory; diagnoses that need a tmp tree skip silently when `-t` is not passed. Avoids scanning an unrelated path and producing misleading findings.
- `metadata` diagnosis: manifest health check switched from `_metadata` (pyarrow's dataset manifest, never produced by gedih3) to `_manifest.txt` (the R2 sentinel the build actually writes). Stops the permanent false "manifest missing" finding on every gedih3 database.
- `metadata` diagnosis: per-partition exceptions and missing fused result slots are surfaced as `scan_error` findings with the underlying error message, not silently rolled into `missing_partition_meta`. Severity escalates to ERROR when any partition errored.

## [0.10.1] - 2026-05-14

### Changed
- `H3BuildLogger.set_post_build_info` (`logger.py`): the post-merge partition-metadata scan no longer pays an O(N) driver-side `glob.glob('*/*<meta>')` + serial `json_read` loop + O(N²) `if g not in indexed_granules` dedupe. Partition enumeration is now `os.scandir`; per-partition JSON reads dispatch via `parallel_map` across workers (new picklable `_scan_partition_meta_post_build_info`); granule dedupe is set-based. Falls back to a serial in-driver loop when no Dask Client is registered (unit-test path). On a continental-scale build (50k partitions, 50k granules) this drops the post-merge phase from ~51 min to seconds — the final hotspot blocking the "SUCCESS: N files exported" success line.
- `parallel_map` (`parallel.py`): now renders a `tqdm` bar in both batched and non-batched paths for TTY liveness, matching the streaming-write + merge phases. The 5%-step `N/M done` INFO log lines are gated behind `GH3_LOG_PROGRESS` (default off) — opt-in for detached / tail-followed log workflows. Dask dashboard remains the canonical live cluster view.

## [0.10.0] - 2026-05-14

### Added
- Pre-flight check for explicit variable typos (`cli/gh3_build.py`). New helper `explicit_vars_missing_in_sample` introspects one sample HDF5 per non-`default` product to verify every requested variable name exists before stage 1 starts. Exits with a clear recipe on miss — catches typos / unknown names / wildcards-matching-nothing before a multi-hour build runs into a runtime `KeyError`.
- Failure telemetry (`gh3builder.py`): per-merge-failure atomic sentinel under `tmp/partitions/_merge_failures/<h3_cell>__year=Y.fail` and per-granule structured failure records in `tmp/partitions/_granule_failures.jsonl`. Both append-only / crash-safe; no driver-side O(N) JSON rewrite. End-of-build advisory groups Stage 1 failures by `(kind, product, var)` with an actionable recovery recipe per class.
- Merge-failure recovery loop: bidirectional granule status (`INDEXED → MERGE_FAILED` on known-bad fragment errors), L1 resume pre-clean that unlinks named-bad fragments + `.tmp` siblings, preventative size-check on source fragments before each merge. Granules whose only fragments were 0-byte get re-extracted on the next resume instead of silently dropping their rows.
- New doctor diagnosis `tmp_partitions_health` — log-driven audit of `tmp/partitions/` post-build forensics (merge_failure sentinels, granule_failures.jsonl summaries, progress↔manifest drift). `--fix` calls `preclean_merge_failures`; refuses to act while a `gh3_build` is live (pgrep + log mtime guard).
- `gh3_update`: startup WARN when source H3 database has non-INDEXED granules, so downstream simplified-dataset consumers know about the gaps before they propagate.
- `GH3_LOG_PROGRESS` env var to opt-in to the periodic `Streaming write: N/M done` INFO line for detached / tail-followed log workflows (default off — `tqdm.set_postfix` already shows the same data on the terminal).
- `GH3_MANIFEST_REFRESH_EVERY` env var (default 1000) — the merge phase now refreshes the database `_manifest.txt` sentinel every N successful merges so consumers reading mid-build see partial-but-fresh state. Each refresh is O(N_merged_so_far) pure in-memory derive + one atomic file write.

### Changed
- `_merge_and_finalize` post-loop derivation: the two driver-side `glob.glob('h3_*/...')` scans over the finalized DB tree are replaced with pure in-memory derivation from `_merge_progress.txt` via the new `_derive_merged_output_paths` helper. Zero GPFS metadata ops at end-of-merge — saves minutes on continental builds.
- `_reconcile_granules_from_disk` Pass A: the two driver-side recursive `glob.glob` calls are replaced with manifest-aware partition listing + `parallel_map` across workers (new module-level `_scan_partition_meta_granules`). At continental scale this turns minutes of driver-serial GPFS metadata work into seconds of distributed scans. Falls back to a single `os.scandir` on `h3_dir` for legacy DBs without a manifest sentinel.
- `_build_add_variables` replaces `glob('h3_dir/h3_*/')` with `os.scandir` (one syscall, same result; cheaper on large DBs).
- Merge-failure WARN line at `gh3builder.py:2227` now logs `os.path.relpath(d, tmp_dir)` (h3_cell + year) instead of `os.path.basename(d)` (year alone). Combined with the `[file=<path>]` suffix that `parquet_merge_files` now attaches inside the exception, a single grep on the WARN line gives both the partition and the exact bad fragment.

### Fixed
- `utils.parquet_merge_files` now wraps per-fragment open + `iter_batches` so any exception carries `[file=<path>]`. Truncated-body parquets that fail mid-stream (class C) surface their source path instead of an opaque Arrow message.
- H3 resolution / partition mismatch on resume now raises `GediValidationError` (mirroring `gedi_version`) instead of silently overriding the user-passed value. CLI argparse defaults for `-h3r` / `-h3p` change from `12 / 3 → None`; canonical defaults applied in the logger on fresh build. Naked resume on a non-default DB is unaffected.

## [0.9.6] - 2026-05-13

### Fixed
- Streaming driver no longer crashes every task with `AssertionError(<TaskState 'spatial_h3_tiles' processing>)`. `client.map(fn, iter, **kwargs)` was registering each kwarg under a scheduler TaskState named after the kwarg, which workers could never resolve. Fix: wrap all broadcast kwargs into a `functools.partial` closure around the worker function, then call `client.map(partial_fn, tasks, pure=False)` with no extra kwargs — dask sees only a single callable + iterable, the partial's captured kwargs are opaque to the scheduler. Memory dedup preserved (one partial in the Blockwise layer, per-task entries remain tiny refs).

## [0.9.5] - 2026-05-13

### Added
- Streaming partition writer (`_write_partitioned_streaming`) — replaces the legacy `ddf.to_parquet(...).persist()` path with a `client.map`-based submission that releases per-task results as they complete via `as_completed` + `fut.release()`. Eliminates the to-parquet-barrier-induced worker memory accumulation (~75 MiB/min observed on continental builds) and surfaces real task-graph progress on the dask dashboard.
- Per-(granule × beam) completion sentinels under `tmp/partitions/_complete/<frag>.done`. Workers emit a sentinel only after every leaf parquet is atomically committed; reconcile then trusts sentinels as proof of completeness, closing the silent data-loss path where a worker killed mid-task could otherwise leave fragments that look "fully on disk" to the legacy heuristic.
- Sentinel-authoritative resume mode: `_reconcile_granules_from_disk` switches between sentinel-aware and legacy-fragment heuristics based on `_complete/` presence, with a one-time migration that emits sentinels for fully-complete granules from a pre-streaming tmp tree.
- `GH3_WRITE_STREAMING` env var (default on) to toggle between streaming and legacy paths; `GH3_WRITE_STREAMING_BATCH` reserved for future inflight tuning.
- Driver-side progress logging (`Driver: ...` markers + 60-second `Streaming write: X/N done` periodic log) so any pre-flight stall is observable from `gh3_build.log`.

### Fixed
- `H3BuildLogger._adding_h3_parts()` no longer short-circuits the resume skip filter when `h3_partition_ids` is missing on the log (the killed-mid-first-write case) — the guard now checks `new_spatial` first, so a same-spatial resume correctly honors the INDEXED granules the reconcile just flipped.
- `_reconcile_granules_from_disk` now requires full beam coverage before flipping a granule INDEXED. Fragment-presence alone (the prior heuristic) could not distinguish a fully-extracted granule from one whose worker was killed mid-write → silently dropped beams' shots on the next resume.
- Streaming driver's `client.scatter` deadlock: wrap-in-singleton-list pattern then drop scatter entirely. Three iterations:
  - `scatter(value, broadcast=True)` on iterable values (list, dict, pyarrow.Schema) scattered element-wise, creating ~41k stray futures and freezing the scheduler.
  - Wrapped `scatter([value], broadcast=True)[0]` fixed the cardinality but `broadcast=True` hung waiting for every SSH-tunneled worker to ACK the broadcast — driver blocked in `futex_wait_queue` for 10+ minutes.
  - Final fix: inline broadcast kwargs into the task graph via `client.map(fn, tasks, **shared_kwargs)` — kwargs deduped across the batch (one ~210 KB blob in the scheduler graph), per-task entries are tiny refs. No scatter, no hang.

## [0.9.4] - 2026-05-13

### Changed
- `gh3builder._write_partitioned` now persists with task fusion enabled (`optimize_graph=True`) and pins `write_metadata_file=False`. Cuts the partition-write memory plateau on continental builds by collapsing the read → transform → write chain into one task per input partition and removing the global `_metadata` aggregation reducer. Output cardinality and hive layout are unchanged (guarded by `tests/test_to_parquet_fusion.py`).

## [0.9.3] - 2026-05-13

### Changed
- `_read_manifest` no longer performs the constant-time mtime freshness check on the consumer side; staleness is now a warning-only condition emitted by producer-side paths, avoiding spurious ERRORs on read-only consumers.
- Manifest-staleness check downgraded from ERROR to WARNING in `gedih3.parallel`.

### Fixed
- `gh3_export(merge=True)` no longer creates the output path as a directory; sidecar dataset-meta is skipped during merge.
- `gh3_export_part` no longer clobbers the output file path via `os.makedirs(odir)` when merging.

### Performance
- Driver/raster `.compute()` paths bypass the dask optimizer's collect-on-cluster wedge.

## [0.9.2] - 2026-05-12

### Added
- New `gedih3.parallel` module: `parallel_map` (promoted from `doctor/parallel.py` with lazy `get_dask_client` import to break the utils ↔ parallel cycle), three parallel walker primitives (`walk_soc_parallel` year/doy, `walk_h3db_parallel` per `h3_NN=*` partition, `walk_flat_parallel` single dir), and `check_manifest_freshness` constant-time mtime smoke test. All walkers are always-parallel; fail-loud on any worker exception so a partial manifest is never written.
- `generate_manifest` now accepts `tree_shape='h3db'|'soc'|'flat'` and an optional pre-computed `files=` list; `write_soc_manifest` also accepts `files=` so `gh3_build -i` can persist the manifest from its in-memory listing at zero extra cost.

### Changed
- Manifest writers now use the dask cluster — driver-side serial recursive globs over multi-million-file SOC/H3 trees are replaced by per-leaf parallel scans. Cold-GPFS resume walks drop from minutes to ~5-15s.
- `soc_file_tree` no-manifest fallback uses `walk_soc_parallel`; the `cli/gh3_build.py` `existing_h5` listing on resume uses it too.
- Producer-driven manifest invariant (R2) enforced across the package: every code path that mutates the SOC or H3 tree refreshes the corresponding manifest at exit. New refresh sites: `s3_etl_subset`, `gh3_build -i` (opportunistic), `gh3_doctor --fix orphans` after removal. `_read_manifest` now performs a constant-time mtime smoke test against the root dir and logs a loud ERROR pointing at the relevant `gh3_doctor --fix` remedy when a producer crashed or files were dropped in externally.
- `doctor/parallel.py` is now a thin shim re-exporting `parallel_map` from `gedih3.parallel`; its doctor-internal scandir helpers stay in place.

## [0.9.1] - 2026-05-12

### Changed
- Attribute copyright to University of Maryland; bump year to 2026.

### Fixed
- `gh3_build` resume no longer validates the build log's expanded variable list against the currently-shipped static manifest. The log is the authoritative source on resume; the static manifest is consulted only for fresh builds with `default` or for resumes where the user explicitly re-requests `default` for a product (regime-aware gating via new `manifest_check_scope` helper). Resumes against databases built under broader earlier manifests now succeed.
- `validate_soc_files` strips `#`-prefixed manifest lines so commented-out entries cannot silently set-mismatch against literal user requests.

## [0.9.0] - 2026-05-11

### Added
- `parallel_map` helper in `doctor/parallel.py` — auto dask-or-serial work distribution; standard parallelism primitive for distributed diagnoses.
- A-priori `gedi_vars_static(product, version)` cached lookup from shipped per-product variable manifests in `data/`.
- SOC manifest sentinel (`_soc_manifest.txt`) — download-side parallel of `_manifest.txt`, refreshed after every download.
- `h3_columns_dtypes` cached in build log; `gh3_load()` builds Dask `_meta` from cache with zero parquet I/O.
- Software Design Priorities and reusable utility (DRY anchors) documentation in CLAUDE.md.

### Changed
- Distributed doctor diagnoses: `parquet_health`, `backfill`, `geoparquet_bbox`, `metadata`, `orphans`, `soc_health`, `log_state` all ship per-partition work to dask workers; O(1) `os.scandir` emptiness checks replace recursive globs.
- Always-parallel execution paths in doctor, builder, and driver — dropped dual-path branching.
- Batched dispatch + parallel bbox-fix in doctor for bounded memory and improved throughput.
- Per-task memory hygiene applied throughout doctor (v0.8.x build-pipeline patterns).
- Atomic writes for `_write_dataframe` and `_write_egi_file` in export pipeline.

### Fixed
- Download resume uses `h5_is_valid` to detect truncated `.h5` files left by SIGKILL.
- Atomic `--report` write in doctor.
- Wire `--scheduler-address` into a `Client` in doctor — the missing piece for parallelism.
- Drop the `cross_rg_overlap` proxy diagnosis (100% false positives).
- Five user-visible bugs from PR #10 review across doctor, logger, and query.
- Packaging: include `gedih3.doctor` and `gedih3.doctor.diagnoses` subpackages.
- Ruff E701 in temp-leak test (expand inline try/except).

### Removed
- Dead helpers in `utils.py` and `doctor`; consolidated SOC manifest handling and DRY-ed arrow-pool drain.

## [0.8.25] - 2026-05-06

### Fixed
- **Coalesce per-file column-chunk reads via `pre_buffer=True`.** `pq.ParquetFile(f)` defaults to `pre_buffer=False` for direct use — that means each of a fragment's ~1,270 column chunks is read as a separate seek+read on the underlying filesystem. On cold shared GPFS at ~10–50 ms per scattered read, that's 12–60 s of pure I/O latency per fragment. `ds.dataset()`'s scanner sets `pre_buffer=True` internally, which is why the dataset path was so much faster — coalesces all column chunks of a row group into a few large sequential reads (~50–100 MB buffered), then decompresses in memory. Per-fragment cost drops 10–30×. Memory remains bounded — `pre_buffer` only caches the current row group's compressed bytes.

### Performance
- Expected merge throughput recovery: most of the v0.8.20 dataset speed restored, while keeping the v0.8.24 per-file iter memory footprint (~1 GB worker plateau).

## [0.8.24] - 2026-05-06

### Removed
- `_make_batch_reconciler(file_schema, target_schema)` — no longer needed. By design all gh3_build fragments share an identical column set and dtypes (they all come from one `dask_geopandas.to_parquet` call), so silent null-fill / drop semantics were defensive overkill.

### Changed
- **Column ordering enforced by pyarrow native projection.** `parquet_merge_files`'s per-file iter loop now passes `columns=target_names` to `pq.ParquetFile.iter_batches()`. PyArrow's C++ reader reads columns in target order and drops any extras at read time. No Python reorder per batch, no per-batch reconciler closure, less I/O when files have extras. A fragment with a missing target column raises from pyarrow — that's the right behavior; it surfaces a serious data invariant violation rather than silently null-filling.

### Performance
- ~250 K Python interpreter ops per merge eliminated (the old reconciler's per-batch loop on drifted fragments). Modest CPU win; main benefit is code simplicity.
- Slightly less I/O when files have columns we don't want (target columns only are read).

## [0.8.23] - 2026-05-06

### Changed
- **Replace `ds.dataset` scanner with explicit per-file iteration in `parquet_merge_files`.** The dataset abstraction retained metadata for all fragments through the scan and held async-I/O / IO-thread buffers that pyarrow's memory pool's `release_unused()` couldn't reach. On a 1,270-column GEDI partition, that hidden retention reached ~13 GB per worker on v0.8.22 (despite `batch_readahead=1, fragment_readahead=1` and `rows_per_group=50k`). New flow:
  - Open one fragment at a time via `pq.ParquetFile(f)` → drain via `iter_batches(batch_size=rows_per_group)` → drop the file reference (its IO state is released) → next fragment.
  - Per-file `_make_batch_reconciler(file_schema, target_schema)` handles column-order drift (12/201 partitions in the wild) and column-set drift defensively. Fast-path returns `None` when the file already matches the target schema, every batch passes through zero-copy. Slow-path uses a precomputed name → column-index map to reorder/null-fill in O(num_columns) per batch.
  - Restore `rows_per_group=100_000` (was 50k in v0.8.22 as a memory mitigation that didn't reach the actual culprit). Better compression and fewer row groups.
- Drop `batch_readahead` / `fragment_readahead` parameters (no longer applicable — no scanner).

### Performance
- Per-merge peak transient memory expected to drop from ~13 GB to ~1–2 GB. Worker high-water-mark RSS should plateau at one fragment's footprint instead of the whole dataset's.
- Throughput trade: synchronous reads vs scanner's async pipelining. On contended GPFS the pipelining wasn't buying us much (we observed 9–10 merges/min with full pipelining, then 2.6/min after capping readahead) — so per-file iter shouldn't significantly underperform.

## [0.8.22] - 2026-05-06

### Fixed
- **Cap pyarrow scanner readahead** in `parquet_merge_files`. PyArrow's dataset scanner defaults to `batch_readahead=16` and `fragment_readahead=4` — that's up to **64 batches in flight at any moment** (pre-decoded async even with `use_threads=False`). On a 1,270-column GEDI partition with `batch_size=100k`, that's ~15 GB of transient prefetch buffer per scan, which became the worker high-water-mark RSS observed by the user. Set both to `1` (one batch + one fragment header in flight at a time). New optional kwargs `batch_readahead` and `fragment_readahead` exposed for tuning.

### Changed
- **Lower default `rows_per_group` from 100,000 to 50,000.** Halves the per-flush `acc` accumulator transient (1,270 cols × 50k rows ≈ 250 MB instead of 500 MB+), and halves the `pa.concat_tables(acc)` peak. Trade: 2× more row groups in output, marginally less compression efficiency, slightly less granular predicate pushdown — invisible for our bulk-read patterns.
- **Explicit cleanup at the end of `parquet_merge_files`**: `del scanner, dataset, writer, acc` and `pyarrow.default_memory_pool().release_unused()` before returning. Ensures heavy refs are dropped and pool memory is returned to the OS before the caller (`h3_merge_files`) does any further work — belt-and-suspenders alongside the trim-plugin's per-task hook.

### Performance
- Per-merge peak transient memory should drop from ~15 GB to ~1 GB — both because the prefetch buffer is now ~1 batch instead of 64, and because each batch is half the size. Worker high-water-mark RSS should plateau at a much lower ceiling.

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
