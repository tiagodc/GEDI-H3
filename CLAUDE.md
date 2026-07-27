# gedih3 — agent context

Python library for turning NASA GEDI spaceborne-lidar granules into analysis-ready,
spatially-indexed parquet databases (H3 hexagons for storage, EGI square pixels for
EASE-Grid/L4B-aligned analysis), queryable and rasterizable at continental scale with
minimal GEDI or programming expertise required of the user.

`README.md` and `docs/` are the user-facing reference. Don't restate them here — this file
is only for what you'd otherwise get wrong.

## Environment

```bash
conda env create -f environment.yml && conda activate gedih3 && pip install -e ".[test]"
pytest tests/ -m "not integration and not slow"     # what CI gates on
ruff check src/                                     # 120 cols
```

`pytest -m integration` needs NASA Earthdata credentials. `tests/TESTING.md` is the safety
contract for anything touching build, update, or export.

## Invariants

Get these wrong and the failure is silent — no exception, just wrong data or a scan that
quietly covers less than you think.

- **Ring-1 overhang.** H3 partitions do not geometrically contain all their shots (child
  cells overhang the parent by ~0.18 × edge). Every ROI → partition selection goes through
  `h3_expand_ring` or the public `gh3_select_partitions`. Exact polygon intersection *looks*
  right and drops boundary shots.
- **`_soc_manifest.txt` is write-only by design.** `_read_soc_manifest` still exists in
  `gedidriver.py` with zero callers — it is a decoy. External population (rsync, a NASA
  delivery) bypasses the producer refresh, and a stale manifest silently narrows every
  scan. SOC enumeration always fans `walk_soc_parallel`. The H3-side `_manifest.txt` *is*
  read; the asymmetry is deliberate.
- **Producer-driven manifest refresh.** Any path that mutates a SOC or H3 tree refreshes
  its manifest before returning. Consumers trust it blindly; the only check is the
  constant-time `check_manifest_freshness` mtime test wired into `_read_manifest`.
- **`parallel_map` raises `GediError` when no dask Client is registered.** There is no
  serial fallback — don't write call sites that assume graceful degradation.
- **No `client.scatter` in the build drivers.** Inline per-task args. Scatter has hung on
  tunnelled clusters. Regression test:
  `tests/test_write_streaming.py::test_streaming_driver_completes_end_to_end`.
- **Every output write goes through `AtomicFileWriter`** (`.tmp` + `os.replace`). The one
  exception is geo-vector formats (geojson/gpkg/shp), which need extension-based driver
  inference and emit sidecars.
- **`h5_is_valid` is the resume gate.** A truncated `.h5` left by a SIGKILL must never
  reach the build.
- **H3 levels are immutable across resumes.** `-h3r`/`-h3p` argparse defaults are `None`,
  not 12/3; `H3BuildLogger.__init__` raises on mismatch. One GEDI version per database.
- **`from_map=False` is deprecated** and slated for removal along with the argument. Don't
  build on it.
- **`filters=` raises if a file's schema lacks a predicate column** rather than silently
  returning unfiltered rows. Predicate columns need not appear in `columns`.
- **A-priori over detection.** If the structure already encodes the answer, look it up
  instead of doing I/O: `gh3_read_meta`, `gedi_vars_static`, `h3_partition_bbox`. The
  question to ask of any new code path — *am I doing work the structure of the data
  already answers?*
- **Dependencies are declared twice.** `pyproject.toml`, and by hand in the
  [conda-forge feedstock](https://github.com/conda-forge/gedih3-feedstock) — including
  `entry_points` for any new CLI. The autotick bot only bumps version and hash, and
  nothing in this repo's CI catches that drift. `osgeo` is the only permitted optional
  import (no PyPI wheels); guard every call site with `try`/`except ImportError` plus a
  working fallback.
- **No countable inventories in docs or agent files** — LOC figures, "N CLI tools", "N
  exception types", file lists. Name the symbol and let the reader grep. Every stale claim
  this repo accumulated was a hand-maintained count. `tests/test_agent_docs.py` fails the
  build on them.

## Architecture in one pass

1. **Download** — `daac.py` → `earthaccess` → GEDI HDF5 into the SOC tree (`year/doy/`).
2. **Build** — `gh3builder.py` reads HDF5, indexes to H3, partitions by cell, writes
   parquet + JSON sidecars. `logger.py` tracks resume state.
3. **Query** — `gh3driver.py` loads partitioned parquet via Dask with spatial, temporal,
   and predicate filtering.
4. **Extract / aggregate** — filter and reduce into flat parquet for external use.
5. **Rasterize** — `raster/` converts to GeoTIFF, with time-series support.

Database root sidecars: `gedih3_build_log.json` (schema, partition level, granule state),
`gedih3_dataset.json` (extracted datasets), `_manifest.txt` (file listing sentinel),
`_bbox_index.parquet` (per-file data envelopes, for skipping provably-empty reads).

## Design priorities

Every change is weighed against these four. They are why the code looks the way it does.

1. **Scalability — high CPU, low driver bottleneck.** Push work to workers. No driver-side
   O(N) filesystem scans; no driver-side inflight throttle; stream `as_completed` for
   long phases.
2. **Low-memory plateau.** Per-worker memory must plateau, not climb, regardless of build
   duration. As much CPU as possible with as little RAM as possible.
3. **Atomic and resumable I/O.** Safety is a first-class concern. Tolerate corrupt inputs
   and destinations — re-merge or skip-and-log rather than failing the whole job.
4. **A-priori knowledge over runtime detection.** Knowing how the data should look, or its
   bounds, saves real I/O.

## Where the details live

| Need | Look in |
|---|---|
| Does a helper for this already exist? | the `find-existing-helper` skill — **check before writing one** |
| Build, merge, resume, download, `gh3_update` | `.claude/rules/build-pipeline.md` (auto-loads) |
| `gh3_load` / `egi_load`, pushdown, bbox index, DuckLake | `.claude/rules/query-engine.md` |
| H3/EGI cell math, CRS, rasterization | `.claude/rules/spatial-indexing.md` |
| CLI structure and shared builders | `.claude/rules/cli.md` |
| `gh3_doctor` diagnoses | `.claude/rules/doctor.md` |
| Test conventions and fixtures | `.claude/rules/tests.md` → `tests/TESTING.md` |
| What a CLI flag does | `--help`, then `docs/user-guide/cli-reference.md` |
| Entry points | `pyproject.toml [project.scripts]` — the source of truth |
| H3 / EGI resolution tables | `gh3_list_resolutions`, `docs/concepts/{h3,egi}-indexing.md` |
| Config env vars | `docs/getting-started/configuration.md` |
| Bumping a version / cutting a release | the `bump-version` then `ship` skills |
| Diagnosing a red CI run | the `monitor-ci` skill |

Rules under `.claude/rules/` carry `paths:` globs and enter context only when you open a
file they cover — so subsystem detail costs nothing until it is relevant.

## Conventions

- numpydoc docstrings on public functions. They are the reference: state a contract in the
  docstring, not in a markdown file that will drift away from it.
- Raise a specific `GediError` subclass from `exceptions.py`, never a bare `Exception`.
- Smallest change that works. Don't refactor adjacent code unless it meaningfully reduces
  risk. Follow the existing pattern before introducing a new abstraction.
- `--help` output stays ASCII — non-ASCII breaks a Windows console.
