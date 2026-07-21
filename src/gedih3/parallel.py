# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Package-wide parallelism primitives for gedih3.

Houses the always-parallel ``parallel_map`` (originally introduced for
gh3_doctor diagnoses; promoted here because the build, download, and
manifest-writing paths all depend on it now) plus three parallel walker
primitives used to regenerate SOC and H3-DB manifests via dask workers
instead of driver-side serial recursive globs.

Design rules (carried forward from the v0.8.x build-pipeline lessons
and the doctor refactor):

  * **Always parallel.** No serial fallback. A registered
    :class:`~dask.distributed.Client` is required; library / notebook
    callers must wrap their call site in ``with Client(...) as c: ...``.
    Single code path means no quietly-different behavior on small inputs.
  * **Workers receive only what they need.** Filter args (exclude
    patterns, glob patterns, …) flow through ``parallel_map``'s
    ``**broadcast`` kwargs — never via closure capture — so worker fns
    stay picklable as top-level module functions.
  * **Stream ``as_completed``** instead of ``persist + compute``.
    Findings accumulate as each future finishes; failures on
    individual items surface as exception objects in the yielded stream.
  * **Driver-side aggregation is cheap.** The walkers flatten + sort
    the per-leaf results on the driver as they arrive — no large
    materialized intermediates.
  * **Fail loud.** A worker exception in a walker aborts the walk;
    an incomplete manifest is a worse footgun than a slow re-walk.
"""

from __future__ import annotations

import fnmatch
import glob
import os
from typing import Any, Callable, Iterator, List, Optional, Tuple

from .exceptions import GediError
from .logging_config import get_logger

logger = get_logger(__name__)


# ─── parallel_map (moved from doctor/parallel.py) ─────────────────────────


def parallel_map(
    items: List[Any],
    fn: Callable[..., Any],
    *,
    args=None,
    desc: str = '',
    unit: str = 'item',
    batch_size: int = 0,
    **broadcast,
) -> Iterator[Tuple[Any, Any]]:
    """Map ``fn`` across ``items`` on a dask cluster.

    Yields ``(item, result)`` tuples in completion order. When ``fn``
    raises on a worker, ``result`` is the exception instance — callers
    decide how to surface it (turn into a finding, log, or re-raise).

    Requires a dask :class:`~dask.distributed.Client` to be registered
    (via :func:`gedih3.utils.get_dask_client`). The gedih3 CLI tools
    create one at startup; library / notebook callers must wrap their
    call site in ``with Client(...) as client: ...``. Raises
    :class:`gedih3.exceptions.GediError` when no client is registered.

    Parameters
    ----------
    items : list
        Iterable of work items (e.g. partition directory paths).
    fn : callable
        Top-level (picklable) function. Signature: ``fn(item, **broadcast)``.
    args : argparse.Namespace, optional
        Accepted for API compatibility with callers that pass it; not
        consumed because the always-parallel path doesn't need a tqdm
        progress bar — the dask dashboard is the live view.
    desc, unit : str
        Dispatch log prefix and unit name used in the periodic
        ``N/M done`` counter line.
    batch_size : int, optional
        When >0, group items into batches of this size and dispatch one
        dask task per batch (each task internally iterates its slice).
        Use this when ``len(items)`` is in the hundreds-of-thousands —
        ``client.map`` builds a task graph proportional to ``len(items)``
        and submission/scheduler overhead dominates the actual work
        beyond ~10k tasks.
    **broadcast
        Constant keyword arguments forwarded to every call of ``fn``.
    """
    del args  # accepted for API stability; not used in always-parallel path
    items = list(items)
    if not items:
        return

    # Lazy import keeps ``utils`` -> ``parallel`` -> ``utils`` from cycling
    # at module-init time. The dask client is only needed at call time
    # anyway.
    from .utils import get_dask_client

    client = get_dask_client()
    if client is None:
        raise GediError(
            "parallel_map requires a registered dask.distributed Client. "
            "CLI tools (gh3_doctor, gh3_extract, gh3_aggregate, …) create "
            "one automatically; library / notebook callers must wrap their "
            "code in `with dask.distributed.Client(...) as client: ...`."
        )

    from dask.distributed import as_completed as dask_as_completed
    from tqdm import tqdm as tqdm_bar

    # Periodic-progress INFO is opt-in only — tqdm already shows live
    # progress in the terminal, and the dask dashboard is the canonical
    # cluster view. Set ``GH3_LOG_PROGRESS=1`` to re-enable the 5%-step
    # INFO lines for detached / tail-followed log workflows.
    log_progress = os.environ.get(
        'GH3_LOG_PROGRESS', '').strip().lower() in ('1', 'true', 'yes', 'on')

    if batch_size and batch_size > 0 and len(items) > batch_size:
        chunks = [items[i:i + batch_size]
                  for i in range(0, len(items), batch_size)]
        logger.info(
            f"{desc}: dispatching {len(chunks)} batches "
            f"of up to {batch_size} {unit}s "
            f"({len(items)} total) to dask cluster (progress on dashboard)"
        )
        # pure=False: every fn shipped through parallel_map reads live
        # filesystem state (directory scans, h5 validity, metadata reads).
        # Dask's default pure=True hashes (fn, args) into a deterministic
        # task key and reuses cached results for identical keys — a repeat
        # of the same scan in one process (manifest write -> doctor scan,
        # --fix -> re-scan) could silently return stale pre-mutation
        # results when the resubmission races the async release of the
        # previous futures.
        futures = client.map(_run_chunk, chunks, fn=fn, broadcast=broadcast,
                             pure=False)

        log_every = max(1, len(chunks) // 20)
        n_done_chunks = 0
        n_done_items = 0
        pbar = tqdm_bar(total=len(items), desc=desc or 'parallel_map', unit=unit)
        try:
            for fut in dask_as_completed(futures):
                try:
                    pairs = fut.result()
                except Exception as e:
                    yield None, e
                    pairs = []
                for it, res in pairs:
                    yield it, res
                    n_done_items += 1
                    pbar.update(1)
                n_done_chunks += 1
                if log_progress and (
                    n_done_chunks == 1 or n_done_chunks == len(chunks)
                    or n_done_chunks % log_every == 0
                ):
                    logger.info(
                        f"{desc}: {n_done_chunks}/{len(chunks)} batches "
                        f"({n_done_items}/{len(items)} {unit}s) done"
                    )
        finally:
            pbar.close()
        return

    logger.info(
        f"{desc}: dispatching {len(items)} tasks to dask cluster "
        f"(progress on dashboard)"
    )
    # pure=False — see the batched branch above: parallel_map fns are
    # impure filesystem readers; key-cached results would be stale.
    futures = client.map(fn, items, pure=False, **broadcast)
    fut2item = {f: it for f, it in zip(futures, items)}

    log_every = max(1, len(items) // 20)
    n_done = 0
    pbar = tqdm_bar(total=len(items), desc=desc or 'parallel_map', unit=unit)
    try:
        for fut in dask_as_completed(futures):
            item = fut2item.get(fut)
            try:
                res = fut.result()
            except Exception as e:
                res = e
            n_done += 1
            pbar.update(1)
            if log_progress and (
                n_done == 1 or n_done == len(items) or n_done % log_every == 0
            ):
                logger.info(f"{desc}: {n_done}/{len(items)} {unit}s done")
            yield item, res
    finally:
        pbar.close()


def _run_chunk(chunk: List[Any], *, fn: Callable[..., Any],
               broadcast: dict) -> List[Tuple[Any, Any]]:
    """Worker entry point for batched dispatch. Top-level so dask can
    pickle it; not for direct use."""
    out = []
    for it in chunk:
        try:
            out.append((it, fn(it, **broadcast)))
        except Exception as e:
            out.append((it, e))
    return out


# ─── parallel walkers ─────────────────────────────────────────────────────
#
# Three primitives, one shared shape:
#   1. driver enumerates leaf directories via os.scandir (bounded)
#   2. parallel_map dispatches per-leaf scan to dask workers
#   3. driver flattens + sorts + returns
#   4. fail-loud on any worker exception (no partial result)
#
# All filter args (pattern, exclude) flow through parallel_map's
# **broadcast — never as closure capture — so worker fns are top-level
# and picklable.


def _scan_doy_dir(doy_dir: str, *, pattern: str,
                  exclude: Optional[List[str]] = None) -> List[str]:
    """SOC walker worker. One non-recursive scandir per doy directory."""
    out = []
    try:
        for entry in os.scandir(doy_dir):
            if not entry.is_file(follow_symlinks=False):
                continue
            name = entry.name
            if not fnmatch.fnmatch(name, pattern):
                continue
            if exclude and any(fnmatch.fnmatch(name, p) for p in exclude):
                continue
            out.append(entry.path)
    except OSError as e:
        # Re-raise so parallel_map surfaces the failure and the walker
        # aborts (R2 contract: never produce a partial manifest).
        raise OSError(f"scandir failed at {doy_dir}: {e}") from e
    return out


def _scan_h3_partition(part_dir: str, *, pattern: str) -> List[str]:
    """H3-DB walker worker. Recursive scan inside one h3 partition dir.
    Handles both flat (``h3_NN=*/foo.parquet``) and year-nested
    (``h3_NN=*/year=NNNN/foo.parquet``) layouts."""
    return sorted(glob.glob(os.path.join(part_dir, '**', pattern),
                            recursive=True))


def _scan_flat_dir(dir_path: str, *, pattern: str) -> List[str]:
    """Flat-tree worker. One non-recursive scandir."""
    out = []
    try:
        for entry in os.scandir(dir_path):
            if entry.is_file(follow_symlinks=False) and fnmatch.fnmatch(
                    entry.name, pattern):
                out.append(entry.path)
    except OSError as e:
        raise OSError(f"scandir failed at {dir_path}: {e}") from e
    return out


def walk_soc_parallel(
    soc_dir: str,
    *,
    pattern: str = 'GEDI*.h5',
    exclude: Optional[List[str]] = None,
    batch_size: int = 32,
) -> List[str]:
    """Parallel year/doy walk over a SOC tree.

    Driver enumerates year and doy subdirs via :func:`os.scandir`
    (bounded: ~7 years × ~300 doys = ~2000 single-level syscalls on
    the driver). Workers run one non-recursive scandir per doy via
    :func:`_scan_doy_dir`, applying ``pattern`` and ``exclude``
    locally so we never ship unwanted paths back across the network.

    Always parallel — requires a registered dask Client.
    """
    year_dirs = []
    try:
        for entry in os.scandir(soc_dir):
            if entry.is_dir(follow_symlinks=False) and entry.name.isdigit():
                year_dirs.append(entry.path)
    except OSError:
        return []

    doy_dirs = []
    for y in sorted(year_dirs):
        try:
            for entry in os.scandir(y):
                if entry.is_dir(follow_symlinks=False):
                    doy_dirs.append(entry.path)
        except OSError:
            continue

    # Degenerate case: no ``YYYY/DOY/`` structure at the root (test
    # fixtures, small ad-hoc deliveries, the very first download of
    # the day before subdirs exist). Treat the root itself as a
    # single leaf.
    if not doy_dirs:
        doy_dirs = [soc_dir]

    files: List[str] = []
    for _, res in parallel_map(
            sorted(doy_dirs),
            _scan_doy_dir,
            desc=f"walk_soc_parallel({soc_dir})",
            unit='doy',
            batch_size=batch_size,
            pattern=pattern,
            exclude=exclude,
    ):
        if isinstance(res, Exception):
            raise GediError(
                f"walk_soc_parallel: worker failed — aborting walk to avoid "
                f"writing a partial manifest. Original: {type(res).__name__}: {res}"
            )
        files.extend(res)

    return sorted(files)


def walk_h3db_parallel(
    db_root: str,
    *,
    pattern: str = '*.parquet',
    batch_size: int = 64,
) -> List[str]:
    """Parallel per-partition walk over an H3 database.

    Driver enumerates ``h3_NN=*`` partition directories via one
    :func:`os.scandir` on the DB root (typically ~10k entries at
    partition level 3). Workers recursively glob each partition
    (handles both flat and ``year=NNNN/``-nested layouts).

    Always parallel — requires a registered dask Client.
    """
    partition_dirs = []
    try:
        for entry in os.scandir(db_root):
            if entry.is_dir(follow_symlinks=False) and entry.name.startswith('h3_'):
                partition_dirs.append(entry.path)
    except OSError:
        return []

    if not partition_dirs:
        return []

    files: List[str] = []
    for _, res in parallel_map(
            sorted(partition_dirs),
            _scan_h3_partition,
            desc=f"walk_h3db_parallel({db_root})",
            unit='partition',
            batch_size=batch_size,
            pattern=pattern,
    ):
        if isinstance(res, Exception):
            raise GediError(
                f"walk_h3db_parallel: worker failed — aborting walk to avoid "
                f"writing a partial manifest. Original: {type(res).__name__}: {res}"
            )
        files.extend(res)

    return sorted(files)


def walk_flat_parallel(
    dir_path: str,
    *,
    pattern: str = '*.parquet',
) -> List[str]:
    """Flat single-directory listing. Provided for API symmetry with the
    other two walkers; no dask dispatch (the tree has a single leaf)."""
    return sorted(_scan_flat_dir(dir_path, pattern=pattern))


# ─── manifest freshness smoke check (R2 + producer-crash guard) ───────────
#
# R2 is producer-driven: every code path that mutates the tree refreshes
# the manifest before returning. A producer that SIGKILLs between writing
# the last file and writing the manifest leaves a stale manifest on disk
# and the next consumer would silently miss the new files.
#
# This cheap smoke check (two ``os.stat`` calls) catches that case and
# emits a loud, actionable error directing the user to the doctor remedy.
# It does NOT auto-refresh — that would violate R2's "consumers trust the
# manifest" contract. The check is constant-time regardless of tree size.


def check_manifest_freshness(
    manifest_path: str,
    root_dir: str,
    *,
    raise_on_stale: bool = False,
    remedy: str = '',
) -> bool:
    """Compare ``mtime(manifest_path)`` against ``mtime(root_dir)``.

    Returns True when the manifest is at least as new as the root
    directory's mtime. Returns False (and logs a loud error / raises)
    when the root dir was touched after the manifest was written —
    indicating either a producer crashed before refreshing the manifest
    or files were dropped in externally (e.g. NASA delivery, manual
    rsync).

    Parameters
    ----------
    manifest_path
        Absolute path to the manifest file.
    root_dir
        Absolute path to the tree the manifest indexes.
    raise_on_stale
        When True, raise :class:`gedih3.exceptions.GediError` on
        staleness. When False (default), log an ERROR and return False.
    remedy
        Shell command the user should run to refresh the manifest,
        included in the error message.
    """
    try:
        m_mtime = os.stat(manifest_path).st_mtime_ns
        r_mtime = os.stat(root_dir).st_mtime_ns
    except OSError:
        # Manifest or root missing — leave the decision to the caller's
        # existing missing-manifest fallback. Not our case to flag.
        return True
    if m_mtime >= r_mtime:
        return True
    msg = (
        f"manifest {manifest_path} is older than {root_dir} "
        f"({m_mtime} < {r_mtime}). The tree was modified after the last "
        f"producer refresh — likely a producer crash or external "
        f"population (NASA delivery, manual rsync)."
    )
    if remedy:
        msg += f" Remedy: {remedy}"
    if raise_on_stale:
        raise GediError(msg)
    # WARNING, not ERROR — the check is a heuristic on directory mtime, not a
    # content check. A create+delete of a short-lived temp file (e.g. atomic
    # writes from an unrelated tool earlier) bumps the dir mtime without
    # invalidating the manifest contents, so this fires routinely on
    # perfectly correct manifests. Downstream code still uses the manifest;
    # this is advisory.
    logger.warning(msg)
    return False
