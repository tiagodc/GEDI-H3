"""Distributed work helpers for gh3_doctor diagnoses.

Backports the v0.8.x build-pipeline lessons to the doctor module. Each
diagnosis that scans every partition / parquet file used to do so
serially on the driver (see git ``e8a966b`` and friends for the same
pattern that was eradicated from the merge phase). On a continental-
scale database that meant hours of cold GPFS I/O blocking the CLI.

This module provides one primitive — :func:`parallel_map` — that ships
per-partition work to a dask cluster when one is registered, and falls
back to a serial loop with a ``progress_iter`` bar when not. Diagnoses
import this and pass a *picklable, side-effect-free* worker function
plus the broadcast kwargs the worker needs.

Design rules (mirrors the build pipeline's merge-phase contract):
  * **Workers receive only what they need.** No ``DoctorContext`` (it
    holds loggers and other process-local state); just the partition
    path plus serializable broadcast kwargs.
  * **Stream ``as_completed``**, not ``persist + compute``. Findings
    accumulate as each future finishes; failures on individual
    partitions surface as exception objects in the yielded stream.
  * **No driver-side throttle.** All futures are submitted at once and
    the dask scheduler distributes; backpressure is its job.
  * **Driver-side aggregation is cheap and incremental.** The caller
    extends a flat ``findings`` list as results arrive — no large
    materialized intermediates.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Iterable, Iterator, List, Tuple

from ..cliutils import progress_iter
from ..logging_config import get_logger
from ..utils import get_dask_client

logger = get_logger(__name__)


def parallel_map(
    items: List[Any],
    fn: Callable[..., Any],
    *,
    args=None,
    desc: str = '',
    unit: str = 'item',
    **broadcast,
) -> Iterator[Tuple[Any, Any]]:
    """Map ``fn`` across ``items`` with optional dask parallelism.

    Yields ``(item, result)`` tuples in completion order. When ``fn``
    raises on a worker, ``result`` is the exception instance — callers
    decide how to surface it (turn into a finding, log, or re-raise).

    Parallelism is automatic:
      * If a dask :class:`~dask.distributed.Client` is registered (via
        ``get_dask_client()``), work is dispatched via ``client.map`` and
        results streamed via ``as_completed``. Progress is visible on
        the dask dashboard; the CLI logs a periodic counter line.
      * Otherwise, runs serially with the same ``progress_iter`` bar
        the rest of the doctor uses.

    Parameters
    ----------
    items : list
        Iterable of work items (e.g. partition directory paths).
    fn : callable
        Top-level (picklable) function. Signature: ``fn(item, **broadcast)``.
    args : argparse.Namespace, optional
        Forwarded to ``progress_iter`` for ``--no-progress`` honoring on
        the serial fallback.
    desc, unit : str
        Progress bar text (serial path) and dispatch log prefix.
    **broadcast
        Constant keyword arguments forwarded to every call of ``fn``.
        For client.map this becomes a per-task kwarg; values should be
        small (lists of column names, product codes), since dask copies
        them to each task. Large structures should be ``client.scatter``
        in advance by the caller.
    """
    items = list(items)
    if not items:
        return

    client = get_dask_client()
    if client is None:
        # Serial fallback — preserves the original UX on machines
        # without a registered dask cluster.
        with progress_iter(items, desc=desc, args=args, unit=unit) as bar:
            for it in bar:
                try:
                    yield it, fn(it, **broadcast)
                except Exception as e:
                    yield it, e
        return

    # Parallel path
    from dask.distributed import as_completed as dask_as_completed

    logger.info(
        f"{desc}: dispatching {len(items)} tasks to dask cluster "
        f"(progress on dashboard)"
    )
    futures = client.map(fn, items, **broadcast)
    fut2item = {f: it for f, it in zip(futures, items)}

    # One log line per ~5% so the CLI doesn't go silent on long runs;
    # the dask dashboard is the real-time view.
    log_every = max(1, len(items) // 20)
    n_done = 0

    for fut in dask_as_completed(futures):
        item = fut2item.get(fut)
        try:
            res = fut.result()
        except Exception as e:
            res = e
        n_done += 1
        if n_done == 1 or n_done == len(items) or n_done % log_every == 0:
            logger.info(f"{desc}: {n_done}/{len(items)} partitions done")
        yield item, res


def partition_is_empty(partition_dir: str) -> bool:
    """O(1) emptiness check via :func:`os.scandir` — no recursive glob.

    Mirrors the build pipeline's emptiness check at
    ``gh3builder.py:1356-1362``. A partition is considered empty when
    it contains no parquet file at any depth (year subdir or root).
    """
    try:
        for entry in os.scandir(partition_dir):
            if entry.name.endswith('.parquet'):
                return False
            if entry.is_dir(follow_symlinks=False):
                # one-level nested check (year=NNNN/*.parquet)
                try:
                    for sub in os.scandir(entry.path):
                        if sub.name.endswith('.parquet'):
                            return False
                except OSError:
                    continue
    except OSError:
        return True
    return True


def list_year_dirs(partition_dir: str) -> List[str]:
    """List immediate subdirectories of a partition directory.

    Single ``os.scandir`` call instead of ``glob.glob('*/')``; same
    primitive the build pipeline uses to enumerate per-h3 work units.
    """
    out = []
    try:
        for entry in os.scandir(partition_dir):
            if entry.is_dir(follow_symlinks=False):
                out.append(entry.path + os.sep)
    except OSError:
        return []
    return sorted(out)


def year_dir_is_empty(year_dir: str) -> bool:
    """O(1) parquet presence check inside one year=*/ directory."""
    try:
        for entry in os.scandir(year_dir):
            if entry.name.endswith('.parquet'):
                return False
    except OSError:
        return True
    return True
