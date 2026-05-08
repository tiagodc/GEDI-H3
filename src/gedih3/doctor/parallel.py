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
    batch_size: int = 0,
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
    batch_size : int, optional
        When >0, group items into batches of this size and dispatch one
        dask task per batch (each task internally iterates its slice).
        Use this when ``len(items)`` is in the hundreds-of-thousands —
        ``client.map`` builds a task graph proportional to ``len(items)``
        and submission/scheduler overhead dominates the actual work
        beyond ~10k tasks. The default (0) preserves one-task-per-item
        behavior, which is correct for partition-level fan-out where
        per-item work is heavy and ``len(items)`` is in the thousands.
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

    if batch_size and batch_size > 0 and len(items) > batch_size:
        # Batched dispatch — one dask task per chunk. Required when
        # ``len(items)`` is in the hundreds of thousands, otherwise
        # task-graph build + scheduler dispatch dominates wall time
        # and the cluster never gets to do real work. Worker still
        # yields per-item results (one ``(item, result)`` per input)
        # so callers see no behavior change.
        chunks = [items[i:i + batch_size]
                  for i in range(0, len(items), batch_size)]
        logger.info(
            f"{desc}: dispatching {len(chunks)} batches "
            f"of up to {batch_size} {unit}s "
            f"({len(items)} total) to dask cluster (progress on dashboard)"
        )
        # The chunk worker is constructed at submission time so
        # ``broadcast`` is captured by closure, not zipped through
        # ``client.map``'s iterables.
        futures = client.map(_run_chunk, chunks, fn=fn, broadcast=broadcast)

        log_every = max(1, len(chunks) // 20)
        n_done_chunks = 0
        n_done_items = 0
        for fut in dask_as_completed(futures):
            try:
                pairs = fut.result()
            except Exception as e:
                # If the chunk dispatch itself failed (pickling, etc.)
                # we surface a single error pair so the caller sees it.
                yield None, e
                pairs = []
            for it, res in pairs:
                yield it, res
                n_done_items += 1
            n_done_chunks += 1
            if (n_done_chunks == 1 or n_done_chunks == len(chunks)
                    or n_done_chunks % log_every == 0):
                logger.info(
                    f"{desc}: {n_done_chunks}/{len(chunks)} batches "
                    f"({n_done_items}/{len(items)} {unit}s) done"
                )
        return

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


def _run_chunk(chunk: List[Any], *, fn: Callable[..., Any],
               broadcast: dict) -> List[Tuple[Any, Any]]:
    """Worker entry point for batched dispatch. Returns one
    ``(item, result)`` pair per chunk member. Per-item exceptions are
    captured in-band so a single bad input does not poison the batch.

    Defined at module scope so dask can pickle it; callers should not
    invoke it directly.
    """
    out = []
    for it in chunk:
        try:
            out.append((it, fn(it, **broadcast)))
        except Exception as e:
            out.append((it, e))
    return out


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
