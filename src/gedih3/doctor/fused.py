# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Fused per-partition scan for gh3_doctor's h3db-tree diagnoses.

When the user asks for multiple per-partition diagnoses in one invocation
(e.g. ``--check db`` or ``--check parquet_health,metadata``), this module
turns N separate ``parallel_map`` dispatches with N file opens per
partition into a single dispatch where each partition's parquet listing,
meta JSON, and parquet footers are opened **once** and shared across
every enabled scan.

The shared state is built by :func:`_build_shared_state` at worker entry,
passed as a ``shared`` dict to each diagnosis's ``_scan_partition_*(dir,
shared=None)`` worker, and dropped when the partition's work is done.
Each scan in :data:`_SCAN_REGISTRY` returns the per-partition payload its
diagnosis already produces today (no schema change), so driver-side
``_finalize_check_from_results`` aggregation is identical to the single-
diagnosis path.

Read-amplification savings on a continental DB scale with the parquet
footer reuse — the heaviest reads — between ``parquet_health``,
``geoparquet_bbox``, and ``backfill``.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .inspect import partition_meta_file, partition_parquet_files
from ..utils import json_read, release_arrow_pool


# Worker registry. Each entry is the picklable per-partition scan callable
# for one diagnosis. The fused worker dispatches enabled scans in
# registry-iteration order — order doesn't matter for correctness because
# every scan returns its own keyed result slot.
#
# Diagnoses populate this at import time via :func:`register_scan`.
_SCAN_REGISTRY: Dict[str, Callable[..., Any]] = {}


def register_scan(name: str, fn: Callable[..., Any]) -> None:
    """Register a per-partition scan worker under ``name``.

    The worker must be picklable (module-level), accept ``partition_dir``
    as the first positional arg, and accept ``shared=None`` plus any
    diagnosis-specific keyword args.
    """
    _SCAN_REGISTRY[name] = fn


def _build_shared_state(partition_dir: str, enabled: Iterable[str]) -> Dict[str, Any]:
    """Open the per-partition state needed by ``enabled`` scans **once**.

    Returns a dict with whichever keys are relevant:

    * ``parquet_files`` : list of parquet paths under the partition.
      Populated whenever any scan that reads parquet is enabled.
    * ``meta_file``     : path to the merged partition meta JSON, or None.
      Populated whenever any scan that touches the meta is enabled.
    * ``meta_dict``     : parsed meta JSON, or None.
      Populated only when ``backfill`` is enabled (only consumer today).

    Parquet footers are *not* prefetched here — the per-file
    ``pq.ParquetFile`` open is deferred to the diagnosis worker so the
    file handle's lifetime is tightly scoped (open → read footer →
    close) and worker RSS doesn't carry N footer handles simultaneously.
    The driver-side win is the *file-listing* and *meta-locate* dedupe;
    the per-file open itself stays inside each diagnosis as today, since
    the footer payload is small and dropping ``pq.ParquetFile`` promptly
    is the established memory plateau pattern (see parquet_health docstring).
    """
    enabled = set(enabled)
    need_parquet_list = bool(enabled & {
        'metadata', 'geoparquet_bbox', 'parquet_health', 'backfill',
    })
    need_meta_file = bool(enabled & {'metadata', 'backfill'})
    need_meta_dict = bool(enabled & {'backfill'})

    shared: Dict[str, Any] = {'partition_dir': partition_dir}

    if need_parquet_list:
        try:
            shared['parquet_files'] = partition_parquet_files(partition_dir)
        except Exception:
            shared['parquet_files'] = []

    if need_meta_file:
        try:
            shared['meta_file'] = partition_meta_file(partition_dir)
        except Exception:
            shared['meta_file'] = None

    if need_meta_dict:
        mf = shared.get('meta_file')
        shared['meta_dict'] = None
        if mf is not None:
            try:
                shared['meta_dict'] = json_read(mf)
            except Exception:
                shared['meta_dict'] = None

    return shared


def fused_scan_partition(
    partition_dir: str,
    *,
    enabled_scans: List[str],
    scan_kwargs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Worker: run every enabled per-partition scan on one partition.

    Returns ``{scan_name: per_partition_result_or_exception}``. A
    per-scan exception is captured in the result dict (mirroring
    ``parallel_map``'s exception-passthrough) so one scan's failure on
    one partition never aborts the others for that same partition.

    ``scan_kwargs`` carries per-diagnosis broadcast args namespaced by
    diagnosis name — e.g. ``{'orphans': {'age_seconds': 86400},
    'backfill': {'products': [...], 'expected_by_product': {...}}}``.
    """
    # Force-import the diagnoses package on the worker side. Each
    # diagnosis module populates ``_SCAN_REGISTRY`` via a
    # :func:`register_scan` call at import time, but workers receive
    # this function by pickle-by-reference — they have never imported
    # the diagnoses submodules, so the worker's ``_SCAN_REGISTRY`` is
    # empty until we trigger the side-effect imports here. Without this,
    # every scan returns None and the driver-side fallback fires for
    # every diagnosis (slow per-diagnosis path).
    from . import diagnoses  # noqa: F401

    scan_kwargs = scan_kwargs or {}
    shared = _build_shared_state(partition_dir, enabled_scans)
    out: Dict[str, Any] = {}

    for name in enabled_scans:
        fn = _SCAN_REGISTRY.get(name)
        if fn is None:
            # Diagnosis isn't fused-aware yet — driver falls back to
            # single-diagnosis dispatch for this name.
            continue
        kwargs = scan_kwargs.get(name, {})
        try:
            out[name] = fn(partition_dir, shared=shared, **kwargs)
        except Exception as e:
            out[name] = e

    # Worker memory hygiene — release any pyarrow buffers that may have
    # been allocated during the per-scan footer reads.
    try:
        release_arrow_pool()
    except Exception:
        pass

    return out


def fused_eligible_names() -> set:
    """Return the set of diagnosis names that participate in fusion.

    The runner uses this to decide whether ``--check X,Y`` is fusible.
    Only diagnoses whose worker has been registered via
    :func:`register_scan` are eligible.
    """
    return set(_SCAN_REGISTRY)
