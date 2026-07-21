# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Diagnosis registry + orchestrator for gh3_doctor.

Each diagnosis is a callable with ``check(ctx) -> Report`` and optional
``fix(ctx, report) -> Report``. Diagnoses register themselves at import time
via :func:`register`; the package's :mod:`diagnoses` subpackage auto-imports
all known diagnosis modules.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .report import Report, DoctorContext, Severity


@dataclass
class Diagnosis:
    name: str
    description: str
    scope: str                          # 'global' or 'partition'
    check: Callable[[DoctorContext], Report]
    fix: Optional[Callable[[DoctorContext, Report], Report]] = None
    aliases: List[str] = None


_REGISTRY: Dict[str, Diagnosis] = {}

# Group aliases users can pass to --check/--fix. Resolved at dispatch time
# against whatever diagnoses have registered.
ALIAS_GROUPS = {
    'db': ['backfill', 'orphans', 'log_state', 'metadata', 'parquet_health', 'geoparquet_bbox'],
    'soc': ['soc_health'],
    'all': None,                         # special: all registered diagnoses
}


def register(name: str, description: str, scope: str = 'global',
             fix: Optional[Callable] = None):
    """Decorator to register a check function.

    Usage::

        @register('orphans', 'Leftover temp + empty partitions', scope='global', fix=orphans_fix)
        def orphans_check(ctx): ...
    """
    if scope not in ('global', 'partition'):
        raise ValueError(f"scope must be 'global' or 'partition', got {scope!r}")

    def decorator(check_fn):
        _REGISTRY[name] = Diagnosis(
            name=name,
            description=description,
            scope=scope,
            check=check_fn,
            fix=fix,
        )
        return check_fn

    return decorator


def get_diagnoses() -> Dict[str, Diagnosis]:
    """Return a copy of the diagnosis registry."""
    return dict(_REGISTRY)


def resolve_names(names: List[str]) -> List[str]:
    """Expand alias groups (``db``, ``soc``, ``all``) into concrete names."""
    if not names:
        return list(_REGISTRY.keys())

    out = []
    seen = set()
    for n in names:
        if n in ALIAS_GROUPS:
            members = ALIAS_GROUPS[n] if ALIAS_GROUPS[n] is not None else list(_REGISTRY.keys())
            for m in members:
                if m in _REGISTRY and m not in seen:
                    out.append(m)
                    seen.add(m)
        elif n in _REGISTRY:
            if n not in seen:
                out.append(n)
                seen.add(n)
        else:
            raise ValueError(
                f"Unknown diagnosis '{n}'. Available: {sorted(_REGISTRY)} "
                f"or aliases: {sorted(ALIAS_GROUPS)}"
            )
    return out


def _fused_check_reports(ctx: DoctorContext, names: List[str]) -> Dict[str, Report]:
    """Dispatch ``names`` as a single per-partition fused scan.

    Returns a ``{name: Report}`` map. Only diagnoses registered with the
    fused registry (``gedih3.doctor.fused.register_scan``) participate;
    the caller is responsible for filtering ``names`` accordingly.

    The single ``parallel_map`` opens each partition's parquet listing
    and meta JSON once and routes the result through every enabled
    scan, then each diagnosis's ``_finalize_*`` driver-side aggregator
    builds its Report. GPFS read amplification drops 3-5× on a
    continental DB vs. one ``parallel_map`` per diagnosis.
    """
    from .fused import fused_scan_partition
    from .parallel import parallel_map
    from .diagnoses.backfill import (
        _active_products, _expected_product_columns, _finalize_backfill_check,
    )
    from .diagnoses.metadata import _finalize_metadata_check
    from .diagnoses.geoparquet_bbox import _finalize_geoparquet_bbox_check
    from .diagnoses.parquet_health import _finalize_parquet_health_check
    from .diagnoses.orphans import _finalize_orphans_check

    # Per-diagnosis broadcast kwargs. Computed once on the driver and
    # forwarded to the worker as a namespaced ``scan_kwargs`` dict.
    scan_kwargs: Dict[str, Dict] = {}
    if 'backfill' in names:
        products = _active_products(ctx)
        if products:
            scan_kwargs['backfill'] = {
                'products': products,
                'expected_by_product': {p: _expected_product_columns(ctx, p) for p in products},
            }
        else:
            # No active products → backfill has nothing to do; drop from
            # the fused dispatch and return its no-op report directly.
            names = [n for n in names if n != 'backfill']
    if 'orphans' in names:
        age_hours = getattr(ctx.args, 'orphan_age_hours', 24.0)
        scan_kwargs['orphans'] = {'age_seconds': age_hours * 3600}

    # Dispatch the fused scan over partition dirs. Each per-partition
    # result is a {scan_name: payload} dict.
    per_partition: Dict[str, Dict[str, object]] = {}
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        fused_scan_partition,
        args=getattr(ctx, 'args', None),
        desc='fused: scanning partitions',
        unit='part',
        enabled_scans=list(names),
        scan_kwargs=scan_kwargs,
    ):
        if isinstance(result, Exception):
            # Whole-partition failure: surface the same scan_error finding
            # each diagnosis would have produced individually.
            per_partition[part_dir] = {n: result for n in names}
        else:
            per_partition[part_dir] = result

    # Split per_partition into a {name: {part_dir: scan_result}} map for
    # each diagnosis's finalize step.
    by_name: Dict[str, Dict[str, object]] = {n: {} for n in names}
    for part_dir, scan_results in per_partition.items():
        # ``scan_results`` is the per-partition dict the fused worker
        # returned (or a {n: Exception} dict if the whole task failed).
        # ``.get(n)`` is None when the worker skipped this scan because
        # the registry didn't have it — most commonly happens when the
        # cluster has stale bytecode predating this refactor.
        get = getattr(scan_results, 'get', None)
        for n in names:
            by_name[n][part_dir] = get(n) if get else None

    # Detect scans where EVERY partition returned None (i.e. fusion
    # produced no usable data for this diagnosis). Fall through to the
    # single-diagnosis dispatch in those cases so the operator gets a
    # correct report instead of a misleading "all errored" finding.
    from ..logging_config import get_logger
    _log = get_logger(__name__)
    failed_in_fusion = []
    for n in list(names):
        if all(v is None for v in by_name[n].values()):
            failed_in_fusion.append(n)
            _log.warning(
                f"fused scan returned no results for {n!r} (cluster may have "
                f"stale bytecode); falling back to per-diagnosis dispatch"
            )
    for n in failed_in_fusion:
        names.remove(n)

    reports: Dict[str, Report] = {}
    if 'metadata' in names:
        reports['metadata'] = _finalize_metadata_check(ctx, by_name['metadata'])
    if 'geoparquet_bbox' in names:
        reports['geoparquet_bbox'] = _finalize_geoparquet_bbox_check(ctx, by_name['geoparquet_bbox'])
    if 'parquet_health' in names:
        reports['parquet_health'] = _finalize_parquet_health_check(ctx, by_name['parquet_health'])
    if 'orphans' in names:
        reports['orphans'] = _finalize_orphans_check(
            ctx, by_name['orphans'], scan_kwargs['orphans']['age_seconds']
        )
    if 'backfill' in names:
        reports['backfill'] = _finalize_backfill_check(ctx, by_name['backfill'])

    return reports


def run_diagnoses(ctx: DoctorContext, names: List[str], mode: str = 'check') -> List[Report]:
    """Run the named diagnoses (or aliases) against ``ctx``.

    When two or more per-partition h3db diagnoses are requested at
    ``mode='check'``, dispatch fuses them into a single per-partition
    scan that opens each partition's files once and routes the per-
    partition payload to every enabled diagnosis's driver-side
    aggregator. Falls back to per-diagnosis dispatch for fix mode and
    for single-diagnosis check invocations.

    Parameters
    ----------
    ctx : DoctorContext
        Shared state.
    names : list of str
        Diagnosis names or alias groups. Empty list = all registered.
    mode : str
        'check' (read-only) or 'fix' (apply remedies where supported).

    Returns
    -------
    list of Report
    """
    if mode not in ('check', 'fix'):
        raise ValueError(f"mode must be 'check' or 'fix', got {mode!r}")

    resolved = resolve_names(names)
    reports = []

    # Identify the subset of resolved diagnoses that are fusion-eligible.
    # Fusion only applies to ``mode='check'`` because each diagnosis's
    # fix flow has independent semantics (some rewrite parquet, some
    # delete dirs, some only log).
    fused_reports: Dict[str, Report] = {}
    if mode == 'check' and len(resolved) >= 2:
        from .fused import fused_eligible_names
        eligible = [n for n in resolved if n in fused_eligible_names()]
        if len(eligible) >= 2:
            try:
                fused_reports = _fused_check_reports(ctx, eligible)
            except Exception as e:
                # Fusion path failure must not block the single-path
                # fallback: log and proceed.
                from ..logging_config import get_logger
                get_logger(__name__).warning(
                    f"fused dispatch failed: {type(e).__name__}: {e}; "
                    f"falling back to per-diagnosis dispatch"
                )
                fused_reports = {}

    for name in resolved:
        diag = _REGISTRY[name]
        if name in fused_reports:
            report = fused_reports[name]
        else:
            try:
                report = diag.check(ctx)
            except Exception as e:
                report = Report(
                    name=name,
                    severity=Severity.ERROR,
                    summary=f"check failed: {type(e).__name__}: {e}",
                    findings=[{'error': str(e), 'type': type(e).__name__}],
                )
                reports.append(report)
                continue

        if mode == 'fix' and report.has_findings and diag.fix is not None:
            try:
                report = diag.fix(ctx, report)
            except Exception as e:
                # Preserve the check report but flag the fix failure.
                report.severity = Severity.ERROR
                report.summary = f"{report.summary} | fix failed: {type(e).__name__}: {e}"
                report.findings.append({'fix_error': str(e), 'type': type(e).__name__})

        reports.append(report)

    return reports
