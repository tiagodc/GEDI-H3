"""geoparquet_bbox diagnosis — partition parquets without a valid bbox.

Detects partition parquets whose GeoParquet ``geo`` schema metadata is missing
or has no usable ``columns.<primary>.bbox``. The bbox is required for valid
GeoParquet (per the spec it is optional in the strict sense, but predicate
pushdown across spatial reads depends on it, and downstream tools like
geopandas / dask_geopandas expect it on production datasets).

Background: pre-v0.8.14, ``parquet_merge_files`` had a ``bbox_threshold=50``
gate that skipped bbox computation for partition-years with many fragments —
which is most continental partitions. v0.8.14 removed the gate; this
diagnosis backfills the bbox on files merged before that fix.

Findings:
  - ``missing_geo``: no ``geo`` key in schema metadata. Cannot be backfilled
    in place (would need a full re-merge from source). Reported only.
  - ``missing_bbox``: ``geo`` exists but no ``columns.<primary>.bbox`` field.
    Fixable via ``parquet_backfill_bbox``.
  - ``invalid_bbox``: bbox present but malformed (wrong length, non-finite).
    Fixable via ``parquet_backfill_bbox``.
  - ``no_geometry``: file has no geometry column (rare; typical for non-spatial
    sidecars). Reported only — no bbox is meaningful here.
"""

from __future__ import annotations

import json
import math
import os
from typing import Optional

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_parquet_files
from ..parallel import parallel_map
from ...utils import parquet_backfill_bbox, release_arrow_pool


def _classify(path: str) -> Optional[str]:
    """Return a finding kind, or None if the file's bbox is OK.

    Memory: footer-only ``pq.read_schema`` (no data buffers) plus a
    JSON parse of the small ``geo`` metadata block. Constant ~KB
    per file — independent of file size or column count.
    """
    import pyarrow.parquet as pq
    schema = None
    try:
        schema = pq.read_schema(path)
    except Exception:
        return 'unreadable'

    try:
        if 'geometry' not in schema.names:
            return 'no_geometry'

        md = schema.metadata or {}
        raw = md.get(b'geo')
        if not raw:
            return 'missing_geo'

        try:
            geo = json.loads(raw)
        except Exception:
            return 'missing_geo'

        primary = geo.get('primary_column', 'geometry')
        col = geo.get('columns', {}).get(primary, {})
        bbox = col.get('bbox')
        if bbox is None:
            return 'missing_bbox'
        if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
            return 'invalid_bbox'
        try:
            if not all(math.isfinite(float(v)) for v in bbox):
                return 'invalid_bbox'
        except (TypeError, ValueError):
            return 'invalid_bbox'
        return None
    finally:
        # Drop the schema reference promptly so its metadata buffers
        # don't accumulate across the per-partition scan.
        del schema


def _scan_partition_bbox(partition_dir: str) -> dict:
    """Worker: classify every parquet under one partition.

    Returns ``{'findings': [...], 'n_ok': int}`` so the driver can
    aggregate counts without holding the per-OK paths. Drains the
    pyarrow allocator at the end so worker RSS plateaus instead of
    accumulating across tasks.
    """
    findings = []
    n_ok = 0
    try:
        for f in partition_parquet_files(partition_dir):
            kind = _classify(f)
            if kind is None:
                n_ok += 1
            else:
                findings.append({'kind': kind, 'path': f})
    finally:
        release_arrow_pool()
    return {'findings': findings, 'n_ok': n_ok}


def geoparquet_bbox_check(ctx: DoctorContext) -> Report:
    findings = []
    n_ok = 0
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition_bbox,
        args=getattr(ctx, 'args', None),
        desc='geoparquet_bbox: scanning partitions',
        unit='part',
    ):
        if isinstance(result, Exception):
            findings.append({
                'kind': 'unreadable',
                'path': part_dir,
                'error': f"{type(result).__name__}: {result}",
            })
            continue
        findings.extend(result['findings'])
        n_ok += result['n_ok']

    n_missing_geo = sum(1 for x in findings if x['kind'] == 'missing_geo')
    n_missing_bbox = sum(1 for x in findings if x['kind'] == 'missing_bbox')
    n_invalid = sum(1 for x in findings if x['kind'] == 'invalid_bbox')
    n_unreadable = sum(1 for x in findings if x['kind'] == 'unreadable')

    if n_missing_geo or n_unreadable:
        severity = Severity.ERROR
    elif findings:
        severity = Severity.WARN
    else:
        severity = Severity.INFO

    summary = (
        f"{n_ok} valid; "
        f"{n_missing_bbox} missing bbox; "
        f"{n_invalid} invalid bbox; "
        f"{n_missing_geo} missing geo metadata; "
        f"{n_unreadable} unreadable"
    )

    recommendations = []
    if n_missing_bbox or n_invalid:
        recommendations.append(
            "gh3_doctor --fix geoparquet_bbox   # rewrite affected files in place"
        )
    if n_missing_geo:
        recommendations.append(
            "Files missing the entire GeoParquet 'geo' metadata cannot be "
            "backfilled; full re-merge from tmp fragments required."
        )

    return Report(
        name='geoparquet_bbox', severity=severity,
        findings=findings, summary=summary, recommendations=recommendations,
    )


def _backfill_one(path: str) -> str:
    """Worker: rewrite one file's bbox metadata. Picklable."""
    return parquet_backfill_bbox(path)


def geoparquet_bbox_fix(ctx: DoctorContext, report: Report) -> Report:
    """Rewrite missing/invalid-bbox files via parquet_backfill_bbox.

    Per-file work is independent (each call rewrites a single
    parquet's metadata block) so we dispatch through ``parallel_map``
    when a dask client is registered — same shape as the check path.
    Without parallelism the fix on a continental DB with thousands of
    bbox-missing files takes hours of single-threaded I/O.
    """
    fixable = {'missing_bbox', 'invalid_bbox'}
    fixed = []
    n_rewritten = 0
    n_already_ok = 0
    n_errors = 0

    targets = [f for f in report.findings if f['kind'] in fixable]
    paths = [f['path'] for f in targets]
    by_path = {f['path']: f for f in targets}
    for path, result in parallel_map(
        paths,
        _backfill_one,
        args=getattr(ctx, 'args', None),
        desc='geoparquet_bbox: backfilling bbox',
        unit='file',
    ):
        f = by_path.get(path, {'path': path})
        if isinstance(result, Exception):
            n_errors += 1
            fixed.append({**f, 'fix_error': f"{type(result).__name__}: {result}"})
            continue
        if result == 'rewritten':
            n_rewritten += 1
            fixed.append({**f, 'action': 'bbox_backfilled'})
        elif result == 'ok':
            n_already_ok += 1
            fixed.append({**f, 'action': 'already_valid'})
        else:
            fixed.append({**f, 'action': result})

    # Preserve unfixable findings as-is (missing_geo, no_geometry, unreadable)
    for f in report.findings:
        if f['kind'] not in fixable:
            fixed.append({**f, 'action': 'reported_only'})

    n_blocking = sum(
        1 for x in fixed
        if x['kind'] in ('missing_geo', 'unreadable')
    )

    report.applied = True
    report.findings = fixed
    if n_errors or n_blocking:
        report.severity = Severity.ERROR if n_blocking else Severity.WARN
        report.summary = (
            f"{n_rewritten} backfilled; {n_already_ok} already valid; "
            f"{n_blocking} unfixable; {n_errors} fix errors"
        )
    else:
        report.severity = Severity.INFO
        report.summary = (
            f"{n_rewritten} backfilled; {n_already_ok} already valid"
        )
    return report


register('geoparquet_bbox', 'partition parquets without valid bbox metadata',
         scope='global', fix=geoparquet_bbox_fix)(geoparquet_bbox_check)
