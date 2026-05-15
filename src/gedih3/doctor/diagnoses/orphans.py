"""orphans diagnosis — leftover temp files and empty partition directories.

Detects:
  - ``*.tmp``, ``*.join.tmp``, ``*.fill.tmp``, ``*.dedup.tmp`` under db / tmp roots.
  - ``_s3_download/`` and ``dask-worker-space/`` directories left behind by
    crashed builds.
  - Empty ``h3_*/`` partition directories (no parquet files).
  - Empty year subdirs.

A finding is only flagged when the file/dir's mtime is older than
``--orphan-age-hours`` (default 24h) so an in-progress build isn't disturbed.

Performance pillar (v0.8.x lessons backport):
  * The temp-file/leftover-dir scan over the database root is pushed
    to workers via :func:`gedih3.doctor.parallel.parallel_map` — one
    task per ``h3_*`` partition. Mirrors the build merge phase's
    ``_list_year_subdirs`` pattern (gh3builder.py:906-924) which
    eliminated the same driver-side recursive walk on continental builds.
  * Empty-partition / empty-year-dir checks are O(1) ``os.scandir``
    instead of ``glob.glob('**/*.parquet', recursive=True)`` (matches
    the build pipeline's emptiness check at gh3builder.py:1356-1362).
  * The temp directory's scan stays driver-side because it's narrow
    and not nested as deeply as the database tree.
"""

from __future__ import annotations

import glob
import os
import shutil
import time
from typing import List

from typing import Optional

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..parallel import parallel_map, partition_is_empty, list_year_dirs, year_dir_is_empty
from ..fused import register_scan


_TMP_PATTERNS = ('*.tmp', '*.join.tmp', '*.fill.tmp', '*.dedup.tmp')
_LEFTOVER_DIRS = ('_s3_download', 'dask-worker-space')


def _scan_partition_orphans(partition_dir: str, age_seconds: float) -> List[dict]:
    """Worker: list orphan temp files / leftover dirs under one partition.

    Uses local recursive globs *bounded to the partition subtree*; there's
    no driver-side aggregation, and the scope is small enough that one
    glob per partition per pattern is fine. The point is the parallel
    fan-out across partitions, not eliminating recursion within one.
    """
    now = time.time()
    found: List[dict] = []

    for pat in _TMP_PATTERNS:
        for f in glob.glob(os.path.join(partition_dir, '**', pat), recursive=True):
            try:
                age = now - os.path.getmtime(f)
            except OSError:
                continue
            if age >= age_seconds:
                found.append({'path': f, 'age_seconds': int(age), 'kind': 'temp_file'})

    for leftover in _LEFTOVER_DIRS:
        for d in glob.glob(os.path.join(partition_dir, '**', leftover), recursive=True):
            try:
                age = now - os.path.getmtime(d)
            except OSError:
                continue
            if age >= age_seconds:
                found.append({'path': d, 'age_seconds': int(age), 'kind': 'leftover_dir'})

    return found


def _scan_root_orphans(root: str, age_seconds: float) -> List[dict]:
    """Driver-side scan for narrow auxiliary roots (typically the tmp_dir).

    Only used for paths *outside* the partition tree (e.g. the user's
    configured tmp directory). The h3_dir's per-partition scan runs
    through :func:`parallel_map` instead, which keeps the metadata-server
    load distributed.
    """
    if not root or not os.path.isdir(root):
        return []
    now = time.time()
    seen: set = set()
    found: List[dict] = []

    for pat in _TMP_PATTERNS:
        for f in glob.glob(os.path.join(root, '**', pat), recursive=True):
            if f in seen:
                continue
            seen.add(f)
            try:
                age = now - os.path.getmtime(f)
            except OSError:
                continue
            if age >= age_seconds:
                found.append({'path': f, 'age_seconds': int(age), 'kind': 'temp_file'})

    for leftover in _LEFTOVER_DIRS:
        for d in glob.glob(os.path.join(root, '**', leftover), recursive=True):
            if d in seen:
                continue
            seen.add(d)
            try:
                age = now - os.path.getmtime(d)
            except OSError:
                continue
            if age >= age_seconds:
                found.append({'path': d, 'age_seconds': int(age), 'kind': 'leftover_dir'})

    return found


def _empty_check_partition(partition_dir: str) -> List[dict]:
    """Worker: O(1) emptiness checks for one partition + its year subdirs."""
    findings: List[dict] = []
    if partition_is_empty(partition_dir):
        findings.append({'path': partition_dir, 'kind': 'empty_partition'})
        # If the partition itself is empty there are no year subdirs to enumerate.
        return findings
    for year_dir in list_year_dirs(partition_dir):
        if year_dir_is_empty(year_dir):
            findings.append({'path': year_dir, 'kind': 'empty_year_dir'})
    return findings


def _scan_partition_orphans_combined(
    partition_dir: str,
    *,
    age_seconds: float,
    shared: Optional[dict] = None,
) -> List[dict]:
    """Fused worker: runs both the tmp/leftover scan and the emptiness
    check in one task per partition, returning the merged finding list.

    ``shared`` is accepted for fused-dispatch API symmetry but not
    consumed — orphans' file/dir globs are recursive within the partition
    subtree and don't overlap with the parquet listing other diagnoses
    cache.
    """
    findings = _scan_partition_orphans(partition_dir, age_seconds)
    findings.extend(_empty_check_partition(partition_dir))
    return findings


register_scan('orphans', _scan_partition_orphans_combined)


def _finalize_orphans_check(
    ctx: DoctorContext,
    per_partition: dict,
    age_seconds: float,
) -> Report:
    """Driver-side aggregation: per-partition findings + tmp_dir scan + dedupe."""
    findings: List[dict] = []
    seen_paths: set = set()

    def _add(items):
        for it in items:
            p = it.get('path')
            if p in seen_paths:
                continue
            seen_paths.add(p)
            findings.append(it)

    for part_dir, result in per_partition.items():
        if isinstance(result, Exception):
            continue
        _add(result)

    # Auxiliary tmp_dir (narrow, driver-side scan is fine).
    _add(_scan_root_orphans(ctx.tmp_dir, age_seconds))

    n_files = sum(1 for f in findings if f['kind'] == 'temp_file')
    n_dirs = sum(1 for f in findings if f['kind'] == 'leftover_dir')
    n_empty = sum(1 for f in findings if f['kind'] in ('empty_partition', 'empty_year_dir'))

    summary = f"{n_files} temp files, {n_dirs} leftover dirs, {n_empty} empty partition/year dirs"
    severity = Severity.INFO if not findings else Severity.WARN
    return Report(name='orphans', severity=severity, findings=findings, summary=summary)


def orphans_check(ctx: DoctorContext) -> Report:
    age_hours = getattr(ctx.args, 'orphan_age_hours', 24.0)
    age_seconds = age_hours * 3600
    args = getattr(ctx, 'args', None)

    per_partition: dict = {}
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition_orphans_combined,
        args=args,
        desc='orphans: scanning partitions',
        unit='part',
        age_seconds=age_seconds,
    ):
        per_partition[part_dir] = result

    return _finalize_orphans_check(ctx, per_partition, age_seconds)


def orphans_fix(ctx: DoctorContext, report: Report) -> Report:
    fixed = []
    for f in report.findings:
        path = f['path']
        kind = f['kind']
        try:
            if kind in ('temp_file',):
                if os.path.isfile(path):
                    os.unlink(path)
                    fixed.append({**f, 'action': 'deleted'})
            elif kind in ('leftover_dir', 'empty_partition', 'empty_year_dir'):
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=False)
                    fixed.append({**f, 'action': 'removed'})
        except Exception as e:
            fixed.append({**f, 'fix_error': f"{type(e).__name__}: {e}"})

    report.applied = True
    report.findings = fixed
    n_errors = sum(1 for x in fixed if 'fix_error' in x)
    if n_errors:
        report.severity = Severity.ERROR
        report.summary = f"cleaned {len(fixed) - n_errors}; failed {n_errors}"
    else:
        report.severity = Severity.INFO
        report.summary = f"cleaned {len(fixed)} orphan items"

    # If any partition dirs were removed, prune them from ctx so subsequent
    # diagnoses don't try to inspect them.
    removed_partitions = {x['path'].rstrip('/').rstrip(os.sep) for x in fixed if x.get('kind') == 'empty_partition'}
    if removed_partitions:
        ctx.partition_dirs = [d for d in ctx.partition_dirs if d.rstrip('/').rstrip(os.sep) not in removed_partitions]

    # R2 producer-driven refresh: if any partition dirs or files were
    # removed, regenerate the H3 manifest so future consumers don't
    # see ghost entries pointing at deleted paths.
    removed_anything = any(
        x.get('action') in ('deleted', 'removed') for x in fixed
    )
    if removed_anything:
        from ...utils import generate_manifest
        try:
            generate_manifest(ctx.h3_dir, tree_shape='h3db')
        except Exception as e:
            # Non-fatal: log but don't fail the fix report. The smoke
            # check at next consumer entry will surface the stale state
            # loudly.
            from ...logging_config import get_logger
            get_logger(__name__).warning(
                f"orphans_fix: manifest regeneration failed: "
                f"{type(e).__name__}: {e}"
            )

    return report


register('orphans', 'leftover temp files + empty partition dirs',
         scope='global', fix=orphans_fix)(orphans_check)
