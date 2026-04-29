"""orphans diagnosis — leftover temp files and empty partition directories.

Detects:
  - ``*.tmp``, ``*.join.tmp``, ``*.fill.tmp``, ``*.dedup.tmp`` under db / tmp roots.
  - ``_s3_download/`` and ``dask-worker-space/`` directories left behind by
    crashed builds.
  - Empty ``h3_*/`` partition directories (no parquet files).
  - Empty year subdirs.

A finding is only flagged when the file/dir's mtime is older than
``--orphan-age-hours`` (default 24h) so an in-progress build isn't disturbed.
"""

from __future__ import annotations

import glob
import os
import shutil
import time

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ...cliutils import progress_iter


_TMP_PATTERNS = ('*.tmp', '*.join.tmp', '*.fill.tmp', '*.dedup.tmp')
_LEFTOVER_DIRS = ('_s3_download', 'dask-worker-space')


def _scan_orphans(roots, age_seconds: float):
    now = time.time()
    found_files = []
    found_dirs = []

    seen_files = set()
    seen_dirs = set()
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for pat in _TMP_PATTERNS:
            for f in glob.glob(os.path.join(root, '**', pat), recursive=True):
                if f in seen_files:
                    continue
                seen_files.add(f)
                try:
                    age = now - os.path.getmtime(f)
                except OSError:
                    continue
                if age >= age_seconds:
                    found_files.append({'path': f, 'age_seconds': int(age), 'kind': 'temp_file'})
        for leftover in _LEFTOVER_DIRS:
            for d in glob.glob(os.path.join(root, '**', leftover), recursive=True):
                if d in seen_dirs:
                    continue
                seen_dirs.add(d)
                try:
                    age = now - os.path.getmtime(d)
                except OSError:
                    continue
                if age >= age_seconds:
                    found_dirs.append({'path': d, 'age_seconds': int(age), 'kind': 'leftover_dir'})

    return found_files, found_dirs


def _empty_partition_dirs(partition_dirs, args=None):
    empty = []
    with progress_iter(partition_dirs,
                       desc="orphans: scanning partitions",
                       args=args, unit="part") as bar:
        for d in bar:
            # A partition is empty if it has no parquet files at any depth.
            if not glob.glob(os.path.join(d, '**', '*.parquet'), recursive=True):
                empty.append({'path': d, 'kind': 'empty_partition'})
    return empty


def _empty_year_dirs(partition_dirs, args=None):
    empty = []
    with progress_iter(partition_dirs,
                       desc="orphans: scanning year subdirs",
                       args=args, unit="part") as bar:
        for d in bar:
            for year_dir in glob.glob(os.path.join(d, '*/')):
                if os.path.isdir(year_dir) and not glob.glob(os.path.join(year_dir, '*.parquet')):
                    empty.append({'path': year_dir, 'kind': 'empty_year_dir'})
    return empty


def orphans_check(ctx: DoctorContext) -> Report:
    age_hours = getattr(ctx.args, 'orphan_age_hours', 24.0)
    age_seconds = age_hours * 3600

    files, dirs = _scan_orphans([ctx.h3_dir, ctx.tmp_dir], age_seconds)
    args = getattr(ctx, 'args', None)
    empties = _empty_partition_dirs(ctx.partition_dirs, args=args)
    empties.extend(_empty_year_dirs(ctx.partition_dirs, args=args))

    findings = files + dirs + empties
    summary = f"{len(files)} temp files, {len(dirs)} leftover dirs, {len(empties)} empty partition/year dirs"
    severity = Severity.INFO if not findings else Severity.WARN
    return Report(name='orphans', severity=severity, findings=findings, summary=summary)


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

    return report


register('orphans', 'leftover temp files + empty partition dirs',
         scope='global', fix=orphans_fix)(orphans_check)
