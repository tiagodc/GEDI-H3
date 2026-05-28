"""soc_health diagnosis — invalid HDF5 files + download log drift.

Detects:
  - SOC HDF5 files that fail :func:`h5_is_valid`.
  - Download log entries marked DOWNLOADED but with the file missing or empty.

Remedies (safe):
  - Mark drifted entries FAILED so the next ``gh3_download --resume`` retries.
  - Invalid HDF5 files are reported only (could be in-progress); deletion is
    not auto-applied (see ``--delete-invalid`` future flag).

Performance pillar (v0.8.x lessons backport):
  * SOC file enumeration always walks the tree in parallel via
    :func:`gedih3.parallel.walk_soc_parallel`. The legacy
    ``_soc_manifest.txt`` shortcut was removed from the read path
    because external population (manual rsync, NASA delivery) bypasses
    the producer-driven refresh and silently narrows the doctor's view.
  * Per-file ``h5_is_valid`` checks are dispatched in parallel via
    :func:`gedih3.doctor.parallel.parallel_map` when a dask client is
    registered.
"""

from __future__ import annotations

import os

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..parallel import parallel_map


def _check_h5_file(path: str) -> dict:
    """Worker: validate one HDF5; return {'valid': bool, 'size': int, 'err': str|None}."""
    from ...utils import h5_is_valid
    try:
        valid = h5_is_valid(path)
        err = None if valid else 'not a valid GEDI HDF5'
    except Exception as e:
        valid = False
        err = f"{type(e).__name__}: {e}"
    try:
        size = os.path.getsize(path)
    except OSError:
        size = -1
    return {'valid': valid, 'size': size, 'err': err}


def _enumerate_soc_files(soc_dir: str) -> list:
    """Return every ``GEDI*.h5`` path under *soc_dir*.

    Always walks the tree in parallel via the registered dask Client.
    We intentionally do not consume ``_soc_manifest.txt`` here:
    external population paths (manual rsync, NASA delivery) bypass the
    producer-driven refresh and would leave the manifest stale relative
    to disk, silently narrowing the doctor's view. Returns the *full*
    file list — unlike :func:`soc_file_tree(..., to_list=True)`, which
    pivots by orbit/track and ``dropna()``s rows where any product is
    absent, silently excluding partial-download granules from
    downstream scans.
    """
    from ...parallel import walk_soc_parallel
    return walk_soc_parallel(soc_dir, pattern='GEDI*.h5')


def soc_health_check(ctx: DoctorContext) -> Report:
    findings = []

    if not ctx.soc_dir or not os.path.isdir(ctx.soc_dir):
        return Report(
            name='soc_health', severity=Severity.INFO,
            summary='no SOC directory configured; skipping',
        )

    soc_files = _enumerate_soc_files(ctx.soc_dir)

    # Per-file ``h5_is_valid`` is sub-second on a healthy file, so a
    # continental SOC tree (millions of files) would dispatch millions
    # of one-task-per-file dask futures — task-graph build and
    # scheduler overhead would dominate, and the cluster would barely
    # touch real work. Batch into chunks of 1000 so we ship at most a
    # few thousand tasks even on the largest trees.
    for f, result in parallel_map(
        soc_files,
        _check_h5_file,
        args=getattr(ctx, 'args', None),
        desc='soc_health: scanning HDF5 files',
        unit='file',
        batch_size=1000,
    ):
        if isinstance(result, Exception):
            findings.append({
                'kind': 'invalid_h5', 'path': f, 'size_bytes': -1,
                'error': f"{type(result).__name__}: {result}",
            })
            continue
        if not result['valid']:
            findings.append({
                'kind': 'invalid_h5', 'path': f,
                'size_bytes': result['size'], 'error': result['err'],
            })

    # Cross-check the download log against disk.
    try:
        from ...logger import SOCDownloadLogger
        soc_log = SOCDownloadLogger(product_vars=None, dir=ctx.soc_dir)
        ctx.soc_logger = soc_log
    except Exception as e:
        findings.append({'kind': 'download_log_unreadable', 'error': f"{type(e).__name__}: {e}"})
        soc_log = None

    if soc_log is not None and getattr(soc_log, 'granule_info', None):
        for g in soc_log.granule_info:
            if g.get('status') != 'DOWNLOADED':
                continue
            # Granule entry doesn't directly name a file path; we infer per-product via SOC tree.
            # If any product's expected file is missing, flag drift.
            # A more thorough check would require the granule's file_path field.
            file_path = g.get('file_path')
            if file_path and (not os.path.exists(file_path) or os.path.getsize(file_path) == 0):
                findings.append({
                    'kind': 'download_log_drift',
                    'granule': {'orbit': g.get('orbit'), 'granule': g.get('granule'), 'track': g.get('track')},
                    'file_path': file_path,
                })

    n_invalid = sum(1 for f in findings if f['kind'] == 'invalid_h5')
    n_drift = sum(1 for f in findings if f['kind'] == 'download_log_drift')

    severity = Severity.INFO
    if n_invalid:
        severity = Severity.WARN
    if any(f['kind'] == 'download_log_unreadable' for f in findings):
        severity = Severity.ERROR

    summary = f"{n_invalid} invalid HDF5 files; {n_drift} download-log drift entries"
    return Report(name='soc_health', severity=severity, findings=findings, summary=summary)


def soc_health_fix(ctx: DoctorContext, report: Report) -> Report:
    fixed = []
    soc_log = getattr(ctx, 'soc_logger', None)
    for f in report.findings:
        kind = f.get('kind')
        if kind == 'download_log_drift' and soc_log is not None:
            try:
                soc_log.update_granule_status(f['granule'], 'FAILED')
                fixed.append({**f, 'action': 'marked_FAILED_for_retry'})
            except Exception as e:
                fixed.append({**f, 'fix_error': f"{type(e).__name__}: {e}"})
        else:
            # invalid_h5 and download_log_unreadable: no auto-fix
            fixed.append({**f, 'action': 'reported_only'})

    if soc_log is not None:
        try:
            soc_log.save_log('UNKNOWN')
        except Exception:
            pass

    n_errors = sum(1 for x in fixed if 'fix_error' in x)
    report.applied = True
    report.findings = fixed
    if n_errors:
        report.severity = Severity.ERROR
    report.summary = f"{sum(1 for x in fixed if x.get('action','').startswith('marked_'))} download entries marked for retry; {n_errors} errors"
    return report


register('soc_health', 'invalid HDF5 + download log drift',
         scope='global', fix=soc_health_fix)(soc_health_check)
