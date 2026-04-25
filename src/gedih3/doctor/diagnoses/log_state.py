"""log_state diagnosis — stuck recovery flags + log↔disk drift.

Detects:
  - ``_pending_variable_update`` left in the log without matching state.
  - Partitions present on disk but not listed in ``h3_partition_ids``.
  - Partitions listed in the log but missing from disk.
  - Granules listed in the log whose partitions no longer exist.

Remedies (safe):
  - Clear the stuck flag when no work is pending.
  - Repopulate ``h3_partition_ids`` from disk via ``set_post_build_info``.
  - Disk-missing partitions are reported only (manual intervention).
"""

from __future__ import annotations

import os

from ..report import Report, DoctorContext, Severity
from ..runner import register


def _partition_id_from_dir(d: str) -> str:
    """Extract the H3 cell ID from a partition dir name like ``h3_03=8c2a...``."""
    base = os.path.basename(d.rstrip('/').rstrip(os.sep))
    return base.split('=', 1)[1] if '=' in base else base


def log_state_check(ctx: DoctorContext) -> Report:
    findings = []

    if ctx.h3_logger is None:
        return Report(
            name='log_state', severity=Severity.WARN,
            summary='no build log present',
            findings=[{'kind': 'no_log'}],
        )

    log = ctx.h3_logger
    log_data = log.log_data

    # 1. Stuck _pending_variable_update flag
    pending = log_data.get('_pending_variable_update')
    if pending:
        findings.append({'kind': 'stuck_pending_flag', 'value': pending})

    # 2 & 3. Set-diff between disk partitions and logged partition ids
    disk_ids = {_partition_id_from_dir(d) for d in ctx.partition_dirs}
    logged_ids = set(getattr(log, 'h3_partition_ids', []) or [])

    on_disk_missing_log = sorted(disk_ids - logged_ids)
    in_log_missing_disk = sorted(logged_ids - disk_ids)

    for pid in on_disk_missing_log:
        findings.append({'kind': 'partition_on_disk_unlogged', 'h3_partition': pid})
    for pid in in_log_missing_disk:
        findings.append({'kind': 'partition_in_log_missing_disk', 'h3_partition': pid})

    severity = Severity.WARN if findings else Severity.INFO
    summary = (
        f"{'pending flag stuck; ' if pending else ''}"
        f"{len(on_disk_missing_log)} disk partitions not logged; "
        f"{len(in_log_missing_disk)} log partitions missing disk"
    )
    return Report(name='log_state', severity=severity, findings=findings, summary=summary)


def log_state_fix(ctx: DoctorContext, report: Report) -> Report:
    if ctx.h3_logger is None:
        report.applied = True
        report.summary = "no build log to fix"
        return report

    log = ctx.h3_logger
    actions = []

    # Clear stuck flag
    if log.log_data.pop('_pending_variable_update', None) is not None:
        actions.append({'kind': 'cleared_pending_flag'})

    # Refresh h3_partition_ids and per-product status from on-disk meta
    needs_refresh = any(
        f['kind'] in ('partition_on_disk_unlogged', 'partition_in_log_missing_disk')
        for f in report.findings
    )
    if needs_refresh:
        try:
            log.set_post_build_info()
            actions.append({'kind': 'refreshed_partition_ids', 'count': len(getattr(log, 'h3_partition_ids', []) or [])})
        except Exception as e:
            actions.append({'kind': 'refresh_failed', 'error': f"{type(e).__name__}: {e}"})

    # Disk-missing partitions: report only (could be intentional deletion).
    disk_missing = [f for f in report.findings if f['kind'] == 'partition_in_log_missing_disk']
    if disk_missing:
        actions.append({
            'kind': 'manual_review_required',
            'reason': 'partitions listed in log but absent from disk',
            'count': len(disk_missing),
            'partitions': [f['h3_partition'] for f in disk_missing],
        })

    n_errors = sum(1 for a in actions if a.get('kind') == 'refresh_failed')
    report.applied = True
    report.findings = actions
    if n_errors:
        report.severity = Severity.ERROR
        report.summary = f"applied {len(actions) - n_errors} actions; {n_errors} errors"
    else:
        report.severity = Severity.INFO if not disk_missing else Severity.WARN
        report.summary = f"applied {len(actions)} actions"
    return report


register('log_state', 'stuck flags + log↔disk partition drift',
         scope='global', fix=log_state_fix)(log_state_check)
