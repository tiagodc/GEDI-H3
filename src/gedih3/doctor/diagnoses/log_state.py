"""log_state diagnosis — stuck recovery flags + log↔disk drift.

Detects:
  - ``_pending_variable_update`` left in the log without matching state.
  - Partitions present on disk but not listed in ``h3_partition_ids``.
  - Partitions listed in the log but missing from disk.
  - Granule status drift — granules in ``granule_info`` with a non-INDEXED
    status whose ``(orbit, granule, track)`` is present in a finalized
    partition's metadata JSON. Caused by the merge-only resume shortcut
    (gh3_build v0.8.9+), which skips reconcile and leaves stale PENDING
    statuses behind even though the data is already on disk.

Remedies (safe):
  - Clear the stuck flag when no work is pending.
  - Repopulate ``h3_partition_ids`` from disk via ``set_post_build_info``.
  - Disk-missing partitions are reported only (manual intervention).
  - Granule status drift: flip stale entries to ``INDEXED`` and save log.
"""

from __future__ import annotations

import glob
import json
import os

from ...config import PARTITION_META_FILENAME
from ..report import Report, DoctorContext, Severity
from ..runner import register


def _indexed_granule_triples_on_disk(h3_dir: str) -> set:
    """Return the set of ``(orbit, granule, track)`` granule IDs found in any
    finalized partition's metadata JSON under ``h3_dir``. Cheap, no parquet
    reads — same logic as Pass A of ``_reconcile_granules_from_disk``.
    """
    triples: set = set()
    if not h3_dir or not os.path.isdir(h3_dir):
        return triples
    meta_files = glob.glob(os.path.join(h3_dir, 'h3_*', f'*{PARTITION_META_FILENAME}'))
    meta_files += glob.glob(os.path.join(h3_dir, 'h3_*', '*', f'*{PARTITION_META_FILENAME}'))
    for mf in meta_files:
        try:
            with open(mf, 'r') as fh:
                data = json.load(fh) or {}
            for g in data.get('granules', []):
                triples.add((g['orbit'], g['granule'], g['track']))
        except Exception:
            continue
    return triples


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

    # 4. Granule status drift — granule_info has non-INDEXED entries whose
    # data is already in a finalized partition. Common after a merge-only
    # resume (gh3_build 0.8.9+ shortcut) which skips reconcile.
    granule_info = getattr(log, 'granule_info', None) or []
    drift_triples = []
    if granule_info:
        on_disk = _indexed_granule_triples_on_disk(ctx.h3_dir)
        for g in granule_info:
            if g.get('status') == 'INDEXED':
                continue
            try:
                triple = (g['orbit'], g['granule'], g['track'])
            except (KeyError, TypeError):
                continue
            if triple in on_disk:
                drift_triples.append(triple)
    for tr in drift_triples:
        findings.append({
            'kind': 'granule_status_drift',
            'orbit': tr[0], 'granule': tr[1], 'track': tr[2],
        })

    severity = Severity.WARN if findings else Severity.INFO
    summary = (
        f"{'pending flag stuck; ' if pending else ''}"
        f"{len(on_disk_missing_log)} disk partitions not logged; "
        f"{len(in_log_missing_disk)} log partitions missing disk; "
        f"{len(drift_triples)} granule status drift"
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

    # Granule status drift — flip stale entries to INDEXED.
    drift_findings = [f for f in report.findings if f['kind'] == 'granule_status_drift']
    if drift_findings:
        drift_keys = {(f['orbit'], f['granule'], f['track']) for f in drift_findings}
        flipped = 0
        for g in (log.granule_info or []):
            try:
                triple = (g['orbit'], g['granule'], g['track'])
            except (KeyError, TypeError):
                continue
            if triple in drift_keys and g.get('status') != 'INDEXED':
                g['status'] = 'INDEXED'
                flipped += 1
        if flipped:
            actions.append({'kind': 'flipped_granule_status', 'count': flipped})
            try:
                # Preserve current top-level log status (e.g. COMPLETED) rather
                # than rewriting it; just persist the granule_info edits.
                log.save_log(log.log_data.get('status', 'COMPLETED'))
            except Exception as e:
                actions.append({
                    'kind': 'log_save_failed',
                    'error': f"{type(e).__name__}: {e}",
                })

    n_errors = sum(1 for a in actions if a.get('kind') in ('refresh_failed', 'log_save_failed'))
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
