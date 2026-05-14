"""tmp_partitions_health diagnosis — post-build forensics on ``tmp/partitions/``.

Audit-only check that NEVER iterates healthy partitions (Pillar 1 — no
driver-side O(N) GPFS scans). It acts strictly on the signals the build
already persisted:

  * ``tmp_dir/_merge_failures/*.fail`` sentinels  (one per failed merge)
  * ``tmp_dir/_granule_failures.jsonl``           (one record per failed granule)
  * ``tmp_dir/_merge_progress.txt`` vs. the DB ``_manifest.txt`` line count

The cost of the check is O(N_failures) + O(1) on the manifest — never
O(N_partitions). All work runs on the driver because the inputs are
narrow (a single sentinel directory + two flat files).

The ``--fix`` variant calls :func:`gh3builder.preclean_merge_failures` to
remove zero-byte / unreadable parquets + ``.tmp`` siblings under every
failed-merge partition. Granule failures are reported only — recovery
belongs in ``gh3_update --recover-missing-vars``. As a hard guard, the
fix refuses to act when a fresh ``gh3_build`` process appears to be
running (recent ``gh3_build.log`` mtime + matching pgrep PID).
"""

from __future__ import annotations

import os
import subprocess
import time
from collections import defaultdict
from typing import List

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ... import gh3builder


# Tolerance for the progress vs. manifest drift check. The merge progress
# file is append-only and can lag the manifest by a few entries during a
# concurrent run; we only flag a meaningful gap.
_DRIFT_TOLERANCE = 5

# Build-active guard: a fresh log + a live pgrep hit means a build is
# concurrently writing tmp/partitions/ and any --fix would race the writer.
_BUILD_LOG_FRESH_SECONDS = 60.0


def _count_manifest_entries(h3_dir: str) -> int:
    """O(1) line count of the DB's ``_manifest.txt`` sentinel.

    Uses :func:`gedih3.utils._read_manifest` when available so the manifest
    cache is shared with the rest of the loader; falls back to a direct
    line-count on the file when the helper is missing or returns None.
    """
    try:
        from ...utils import _read_manifest
        lines = _read_manifest(h3_dir)
        if lines is not None:
            return len(lines)
    except Exception:
        pass

    from ...config import MANIFEST_FILENAME
    manifest_path = os.path.join(h3_dir, MANIFEST_FILENAME)
    if not os.path.isfile(manifest_path):
        return 0
    try:
        with open(manifest_path, 'r') as f:
            return sum(1 for line in f if line.strip())
    except OSError:
        return 0


def _count_merge_progress_entries(tmp_dir: str) -> int:
    """O(1) line count of ``_merge_progress.txt`` under tmp_dir."""
    path = os.path.join(tmp_dir, '_merge_progress.txt')
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, 'r') as f:
            return sum(1 for line in f if line.strip())
    except OSError:
        return 0


def _build_is_active(h3_dir: str, tmp_dir: str):
    """Return ``(active: bool, info: dict)``.

    Cheap signal: a ``gh3_build.log`` whose mtime is < 60 s old AND a
    matching ``pgrep -f gh3_build`` hit. Run only at fix time. False
    negative (build with stale log) is acceptable — the user can re-run
    after the build settles.
    """
    candidates = [
        os.path.join(h3_dir, 'gh3_build.log'),
        os.path.join(os.path.dirname(h3_dir.rstrip('/')), 'gh3_build.log'),
        os.path.join(tmp_dir, 'gh3_build.log') if tmp_dir else None,
    ]
    fresh_log = None
    for c in candidates:
        if not c or not os.path.isfile(c):
            continue
        try:
            age = time.time() - os.path.getmtime(c)
        except OSError:
            continue
        if age < _BUILD_LOG_FRESH_SECONDS:
            fresh_log = c
            break

    if fresh_log is None:
        return False, {}

    try:
        res = subprocess.run(
            ['pgrep', '-f', 'gh3_build'],
            capture_output=True, text=True, timeout=5,
        )
        pids = [p for p in res.stdout.split() if p.strip()]
    except (OSError, subprocess.SubprocessError):
        pids = []

    if pids:
        return True, {'log': fresh_log, 'pid': int(pids[0])}
    return False, {}


def tmp_partitions_health_check(ctx: DoctorContext) -> Report:
    findings: List[dict] = []
    tmp_dir = getattr(ctx, 'tmp_dir', None)

    if not tmp_dir or not os.path.isdir(tmp_dir):
        return Report(
            name='tmp_partitions_health',
            severity=Severity.INFO,
            findings=[],
            summary='no tmp_dir present (one-shot build); nothing to audit',
        )

    # 1. Merge-failure sentinels — one finding per recorded failure.
    sentinels = gh3builder._scan_merge_failure_sentinels(tmp_dir)
    for partition_dir, err in sentinels.items():
        findings.append({
            'kind': 'merge_failure',
            'partition_dir': partition_dir,
            'error': err,
        })

    # 2. Granule failures — grouped by ``kind``; up to 5 examples per group.
    granule_records = gh3builder._read_granule_failures(tmp_dir)
    by_kind: dict = defaultdict(list)
    for rec in granule_records:
        by_kind[rec.get('kind', 'unknown')].append(rec)
    for kind, recs in by_kind.items():
        findings.append({
            'kind': 'granule_failures',
            'failure_kind': kind,
            'count': len(recs),
            'examples': recs[:5],
        })

    # 3. Progress vs. manifest drift — O(1) on both ends.
    progress_count = _count_merge_progress_entries(tmp_dir)
    manifest_count = _count_manifest_entries(ctx.h3_dir)
    if progress_count > manifest_count + _DRIFT_TOLERANCE:
        findings.append({
            'kind': 'progress_manifest_drift',
            'progress_count': progress_count,
            'manifest_count': manifest_count,
        })

    n_merge = sum(1 for f in findings if f['kind'] == 'merge_failure')
    n_gran_groups = sum(1 for f in findings if f['kind'] == 'granule_failures')
    n_gran_records = sum(f['count'] for f in findings if f['kind'] == 'granule_failures')
    n_drift = sum(1 for f in findings if f['kind'] == 'progress_manifest_drift')

    summary = (
        f"{n_merge} failed merges, "
        f"{n_gran_records} granule failures across {n_gran_groups} kind(s), "
        f"{n_drift} drift finding(s)"
    )
    severity = Severity.WARN if findings else Severity.INFO
    return Report(
        name='tmp_partitions_health',
        severity=severity,
        findings=findings,
        summary=summary,
    )


def tmp_partitions_health_fix(ctx: DoctorContext, report: Report) -> Report:
    tmp_dir = getattr(ctx, 'tmp_dir', None)
    if not tmp_dir or not os.path.isdir(tmp_dir):
        report.applied = True
        report.severity = Severity.INFO
        report.summary = 'no tmp_dir present; nothing to fix'
        return report

    # Build-active guard — refuse to mutate state while a build is writing.
    active, info = _build_is_active(ctx.h3_dir, tmp_dir)
    if active:
        report.applied = True
        report.severity = Severity.ERROR
        report.findings = [{
            'kind': 'build_active',
            'pid': info.get('pid'),
            'log': info.get('log'),
            'message': 'refusing to fix while gh3_build appears to be running',
        }]
        report.summary = f"build active (pid={info.get('pid')}); fix skipped"
        return report

    new_findings: List[dict] = []
    preclean_done = False
    preclean_stats: dict = {}

    for f in report.findings:
        kind = f.get('kind')
        if kind == 'merge_failure':
            if not preclean_done:
                try:
                    preclean_stats = gh3builder.preclean_merge_failures(tmp_dir)
                    preclean_done = True
                except Exception as e:
                    new_findings.append({
                        **f,
                        'fix_error': f"{type(e).__name__}: {e}",
                    })
                    continue
            new_findings.append({
                **f,
                'action': 'preclean_merge_failures',
                'preclean_stats': preclean_stats,
            })
        elif kind == 'granule_failures':
            new_findings.append({
                **f,
                'action': 'reported_only',
                'recommendation': (
                    'gh3_update --recover-missing-vars '
                    f"(failure_kind={f.get('failure_kind')})"
                ),
            })
        elif kind == 'progress_manifest_drift':
            new_findings.append({
                **f,
                'action': 'reported_only',
                'recommendation': 'gh3_doctor --fix metadata',
            })
        else:
            new_findings.append(f)

    report.applied = True
    report.findings = new_findings
    n_errors = sum(1 for x in new_findings if 'fix_error' in x)
    if n_errors:
        report.severity = Severity.ERROR
        report.summary = f"{report.summary} | {n_errors} fix error(s)"
    else:
        # WARN remains when granule_failures / drift are reported-only;
        # downgrade to INFO only when every finding had an action that
        # actually resolved something on disk.
        any_unresolved = any(
            x.get('action') == 'reported_only' for x in new_findings
        )
        if preclean_done and not any_unresolved:
            report.severity = Severity.INFO
            report.summary = (
                f"precleaned {preclean_stats.get('partitions_cleaned', 0)} partitions: "
                f"{preclean_stats.get('parquets_removed', 0)} parquets, "
                f"{preclean_stats.get('tmps_removed', 0)} tmps removed"
            )
        elif preclean_done:
            report.severity = Severity.WARN
            report.summary = (
                f"precleaned {preclean_stats.get('partitions_cleaned', 0)} partitions; "
                f"granule/drift findings reported for follow-up"
            )

    return report


register(
    'tmp_partitions_health',
    'tmp/partitions/ post-build forensics (merge failures + granule failures + progress drift)',
    scope='global',
    fix=tmp_partitions_health_fix,
)(tmp_partitions_health_check)
