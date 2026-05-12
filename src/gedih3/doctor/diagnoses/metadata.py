"""metadata diagnosis — partition meta JSON + dataset manifest health.

Detects:
  - Partitions whose merged meta JSON file is missing.
  - A stale or absent dataset-level Parquet ``_metadata`` manifest.

Remedies (safe):
  - Regenerate missing partition meta via ``h3_merge_metadata``.
  - Regenerate manifest via ``generate_manifest``.

Performance pillar (v0.8.x lessons backport):
  * Both the missing-meta check and the newest-parquet-mtime walk are
    pushed to workers via :func:`gedih3.doctor.parallel.parallel_map`.
    The driver receives a single bool / float per partition and
    aggregates — no driver-side recursive glob over h3_*//*/*.parquet
    on continental DBs.
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_meta_file, partition_parquet_files
from ..parallel import parallel_map


def _missing_meta_for_partition(partition_dir: str) -> bool:
    """Worker: True iff the partition has no merged meta JSON."""
    return partition_meta_file(partition_dir) is None


def _newest_mtime_in_partition(partition_dir: str) -> float:
    """Worker: max mtime across the partition's parquet files (0 if none)."""
    newest = 0.0
    for f in partition_parquet_files(partition_dir):
        try:
            m = os.path.getmtime(f)
        except OSError:
            continue
        if m > newest:
            newest = m
    return newest


def _missing_partition_meta(ctx: DoctorContext):
    missing = []
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _missing_meta_for_partition,
        args=getattr(ctx, 'args', None),
        desc='metadata: scanning partitions',
        unit='part',
    ):
        if isinstance(result, Exception):
            # Treat read failure as "missing" — the fix step will retry.
            missing.append(part_dir)
        elif result:
            missing.append(part_dir)
    return missing


def _manifest_stale(ctx: DoctorContext) -> Tuple[Optional[str], str, Optional[float]]:
    """Return ('missing'|'stale'|None, manifest_path, newest_partition_mtime).

    The newest-parquet sweep is parallelized across partitions.
    """
    manifest = os.path.join(ctx.h3_dir, '_metadata')
    if not os.path.exists(manifest):
        return 'missing', manifest, None
    manifest_mtime = os.path.getmtime(manifest)

    newest = manifest_mtime
    for _, result in parallel_map(
        ctx.partition_dirs,
        _newest_mtime_in_partition,
        args=getattr(ctx, 'args', None),
        desc='metadata: collecting partition mtimes',
        unit='part',
    ):
        if isinstance(result, Exception):
            continue
        if result > newest:
            newest = result

    if newest > manifest_mtime + 1.0:    # 1s tolerance for filesystem clock drift
        return 'stale', manifest, newest
    return None, manifest, newest


def metadata_check(ctx: DoctorContext) -> Report:
    findings = []
    missing = _missing_partition_meta(ctx)
    for d in missing:
        findings.append({'kind': 'missing_partition_meta', 'partition_dir': d})

    state, manifest, _ = _manifest_stale(ctx)
    if state is not None:
        findings.append({'kind': f'manifest_{state}', 'path': manifest})

    severity = Severity.WARN if findings else Severity.INFO
    summary = f"{len(missing)} partitions missing meta; manifest {state or 'ok'}"
    return Report(name='metadata', severity=severity, findings=findings, summary=summary)


def metadata_fix(ctx: DoctorContext, report: Report) -> Report:
    from ...gh3builder import h3_merge_metadata
    from ...utils import generate_manifest

    fixed = []
    for f in report.findings:
        kind = f.get('kind')
        try:
            if kind == 'missing_partition_meta':
                h3_merge_metadata(f['partition_dir'])
                fixed.append({'partition_dir': f['partition_dir'], 'action': 'regenerated_meta'})
            elif kind in ('manifest_missing', 'manifest_stale'):
                generate_manifest(ctx.h3_dir, tree_shape='h3db')
                fixed.append({'path': f.get('path'), 'action': f"regenerated_{kind}"})
        except Exception as e:
            fixed.append({**f, 'fix_error': f"{type(e).__name__}: {e}"})

    report.applied = True
    report.findings = fixed
    n_errors = sum(1 for x in fixed if 'fix_error' in x)
    if n_errors:
        report.severity = Severity.ERROR
        report.summary = f"regenerated {len(fixed) - n_errors}; failed {n_errors}"
    else:
        report.severity = Severity.INFO
        report.summary = f"regenerated {len(fixed)} metadata files"
    return report


register('metadata', 'partition meta JSON + dataset manifest health',
         scope='global', fix=metadata_fix)(metadata_check)
