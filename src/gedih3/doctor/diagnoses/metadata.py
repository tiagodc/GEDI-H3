"""metadata diagnosis — partition meta JSON + dataset manifest health.

Detects:
  - Partitions whose merged meta JSON file is missing.
  - A stale or absent dataset-level Parquet ``_metadata`` manifest.

Remedies (safe):
  - Regenerate missing partition meta via ``h3_merge_metadata``.
  - Regenerate manifest via ``generate_manifest``.
"""

from __future__ import annotations

import glob
import os

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_meta_file
from ...cliutils import progress_iter


def _missing_partition_meta(ctx: DoctorContext):
    missing = []
    with progress_iter(ctx.partition_dirs,
                       desc="metadata: scanning partitions",
                       args=getattr(ctx, 'args', None),
                       unit="part") as bar:
        for d in bar:
            if partition_meta_file(d) is None:
                missing.append(d)
    return missing


def _manifest_stale(h3_dir: str):
    """Return ('missing'|'stale'|None, manifest_path, newest_partition_mtime)."""
    manifest = os.path.join(h3_dir, '_metadata')
    if not os.path.exists(manifest):
        return 'missing', manifest, None
    manifest_mtime = os.path.getmtime(manifest)
    newest = manifest_mtime
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet')):
        m = os.path.getmtime(f)
        if m > newest:
            newest = m
    if newest > manifest_mtime + 1.0:    # 1s tolerance for filesystem clock drift
        return 'stale', manifest, newest
    return None, manifest, newest


def metadata_check(ctx: DoctorContext) -> Report:
    findings = []
    missing = _missing_partition_meta(ctx)
    for d in missing:
        findings.append({'kind': 'missing_partition_meta', 'partition_dir': d})

    state, manifest, _ = _manifest_stale(ctx.h3_dir)
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
                generate_manifest(ctx.h3_dir)
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
