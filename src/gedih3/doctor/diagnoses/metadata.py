# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""metadata diagnosis — partition meta JSON + dataset manifest health.

Detects:
  - Partitions whose merged meta JSON file is missing.
  - A stale or absent ``_manifest.txt`` R2 sentinel at the database root.

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
from typing import Dict, Optional, Tuple

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_meta_file, partition_parquet_files
from ..parallel import parallel_map
from ..fused import register_scan


def _scan_partition_metadata(
    partition_dir: str,
    *,
    shared: Optional[dict] = None,
) -> dict:
    """Worker: per-partition slice of the metadata diagnosis.

    Returns ``{'missing_meta': bool, 'newest_mtime': float}``. Fused-aware:
    when ``shared`` is provided, reads the cached parquet listing + meta
    file path instead of re-globbing the partition directory.
    """
    # Meta presence — uses shared['meta_file'] if available, falls back to
    # the single-scan glob otherwise.
    if shared is not None and 'meta_file' in shared:
        meta_file = shared['meta_file']
    else:
        meta_file = partition_meta_file(partition_dir)
    missing_meta = meta_file is None

    # Newest-parquet mtime — uses shared['parquet_files'] when present.
    if shared is not None and 'parquet_files' in shared:
        parquet_files = shared['parquet_files']
    else:
        parquet_files = partition_parquet_files(partition_dir)
    newest = 0.0
    for f in parquet_files:
        try:
            m = os.path.getmtime(f)
        except OSError:
            continue
        if m > newest:
            newest = m

    return {'missing_meta': missing_meta, 'newest_mtime': newest}


register_scan('metadata', _scan_partition_metadata)


def _finalize_metadata_check(
    ctx: DoctorContext,
    per_partition: Dict[str, dict],
) -> Report:
    """Driver-side aggregation of metadata per-partition results.

    ``per_partition`` maps partition_dir to the dict returned by
    :func:`_scan_partition_metadata`, or to an Exception captured by the
    fused worker.
    """
    findings = []
    missing = []
    scan_errors = []
    newest_partition_mtime = 0.0
    for part_dir, result in per_partition.items():
        if result is None:
            # Fused-dispatch produced no result slot for this diagnosis on
            # this partition — treat as scan_error so the operator sees a
            # diagnostic (not a misleading missing-meta count).
            scan_errors.append({
                'partition_dir': part_dir,
                'error': 'no fused result (worker may have stale bytecode)',
            })
            continue
        if isinstance(result, Exception):
            scan_errors.append({
                'partition_dir': part_dir,
                'error': f"{type(result).__name__}: {result}",
            })
            continue
        if result.get('missing_meta'):
            missing.append(part_dir)
        m = result.get('newest_mtime', 0.0)
        if m > newest_partition_mtime:
            newest_partition_mtime = m

    for d in missing:
        findings.append({'kind': 'missing_partition_meta', 'partition_dir': d})
    for se in scan_errors:
        findings.append({'kind': 'scan_error', **se})

    # gh3 builds produce ``_manifest.txt`` (the R2 sentinel), NOT the
    # pyarrow ``_metadata`` dataset manifest. The legacy check looked
    # for ``_metadata`` and so reported "manifest missing" on every
    # gh3 database — false positive. Check the file the build actually
    # writes.
    from ...config import MANIFEST_FILENAME
    manifest = os.path.join(ctx.h3_dir, MANIFEST_FILENAME)
    if not os.path.exists(manifest):
        state = 'missing'
    else:
        manifest_mtime = os.path.getmtime(manifest)
        # 1s tolerance for filesystem clock drift, matching the legacy check.
        state = 'stale' if newest_partition_mtime > manifest_mtime + 1.0 else None
    if state is not None:
        findings.append({'kind': f'manifest_{state}', 'path': manifest})

    if scan_errors:
        severity = Severity.ERROR
    elif findings:
        severity = Severity.WARN
    else:
        severity = Severity.INFO
    summary = (
        f"{len(missing)} partitions missing meta; manifest {state or 'ok'}"
        + (f"; {len(scan_errors)} scan errors" if scan_errors else '')
    )
    return Report(name='metadata', severity=severity, findings=findings, summary=summary)


def metadata_check(ctx: DoctorContext) -> Report:
    per_partition: Dict[str, dict] = {}
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition_metadata,
        args=getattr(ctx, 'args', None),
        desc='metadata: scanning partitions',
        unit='part',
    ):
        per_partition[part_dir] = result
    return _finalize_metadata_check(ctx, per_partition)


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
