"""parquet_health diagnosis — corrupt files, duplicate shots, schema drift.

Three sub-checks bundled because they share the partition/parquet scan:

  - **corrupt**: open each parquet file with ``pq.ParquetFile``; flag exceptions.
    No remedy (would risk deleting user data); reported only.
  - **duplicate_shots**: count duplicate ``shot_number`` rows per partition file.
    Remedy: ``parquet_dedup_partition`` (streaming, keep-first).
  - **schema_drift**: compare each partition's column set to the modal column
    set; flag outliers. Check-only — recommend running ``--fix backfill`` to
    re-join the missing product columns.
"""

from __future__ import annotations

import os
from collections import Counter
from typing import Dict, List

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_parquet_files


def _open_safely(path):
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        # Touch metadata to force lazy errors.
        _ = pf.metadata.num_row_groups
        pf.close()
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


def _count_duplicates(path) -> int:
    import pandas as pd
    try:
        df = pd.read_parquet(path, columns=['shot_number'])
    except Exception:
        return -1
    if df.empty:
        return 0
    return int(df['shot_number'].duplicated().sum())


def _partition_columns(path) -> List[str]:
    import pyarrow.parquet as pq
    try:
        return list(pq.read_schema(path).names)
    except Exception:
        return []


def parquet_health_check(ctx: DoctorContext) -> Report:
    findings = []
    schema_by_part: Dict[str, frozenset] = {}

    for d in ctx.partition_dirs:
        for pq_file in partition_parquet_files(d):
            err = _open_safely(pq_file)
            if err:
                findings.append({'kind': 'corrupt', 'path': pq_file, 'error': err})
                continue

            dup = _count_duplicates(pq_file)
            if dup > 0:
                findings.append({'kind': 'duplicate_shots', 'path': pq_file, 'duplicates': dup})
            elif dup == -1:
                findings.append({'kind': 'unreadable_shot_number', 'path': pq_file})

            cols = _partition_columns(pq_file)
            schema_by_part.setdefault(d, frozenset()).__sizeof__()  # noop
            schema_by_part[d] = schema_by_part.get(d, frozenset()) | frozenset(cols)

    # Schema drift: find the modal column set and flag partitions that differ.
    if len(schema_by_part) >= 3:
        counts = Counter(schema_by_part.values())
        modal_schema, _ = counts.most_common(1)[0]
        for part_dir, cols in schema_by_part.items():
            if cols != modal_schema:
                missing = sorted(modal_schema - cols)
                extra = sorted(cols - modal_schema)
                findings.append({
                    'kind': 'schema_drift',
                    'partition_dir': part_dir,
                    'missing_columns': missing,
                    'extra_columns': extra,
                })

    n_corrupt = sum(1 for f in findings if f['kind'] == 'corrupt')
    n_dup = sum(1 for f in findings if f['kind'] == 'duplicate_shots')
    n_drift = sum(1 for f in findings if f['kind'] == 'schema_drift')

    if n_corrupt:
        severity = Severity.ERROR
    elif findings:
        severity = Severity.WARN
    else:
        severity = Severity.INFO

    summary = f"{n_corrupt} corrupt, {n_dup} files with duplicates, {n_drift} schema-drift partitions"

    recommendations = []
    if n_drift:
        recommendations.append(
            "gh3_doctor --fix backfill   # refill missing-column partitions from source"
        )
    if n_corrupt:
        recommendations.append(
            "Review corrupt files before deletion; gh3_doctor will not auto-delete data."
        )

    return Report(
        name='parquet_health', severity=severity,
        findings=findings, summary=summary, recommendations=recommendations,
    )


def parquet_health_fix(ctx: DoctorContext, report: Report) -> Report:
    """Fix only what's safely fixable: duplicate_shots."""
    from ..parquet_ops import parquet_dedup_partition

    fixed = []
    for f in report.findings:
        kind = f.get('kind')
        if kind == 'duplicate_shots':
            try:
                dropped = parquet_dedup_partition(f['path'])
                fixed.append({**f, 'action': 'deduplicated', 'dropped': dropped})
            except Exception as e:
                fixed.append({**f, 'fix_error': f"{type(e).__name__}: {e}"})
        elif kind in ('corrupt', 'schema_drift', 'unreadable_shot_number'):
            # Preserve as-is — no auto-fix.
            fixed.append({**f, 'action': 'reported_only'})
        else:
            fixed.append(f)

    n_errors = sum(1 for x in fixed if 'fix_error' in x)
    n_corrupt = sum(1 for x in fixed if x['kind'] == 'corrupt')
    report.applied = True
    report.findings = fixed
    if n_errors or n_corrupt:
        report.severity = Severity.ERROR if n_corrupt else Severity.WARN
        report.summary = (
            f"{sum(1 for x in fixed if x.get('action') == 'deduplicated')} dedups; "
            f"{n_corrupt} corrupt unresolved; {n_errors} fix errors"
        )
    else:
        report.severity = Severity.INFO
        report.summary = f"{sum(1 for x in fixed if x.get('action') == 'deduplicated')} dedups complete"
    return report


register('parquet_health', 'corrupt files + duplicate shots + schema drift',
         scope='global', fix=parquet_health_fix)(parquet_health_check)
