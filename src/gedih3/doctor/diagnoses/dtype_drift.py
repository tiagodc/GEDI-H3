# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""dtype_drift diagnosis — partition columns stored in a type other than the source's.

The reference is the source HDF5 of the database's own GEDI release: one
sample granule is pushed through the exact Stage 1 schema derivation a build
uses (:func:`gedih3.gh3builder._source_write_schema`), so "correct" means
"what ``gh3_build`` writes for this release today". Every partition file's
footer is then compared against it.

Typical origin: a database built by an older gedih3 whose schema sample came
from another release sharing the SOC tree — e.g. ``worldcover_class`` is
``int32`` in L4C V002 but ``uint8`` in V003. Since 0.18 an update aligns new
data to the database's stored types (so nothing is lost), and this diagnosis
is how the database itself is moved to the source types.

Findings:
  - ``dtype_drift``: file stores one or more columns in a non-source type.
    Fixable: the file is rewritten with a safe cast (lossless, or the file is
    left untouched and the error reported).
  - ``unreadable``: footer could not be read. Reported only.
  - ``no_reference``: no SOC source of the database's release was found, so
    there is nothing to compare against. Reported only.

The fix rewrites whole files (Parquet cannot retype a column in place), so it
costs a full read + write of every affected file. It is resumable — files
already in the source types are skipped — and refuses to run while a
``gh3_build`` is live, since a concurrent merge could replace the same file.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_parquet_files
from ..parallel import parallel_map
from ...utils import release_arrow_pool

# Bounded search for a sample granule, in ``year/doy`` directories.
_MAX_SAMPLE_DAYS = 60


def _sample_granule(soc_dir: str, version: int, products: List[str],
                    latest: Optional[str] = None) -> Optional[Dict[str, str]]:
    """A public-release granule of ``version`` that has every product.

    A-priori search: starts at ``latest`` (``YYYY-MM-DD``, the database's own
    ``date_range`` end, where files of its release are known to exist) and
    walks ``year/doy`` directories backwards, at most ``_MAX_SAMPLE_DAYS``
    of them — one ``scandir`` each, never a tree walk. Only canonical release
    filenames (``..._V00N.h5``) qualify, so internal variants (``_SGS``,
    ``_7algs``) never become the reference.
    """
    from datetime import date, timedelta
    from ...gedidriver import soc_file_tree
    suffix = f'_V{version:03d}.h5'
    try:
        day = date.fromisoformat(str(latest)[:10]) if latest else date.today()
    except ValueError:
        day = date.today()
    for _ in range(_MAX_SAMPLE_DAYS):
        ddir = os.path.join(soc_dir, f'{day.year}', f'{day.timetuple().tm_yday:03d}')
        day -= timedelta(days=1)
        try:
            files = [e.path for e in os.scandir(ddir) if e.name.startswith('GEDI') and e.name.endswith(suffix)]
        except OSError:
            continue
        for soc in (soc_file_tree(files, to_list=True) if files else []):
            if all(p in soc for p in products):
                return {p: soc[p] for p in products}
    return None


def _reference_types(ctx: DoctorContext):
    """``({column: arrow_type_str}, reason)`` from the database's own release."""
    from ...gh3builder import _parquet_storage_type, _source_write_schema
    log = ctx.h3_logger
    if log is None or not getattr(log, 'log_data', None):
        return None, 'no build log'
    version = log.gedi_version
    products = {p: (v or {}).get('variables') for p, v in (log.log_data.get('products') or {}).items()}
    if version is None or not products:
        return None, 'build log records no GEDI release or products'
    if not ctx.soc_dir or not os.path.isdir(ctx.soc_dir):
        return None, f'no SOC directory (set --soc-dir); tried {ctx.soc_dir!r}'
    date_range = log.log_data.get('date_range') or [None, None]
    soc = _sample_granule(ctx.soc_dir, version, sorted(products), latest=date_range[-1])
    if soc is None:
        return None, f'no V{version:03d} granule with {sorted(products)} under {ctx.soc_dir}'
    schema = _source_write_schema(soc, products, res=log.res, part=log.part, version=version)
    # Compare in the stored form: files on disk can only hold Parquet's types.
    return {f.name: str(_parquet_storage_type(f.type)) for f in schema}, None


def _scan_partition_dtypes(partition_dir: str, *, reference: Dict[str, str]) -> dict:
    """Worker: footer-compare every parquet under one partition to ``reference``."""
    import pyarrow.parquet as pq
    findings = []
    n_ok = 0
    try:
        for f in partition_parquet_files(partition_dir):
            try:
                schema = pq.read_schema(f)
            except Exception as e:
                findings.append({'kind': 'unreadable', 'path': f, 'error': f"{type(e).__name__}: {e}"})
                continue
            drift = {
                n: [str(schema.field(n).type), reference[n]] for n in schema.names
                if n in reference and str(schema.field(n).type) != reference[n]
            }
            if drift:
                findings.append({'kind': 'dtype_drift', 'path': f, 'columns': drift})
            else:
                n_ok += 1
    finally:
        release_arrow_pool()
    return {'findings': findings, 'n_ok': n_ok}


def dtype_drift_check(ctx: DoctorContext) -> Report:
    reference, reason = _reference_types(ctx)
    if reference is None:
        return Report(
            name='dtype_drift', severity=Severity.INFO,
            findings=[{'kind': 'no_reference', 'reason': reason}],
            summary=f'skipped: {reason}',
        )
    findings: List[dict] = []
    n_ok = 0
    # The reference is baked into a partial rather than broadcast as a
    # kwarg: dask turns a large dict kwarg into a graph dependency
    # (``<TaskState 'reference' processing>``), the same trap the Stage 1
    # driver documents for its broadcast values.
    import functools
    scan = functools.partial(_scan_partition_dtypes, reference=reference)
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        scan,
        args=getattr(ctx, 'args', None),
        desc='dtype_drift: scanning partitions',
        unit='part',
    ):
        if isinstance(result, Exception):
            findings.append({'kind': 'unreadable', 'path': part_dir, 'error': f"{type(result).__name__}: {result}"})
            continue
        findings.extend(result['findings'])
        n_ok += result['n_ok']

    drifted = [f for f in findings if f['kind'] == 'dtype_drift']
    n_unreadable = sum(1 for f in findings if f['kind'] == 'unreadable')
    by_column: Dict[str, Dict[str, int]] = {}
    for f in drifted:
        for col, (stored, _) in f['columns'].items():
            by_column.setdefault(col, {}).setdefault(stored, 0)
            by_column[col][stored] += 1
    cols = '; '.join(
        f"{c}: {', '.join(f'{n} file(s) {t}' for t, n in sorted(s.items()))} (source {reference[c]})"
        for c, s in sorted(by_column.items())
    )
    summary = f"{n_ok} files match the source; {len(drifted)} drift" + (f" [{cols}]" if cols else "")
    if n_unreadable:
        summary += f"; {n_unreadable} unreadable"
    recommendations = []
    if drifted:
        recommendations.append(
            f"gh3_doctor -i {ctx.h3_dir} --soc-dir {ctx.soc_dir} --fix dtype_drift   "
            f"# rewrites {len(drifted)} file(s); stop any gh3_build first"
        )
    return Report(
        name='dtype_drift',
        severity=Severity.ERROR if n_unreadable else (Severity.WARN if drifted else Severity.INFO),
        findings=findings, summary=summary, recommendations=recommendations,
    )


def _cast_one(item) -> str:
    """Worker: retype one partition file and refresh its per-year metadata JSON.

    ``item`` is ``(path, ((column, target_type), ...))`` so each task carries
    only its own targets — no whole-report broadcast, and no dict dask could
    mistake for graph structure.
    """
    from ...utils import parquet_cast_columns
    from ...gh3builder import _refresh_year_columns_meta
    path, targets = item
    try:
        result = parquet_cast_columns(path, dict(targets))
        if result == 'rewritten':
            _refresh_year_columns_meta(path)
        return result
    finally:
        release_arrow_pool()


def dtype_drift_fix(ctx: DoctorContext, report: Report) -> Report:
    """Rewrite drifted files in the source types, then refresh the metadata that caches dtypes."""
    from .tmp_partitions_health import _build_is_active
    from ...gh3builder import h3_merge_metadata
    from ...utils import generate_manifest

    active, info = _build_is_active(ctx.h3_dir, ctx.tmp_dir)
    if active:
        report.applied = False
        report.severity = Severity.ERROR
        report.summary = (
            f"refused: a gh3_build appears to be running (pid {info.get('pid')}); a concurrent "
            f"merge could replace the same files. Re-run after it finishes."
        )
        return report

    targets_by_path = {
        f['path']: {c: ref for c, (_, ref) in f['columns'].items()}
        for f in report.findings if f['kind'] == 'dtype_drift'
    }
    by_path = {f['path']: f for f in report.findings if f['kind'] == 'dtype_drift'}
    fixed: List[dict] = []
    touched_cells = set()
    retyped: Dict[str, str] = {}
    failed_cols = set()
    n_rewritten = n_errors = 0
    # Batched dispatch keeps the graph small on continental DBs (one item
    # per file, tens of thousands of files).
    for item, result in parallel_map(
        [(p, tuple(sorted(t.items()))) for p, t in targets_by_path.items()],
        _cast_one,
        args=getattr(ctx, 'args', None),
        desc='dtype_drift: rewriting files',
        unit='file',
        batch_size=64,
    ):
        path = item[0] if item is not None else '<unknown>'
        f = by_path.get(path, {'path': path, 'kind': 'dtype_drift'})
        if isinstance(result, Exception):
            n_errors += 1
            failed_cols.update(targets_by_path.get(path, {}))
            fixed.append({**f, 'fix_error': f"{type(result).__name__}: {result}"})
            continue
        n_rewritten += result == 'rewritten'
        fixed.append({**f, 'action': 'retyped' if result == 'rewritten' else 'already_matching'})
        touched_cells.add(os.path.dirname(os.path.dirname(path)))
        retyped.update(targets_by_path.get(path, {}))
    for f in report.findings:
        if f['kind'] != 'dtype_drift':
            fixed.append({**f, 'action': 'reported_only'})

    if touched_cells:
        for _cell, _res in parallel_map(sorted(touched_cells), h3_merge_metadata,
                                        args=getattr(ctx, 'args', None),
                                        desc='dtype_drift: merging cell metadata', unit='part'):
            pass
    # The build log's cached dtype moves only for columns now uniform across
    # every file: a column that still drifts somewhere keeps its old record,
    # so a later update keeps aligning to (and merging into) what is on disk.
    retyped = {c: t for c, t in retyped.items() if c not in failed_cols}
    log = ctx.h3_logger
    if retyped and log is not None:
        dtypes = dict(getattr(log, 'h3_columns_dtypes', None) or log.log_data.get('h3_columns_dtypes') or {})
        dtypes.update(retyped)
        log.h3_columns_dtypes = dtypes
        log.save_log(log.log_data.get('status', 'COMPLETED'))
    if n_rewritten:
        generate_manifest(ctx.h3_dir, tree_shape='h3db')

    report.applied = True
    report.findings = fixed
    report.severity = Severity.WARN if n_errors else Severity.INFO
    report.summary = f"{n_rewritten} file(s) retyped; {n_errors} fix error(s)"
    if n_rewritten and os.path.exists(os.path.join(ctx.h3_dir, 'gedi.ducklake')):
        report.recommendations = [
            f"gh3_build_ducklake -d {ctx.h3_dir}   # the DuckLake catalog still records the old types"
        ]
    return report


register('dtype_drift', 'partition columns stored in a type other than the source HDF5',
         scope='global', fix=dtype_drift_fix)(dtype_drift_check)
