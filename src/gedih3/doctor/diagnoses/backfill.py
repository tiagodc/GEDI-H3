"""backfill diagnosis — fill NaN gaps in product columns from source HDF5s.

The detection runs row-level: for each partition × product, we count nulls per
granule by grouping on ``root_file_l2a``. Findings include the partition file,
the product, and the per-granule null counts.

The fix:
  1. Group findings by source granule × product to compute the minimal set of
     HDF5 files to read.
  2. (S3 mode) run :func:`s3_etl_subset` to populate a temp dir with just those
     granules' source files for the requested products. Cleanup in ``finally``.
  3. For each affected partition file, read the relevant ``[shot_number, vars]``
     from the source HDF5(s), suffix columns with ``_<prod>``, write to a
     temporary patch parquet, and call :func:`parquet_fill_columns` to merge.
  4. Mark each healed granule × product as ``INDEXED`` in the build log.
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
from typing import Dict, List, Optional, Set, Tuple

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import (
    load_partition_meta, partition_parquet_files,
    product_columns_in_schema, per_granule_null_counts,
)
from ..parallel import parallel_map
from ..parquet_ops import parquet_fill_columns
from ...cliutils import progress_iter

GranuleKey = Tuple[int, int, int]


def _active_products(ctx: DoctorContext, requested: Optional[List[str]] = None) -> List[str]:
    if requested:
        return [p.upper() for p in requested]
    if ctx.h3_logger is None or not ctx.h3_logger.product_vars:
        return []
    return list(ctx.h3_logger.product_vars.keys())


def _vars_for_product(ctx: DoctorContext, product: str) -> List[str]:
    """Return the variable list recorded for a product in the build log.

    Doctor never widens the schema — it only fills columns that should be there.
    The variable list comes from the build log so we read exactly the columns
    that gh3_build / _build_add_variables would have written.
    """
    if ctx.h3_logger is None:
        return []
    vars_list = (ctx.h3_logger.product_vars or {}).get(product) or []
    # `gh3_build` always includes shot_number, but the stored list may omit it.
    return list(vars_list)


def _expected_product_columns(ctx: DoctorContext, product: str) -> List[str]:
    """Compute the suffixed column names a product *should* contribute."""
    suffix = f"_{product.lower()}"
    cols = []
    for v in _vars_for_product(ctx, product):
        if v == 'shot_number':
            continue
        cols.append(v if str(v).lower().endswith(suffix) else f"{v}{suffix}")
    return cols


def _scan_partition_backfill(
    partition_dir: str,
    products: List[str],
    expected_by_product: Dict[str, List[str]],
) -> List[dict]:
    """Worker: scan one partition for missing/NaN gaps. Returns finding dicts.

    Self-contained — receives only the partition path and the small per-
    product expected-columns map (computed once on the driver and
    broadcast). No DoctorContext serialization.
    """
    findings: List[dict] = []
    meta = load_partition_meta(partition_dir)
    if meta is None:
        return findings

    partition_cols = list(meta.get('columns', []))
    partition_cols_lower = {str(c).lower() for c in partition_cols}

    # 1. MISSING_COLUMN: a product the log expects whose suffix
    # doesn't appear in this partition's columns.
    for prod in products:
        expected = expected_by_product.get(prod) or []
        if not expected:
            continue
        present = any(c.lower() in partition_cols_lower for c in expected)
        if not present:
            findings.append({
                'kind': 'missing_column',
                'partition_dir': partition_dir,
                'product': prod,
                'expected_columns': expected,
                'granules': meta.get('granules', []),
            })

    # 2. PARTIAL_NAN: column present but specific granules have NaN.
    prod_columns = product_columns_in_schema(partition_cols, products)
    if not prod_columns:
        return findings

    for pq_file in partition_parquet_files(partition_dir):
        try:
            nulls = per_granule_null_counts(pq_file, prod_columns)
        except Exception as e:
            findings.append({
                'kind': 'scan_error',
                'partition_dir': partition_dir,
                'parquet_file': pq_file,
                'error': f"{type(e).__name__}: {e}",
            })
            continue
        for gran_key, per_prod in nulls.items():
            for prod, n_nulls in per_prod.items():
                findings.append({
                    'kind': 'partial_nan',
                    'partition_dir': partition_dir,
                    'parquet_file': pq_file,
                    'product': prod,
                    'granule': {'orbit': gran_key[0], 'granule': gran_key[1], 'track': gran_key[2]},
                    'null_rows': n_nulls,
                })

    return findings


def backfill_check(ctx: DoctorContext) -> Report:
    products = _active_products(ctx)
    if not products:
        return Report(
            name='backfill', severity=Severity.INFO,
            summary='no active products in build log; nothing to backfill',
        )

    # Compute the per-product expected-column lists once on the driver
    # so workers don't need DoctorContext.
    expected_by_product = {p: _expected_product_columns(ctx, p) for p in products}

    findings: List[dict] = []
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition_backfill,
        args=getattr(ctx, 'args', None),
        desc='backfill: scanning partitions',
        unit='part',
        products=products,
        expected_by_product=expected_by_product,
    ):
        if isinstance(result, Exception):
            findings.append({
                'kind': 'scan_error',
                'partition_dir': part_dir,
                'error': f"{type(result).__name__}: {result}",
            })
            continue
        findings.extend(result)

    n_missing = sum(1 for f in findings if f['kind'] == 'missing_column')
    n_partial = sum(1 for f in findings if f['kind'] == 'partial_nan')
    n_errors = sum(1 for f in findings if f['kind'] == 'scan_error')

    severity = Severity.INFO if not findings else Severity.WARN
    if n_errors:
        severity = Severity.ERROR

    summary = (
        f"{n_missing} (partition × product) missing-column gaps, "
        f"{n_partial} (granule × product) partial-NaN gaps"
    )

    recommendations = []
    if findings:
        recommendations.append("gh3_doctor --fix backfill           # heal from local SOC dir")
        recommendations.append("gh3_doctor --fix backfill --s3      # heal via NASA S3 ETL temp")
        if getattr(ctx.args, 'online', False):
            recommendations.append("gh3_doctor --online                 # see per-gap availability")
        else:
            recommendations.append("gh3_doctor --online                 # check NASA upstream availability")

    return Report(
        name='backfill', severity=severity,
        findings=findings, summary=summary, recommendations=recommendations,
    )


def _granules_needing_fill(report: Report) -> Set[Tuple[int, int, int, str]]:
    """Collect (orbit, granule, track, product) tuples that need source data."""
    needed = set()
    for f in report.findings:
        if f['kind'] == 'missing_column':
            for g in f.get('granules', []):
                needed.add((g['orbit'], g['granule'], g['track'], f['product']))
        elif f['kind'] == 'partial_nan':
            g = f['granule']
            needed.add((g['orbit'], g['granule'], g['track'], f['product']))
    return needed


def _build_soc_tree(soc_source):
    """Wrap soc_file_tree to handle string paths uniformly."""
    from ...gh3builder import soc_file_tree
    if isinstance(soc_source, str):
        return soc_file_tree(soc_source, to_list=False)
    if isinstance(soc_source, list):
        return soc_file_tree(soc_source, to_list=False)
    return {}


def _read_patch_for_partition(
    affected_granules_by_product: Dict[str, List[GranuleKey]],
    soc_tree: dict,
    ctx: DoctorContext,
) -> Optional[str]:
    """Build a single patch parquet containing [shot_number] + product cols.

    Returns the path to a temp parquet (caller must delete) or None when no
    source files were available.
    """
    import pandas as pd
    from ...gedidriver import load_h5

    frames = []
    for prod, granules in affected_granules_by_product.items():
        var_list = _vars_for_product(ctx, prod)
        if not var_list:
            continue
        cols_to_read = ['shot_number'] + [v for v in var_list if v != 'shot_number']
        suffix = f"_{prod.lower()}"
        for orbit, gran, track in granules:
            orb_track = f"O{orbit:05d}_{gran:02d}_T{track:05d}"
            soc_files = soc_tree.get(orb_track)
            if not soc_files or prod not in soc_files:
                continue
            try:
                df = load_h5(soc_files[prod], columns=cols_to_read, include_source=False, dropna=False)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            df = df.rename(columns=lambda x: x if x == 'shot_number' or str(x).endswith(suffix) else f"{x}{suffix}")
            if df.index.name == 'shot_number':
                df = df.reset_index()
            frames.append(df)

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = merged.drop_duplicates(subset='shot_number', keep='first')

    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    tmp.close()
    merged.to_parquet(tmp.name, engine='pyarrow', index=False)
    return tmp.name


def backfill_fix(ctx: DoctorContext, report: Report) -> Report:
    if not report.has_findings:
        report.applied = True
        report.summary = "nothing to fix"
        return report

    use_s3 = bool(getattr(ctx.args, 's3', False))
    s3_tmp_dir = None

    # Build the soc_source: either local SOC dir or an S3 ETL temp dir.
    if use_s3:
        from ...gh3builder import s3_etl_subset

        if ctx.h3_logger is None:
            report.applied = True
            report.severity = Severity.ERROR
            report.summary = "S3 backfill requires a build log to know spatial/temporal/products"
            return report

        # Gather products + variable lists for the granules that need filling.
        needed = _granules_needing_fill(report)
        product_vars = {}
        for *_, prod in needed:
            if prod not in product_vars:
                vars_list = _vars_for_product(ctx, prod)
                product_vars[prod] = vars_list if vars_list else None

        s3_tmp_dir = os.path.join(ctx.tmp_dir or os.path.join(ctx.h3_dir, '.tmp'), '_doctor_s3_backfill')
        os.makedirs(s3_tmp_dir, exist_ok=True)
        try:
            s3_etl_subset(
                product_vars=product_vars,
                spatial=getattr(ctx.h3_logger, 'spatial', None),
                temporal=getattr(ctx.h3_logger, 'temporal', None),
                version=getattr(ctx.h3_logger, 'gedi_version', None),
                odir=s3_tmp_dir,
                ensure_l2a=False,
            )
            soc_source = s3_tmp_dir
        except Exception as e:
            shutil.rmtree(s3_tmp_dir, ignore_errors=True)
            report.applied = True
            report.severity = Severity.ERROR
            report.summary = f"S3 ETL failed: {type(e).__name__}: {e}"
            return report
    else:
        soc_source = ctx.soc_dir

    if not soc_source or not os.path.isdir(soc_source):
        report.applied = True
        report.severity = Severity.ERROR
        report.summary = (
            f"no SOC source available (use --s3 or set --soc-dir or GH3_DEFAULT_SOC_DIR). "
            f"Source attempted: {soc_source!r}"
        )
        return report

    soc_tree = _build_soc_tree(soc_source)

    # Group findings by partition directory so we minimize file rewrites.
    by_partition: Dict[str, Dict[str, Set[GranuleKey]]] = {}
    for f in report.findings:
        kind = f.get('kind')
        if kind == 'partial_nan':
            part = f['partition_dir']
            prod = f['product']
            g = f['granule']
            by_partition.setdefault(part, {}).setdefault(prod, set()).add((g['orbit'], g['granule'], g['track']))
        elif kind == 'missing_column':
            part = f['partition_dir']
            prod = f['product']
            for g in f.get('granules', []):
                by_partition.setdefault(part, {}).setdefault(prod, set()).add((g['orbit'], g['granule'], g['track']))

    healed = []
    not_available = []
    error_actions = []

    try:
        for part_dir, prod_grans in by_partition.items():
            affected_granules_by_product = {p: list(s) for p, s in prod_grans.items()}
            patch = None
            try:
                patch = _read_patch_for_partition(affected_granules_by_product, soc_tree, ctx)
                if patch is None:
                    for prod, grans in affected_granules_by_product.items():
                        for g in grans:
                            not_available.append({
                                'partition_dir': part_dir, 'product': prod,
                                'granule': {'orbit': g[0], 'granule': g[1], 'track': g[2]},
                                'reason': 'source_not_in_soc_tree',
                            })
                    continue

                for pq_file in partition_parquet_files(part_dir):
                    try:
                        parquet_fill_columns(pq_file, [patch])
                    except Exception as e:
                        error_actions.append({
                            'partition_dir': part_dir, 'parquet_file': pq_file,
                            'fix_error': f"{type(e).__name__}: {e}",
                        })
                        continue

                # Mark per-granule per-product status as INDEXED on success.
                if ctx.h3_logger is not None:
                    for prod, grans in affected_granules_by_product.items():
                        for g in grans:
                            ctx.h3_logger.mark_granule_product(
                                {'orbit': g[0], 'granule': g[1], 'track': g[2]},
                                prod, 'INDEXED',
                            )
                            healed.append({
                                'partition_dir': part_dir, 'product': prod,
                                'granule': {'orbit': g[0], 'granule': g[1], 'track': g[2]},
                                'action': 'filled',
                            })
            finally:
                if patch and os.path.exists(patch):
                    os.unlink(patch)
    finally:
        if s3_tmp_dir and os.path.exists(s3_tmp_dir):
            shutil.rmtree(s3_tmp_dir, ignore_errors=True)

    findings = healed + not_available + error_actions
    report.applied = True
    report.findings = findings
    if error_actions:
        report.severity = Severity.ERROR
        report.summary = (
            f"{len(healed)} healed, {len(not_available)} unavailable, {len(error_actions)} errors"
        )
    elif not_available:
        report.severity = Severity.WARN
        report.summary = f"{len(healed)} healed; {len(not_available)} still unavailable"
        report.recommendations = [
            "gh3_doctor --fix backfill --s3   # try S3 if local SOC tree is incomplete",
            "gh3_doctor --online              # check whether NASA has the missing granules",
        ]
    else:
        report.severity = Severity.INFO
        report.summary = f"healed {len(healed)} (granule × product) gaps"

    return report


register('backfill', 'fill NaN/missing-column gaps from source HDF5s',
         scope='global', fix=backfill_fix)(backfill_check)
