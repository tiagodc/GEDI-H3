# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

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
from ..fused import register_scan
from ...cliutils import progress_iter
from ...utils import release_arrow_pool

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
    *,
    shared: Optional[dict] = None,
) -> List[dict]:
    """Worker: scan one partition for missing/NaN gaps. Returns finding dicts.

    Self-contained — receives only the partition path and the small per-
    product expected-columns map (computed once on the driver and
    broadcast). No DoctorContext serialization.

    Fused-aware: when ``shared`` is provided, reads the cached partition
    meta dict and parquet listing rather than re-opening / re-globbing.
    """
    findings: List[dict] = []
    if shared is not None and 'meta_dict' in shared:
        meta = shared['meta_dict']
    else:
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

    if shared is not None and 'parquet_files' in shared:
        files_iter = shared['parquet_files']
    else:
        files_iter = partition_parquet_files(partition_dir)
    for pq_file in files_iter:
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


register_scan('backfill', _scan_partition_backfill)


def _finalize_backfill_check(
    ctx: DoctorContext,
    per_partition: Dict[str, List[dict]],
) -> Report:
    """Driver-side aggregation of backfill per-partition findings."""
    findings: List[dict] = []
    for part_dir, result in per_partition.items():
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

    per_partition: Dict[str, List[dict]] = {}
    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition_backfill,
        args=getattr(ctx, 'args', None),
        desc='backfill: scanning partitions',
        unit='part',
        products=products,
        expected_by_product=expected_by_product,
    ):
        per_partition[part_dir] = result
    return _finalize_backfill_check(ctx, per_partition)


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


# Per-worker-process soc_tree cache. Each dask worker is a separate
# Python process so this dict is private to that process; subsequent
# tasks on the same worker reuse the prebuilt tree instead of re-walking
# the SOC manifest. With the v0.8.x SOC manifest sentinel the rebuild
# is fast (manifest + fnmatch) but still nontrivial on a continental tree.
_soc_tree_cache: Dict[str, dict] = {}


def _get_soc_tree(soc_source: str) -> dict:
    """Process-local cache of ``soc_file_tree`` for a given source path."""
    cached = _soc_tree_cache.get(soc_source)
    if cached is not None:
        return cached
    tree = _build_soc_tree(soc_source)
    _soc_tree_cache[soc_source] = tree
    return tree


def _read_patch_for_partition(
    affected_granules_by_product: Dict[str, List[GranuleKey]],
    soc_tree: dict,
    vars_per_product: Dict[str, List[str]],
) -> Optional[str]:
    """Build a single patch parquet containing [shot_number] + product cols.

    Returns the path to a temp parquet (caller must delete) or None when no
    source files were available. Takes ``vars_per_product`` directly
    (no DoctorContext) so the function is picklable for use as a dask
    worker payload.

    Memory pillar (v0.8.x lessons applied):
      * Each per-granule HDF5 frame is **streamed straight into a
        single ``pq.ParquetWriter``** — never accumulated in a
        ``frames=[]`` list and concatenated. The previous
        ``pd.concat(frames).drop_duplicates().to_parquet()`` loaded every
        affected granule × every requested column into memory at once
        (multi-GB on partitions affecting many granules).
      * Cross-granule ``shot_number`` deduplication uses an int64 set as
        an "already-emitted" gate so a duplicate shot from a later
        granule is dropped at write time. The set is bounded to the
        unique shots already written (which equals patch row count, not
        column-times-row product) — same lower bound the build pipeline
        accepts in ``parquet_dedup_partition``.
      * Frame buffers are released between granules (``del df``) and the
        arrow allocator is drained at writer close.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ...gedidriver import load_h5

    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    tmp.close()
    out_path = tmp.name

    writer: Optional[pq.ParquetWriter] = None
    seen_shots: set = set()
    schema_pa = None
    columns_order: Optional[List[str]] = None
    n_written = 0
    raised = False

    try:
        for prod, granules in affected_granules_by_product.items():
            var_list = vars_per_product.get(prod) or []
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
                    df = load_h5(soc_files[prod], columns=cols_to_read,
                                 include_source=False, dropna=False)
                except Exception:
                    continue
                if df is None or df.empty:
                    continue

                df = df.rename(columns=lambda x: x if x == 'shot_number' or str(x).endswith(suffix) else f"{x}{suffix}")
                if df.index.name == 'shot_number':
                    df = df.reset_index()

                # Drop already-emitted shot_numbers from prior granules.
                if seen_shots:
                    keep_mask = ~df['shot_number'].isin(seen_shots)
                    if not keep_mask.any():
                        del df
                        continue
                    df = df.loc[keep_mask]
                seen_shots.update(df['shot_number'].to_numpy().tolist())

                if writer is None:
                    # Lock the schema on the first non-empty frame so
                    # subsequent frames are projected/coerced into it.
                    columns_order = list(df.columns)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    schema_pa = table.schema
                    writer = pq.ParquetWriter(out_path, schema_pa, compression='zstd')
                    writer.write_table(table)
                    n_written += table.num_rows
                    del table, df
                else:
                    # Project to the locked schema; columns missing from
                    # this frame become null, extras are dropped (the
                    # patch represents the union of source vars).
                    for c in columns_order:
                        if c not in df.columns:
                            df[c] = None
                    df = df[columns_order]
                    table = pa.Table.from_pandas(df, schema=schema_pa, preserve_index=False)
                    writer.write_table(table)
                    n_written += table.num_rows
                    del table, df
    except Exception:
        # Anything that escapes the write loop must NOT leak the temp
        # parquet at ``out_path``. The post-finally ``n_written == 0``
        # cleanup path below is unreachable on exception (the
        # ``return`` is past the try/finally), and the caller in
        # ``_heal_partition`` only knows the path through this
        # function's return value — when we raise, ``patch`` stays
        # ``None`` on the caller side and its finally cleanup skips
        # the unlink.
        raised = True
        raise
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
            del writer
        release_arrow_pool()
        if raised and os.path.exists(out_path):
            try:
                os.unlink(out_path)
            except OSError:
                pass

    if n_written == 0:
        try:
            os.unlink(out_path)
        except OSError:
            pass
        return None

    return out_path


def _heal_partition(
    item: Tuple[str, Dict[str, List[GranuleKey]]],
    *,
    soc_source: str,
    vars_per_product: Dict[str, List[str]],
) -> Dict[str, list]:
    """Worker: read source HDF5 patch, apply parquet_fill_columns to every
    parquet under one partition, return healed/unavailable/error finding lists.

    ``item`` carries the per-partition payload as a tuple
    ``(partition_dir, affected_granules_by_product)`` so each dask task
    serializes only its own work — no whole-database broadcast dict.
    Self-contained — no DoctorContext serialization. The build-log
    INDEXED marking stays on the driver because it mutates shared state.
    """
    partition_dir, affected_granules_by_product = item
    healed: list = []
    not_available: list = []
    error_actions: list = []

    # Process-local soc_tree cache (v0.8.x lesson: don't redo work
    # the structure of the data already answers).
    soc_tree = _get_soc_tree(soc_source)
    patch = None
    try:
        patch = _read_patch_for_partition(
            affected_granules_by_product, soc_tree, vars_per_product,
        )
        if patch is None:
            for prod, grans in affected_granules_by_product.items():
                for g in grans:
                    not_available.append({
                        'partition_dir': partition_dir, 'product': prod,
                        'granule': {'orbit': g[0], 'granule': g[1], 'track': g[2]},
                        'reason': 'source_not_in_soc_tree',
                    })
            return {'healed': healed, 'not_available': not_available, 'error_actions': error_actions}

        had_error = False
        for pq_file in partition_parquet_files(partition_dir):
            try:
                parquet_fill_columns(pq_file, [patch])
            except Exception as e:
                had_error = True
                error_actions.append({
                    'partition_dir': partition_dir, 'parquet_file': pq_file,
                    'fix_error': f"{type(e).__name__}: {e}",
                })

        if not had_error:
            for prod, grans in affected_granules_by_product.items():
                for g in grans:
                    healed.append({
                        'partition_dir': partition_dir, 'product': prod,
                        'granule': {'orbit': g[0], 'granule': g[1], 'track': g[2]},
                        'action': 'filled',
                    })
    finally:
        if patch and os.path.exists(patch):
            try:
                os.unlink(patch)
            except OSError:
                pass
        # Drain transient arrow buffers before yielding the worker slot
        # to the next task — same pattern parquet_merge_files uses on
        # the build side.
        release_arrow_pool()

    return {'healed': healed, 'not_available': not_available, 'error_actions': error_actions}


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

    healed: list = []
    not_available: list = []
    error_actions: list = []

    # Pre-compute the per-product variable lists once on the driver and
    # broadcast them to every worker — the only ctx coupling that used
    # to block parallelization. ctx.h3_logger.product_vars is small
    # (dozens of strings) so direct broadcast is fine.
    products_in_use = sorted({p for prod_grans in by_partition.values() for p in prod_grans})
    vars_per_product = {p: _vars_for_product(ctx, p) for p in products_in_use}

    # Pack each partition's payload into the work item itself so dask
    # serializes only the slice each task needs — no whole-database
    # broadcast dict. ``_heal_partition`` unpacks the tuple worker-side.
    items = [
        (part_dir, {prod: list(grans) for prod, grans in prod_grans.items()})
        for part_dir, prod_grans in by_partition.items()
    ]

    try:
        from ..parallel import parallel_map
        for item, result in parallel_map(
            items,
            _heal_partition,
            args=getattr(ctx, 'args', None),
            desc='backfill: healing partitions',
            unit='part',
            soc_source=soc_source,
            vars_per_product=vars_per_product,
        ):
            part_dir = item[0] if item is not None else '<unknown>'
            if isinstance(result, Exception):
                error_actions.append({
                    'partition_dir': part_dir,
                    'fix_error': f"{type(result).__name__}: {result}",
                })
                continue
            healed.extend(result['healed'])
            not_available.extend(result['not_available'])
            error_actions.extend(result['error_actions'])

        # Build-log INDEXED marking stays on the driver — it mutates
        # shared state (the in-memory log) that the workers don't have.
        if ctx.h3_logger is not None:
            for h in healed:
                ctx.h3_logger.mark_granule_product(
                    h['granule'], h['product'], 'INDEXED',
                )
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
