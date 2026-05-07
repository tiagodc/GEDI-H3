"""parquet_health diagnosis — corrupt files, duplicate shots, schema drift.

Three sub-checks bundled because they share the partition/parquet scan:

  - **corrupt**: open each parquet file with ``pq.ParquetFile``; flag exceptions.
    No remedy (would risk deleting user data); reported only.
  - **duplicate_shots**: count duplicate ``shot_number`` rows per partition file.
    Streamed row-group-at-a-time with a hash seen-set so memory is bounded
    by ``O(unique shots)`` rather than the full column. Remedy:
    ``parquet_dedup_partition`` (also streaming, keep-first).
  - **schema_drift**: compare each partition's column set to the modal column
    set; flag outliers. Check-only — recommend running ``--fix backfill`` to
    re-join the missing product columns.

Per-partition work runs through :func:`gedih3.doctor.parallel.parallel_map`
so a registered dask client distributes the I/O.

Memory pillar (v0.8.x lessons applied):
  * One ``pq.ParquetFile`` open per file (was three: open_safely +
    count_duplicates + partition_columns).
  * ``shot_number`` is **streamed via ``iter_batches``** with capped
    readahead (``batch_readahead=1``, ``fragment_readahead=1``) and
    ``pre_buffer=True`` for I/O coalescing on shared GPFS — never the
    full ``pd.read_parquet(columns=['shot_number'])`` pull that used
    to spike workers to ~800 MB on continental partitions.
  * Explicit ``del pf`` + ``pa.default_memory_pool().release_unused()``
    after each file so worker RSS plateaus instead of climbing.
"""

from __future__ import annotations

from collections import Counter
from typing import Dict, List

from ..report import Report, DoctorContext, Severity
from ..runner import register
from ..inspect import partition_parquet_files
from ..parallel import parallel_map


# Memory-bounded readahead — same caps the build merge phase landed on
# (see commit f718590 / v0.8.22). batch_readahead/fragment_readahead=1
# limits the pyarrow scanner's prefetch so a single partition scan
# can't balloon a worker. ROW_GROUP_BATCH is the per-batch row count
# pulled across iter_batches calls; small enough to bound RSS, large
# enough to keep arrow's vectorized paths warm.
_ROW_GROUP_BATCH = 50_000
_PRE_BUFFER = True


def _release_arrow_pool() -> None:
    """Best-effort drain of pyarrow's allocator after each file.

    Mirrors the build's ``parquet_merge_files`` pattern (utils.py:1469):
    pyarrow's transient read/write buffers don't always return to the
    OS at GC time; explicit release keeps long-running worker RSS flat.
    """
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def _scan_one_file(pq_file: str) -> dict:
    """Single-pass per-file scan: corrupt? dup count? column union?

    One ``pq.ParquetFile`` open feeds all three checks. Memory is
    **bounded to one row-group**, regardless of file size:

      * ``intra_rg_dups`` is computed via ``pc.value_counts`` per
        row-group (peak working set: one batch's ``shot_number`` plus
        a C++ hashmap of size O(unique values in that batch) — a few
        MB for 50k-row groups).
      * Cross-row-group duplicates are detected by walking the parquet
        metadata's per-row-group min/max stats — **no extra I/O**.
        Overlapping ranges trigger a `cross_rg_overlap` flag so the
        --fix path knows to run the real streaming dedup.

    Returns a dict (never raises): keys ``corrupt`` (bool), ``error``
    (str|None), ``duplicates`` (int), ``cross_rg_overlap`` (bool),
    ``unreadable_shot_number`` (bool), ``columns`` (list[str]).
    """
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    out = {
        'corrupt': False,
        'error': None,
        'duplicates': 0,
        'cross_rg_overlap': False,
        'unreadable_shot_number': False,
        'columns': [],
    }

    pf = None
    try:
        pf = pq.ParquetFile(pq_file, pre_buffer=_PRE_BUFFER)
        # Header validity gate (touches metadata to force lazy errors).
        n_rg = pf.metadata.num_row_groups
        schema_names = list(pf.schema_arrow.names)
        out['columns'] = schema_names

        if 'shot_number' in schema_names:
            try:
                shot_idx = schema_names.index('shot_number')

                # --- Pass 1: intra-row-group duplicates via pc.value_counts ---
                # Memory ceiling: one row group's int64 column + native
                # hashmap over its unique values. Reading row group by
                # row group with ``read_row_group`` (rather than
                # ``iter_batches`` with a large batch_size that can span
                # row groups) keeps the per-call working-set bounded by
                # the file's row_group_size — typically 50k rows on
                # v0.8.22+ databases.
                intra_dups = 0
                for rg_idx in range(n_rg):
                    table = pf.read_row_group(rg_idx, columns=['shot_number'])
                    arr = table.column('shot_number')
                    vc = pc.value_counts(arr)
                    counts_np = vc.field('counts').to_numpy(zero_copy_only=False)
                    if counts_np.size:
                        intra_dups += int((counts_np - 1).clip(min=0).sum())
                    # Drop transient buffers before the next RG lands.
                    del arr, vc, counts_np, table
                out['duplicates'] = intra_dups

                # --- Pass 2 (free): cross-row-group overlap via stats ---
                # parquet writers record per-RG min/max for primitive types
                # by default. If RG ranges don't overlap, no cross-RG dups
                # are possible — the intra count above is exact. If they do
                # overlap, flag it so --fix runs the streaming dedup that
                # walks the whole file.
                ranges = []
                stats_complete = True
                for rg_idx in range(n_rg):
                    rg = pf.metadata.row_group(rg_idx)
                    stats = rg.column(shot_idx).statistics
                    if stats is None or not stats.has_min_max:
                        stats_complete = False
                        break
                    ranges.append((stats.min, stats.max))
                if stats_complete and ranges:
                    ranges.sort()
                    for i in range(1, len(ranges)):
                        if ranges[i][0] <= ranges[i - 1][1]:
                            out['cross_rg_overlap'] = True
                            break
                elif not stats_complete:
                    # Stats absent: we can't prove no cross-RG dups. Be
                    # conservative — flag overlap so --fix re-checks.
                    out['cross_rg_overlap'] = True
            except Exception:
                out['unreadable_shot_number'] = True
    except Exception as e:
        out['corrupt'] = True
        out['error'] = f"{type(e).__name__}: {e}"
    finally:
        if pf is not None:
            try:
                pf.close()
            except Exception:
                pass
            del pf
        _release_arrow_pool()

    return out


def _scan_partition(partition_dir: str) -> dict:
    """Worker: scan one partition, return per-file findings + column union.

    Per-file work goes through :func:`_scan_one_file` so each parquet
    is opened exactly once (header + shot_number stream + schema all in
    one pass) and its allocator freed before the next.
    """
    findings = []
    cols_union: set = set()
    for pq_file in partition_parquet_files(partition_dir):
        info = _scan_one_file(pq_file)
        if info['corrupt']:
            findings.append({'kind': 'corrupt', 'path': pq_file, 'error': info['error']})
            continue
        if info['unreadable_shot_number']:
            findings.append({'kind': 'unreadable_shot_number', 'path': pq_file})
        if info['duplicates'] > 0 or info['cross_rg_overlap']:
            findings.append({
                'kind': 'duplicate_shots', 'path': pq_file,
                'duplicates': info['duplicates'],
                'cross_rg_overlap': info['cross_rg_overlap'],
            })
        cols_union.update(info['columns'])

    return {'findings': findings, 'columns': frozenset(cols_union)}


def parquet_health_check(ctx: DoctorContext) -> Report:
    findings: List[dict] = []
    schema_by_part: Dict[str, frozenset] = {}

    for part_dir, result in parallel_map(
        ctx.partition_dirs,
        _scan_partition,
        args=getattr(ctx, 'args', None),
        desc='parquet_health: scanning partitions',
        unit='part',
    ):
        if isinstance(result, Exception):
            findings.append({
                'kind': 'scan_error',
                'partition_dir': part_dir,
                'error': f"{type(result).__name__}: {result}",
            })
            continue
        findings.extend(result['findings'])
        schema_by_part[part_dir] = result['columns']

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
    n_scan_err = sum(1 for f in findings if f['kind'] == 'scan_error')

    if n_corrupt or n_scan_err:
        severity = Severity.ERROR
    elif findings:
        severity = Severity.WARN
    else:
        severity = Severity.INFO

    summary = f"{n_corrupt} corrupt, {n_dup} files with duplicates, {n_drift} schema-drift partitions"
    if n_scan_err:
        summary += f", {n_scan_err} partitions errored during scan"

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
        elif kind in ('corrupt', 'schema_drift', 'unreadable_shot_number', 'scan_error'):
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
