"""Read-only inspectors shared across diagnoses.

These helpers do not mutate the database. They consolidate filesystem and
parquet I/O so individual diagnoses don't re-walk the same directories.
"""

from __future__ import annotations

import glob
import os
from typing import Dict, Iterable, List, Optional, Tuple

from ..config import PARTITION_META_FILENAME
from ..utils import json_read, release_arrow_pool

GranuleKey = Tuple[int, int, int]   # (orbit, orbit_granule, track)


def discover_partition_dirs(h3_dir: str) -> List[str]:
    """List ``h3_*/`` directories under the database root."""
    return sorted(d for d in glob.glob(os.path.join(h3_dir, 'h3_*/')) if os.path.isdir(d))


def partition_meta_file(partition_dir: str) -> Optional[str]:
    """Locate the merged partition-level metadata file.

    Per ``h3_merge_metadata`` (gh3builder.py:475), each partition has a single
    ``<h3_part>_gedih3_partition_meta.json`` at its root. Returns None if absent.
    """
    files = glob.glob(os.path.join(partition_dir, f'*{PARTITION_META_FILENAME}'))
    if not files:
        return None
    # Prefer the file whose basename has no year prefix (root-level meta).
    # h3_write_metadata produces ``<h3_part>.<year>_gedih3_partition_meta.json``
    # per year; h3_merge_metadata writes ``<h3_part>_gedih3_partition_meta.json``.
    root = [f for f in files if os.path.basename(f).count('.') == 1]
    return root[0] if root else files[0]


def load_partition_meta(partition_dir: str) -> Optional[dict]:
    """Read the merged partition meta JSON (or None if missing)."""
    f = partition_meta_file(partition_dir)
    if f is None:
        return None
    try:
        return json_read(f)
    except Exception:
        return None


def partition_parquet_files(partition_dir: str) -> List[str]:
    """List the per-year parquet files for one partition."""
    return sorted(glob.glob(os.path.join(partition_dir, '*', '*.parquet')))


def per_granule_null_counts(
    parquet_file: str,
    product_columns: Dict[str, List[str]],
    granule_lookup: Optional[Dict[str, GranuleKey]] = None,
) -> Dict[GranuleKey, Dict[str, int]]:
    """Count nulls per (granule, product) pair within one parquet file.

    Granule grouping uses the ``root_file_l2a`` column (the canonical source-
    granule pointer written by ``h3_write_metadata``). This matches the granule
    identifiers stored in partition metadata exactly, avoiding any shot_number
    decoding ambiguity.

    Memory pillar (v0.8.x lessons applied):
      * **Streamed row-group at a time** via ``pq.ParquetFile.iter_batches``
        with ``pre_buffer=True`` and a small ``batch_size`` — not the full
        ``pd.read_parquet(columns=cols_to_read)`` that used to materialize
        every product column × millions of rows in one shot (multiple GB
        per file on continental partitions).
      * Each batch's null counts are aggregated into a small accumulator
        dict (``O(num_granules × num_products)``) and the batch is dropped
        before the next is read — so worker RSS is bounded by one batch
        regardless of file size.
      * Native arrow null counting + ``groupby`` on a per-batch
        DataFrame; no full-file ``isnull().any(axis=1)`` allocation.
      * ``pa.default_memory_pool().release_unused()`` at end so the
        worker doesn't drag transient buffers into the next task.

    Parameters
    ----------
    parquet_file : str
        Path to a partition parquet file.
    product_columns : dict
        Map of product code → list of product-suffixed column names to scan.
    granule_lookup : dict, optional
        Precomputed map ``{root_file_l2a_basename: (orbit, granule, track)}``.
        If omitted, GEDIFile is invoked once per unique filename.

    Returns
    -------
    dict
        ``{(orbit, orbit_granule, track): {product_code: null_count}}``. Only
        granule × product combos with at least one null are returned.
    """
    import pyarrow.parquet as pq
    from ..gedidriver import GEDIFile

    cols_to_read = ['shot_number', 'root_file_l2a']
    for cols in product_columns.values():
        cols_to_read.extend(cols)
    cols_to_read = list(dict.fromkeys(cols_to_read))

    if granule_lookup is None:
        granule_lookup = {}
    out: Dict[GranuleKey, Dict[str, int]] = {}

    pf = None
    try:
        pf = pq.ParquetFile(parquet_file, pre_buffer=True)
        schema_names = set(pf.schema_arrow.names)
        if 'root_file_l2a' not in schema_names:
            return {}

        # Drop columns the file doesn't carry (bare-DB partitions for
        # products that haven't been merged in yet).
        cols_actually_present = [c for c in cols_to_read if c in schema_names]
        if 'root_file_l2a' not in cols_actually_present:
            return {}

        # Per-product list of columns that exist in this file. Computed
        # once before the iter_batches loop.
        present_per_product = {
            prod: [c for c in cols if c in schema_names]
            for prod, cols in product_columns.items()
        }
        present_per_product = {p: cs for p, cs in present_per_product.items() if cs}
        if not present_per_product:
            return {}

        for batch in pf.iter_batches(
            batch_size=50_000,
            columns=cols_actually_present,
            use_threads=False,
        ):
            # Convert to pandas only for the per-batch groupby — the
            # whole-file equivalent was the memory hog.
            df = batch.to_pandas()
            if df.empty:
                del df, batch
                continue
            for prod, present_cols in present_per_product.items():
                null_mask = df[present_cols].isnull().any(axis=1)
                if not null_mask.any():
                    continue
                nulls_by_file = df.loc[null_mask].groupby('root_file_l2a').size()
                for fname, count in nulls_by_file.items():
                    key = granule_lookup.get(fname)
                    if key is None:
                        gf = GEDIFile(fname)
                        key = (gf.orbit, gf.orbit_granule, gf.track)
                        granule_lookup[fname] = key
                    cur = out.setdefault(key, {})
                    cur[prod] = cur.get(prod, 0) + int(count)
            del df, batch
    finally:
        if pf is not None:
            try:
                pf.close()
            except Exception:
                pass
            del pf
        release_arrow_pool()

    return out


def product_columns_in_schema(columns: Iterable[str], products: Iterable[str]) -> Dict[str, List[str]]:
    """Group columns by product suffix.

    Returns ``{product_code: [columns...]}`` for each product whose suffix
    appears at least once. Products with no matching column are omitted.
    """
    cols = list(columns)
    out: Dict[str, List[str]] = {}
    for prod in products:
        suffix = f"_{prod.lower()}"
        matched = [c for c in cols if str(c).lower().endswith(suffix)]
        if matched:
            out[prod] = matched
    return out
