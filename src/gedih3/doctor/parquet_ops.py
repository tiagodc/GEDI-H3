# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Streaming parquet operations specific to gh3_doctor.

Both functions process parquet files row-group by row-group to bound memory.

- :func:`parquet_fill_columns` now lives in :mod:`gedih3.utils` (the build's
  product backfill uses it too) and is re-exported here: the **fill**
  counterpart of :func:`gedih3.utils.parquet_join_columns` — null/NaN base
  cells take the patch value, existing values are never overwritten.
- :func:`parquet_dedup_partition` rewrites a single parquet file dropping
  duplicate ``shot_number`` rows, keeping the first occurrence.
"""

from __future__ import annotations

import os
from typing import List, Optional

from ..utils import release_arrow_pool, parquet_fill_columns  # noqa: F401  (re-export)


def parquet_dedup_partition(
    pq_file: str,
    key_col: str = 'shot_number',
    keep: str = 'first',
    tmp_suffix: str = '.dedup.tmp',
) -> int:
    """Rewrite a parquet file dropping duplicate ``key_col`` rows.

    Streams row-group by row-group: only one row group plus the cumulative
    set of seen keys is held in memory. ``shot_number`` is int64 so the seen-set
    cost is ~8 bytes/row.

    Parameters
    ----------
    pq_file : str
        Parquet file to rewrite in-place (via temp + atomic rename).
    key_col : str, default 'shot_number'
        Column to deduplicate on.
    keep : {'first', 'last'}, default 'first'
        Which duplicate to keep. ``'last'`` requires a second pass.
    tmp_suffix : str
        Suffix for the temp file used during atomic rewrite.

    Returns
    -------
    int
        Number of rows dropped.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if keep not in ('first', 'last'):
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    pf = pq.ParquetFile(pq_file, pre_buffer=True)
    schema = pf.schema_arrow

    if keep == 'last':
        # Walk once to find the last row-group index for each key, then walk
        # again writing only those rows. Memory cost: one int64 set + one int set.
        last_rg_for_key = {}
        for rg_idx in range(pf.metadata.num_row_groups):
            keys = pf.read_row_group(rg_idx, columns=[key_col]).column(key_col).to_pylist()
            for k in keys:
                last_rg_for_key[k] = rg_idx
        # Group keys to drop by row group: every row whose RG isn't its last
        # must be excluded. Easier path: invert into a "keep" set per RG.
        keep_set_per_rg = {}
        for k, rg in last_rg_for_key.items():
            keep_set_per_rg.setdefault(rg, set()).add(k)
    else:
        keep_set_per_rg = None
        seen = set()

    pardir = os.path.dirname(pq_file) or '.'
    os.makedirs(pardir, exist_ok=True)
    temp_ofile = pq_file + tmp_suffix
    dropped = 0

    with pq.ParquetWriter(temp_ofile, schema, compression='zstd') as writer:
        for rg_idx in range(pf.metadata.num_row_groups):
            table = pf.read_row_group(rg_idx)
            keys = table.column(key_col).to_pylist()

            if keep == 'first':
                mask = []
                for k in keys:
                    if k in seen:
                        mask.append(False)
                    else:
                        seen.add(k)
                        mask.append(True)
            else:
                rg_keep = keep_set_per_rg.get(rg_idx, set())
                mask = [k in rg_keep for k in keys]
                # For 'last' keep semantics, also collapse intra-RG duplicates
                # by tracking which keys we've already emitted in this RG.
                emitted = set()
                final_mask = []
                for keep_row, k in zip(mask, keys):
                    if keep_row and k not in emitted:
                        emitted.add(k)
                        final_mask.append(True)
                    else:
                        final_mask.append(False)
                mask = final_mask

            kept_rows = sum(mask)
            dropped += len(mask) - kept_rows

            if kept_rows == len(mask):
                writer.write_table(table)
            elif kept_rows > 0:
                writer.write_table(table.filter(pa.array(mask)))
            # kept_rows == 0: write nothing for this row group

    pf.close()
    del pf
    release_arrow_pool()
    try:
        os.replace(temp_ofile, pq_file)
    except OSError:
        if os.path.exists(temp_ofile):
            os.unlink(temp_ofile)
        raise

    return dropped
