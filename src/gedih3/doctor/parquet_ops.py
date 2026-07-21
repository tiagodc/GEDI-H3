# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Streaming parquet operations specific to gh3_doctor.

Both functions process parquet files row-group by row-group to bound memory.
They live here (not in :mod:`gedih3.utils`) so the doctor introduces no edits
to currently-working tools. Promotion to a shared utility is a one-line move
if other modules ever need them.

- :func:`parquet_fill_columns` is the **fill** counterpart of
  :func:`gedih3.utils.parquet_join_columns`. Where the latter drops columns
  that already exist in the base file, this one merges them via
  :meth:`pandas.Series.combine_first` so non-NaN values in the base are
  preserved and NaN values get filled from the patch.
- :func:`parquet_dedup_partition` rewrites a single parquet file dropping
  duplicate ``shot_number`` rows, keeping the first occurrence.
"""

from __future__ import annotations

import os
from typing import List, Optional

from ..utils import release_arrow_pool


def parquet_fill_columns(
    base_file: str,
    patch_files: List[str],
    ofile: Optional[str] = None,
    key_col: str = 'shot_number',
    tmp_suffix: str = '.fill.tmp',
) -> str:
    """Fill NaN values in ``base_file`` from one or more ``patch_files``.

    For each column shared by the base and patch:

    - existing non-NaN values in the base are preserved.
    - NaN values in the base are replaced where the patch provides a value
      for the same ``key_col``.

    Columns present only in the patch are appended (left-join).
    Columns present only in the base are unchanged.

    The output is streamed row-group by row-group; only one row group plus the
    full patch frames are held in memory at once. Patches are read in full,
    indexed by ``key_col``, so callers should pre-restrict patches to one
    partition's relevant rows.

    Parameters
    ----------
    base_file : str
        Existing parquet file. Determines schema base, row order, row group size.
    patch_files : list of str
        Parquet files containing ``key_col`` plus the columns to merge in.
    ofile : str, optional
        Output path. Defaults to ``base_file`` (in-place rewrite via temp).
    key_col : str, default 'shot_number'
        Join key (column, not index).
    tmp_suffix : str
        Suffix for the temp file used during atomic rewrite.

    Returns
    -------
    str
        Path to the written file.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not patch_files:
        raise ValueError("patch_files must contain at least one path")

    if ofile is None:
        ofile = base_file

    # pre_buffer=True coalesces per-row-group column-chunk reads on
    # shared GPFS — same lesson as parquet_merge_files (commit e8a966b).
    base_pf = pq.ParquetFile(base_file, pre_buffer=True)
    base_schema = base_pf.schema_arrow
    base_cols = list(base_schema.names)
    base_col_set = set(base_cols)

    # Load patch frames; track columns shared with base (filled) vs new (appended).
    fill_data = {}        # col_name -> Series indexed by key_col
    append_data = {}      # patch_file -> DataFrame of new columns indexed by key_col
    appended_fields = []  # arrow Fields for the combined schema

    for pf_path in patch_files:
        pf_schema = pq.read_schema(pf_path)
        pf_cols = list(pf_schema.names)
        if key_col not in pf_cols:
            raise ValueError(f"patch file {pf_path!r} missing key column {key_col!r}")

        fill_cols = [c for c in pf_cols if c in base_col_set and c != key_col]
        new_cols = [c for c in pf_cols if c not in base_col_set and c != key_col]

        if not fill_cols and not new_cols:
            continue

        cols_to_load = [key_col] + fill_cols + new_cols
        df = pd.read_parquet(pf_path, columns=cols_to_load)
        df = df.set_index(key_col)
        df = df[~df.index.duplicated(keep='first')]

        for c in fill_cols:
            # Last patch wins for fills targeting the same column.
            fill_data[c] = df[c]

        if new_cols:
            append_data[pf_path] = df[new_cols]
            for c in new_cols:
                base_col_set.add(c)
                appended_fields.append(pf_schema.field(c))

    combined_schema = base_schema
    for f in appended_fields:
        combined_schema = combined_schema.append(f)
    if base_schema.metadata:
        combined_schema = combined_schema.with_metadata(base_schema.metadata)

    pardir = os.path.dirname(ofile) or '.'
    os.makedirs(pardir, exist_ok=True)
    temp_ofile = ofile + tmp_suffix

    with pq.ParquetWriter(temp_ofile, combined_schema, compression='zstd') as writer:
        for rg_idx in range(base_pf.metadata.num_row_groups):
            batch = base_pf.read_row_group(rg_idx).to_pandas()

            idx_name = batch.index.name
            if idx_name:
                batch = batch.reset_index()

            batch_indexed = batch.set_index(key_col)

            # Fill: combine_first on shared columns. Existing non-NaN in the base
            # wins; NaN in the base gets filled from the patch.
            for col, patch_series in fill_data.items():
                if col in batch_indexed.columns:
                    aligned = patch_series.reindex(batch_indexed.index)
                    batch_indexed[col] = batch_indexed[col].combine_first(aligned)

            # Append: left-join new columns.
            for new_df in append_data.values():
                batch_indexed = batch_indexed.join(new_df, how='left')

            batch = batch_indexed.reset_index()
            if idx_name:
                batch = batch.set_index(idx_name)

            cols_to_select = [c for c in combined_schema.names if c in batch.columns]
            batch = batch[cols_to_select]

            writer.write_table(pa.Table.from_pandas(batch, schema=combined_schema))

    base_pf.close()
    del base_pf
    # Drain pyarrow's allocator now so the temp buffers don't drag into
    # the next worker task. Mirrors parquet_merge_files (utils.py).
    release_arrow_pool()
    try:
        os.replace(temp_ofile, ofile)
    except OSError:
        if os.path.exists(temp_ofile):
            os.unlink(temp_ofile)
        raise

    return ofile


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
