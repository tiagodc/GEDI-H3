"""Tests for the streaming parquet operations used by gh3_doctor."""

import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gedih3.doctor.parquet_ops import parquet_fill_columns, parquet_dedup_partition


def _write(path, df, row_group_size=None):
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=row_group_size)


def test_fill_columns_preserves_existing_non_nan(tmp_dir):
    base = pd.DataFrame({
        'shot_number': [1, 2, 3, 4, 5],
        'rh_98_l2a': [10.0, 20.0, 30.0, 40.0, 50.0],
        'agbd_l4a': [1.0, np.nan, 3.0, np.nan, np.nan],
    })
    base_file = os.path.join(tmp_dir, 'base.parquet')
    _write(base_file, base, row_group_size=2)

    patch = pd.DataFrame({
        'shot_number': [2, 3, 4, 5, 6],
        'agbd_l4a': [222.0, 333.0, 444.0, 555.0, 999.0],
    })
    patch_file = os.path.join(tmp_dir, 'patch.parquet')
    _write(patch_file, patch)

    parquet_fill_columns(base_file, [patch_file])
    out = pd.read_parquet(base_file).set_index('shot_number')

    # shot 1 had non-NaN agbd; preserved
    assert out.loc[1, 'agbd_l4a'] == 1.0
    # shot 2 had NaN; filled from patch
    assert out.loc[2, 'agbd_l4a'] == 222.0
    # shot 3 had non-NaN; PATCH MUST NOT OVERWRITE (the contract)
    assert out.loc[3, 'agbd_l4a'] == 3.0
    # shot 4 had NaN; filled
    assert out.loc[4, 'agbd_l4a'] == 444.0
    # shot 5 had NaN; filled
    assert out.loc[5, 'agbd_l4a'] == 555.0


def test_fill_columns_appends_new_columns(tmp_dir):
    base = pd.DataFrame({'shot_number': [1, 2, 3], 'rh_98_l2a': [10.0, 20.0, 30.0]})
    base_file = os.path.join(tmp_dir, 'base.parquet')
    _write(base_file, base)

    patch = pd.DataFrame({'shot_number': [1, 2, 3], 'wsci_l4c': [0.1, 0.2, 0.3]})
    patch_file = os.path.join(tmp_dir, 'patch.parquet')
    _write(patch_file, patch)

    parquet_fill_columns(base_file, [patch_file])
    out = pd.read_parquet(base_file).set_index('shot_number')

    assert 'wsci_l4c' in out.columns
    assert list(out['wsci_l4c']) == [0.1, 0.2, 0.3]


def test_fill_columns_streams_multi_row_groups(tmp_dir):
    """Confirm the streaming implementation handles >1 row group correctly."""
    base = pd.DataFrame({
        'shot_number': list(range(1, 11)),
        'agbd_l4a': [1.0, np.nan, 3.0, np.nan, 5.0, np.nan, 7.0, np.nan, 9.0, np.nan],
    })
    base_file = os.path.join(tmp_dir, 'base.parquet')
    _write(base_file, base, row_group_size=3)
    assert pq.ParquetFile(base_file).metadata.num_row_groups > 1

    patch = pd.DataFrame({'shot_number': list(range(1, 11)),
                          'agbd_l4a': [111.0] * 10})
    patch_file = os.path.join(tmp_dir, 'patch.parquet')
    _write(patch_file, patch)

    parquet_fill_columns(base_file, [patch_file])
    out = pd.read_parquet(base_file).set_index('shot_number')
    # Existing values preserved; NaNs filled with 111.
    assert out.loc[1, 'agbd_l4a'] == 1.0
    assert out.loc[2, 'agbd_l4a'] == 111.0
    assert out.loc[10, 'agbd_l4a'] == 111.0


def test_dedup_keep_first(tmp_dir):
    df = pd.DataFrame({
        'shot_number': [1, 2, 2, 3, 4, 1, 5],
        'val': [10, 20, 22, 30, 40, 11, 50],
    })
    f = os.path.join(tmp_dir, 'd.parquet')
    _write(f, df, row_group_size=3)

    dropped = parquet_dedup_partition(f, keep='first')
    assert dropped == 2
    out = pd.read_parquet(f)
    assert list(out['shot_number']) == [1, 2, 3, 4, 5]
    assert list(out['val']) == [10, 20, 30, 40, 50]


def test_dedup_keep_last(tmp_dir):
    df = pd.DataFrame({
        'shot_number': [1, 2, 2, 3, 1, 4],
        'val': [10, 20, 22, 30, 11, 40],
    })
    f = os.path.join(tmp_dir, 'd.parquet')
    _write(f, df, row_group_size=2)

    dropped = parquet_dedup_partition(f, keep='last')
    assert dropped == 2
    out = pd.read_parquet(f).sort_values('shot_number').reset_index(drop=True)
    assert list(out['shot_number']) == [1, 2, 3, 4]
    assert out.set_index('shot_number').loc[1, 'val'] == 11
    assert out.set_index('shot_number').loc[2, 'val'] == 22


def test_dedup_no_duplicates_is_noop(tmp_dir):
    df = pd.DataFrame({'shot_number': [1, 2, 3], 'val': [10, 20, 30]})
    f = os.path.join(tmp_dir, 'clean.parquet')
    _write(f, df)
    dropped = parquet_dedup_partition(f)
    assert dropped == 0
    out = pd.read_parquet(f)
    assert len(out) == 3
