"""Regression tests for the H3 load → metadata cascade bug + the
EGI merge-mode `is_file_path=True` makedirs bug.

Bug history: `_meta_from_dtype_dict` produced a meta without a named index,
which made the lazy ddf's `index.name = None` while each computed partition
had a proper `h3_12` index. `_detect_export_params` then inferred the wrong
`index_level` from the only h3 column present (the partition column h3_03)
and wrote it into the simplified-dataset sidecar. On subsequent loads of
that sidecar, `_load_dataset` saw the file's `h3_12` index disagree with
the sidecar's `index_level: 3` and "restored" (destroyed) the correct
index by replacing it with the h3_03 partition column. Downstream
`gh3_aggregate` then hit `H3ResMismatchError: Invalid parent resolution
4 for cell ...` (asking for an L4 parent of an L3 cell).

These tests cover the two surgical fixes:
  1. `_meta_from_dtype_dict(index_name=...)` sets the index name.
  2. `_load_dataset` trusts an already-correctly-named spatial index even
     when the sidecar says otherwise.
"""
from __future__ import annotations
import json
import os

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from gedih3.gh3driver import _meta_from_dtype_dict
from gedih3.cliutils import load_data_from_source


def test_meta_from_dtype_dict_sets_index_name():
    """Bug regression: synthetic meta must carry the named index that
    the parquet reader will produce at compute time, so the lazy ddf
    metadata matches each computed partition."""
    col_dtypes = {
        'rh_098_l2a': 'float',
        'shot_number': 'uint64',
        'geometry': 'binary',
    }
    meta = _meta_from_dtype_dict(
        col_dtypes,
        columns=['rh_098_l2a'],
        part_col='h3_03',
        index_name='h3_12',
    )
    assert meta is not None
    assert meta.index.name == 'h3_12', \
        f"meta must adopt the requested index name; got {meta.index.name!r}"
    # part_col still appended as a regular column (gh3_load_hex adds it post-read)
    assert 'h3_03' in meta.columns
    # data column present
    assert 'rh_098_l2a' in meta.columns


def test_meta_from_dtype_dict_without_index_name_is_unnamed():
    """No index_name arg → index unchanged (default RangeIndex, no name)."""
    meta = _meta_from_dtype_dict(
        {'rh_098_l2a': 'float'},
        columns=['rh_098_l2a'],
        part_col='h3_03',
    )
    assert meta is not None
    assert meta.index.name is None


def test_load_dataset_trusts_file_index_over_wrong_sidecar(tmp_path):
    """Bug regression: when the sidecar says index_level=3 but the parquet
    files actually have h3_12 as the named index, `_load_dataset` must
    trust the file. Pre-fix, the loader "restored" the wrong sidecar
    index, demoting h3_12 → h3_03 silently."""
    d = tmp_path / "fake_h3_dataset"
    d.mkdir()

    # Two partitions, each with h3_12 named index and h3_03 partition column.
    # Cell hashes are synthetic but use the expected length/prefix pattern.
    for h3_03 in ('830e41fffffffff', '830e43fffffffff'):
        h3_12_cells = [f'8c{h3_03[2:14]}{c}ff' for c in ('a', 'b', 'c')]
        df = pd.DataFrame(
            {'rh_098_l2a': [10.0, 20.0, 30.0],
             'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)],
             'h3_03': [h3_03] * 3},
            index=pd.Index(h3_12_cells, name='h3_12'),
        )
        gpd.GeoDataFrame(df, geometry='geometry', crs=4326).to_parquet(
            d / f"{h3_03}.parquet"
        )

    # Sidecar with the buggy index_level (this is exactly what the pre-fix
    # extract was writing, and what existing on-disk datasets will have).
    sidecar = {
        'metadata': {'package_version': 'test', 'format': 'simplified'},
        'file_format': 'parquet',
        'index_type': 'h3',
        'index_level': 3,  # WRONG — files are indexed at h3_12
        'columns': ['rh_098_l2a', 'geometry', 'h3_03'],
        'partition_ids': ['830e41fffffffff', '830e43fffffffff'],
        'h3_partition_level': 3,
    }
    (d / 'gedih3_dataset.json').write_text(json.dumps(sidecar))

    ddf = load_data_from_source(
        str(d), columns=['rh_098_l2a'], region=None, query=None, logger=None
    )

    assert ddf.index.name == 'h3_12', (
        f"loader must trust the parquet's h3_12 index even when sidecar lies; "
        f"got {ddf.index.name!r}"
    )

    # Each computed partition must also keep h3_12 as its index name.
    p0 = ddf.partitions[0].compute()
    assert p0.index.name == 'h3_12'
    # Original data column survives.
    assert 'rh_098_l2a' in p0.columns


def test_egi_export_part_merge_mode_writes_a_file_not_a_dir(tmp_path):
    """Bug regression: `egi_export_part(..., is_file_path=True)` (the merge-mode
    entry point from `gh3_export`) used to call `os.makedirs(odir, exist_ok=True)`
    unconditionally, turning the user's merge file path into a *directory*
    before `_write_egi_file`'s AtomicFileWriter tried to `os.replace` the
    `.tmp` file onto it — IsADirectoryError, every merge run aborts after the
    aggregate is already computed (hours of compute wasted)."""
    import numpy as np
    from gedih3.gh3driver import egi_export_part
    from gedih3.egi.core import to_hash

    output_file = tmp_path / "merged_egi11.parquet"

    # Tiny EGI-indexed grid
    xs = np.linspace(-1_000_000.0, 1_000_000.0, 10)
    ys = np.linspace(-1_000_000.0, 1_000_000.0, 10)
    gx, gy = np.meshgrid(xs, ys)
    gx, gy = gx.ravel(), gy.ravel()
    df = pd.DataFrame(
        {'rh_098_l2a': np.random.uniform(5, 30, len(gx)),
         'geometry': [Point(x, y) for x, y in zip(gx, gy)],
         'egi12': to_hash(gx, gy, level=12)},
        index=pd.Index(to_hash(gx, gy, level=1), name='egi01'),
    )
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:6933')

    result = egi_export_part(gdf, odir=str(output_file), fmt='parquet', is_file_path=True)

    assert os.path.isfile(str(output_file)), (
        "merge-mode output must be a file, not a directory"
    )
    assert not os.path.isdir(str(output_file))
    # Caller contract: returns the written file path.
    assert result == str(output_file)
    # No stale .tmp left behind.
    leftover = [f for f in os.listdir(tmp_path) if '.tmp' in f]
    assert leftover == [], f"unexpected .tmp leftover: {leftover}"
