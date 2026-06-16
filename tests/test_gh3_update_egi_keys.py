"""Regression tests for gh3_update's EGI partition-file -> H3 source mapping.

``_update_egi_partitions`` resolves each dataset partition file to its source
H3 partitions through the ``egi_to_h3`` dict built by ``_prepare_egi_loading``.
Two bugs made that lookup always miss, so every update wrote all-NaN columns
and reported success:

  1. ``egi_to_h3`` is keyed by the *numeric* EGI hash (``egi_tiles.index``
     values), but the lookup used the filename stem *string* — str never
     equals uint64, so ``.get`` returned ``[]`` for every file.
  2. The dataset's ``egi_partition_level`` was read from metadata but never
     passed to ``_prepare_egi_loading``, which therefore built the map at the
     default level 12 — mismatching any dataset partitioned at another level
     even once the key type was fixed.
"""
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point


EGI_HASH = np.uint64(12077045000000000000)
SN_COL = 'shot_number_l2a'
NEW_COL = 'rh_098_l2a'


def _write_partition_file(fpath, n=3):
    """EGI dataset partition files are GeoParquet (geometry column kept)."""
    gdf = gpd.GeoDataFrame({
        SN_COL: np.arange(101, 101 + n, dtype=np.int64),
        'agbd_l4a': np.linspace(10.0, 30.0, n),
    }, geometry=[Point(-50.5, 0.5)] * n, crs=4326)
    gdf.to_parquet(fpath)
    return fpath


@pytest.fixture
def egi_dataset_file(tmp_dir):
    """One EGI partition file named by its decimal hash, as _write_dataframe
    produces (oname = str(part_id))."""
    return _write_partition_file(os.path.join(tmp_dir, f"{EGI_HASH}.parquet"))


def _run_update(egi_dataset_file, monkeypatch, egi_partition_level):
    import logging

    import gedih3.gh3driver as gh3
    import gedih3.utils
    from gedih3.cli.gh3_update import _update_egi_partitions

    captured = {}

    def fake_prepare(region, db_path, partition_level=12):
        captured['partition_level'] = partition_level
        # Keys as produced for real: numeric values from egi_tiles.index.
        return None, {EGI_HASH: ['838040fffffffff']}, 'h3_03', None

    def fake_load_hex(h3_dir, columns=None, **kwargs):
        return pd.DataFrame({
            SN_COL: np.array([101, 102, 103], dtype=np.int64),
            NEW_COL: [5.0, 6.0, 7.0],
        })

    monkeypatch.setattr(gh3, '_prepare_egi_loading', fake_prepare)
    monkeypatch.setattr(gh3, 'gh3_load_hex', fake_load_hex)
    monkeypatch.setattr(gedih3.utils, 'smart_exists', lambda p: True)

    _update_egi_partitions(
        dataset_path=os.path.dirname(egi_dataset_file),
        db_path='/fake/db',
        data_files=[egi_dataset_file],
        fmt='parquet',
        new_cols=[NEW_COL],
        sn_col=SN_COL,
        query_filter=None,
        extra_filter_cols=[],
        dataset_meta={'egi_partition_level': egi_partition_level},
        logger=logging.getLogger('test'),
    )
    return captured


class TestUpdateEgiPartitionKeyLookup:
    def test_numeric_filename_matches_numeric_keys(self, egi_dataset_file, monkeypatch):
        """The filename stem (decimal string) must resolve against the
        numeric egi_to_h3 keys — previously every lookup missed and the new
        column was silently NaN-filled."""
        _run_update(egi_dataset_file, monkeypatch, egi_partition_level=12)

        out = pd.read_parquet(egi_dataset_file)
        assert NEW_COL in out.columns
        assert out[NEW_COL].tolist() == [5.0, 6.0, 7.0], (
            "new column NaN-filled — the egi_to_h3 lookup missed (str vs "
            "numeric key mismatch)"
        )

    def test_dataset_partition_level_threaded(self, egi_dataset_file, monkeypatch):
        """_prepare_egi_loading must receive the dataset's egi_partition_level
        so the egi_to_h3 keys match the filename hashes' level."""
        captured = _run_update(egi_dataset_file, monkeypatch, egi_partition_level=10)
        assert captured['partition_level'] == 10

    def test_non_numeric_filename_skipped_gracefully(self, tmp_dir, monkeypatch):
        """A stray non-EGI file in the dataset dir falls back to NaN-fill
        with a warning instead of crashing."""
        fpath = _write_partition_file(os.path.join(tmp_dir, "notes.parquet"), n=1)

        _run_update(fpath, monkeypatch, egi_partition_level=12)

        out = pd.read_parquet(fpath)
        assert out[NEW_COL].isna().all()
