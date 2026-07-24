"""Region-based partition selection for simplified datasets.

A simplified dataset is one file per spatial partition, so a region tells
you which files can possibly contribute before anything is read. Until
this was wired in, ``_load_dataset`` scheduled *every* file and clipped
rows afterwards — a 1-degree query against a global 12,461-partition
dataset read all 12,461 (25 of them can hold data).

The tests that matter here are the equivalence ones: pruning must never
change the answer, only the work.
"""

import json
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, box

import gedih3.gh3driver as gh3
from gedih3.config import DATASET_META_FILENAME


# ----------------------------------------------------------------- fixtures

def _write_dataset(root, frames, index_type, index_level):
    """Write one parquet per partition + the sidecar, like gh3_extract does."""
    root.mkdir(parents=True, exist_ok=True)
    for pid, gdf in frames.items():
        gdf.to_parquet(root / f"{pid}.parquet")
    meta = {
        'file_format': 'parquet',
        'index_type': index_type,
        'index_level': index_level,
        'columns': sorted(next(iter(frames.values())).columns),
        'partition_ids': sorted(frames),
        'n_files': len(frames),
        'tool': 'test',
    }
    (root / DATASET_META_FILENAME).write_text(json.dumps(meta))
    return str(root)


@pytest.fixture
def h3_dataset(tmp_path):
    """Points in three well-separated H3 L3 partitions."""
    import h3

    sites = [(-51.5, 0.5), (-111.0, 52.8), (135.4, 52.8)]
    frames = {}
    for lon, lat in sites:
        pts = [(lon + dx, lat + dy) for dx in (-0.05, 0.0, 0.05) for dy in (-0.05, 0.0, 0.05)]
        cell = h3.latlng_to_cell(lat, lon, 3)
        gdf = gpd.GeoDataFrame(
            {'value': np.arange(len(pts), dtype='float32'),
             'h3_12': [h3.latlng_to_cell(y, x, 12) for x, y in pts]},
            geometry=[Point(x, y) for x, y in pts],
            crs=4326,
        ).set_index('h3_12')
        frames[cell] = gdf
    return _write_dataset(tmp_path / 'h3ds', frames, 'h3', 12), frames


@pytest.fixture
def egi_dataset(tmp_path):
    """Points in three distinct EGI level-12 partitions."""
    from gedih3 import egi

    sites = [(-51.5, 0.5), (-111.0, 52.8), (135.4, 52.8)]
    frames = {}
    for lon, lat in sites:
        pts = [(lon + dx, lat + dy) for dx in (-0.05, 0.0, 0.05) for dy in (-0.05, 0.0, 0.05)]
        gser = gpd.GeoSeries([Point(x, y) for x, y in pts], crs=4326).to_crs(egi.EGI_CRS_STRING)
        tile = int(egi.to_hash(float(gser.iloc[0].x), float(gser.iloc[0].y), level=12))
        gdf = gpd.GeoDataFrame(
            {'value': np.arange(len(pts), dtype='float32')},
            geometry=[Point(x, y) for x, y in pts],
            crs=4326,
        )
        frames[str(tile)] = gdf
    return _write_dataset(tmp_path / 'egids', frames, 'egi', 1), frames


# ------------------------------------------------------------ id classifier

def test_partition_kind_detection():
    assert gh3._dataset_partition_kind(['830e41fffffffff', '830e43fffffffff']) == 'h3'
    assert gh3._dataset_partition_kind(['12034056000000000000']) == 'egi'
    # An EGI hash fed to h3.is_valid_cell raises OverflowError — must not escape
    assert gh3._dataset_partition_kind(['tile_a', 'tile_b']) is None
    assert gh3._dataset_partition_kind(['830e41fffffffff', 'tile_b']) is None
    assert gh3._dataset_partition_kind([]) is None


# --------------------------------------------------------------- selection

def test_h3_selection_keeps_only_touched_partitions(h3_dataset):
    path, frames = h3_dataset
    files = sorted(str(p) for p in __import__('pathlib').Path(path).glob('*.parquet'))
    sel = gh3._select_dataset_files(files, [-52, 0, -51, 1])
    assert len(sel) < len(files)
    assert all(os.path.basename(f).startswith('83') for f in sel)


def test_egi_selection_keeps_only_touched_partitions(egi_dataset):
    path, frames = egi_dataset
    files = sorted(str(p) for p in __import__('pathlib').Path(path).glob('*.parquet'))
    sel = gh3._select_dataset_files(files, [-52, 0, -51, 1])
    assert 0 < len(sel) < len(files)


def test_no_region_returns_input_untouched():
    files = ['/d/830e41fffffffff.parquet']
    assert gh3._select_dataset_files(files, None) is files


def test_unparseable_names_fall_back_to_all_files():
    files = ['/d/tile_a.parquet', '/d/tile_b.parquet']
    assert gh3._select_dataset_files(files, [-52, 0, -51, 1]) == files


def test_empty_intersection_keeps_one_file_for_schema(h3_dataset):
    path, _ = h3_dataset
    files = sorted(str(p) for p in __import__('pathlib').Path(path).glob('*.parquet'))
    sel = gh3._select_dataset_files(files, [100, -80, 101, -79])
    assert len(sel) == 1  # graph stays well-formed; the clip yields no rows


# ------------------------------------------------------------- equivalence

def _rows(df):
    return sorted(zip(df['value'].tolist(), df.geometry.x.round(6), df.geometry.y.round(6)))


@pytest.mark.parametrize('lazy', [True, False])
def test_pruning_does_not_change_the_answer(h3_dataset, monkeypatch, lazy):
    """The whole point: same rows, fewer files."""
    path, _ = h3_dataset
    region = [-52, 0, -51, 1]

    pruned = gh3._load_dataset(path, region=region, lazy=lazy)
    if lazy:
        pruned = pruned.compute()

    monkeypatch.setattr(gh3, '_select_dataset_files', lambda files, region, logger=None: files)
    unpruned = gh3._load_dataset(path, region=region, lazy=lazy)
    if lazy:
        unpruned = unpruned.compute()

    assert _rows(pruned) == _rows(unpruned)
    assert len(pruned) > 0


def test_eager_mode_applies_the_region_clip(h3_dataset):
    """Eager loads used to hand back unfiltered data."""
    path, frames = h3_dataset
    total = sum(len(g) for g in frames.values())

    clipped = gh3._load_dataset(path, region=[-52, 0, -51, 1], lazy=False)
    assert 0 < len(clipped) < total
    roi = box(-52, 0, -51, 1)
    assert clipped.geometry.within(roi).all()


def test_egi_dataset_load_is_pruned_and_correct(egi_dataset):
    path, frames = egi_dataset
    region = [-52, 0, -51, 1]

    got = gh3._load_dataset(path, region=region, lazy=True).compute()
    assert len(got) > 0
    assert got.geometry.within(box(*region)).all()
