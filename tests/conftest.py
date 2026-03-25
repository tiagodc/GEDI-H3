"""
Shared pytest fixtures for gedih3 test suite.

Provides reusable fixtures for temporary directories, sample DataFrames,
synthetic database helpers, and Dask DataFrames used across multiple test modules.
"""

import os
import json
import pytest
import tempfile
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope='session')
def persistent_test_dir():
    """If GH3_TEST_OUTPUT_DIR is set, use it (no cleanup). Otherwise None."""
    override = os.environ.get('GH3_TEST_OUTPUT_DIR')
    if override:
        os.makedirs(override, exist_ok=True)
        return Path(override)
    return None


@pytest.fixture
def tmp_dir():
    """Temporary directory with automatic cleanup."""
    d = tempfile.mkdtemp(prefix="gedih3_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_gdf():
    """Minimal GeoDataFrame with GEDI-like columns."""
    n = 100
    np.random.seed(42)
    lats = np.random.uniform(-10, 10, n)
    lons = np.random.uniform(-60, -50, n)
    df = pd.DataFrame({
        'lat_lowestmode_l2a': lats,
        'lon_lowestmode_l2a': lons,
        'agbd_l4a': np.random.uniform(0, 300, n),
        'rh_098_l2a': np.random.uniform(0, 50, n),
        'shot_number_l2a': np.arange(n, dtype=np.int64),
        'quality_flag_l2a': np.random.choice([0, 1], n),
        'h3_03': ['83184bfffffffff'] * n,
    })
    geometry = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    return gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')


@pytest.fixture
def sample_ddf(sample_gdf):
    """Dask GeoDataFrame version of sample_gdf."""
    import dask_geopandas
    return dask_geopandas.from_geopandas(sample_gdf, npartitions=4)


# ---------------------------------------------------------------------------
# Synthetic database helpers (shared across test modules)
# ---------------------------------------------------------------------------

def make_gedi_parquet(path, n=50, extra_cols=None, shot_offset=0):
    """Create a minimal GEDI-like GeoParquet file.

    Parameters
    ----------
    path : str
        Output parquet file path.
    n : int
        Number of rows.
    extra_cols : dict, optional
        Additional columns {name: values_or_None}.
    shot_offset : int
        Starting shot_number value.

    Returns
    -------
    GeoDataFrame
    """
    np.random.seed(42 + shot_offset)
    lats = np.random.uniform(-1, 1, n)
    lons = np.random.uniform(-51, -50, n)
    data = {
        'shot_number': np.arange(shot_offset, shot_offset + n, dtype=np.uint64),
        'agbd_l4a': np.random.uniform(0, 300, n),
        'rh_098_l2a': np.random.uniform(0, 50, n),
    }
    if extra_cols:
        for col, vals in extra_cols.items():
            data[col] = vals if vals is not None else np.random.uniform(0, 1, n)
    geometry = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    gdf = gpd.GeoDataFrame(data, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(path)
    return gdf


def make_partition_dir(base_dir, h3_part='83184bfffffffff', year='2020',
                       n=50, extra_cols=None, shot_offset=0, granules=None):
    """Create an H3 partition directory with parquet + metadata files.

    Returns
    -------
    tuple
        (partition_dir_path, parquet_file_path)
    """
    part_dir = os.path.join(base_dir, f'h3_03={h3_part}')
    year_dir = os.path.join(part_dir, f'year={year}')
    os.makedirs(year_dir, exist_ok=True)

    pq_path = os.path.join(year_dir, f'{h3_part}.{year}.0.parquet')
    gdf = make_gedi_parquet(pq_path, n=n, extra_cols=extra_cols,
                            shot_offset=shot_offset)

    if granules is None:
        granules = [{'orbit': 1, 'granule': 1, 'track': 1}]

    meta = {
        'h3_partition': h3_part,
        'columns': list(gdf.columns),
        'granules': granules,
        'date_range': ['2020-01-01', '2020-03-31'],
        'l2a_version': 2,
    }
    meta_path = os.path.join(part_dir, f'{h3_part}.metadata.json')
    with open(meta_path, 'w') as f:
        json.dump(meta, f)

    return part_dir, pq_path


def make_build_log(log_dir, status='COMPLETED', products=None,
                   granules=None, gedi_version=2, h3_resolution=12,
                   h3_partition=3, spatial=None, temporal=None,
                   h3_partition_ids=None, h3_columns=None,
                   date_range=None, pending_var_update=None):
    """Create a synthetic build log file.

    Returns
    -------
    str
        Path to the created log file.
    """
    if products is None:
        products = {
            'L2A': {'status': status, 'variables': ['rh_098', 'shot_number']},
            'L4A': {'status': status, 'variables': ['agbd', 'shot_number']},
        }
    if granules is None:
        granules = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'INDEXED'},
        ]
    if spatial is None:
        spatial = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-50,0],[-50,1],[-51,1],[-51,0],[-50,0]]]}}]}'

    log = {
        'metadata': {'package_version': '0.1.1'},
        'gedi_version': gedi_version,
        'status': status,
        'h3_resolution_level': h3_resolution,
        'h3_partition_level': h3_partition,
        'spatial_filter': spatial,
        'temporal_filter': temporal or ['2020-01-01', '2020-03-31'],
        'products': products,
        'granules': granules,
        'h3_partition_ids': h3_partition_ids or ['838041fffffffff'],
        'h3_columns': h3_columns or ['agbd_l4a', 'rh_098_l2a', 'geometry', 'datetime'],
        'date_range': date_range or ['2020-01-11', '2020-03-15'],
    }
    if pending_var_update:
        log['_pending_variable_update'] = pending_var_update

    log_path = os.path.join(log_dir, 'gedih3_build_log.json')
    os.makedirs(log_dir, exist_ok=True)
    with open(log_path, 'w') as f:
        json.dump(log, f)
    return log_path


@pytest.fixture
def mini_h3_database(tmp_dir):
    """Synthetic H3 database: 2 partitions, build log, metadata.

    Returns the database root directory path.
    """
    db_dir = os.path.join(tmp_dir, 'h3_database')
    os.makedirs(db_dir, exist_ok=True)

    cell_a = '83184bfffffffff'
    cell_b = '83184afffffffff'

    make_partition_dir(db_dir, h3_part=cell_a, n=30, shot_offset=0,
                       granules=[{'orbit': 1, 'granule': 1, 'track': 1}])
    make_partition_dir(db_dir, h3_part=cell_b, n=20, shot_offset=30,
                       granules=[{'orbit': 2, 'granule': 1, 'track': 2}])

    make_build_log(
        db_dir,
        h3_partition_ids=[cell_a, cell_b],
        granules=[
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'INDEXED'},
        ],
        h3_columns=['shot_number', 'agbd_l4a', 'rh_098_l2a', 'geometry'],
        date_range=['2020-01-01', '2020-03-31'],
    )
    return db_dir


@pytest.fixture
def mini_extracted_dataset(tmp_dir):
    """Synthetic extracted dataset with gedih3_dataset.json metadata.

    Returns the dataset directory path.
    """
    ds_dir = os.path.join(tmp_dir, 'extracted')
    os.makedirs(ds_dir, exist_ok=True)

    # Write 2 partition files
    for i, part_id in enumerate(['83184bfffffffff', '83184afffffffff']):
        make_gedi_parquet(
            os.path.join(ds_dir, f'{part_id}.parquet'),
            n=20, shot_offset=i * 20,
        )

    # Write dataset metadata
    meta = {
        'index_type': 'h3',
        'index_level': 12,
        'partition_level': 3,
        'partition_ids': ['83184bfffffffff', '83184afffffffff'],
        'columns': ['shot_number', 'agbd_l4a', 'rh_098_l2a'],
        'file_format': 'parquet',
        'source_database': '/mock/h3_database',
        'tool': 'gh3_extract',
    }
    with open(os.path.join(ds_dir, 'gedih3_dataset.json'), 'w') as f:
        json.dump(meta, f)

    return ds_dir
