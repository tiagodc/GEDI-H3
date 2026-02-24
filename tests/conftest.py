"""
Shared pytest fixtures for gedih3 test suite.

Provides reusable fixtures for temporary directories, sample DataFrames,
and Dask DataFrames used across multiple test modules.
"""

import shutil
import tempfile

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point


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
    df = pd.DataFrame(
        {
            "lat_lowestmode_l2a": lats,
            "lon_lowestmode_l2a": lons,
            "agbd_l4a": np.random.uniform(0, 300, n),
            "rh_098_l2a": np.random.uniform(0, 50, n),
            "shot_number_l2a": np.arange(n, dtype=np.int64),
            "quality_flag_l2a": np.random.choice([0, 1], n),
            "h3_03": ["83184bfffffffff"] * n,
        }
    )
    geometry = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")


@pytest.fixture
def sample_ddf(sample_gdf):
    """Dask GeoDataFrame version of sample_gdf."""
    import dask_geopandas

    return dask_geopandas.from_geopandas(sample_gdf, npartitions=4)
