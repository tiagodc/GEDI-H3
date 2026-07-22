"""
Regression tests for the object-Series returned by the tiled rasterizers.

rasterize_partition (EGI) and rasterize_h3_partition (H3) return a pd.Series
whose elements are xarray Datasets, consumed downstream by map_partitions.
Building that Series with pd.Series([...]) coerces through np.asarray, which
xarray refuses — and both call sites sat inside a broad `except Exception` that
logged at debug, so the failure surfaced as "0 tiles rasterized" with no error.

These tests assert the Series is actually populated, independently of the
pandas version that happens to be installed.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import geopandas as gpd
from shapely.geometry import Point

from gedih3.utils import object_series


# =============================================================================
# Test: the shared constructor
# =============================================================================

class TestObjectSeries:

    def test_holds_datasets_intact(self):
        ds = xr.Dataset({'v': (('y', 'x'), np.zeros((2, 2)))})
        series = object_series([ds, ds])

        assert isinstance(series, pd.Series)
        assert series.dtype == object
        assert len(series) == 2
        assert all(isinstance(item, xr.Dataset) for item in series)
        assert series.iat[0] is ds

    def test_plain_constructor_is_why_this_helper_exists(self):
        """Document the failure mode the helper works around."""
        ds = xr.Dataset({'v': (('y', 'x'), np.zeros((2, 2)))})
        arr = np.empty(2, dtype=object)
        with pytest.raises(TypeError, match='xarray.Dataset into a numpy array'):
            arr[:] = [ds, ds]

    def test_empty(self):
        series = object_series([])
        assert len(series) == 0
        assert series.dtype == object

    def test_preserves_order(self):
        items = [xr.Dataset({'v': (('x',), np.array([i]))}) for i in range(4)]
        series = object_series(items)
        assert [int(s['v'].values[0]) for s in series] == [0, 1, 2, 3]


# =============================================================================
# Test: EGI tiled rasterizer
# =============================================================================

LEVEL = 6


def _egi_gdf(tile_pixel_counts):
    """EGI-indexed GeoDataFrame with `count` pixels in each given outer tile."""
    from gedih3 import egi
    from gedih3.egi.config import LIMITS, OUTER_RES

    hashes, values, points = [], [], []
    value = 0.0
    for (px_outer, py_outer), count in tile_pixel_counts.items():
        res = egi.get_resolution(LEVEL)
        x0 = LIMITS['lon_w'] + px_outer * OUTER_RES
        y0 = LIMITS['lat_s'] + py_outer * OUTER_RES
        for i in range(count):
            x = x0 + res * (i + 0.5)
            y = y0 + res * (i + 0.5)
            hashes.append(egi.to_hash(x, y, level=LEVEL))
            value += 1.0
            values.append(value)
            points.append(Point(x, y))

    return gpd.GeoDataFrame(
        {'val': values}, geometry=points, crs='EPSG:6933',
        index=pd.Index(np.array(hashes, dtype=np.uint64), name=f'egi{LEVEL:02d}'),
    )


class TestEGIRasterizePartition:

    def test_returns_populated_series(self):
        from gedih3 import egi

        rasters = egi.rasterize_partition(_egi_gdf({(100, 50): 5}))

        assert len(rasters) == 1
        assert isinstance(rasters, pd.Series)
        assert all(isinstance(r, xr.Dataset) for r in rasters)
        assert 'val' in rasters.iat[0].data_vars

    def test_one_element_per_outer_tile(self):
        from gedih3 import egi

        rasters = egi.rasterize_partition(_egi_gdf({(100, 50): 5, (101, 50): 2}))

        assert len(rasters) == 2
        assert all(isinstance(r, xr.Dataset) for r in rasters)

    def test_empty_input_returns_empty_series(self):
        from gedih3 import egi

        empty = _egi_gdf({(100, 50): 1}).iloc[0:0]
        rasters = egi.rasterize_partition(empty)

        assert len(rasters) == 0
        assert rasters.dtype == object


# =============================================================================
# Test: H3 tiled rasterizer
# =============================================================================

def _h3_gdf(level=6, n=12):
    """H3-indexed GeoDataFrame of n neighbouring cells at `level`."""
    import h3

    origin = h3.latlng_to_cell(0.5, -50.5, level)
    cells = list(h3.grid_disk(origin, 2))[:n]
    points, values = [], []
    for i, cell in enumerate(cells):
        lat, lon = h3.cell_to_latlng(cell)
        points.append(Point(lon, lat))
        values.append(float(i))

    return gpd.GeoDataFrame(
        {'val': values}, geometry=points, crs='EPSG:4326',
        index=pd.Index(cells, name=f'h3_{level:02d}'),
    )


class TestH3RasterizePartition:

    def test_preaggregated_partition_returns_populated_series(self):
        """partition_level >= h3_level: the whole partition is one tile.

        This is the path gh3_rasterize takes over a gh3_aggregate output — the
        common case, and it had no test coverage at all.
        """
        from gedih3.raster.h3_raster import rasterize_h3_partition

        rasters = rasterize_h3_partition(_h3_gdf(level=6), partition_level=6)

        assert len(rasters) == 1
        assert isinstance(rasters.iat[0], xr.Dataset)
        assert 'val' in rasters.iat[0].data_vars

    def test_split_by_parent_returns_populated_series(self):
        """partition_level < h3_level: split into one tile per parent cell."""
        from gedih3.raster.h3_raster import rasterize_h3_partition

        rasters = rasterize_h3_partition(_h3_gdf(level=6), partition_level=4)

        assert len(rasters) >= 1
        assert all(isinstance(r, xr.Dataset) for r in rasters)

    def test_empty_input_returns_empty_series(self):
        from gedih3.raster.h3_raster import rasterize_h3_partition

        rasters = rasterize_h3_partition(_h3_gdf().iloc[0:0])

        assert len(rasters) == 0
        assert rasters.dtype == object
