"""Regression tests for geodf_to_raster's outer-tile guard.

A raster covers exactly one level-12 outer tile; pixels from any other tile
carry inner indices relative to *their own* tile and would land at wrong map
coordinates if placed in this tile's grid, so they must be skipped. Healthy
post-fix pipelines (to_hash boundary-overflow carry + egi_load spillover
filter) always deliver single-tile inputs — mixed tiles only arrive from
legacy datasets or multi-tile API calls. The guard therefore:

  * accepts an explicit ``outer_tile=`` from callers that know their tile
    (a-priori knowledge over runtime detection),
  * accepts a no-hint input only when it resolves to a single tile,
  * raises ``GediRasterizationError`` on a genuine multi-tile input with no
    hint, rather than guessing a winner and silently dropping the rest
    (the legacy code rasterized an unseeded 100-row sample's dominant tile),
  * warns loudly whenever stray pixels are skipped against an explicit tile.

TimeSeriesRasterizer legitimately aggregates ROIs spanning multiple outer
tiles, so its EGI path now splits per tile and merges instead of relying on
the single-tile fallback.
"""
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point

from gedih3 import egi
from gedih3.egi.config import LIMITS, OUTER_RES

LEVEL = 6


def _tile_coords(px_outer, py_outer, n):
    """n pixel-center coordinates inside outer tile (px_outer, py_outer)."""
    res = egi.get_resolution(LEVEL)
    x0 = LIMITS['lon_w'] + px_outer * OUTER_RES
    y0 = LIMITS['lat_s'] + py_outer * OUTER_RES
    xs = x0 + res * (np.arange(n) + 0.5)
    ys = y0 + res * (np.arange(n) + 0.5)
    return xs, ys


def _egi_gdf(tile_pixel_counts):
    """EGI-indexed GeoDataFrame with `count` pixels in each given tile."""
    hashes, values, points = [], [], []
    val = 0.0
    for (px_outer, py_outer), count in tile_pixel_counts.items():
        xs, ys = _tile_coords(px_outer, py_outer, count)
        for x, y in zip(xs, ys):
            hashes.append(egi.to_hash(x, y, level=LEVEL))
            val += 1.0
            values.append(val)
            points.append(Point(x, y))
    gdf = gpd.GeoDataFrame(
        {'val': values}, geometry=points, crs='EPSG:6933',
        index=pd.Index(np.array(hashes, dtype=np.uint64), name=f'egi{LEVEL:02d}'),
    )
    return gdf


def _egi12_id(px_outer, py_outer):
    return int(np.uint64(12 * 10**18)
               + np.uint64(px_outer) * np.uint64(10**15)
               + np.uint64(py_outer) * np.uint64(10**12))


@pytest.fixture
def raster_caplog(caplog):
    """caplog wired to the egi.raster module logger directly — the package
    root logger ('gedih3') carries its own handler with propagate=False, so
    records never reach caplog's root-level handler on their own."""
    lg = logging.getLogger('gedih3.egi.raster')
    lg.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger='gedih3.egi.raster'):
            yield caplog
    finally:
        lg.removeHandler(caplog.handler)


class TestGeodfToRasterTileGuard:
    def test_single_tile_no_warning(self, raster_caplog):
        gdf = _egi_gdf({(100, 50): 5})
        ras = egi.geodf_to_raster(gdf)
        assert int(np.isfinite(ras['val'].values).sum()) == 5
        assert not raster_caplog.records

    def test_mixed_tiles_no_hint_raises(self):
        from gedih3.exceptions import GediRasterizationError

        gdf = _egi_gdf({(100, 50): 5, (101, 50): 2})
        # No outer_tile and genuinely multi-tile: refuse rather than guess a
        # winner and silently drop the other tile's pixels.
        with pytest.raises(GediRasterizationError, match='outer tile'):
            egi.geodf_to_raster(gdf)

    def test_explicit_outer_tile_selects_requested_tile(self, raster_caplog):
        gdf = _egi_gdf({(100, 50): 5, (101, 50): 2})
        ras = egi.geodf_to_raster(gdf, outer_tile=_egi12_id(101, 50))

        # The minority tile was requested explicitly: its 2 pixels survive.
        assert int(np.isfinite(ras['val'].values).sum()) == 2
        left = ras.rio.transform().c
        assert left == pytest.approx(LIMITS['lon_w'] + 101 * OUTER_RES)
        assert 'skipping 5 pixel(s)' in raster_caplog.text


class TestRasterizePartitionMultiTile:
    def test_no_pixels_lost_across_tiles(self):
        gdf = _egi_gdf({(100, 50): 5, (101, 50): 2})
        rasters = egi.rasterize_partition(gdf)
        assert len(rasters) == 2
        total = sum(int(np.isfinite(r['val'].values).sum()) for r in rasters)
        assert total == 7
        ids = sorted(r['val'].attrs['egi12_id'] for r in rasters)
        assert ids == sorted([_egi12_id(100, 50), _egi12_id(101, 50)])


class TestTimeSeriesRasterizerMultiTile:
    def test_rasterize_merges_all_tiles(self):
        from gedih3.raster.timeseries import TimeSeriesRasterizer

        init_df = pd.DataFrame({
            'datetime': pd.to_datetime(['2020-01-01', '2020-06-01']),
            'val': [1.0, 2.0],
        })
        tsr = TimeSeriesRasterizer(init_df, use_egi=True, target_level=LEVEL,
                                   columns=['val'])

        gdf = _egi_gdf({(100, 50): 5, (101, 50): 2})
        merged = tsr._rasterize(gdf)
        assert int(np.isfinite(merged['val'].values).sum()) == 7
