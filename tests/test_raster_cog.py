"""Image output is a Cloud Optimized GeoTIFF, LZW-compressed, by default.

A COG is internally tiled and carries an overview pyramid, so a viewer or a
range-reading client fetches only the bytes it needs. It is still a valid
GeoTIFF, so nothing that read the previous output stops working.

The GDAL COG driver takes its own creation-option names (BLOCKSIZE, not
TILED/BLOCKXSIZE). Handing it the GeoTIFF names is silently ignored and you
fall back to the driver defaults — hence a separate options builder, and the
test below that pins the difference.
"""
import os

import numpy as np
import pytest
import rasterio
import xarray as xr

from gedih3.raster import export_raster, get_cog_options, get_geotiff_options


@pytest.fixture
def big_raster():
    """Large enough that GDAL actually builds an overview pyramid."""
    n = 1024
    data = np.random.default_rng(0).random((n, n)).astype('float32')
    xds = xr.Dataset(
        {'val': (('y', 'x'), data)},
        coords={'y': np.linspace(10, 0, n), 'x': np.linspace(0, 10, n)},
    )
    return xds.rio.write_crs('EPSG:4326')


class TestCogOptions:

    def test_uses_the_cog_driver_and_its_option_names(self):
        opts = get_cog_options()

        assert opts['driver'] == 'COG'
        assert opts['COMPRESS'] == 'LZW'
        assert opts['BLOCKSIZE'] == 256
        # TILED/BLOCKXSIZE are GTiff spellings; the COG driver ignores them.
        assert 'TILED' not in opts and 'BLOCKXSIZE' not in opts

    def test_geotiff_options_unchanged(self):
        opts = get_geotiff_options()

        assert 'driver' not in opts
        assert opts['TILED'] == 'YES'
        assert opts['compress'] == 'LZW'


class TestExportRasterDefaults:

    def test_default_output_is_a_cog(self, big_raster, tmp_dir):
        path = export_raster(big_raster, os.path.join(tmp_dir, 'cog.tif'))

        with rasterio.open(path) as ds:
            assert ds.profile['tiled'] is True
            assert ds.profile['blockxsize'] == 256
            assert ds.profile['compress'] == 'lzw'
            # The pyramid is what makes it *cloud optimized*.
            assert ds.overviews(1), 'COG output carries no overviews'

    def test_opt_out_gives_a_plain_geotiff(self, big_raster, tmp_dir):
        path = export_raster(big_raster, os.path.join(tmp_dir, 'plain.tif'),
                             cog=False)

        with rasterio.open(path) as ds:
            assert ds.profile['compress'] == 'lzw'   # LZW either way
            assert not ds.overviews(1)

    def test_compression_choice_is_honoured(self, big_raster, tmp_dir):
        path = export_raster(big_raster, os.path.join(tmp_dir, 'zstd.tif'),
                             compress='ZSTD')

        with rasterio.open(path) as ds:
            assert ds.profile['compress'] == 'zstd'

    def test_creates_missing_parent_directories(self, big_raster, tmp_dir):
        path = export_raster(big_raster,
                             os.path.join(tmp_dir, 'nested', 'deep', 'r.tif'))
        assert os.path.exists(path)


def _fine_egi_ddf(level=4, n=40):
    """One EGI partition whose raster is far larger than a single block.

    geodf_to_raster always lays out the full level-12 tile grid, so a level-4
    (~100 m) tile is ~1600x1600 pixels no matter how few are filled — big
    enough that GDAL builds an overview pyramid. A level-6 tile is 160x160,
    smaller than one 256-pixel block, and correctly gets no overviews.
    """
    import dask_geopandas
    import geopandas as gpd
    import pandas as pd

    from gedih3 import egi
    from gedih3.egi.config import LIMITS, OUTER_RES

    res = egi.get_resolution(level)
    x0 = LIMITS['lon_w'] + 100 * OUTER_RES
    y0 = LIMITS['lat_s'] + 50 * OUTER_RES
    xs = x0 + res * (np.arange(n) + 0.5)
    ys = y0 + res * (np.arange(n) + 0.5)
    hashes = np.array([egi.to_hash(x, y, level=level) for x, y in zip(xs, ys)],
                      dtype=np.uint64)

    gdf = gpd.GeoDataFrame(
        {'val': np.arange(n, dtype=float)},
        geometry=gpd.points_from_xy(xs, ys), crs='EPSG:6933',
        index=pd.Index(hashes, name=f'egi{level:02d}'),
    )
    return dask_geopandas.from_geopandas(gdf, npartitions=1)


class TestTiledPipelineProducesCogs:

    def test_gh3_rasterize_tiles_are_cogs(self, tmp_dir):
        import gedih3 as gh3

        paths = gh3.gh3_rasterize(_fine_egi_ddf(), os.path.join(tmp_dir, 'tiles'),
                                  show_progress=False)

        with rasterio.open(paths[0]) as ds:
            assert ds.profile['tiled'] is True
            assert ds.profile['compress'] == 'lzw'
            assert ds.overviews(1), 'tiled COG output carries no overviews'

    def test_no_cog_opt_out_reaches_the_tiles(self, tmp_dir):
        import gedih3 as gh3

        paths = gh3.gh3_rasterize(_fine_egi_ddf(),
                                  os.path.join(tmp_dir, 'plain_tiles'),
                                  cog=False, show_progress=False)

        with rasterio.open(paths[0]) as ds:
            assert ds.profile['compress'] == 'lzw'
            assert not ds.overviews(1)

    def test_merged_output_is_a_cog_too(self, tmp_dir):
        import gedih3 as gh3

        path = gh3.gh3_rasterize(_fine_egi_ddf(),
                                 os.path.join(tmp_dir, 'merged.tif'),
                                 merge=True, show_progress=False)

        with rasterio.open(path) as ds:
            assert ds.overviews(1)


def test_egi_export_raster_is_an_alias():
    """egi.export_raster used to inline its own options and so missed COG,
    directory creation and compress='NONE' handling."""
    from gedih3 import egi
    from gedih3.raster import export_raster as parent

    import inspect

    sig = inspect.signature(egi.export_raster)
    assert 'cog' in sig.parameters
    assert sig.parameters['cog'].default is True
    assert set(sig.parameters) == set(inspect.signature(parent).parameters)
