"""gh3_export() routes raster formats through the rasterization pipeline.

Before this, the two index types disagreed about what `fmt='tif'` meant: EGI
had a raster branch inside `_write_egi_file` that called `geodf_to_raster`
directly (no outer-tile invariant, no VRT, naming by partition hash), and H3
had no branch at all — `gh3_export(ddf, out, fmt='tif')` raised
"Unsupported export format: tif". Both now delegate to `gh3_rasterize`, so
there is one rasterization implementation behind every tool.

Offline: synthetic frames, no database, no network.
"""
import glob
import os

import dask_geopandas
import numpy as np
import pytest

import gedih3 as gh3
from gedih3.exceptions import GediValidationError

from test_rasterize_partition_series import _egi_gdf, _h3_gdf


def _egi_ddf():
    return dask_geopandas.from_geopandas(_egi_gdf({(100, 50): 6}), npartitions=1)


def _h3_ddf():
    import h3 as h3lib

    gdf = _h3_gdf(level=6, n=8)
    gdf['h3_03'] = h3lib.cell_to_parent(gdf.index[0], 3)
    return dask_geopandas.from_geopandas(gdf, npartitions=1)


@pytest.mark.parametrize('maker', [_egi_ddf, _h3_ddf], ids=['egi', 'h3'])
class TestExportGeoTIFF:

    def test_tiled_writes_tifs_and_a_sidecar(self, maker, tmp_dir):
        out = os.path.join(tmp_dir, 'tiled')
        ofiles = gh3.gh3_export(maker(), output=out, fmt='tif',
                                show_progress=False)

        assert ofiles and all(p.endswith('.tif') for p in ofiles)
        assert all(os.path.exists(p) for p in ofiles)
        # The dataset sidecar still lands next to the tiles, so downstream
        # tools keep recognising the output as a dataset.
        assert os.path.exists(os.path.join(out, 'gedih3_dataset.json'))

    def test_merged_writes_one_file(self, maker, tmp_dir):
        out = os.path.join(tmp_dir, 'merged')
        ofiles = gh3.gh3_export(maker(), output=out, fmt='tif', merge=True,
                                show_progress=False)

        assert len(ofiles) == 1
        assert ofiles[0].endswith('.tif') and os.path.exists(ofiles[0])

    def test_tiled_output_carries_the_right_crs(self, maker, tmp_dir):
        import rioxarray

        out = os.path.join(tmp_dir, 'crs')
        ofiles = gh3.gh3_export(maker(), output=out, fmt='tif',
                                show_progress=False)

        expected = 6933 if maker is _egi_ddf else 4326
        assert rioxarray.open_rasterio(ofiles[0]).rio.crs.to_epsg() == expected


class TestExportRasterEdges:

    def test_netcdf_is_tiled_only(self, tmp_dir):
        """merge writes GeoTIFF only — say so instead of emitting a GeoTIFF
        under a .nc name."""
        with pytest.raises(GediValidationError, match='GeoTIFF only'):
            gh3.gh3_export(_h3_ddf(), output=os.path.join(tmp_dir, 'm'),
                           fmt='nc', merge=True, show_progress=False)

    def test_netcdf_tiled_works(self, tmp_dir):
        out = os.path.join(tmp_dir, 'nc')
        ofiles = gh3.gh3_export(_h3_ddf(), output=out, fmt='nc',
                                show_progress=False)

        assert ofiles and all(p.endswith('.nc') for p in ofiles)

    def test_vector_and_columnar_formats_are_untouched(self, tmp_dir):
        """The non-raster branch is unchanged.

        It goes through dask_safe_wait, which — unlike the raster path's
        dask_safe_collect — has no no-client fallback, hence the local
        cluster here.
        """
        from dask.distributed import Client

        out = os.path.join(tmp_dir, 'pq')
        with Client(n_workers=1, threads_per_worker=1, processes=False,
                    dashboard_address=':0'):
            ofiles = gh3.gh3_export(_h3_ddf(), output=out, fmt='parquet',
                                    show_progress=False)

        assert ofiles and all(p.endswith('.parquet') for p in ofiles)
        assert os.path.exists(os.path.join(out, 'gedih3_dataset.json'))

    def test_egi_tiles_keep_partition_naming(self, tmp_dir):
        """Level-12 partitions still land on <egi12 hash>.tif, as before."""
        out = os.path.join(tmp_dir, 'names')
        ofiles = gh3.gh3_export(_egi_ddf(), output=out, fmt='tif',
                                show_progress=False)

        expected = int(np.uint64(12 * 10**18)
                       + np.uint64(100) * np.uint64(10**15)
                       + np.uint64(50) * np.uint64(10**12))
        assert [os.path.basename(p) for p in ofiles] == [f'{expected}.tif']

    def test_tiled_raster_export_builds_a_mosaic(self, tmp_dir):
        """gh3_export inherits the VRT that only the raster pipeline built."""
        import dask.dataframe as dd

        out = os.path.join(tmp_dir, 'vrt')
        ddf = dd.concat([
            dask_geopandas.from_geopandas(_egi_gdf({tile: 4}), npartitions=1)
            for tile in [(100, 50), (101, 50)]
        ])
        gh3.gh3_export(ddf, output=out, fmt='tif', show_progress=False)

        assert glob.glob(os.path.join(out, '*.vrt'))
