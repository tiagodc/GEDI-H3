"""The `gh3_aggregate -R` branch delegates to gh3_rasterize().

That branch used to carry its own copy of the index-type dispatch, a non-Dask
fallback naming tiles ``tile_0.tif``, and a second VRT build on top of the one
``rasterize_and_export_partitions`` already does. All of it now routes through
``gh3.gh3_rasterize`` so the CLI and the Python API cannot diverge.

These call ``_export_data`` directly with a synthetic aggregate — the in-process
CLI pattern used by ``test_gh3_update_filter_fallback.py`` — so no database,
network or Dask cluster is needed.
"""
import argparse
import glob
import logging
import os

import dask_geopandas
import pytest

from gedih3.cli.gh3_aggregate import _export_data

from test_rasterize_partition_series import _egi_gdf, _h3_gdf


def _args(**over):
    base = dict(rasterize=True, merge=False, compress='LZW', quiet=True,
                format='parquet', database='/fake/db')
    base.update(over)
    return argparse.Namespace(**base)


def _export(aggdf, out, args, part_col=None, use_egi=True):
    _export_data(
        aggdf, export_func=None, part_col=part_col, output_dir=out, args=args,
        use_egi=use_egi, egi_agg_level=6, egi_partition_level=12, agg='mean',
        logger=logging.getLogger('test'),
    )


class TestAggregateRasterizeBranch:

    def test_egi_tiled_writes_tiles_and_one_vrt(self, tmp_dir):
        out = os.path.join(tmp_dir, 'egi_R')
        ddf = dask_geopandas.from_geopandas(_egi_gdf({(100, 50): 6}), npartitions=1)

        _export(ddf, out, _args(), part_col='egi12')

        tifs = glob.glob(os.path.join(out, '*.tif'))
        assert len(tifs) == 1
        # Named from the egi12_id attr — not the old positional 'tile_0.tif'.
        assert not os.path.basename(tifs[0]).startswith('tile_')
        # Exactly one VRT: the duplicate build in the CLI is gone.
        assert len(glob.glob(os.path.join(out, '*.vrt'))) <= 1

    def test_egi_merged_writes_single_file(self, tmp_dir):
        out = os.path.join(tmp_dir, 'egi_R_merged')
        ddf = dask_geopandas.from_geopandas(_egi_gdf({(100, 50): 6}), npartitions=1)

        _export(ddf, out, _args(merge=True), part_col='egi12')

        assert os.path.exists(out + '.tif')

    def test_h3_tiled(self, tmp_dir):
        out = os.path.join(tmp_dir, 'h3_R')
        ddf = dask_geopandas.from_geopandas(_h3_gdf(), npartitions=1)

        _export(ddf, out, _args(), use_egi=False)

        assert len(glob.glob(os.path.join(out, '*.tif'))) >= 1

    def test_non_raster_export_untouched(self, tmp_dir, monkeypatch):
        """The `else` branch still goes to gh3_export(), unchanged."""
        seen = {}

        def fake_export(ddf, **kwargs):
            seen.update(kwargs)

        monkeypatch.setattr('gedih3.gh3driver.gh3_export', fake_export)
        out = os.path.join(tmp_dir, 'vector')
        ddf = dask_geopandas.from_geopandas(_egi_gdf({(100, 50): 6}), npartitions=1)

        _export(ddf, out, _args(rasterize=False), part_col='egi12')

        assert seen['output'] == out
        assert seen['tool'] == 'gh3_aggregate'
        assert seen['egi_aggregation_level'] == 6
