"""Tests for the public one-call rasterization API.

``gh3_rasterize`` is the index-aware entry point: it takes either a dataset
directory or an already-loaded (Dask) frame, picks the rasterizer from the
spatial index type, and fans the work across every partition. Both the
``gh3_rasterize`` CLI and ``gh3_aggregate -R`` delegate to it, so the Python
API and the CLIs cannot diverge.

``gh3_to_raster`` is the single-frame counterpart, dispatching on index type
to ``h3_to_raster`` (EPSG:4326) or ``egi.geodf_to_raster`` (EPSG:6933).

Everything here is offline: synthetic frames, no database, no dask Client.
"""
import json
import logging
import os

import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr

import gedih3 as gh3
from gedih3.exceptions import GediValidationError, GediRasterizationError

# Reuse the canonical synthetic-frame builders rather than growing a third copy.
from test_rasterize_partition_series import LEVEL, _egi_gdf, _h3_gdf


def _egi12_id(px_outer, py_outer):
    return int(np.uint64(12 * 10**18)
               + np.uint64(px_outer) * np.uint64(10**15)
               + np.uint64(py_outer) * np.uint64(10**12))


def _egi_ddf(tile_pixel_counts):
    """Dask EGI frame with ONE outer tile per partition — the real layout.

    ``egi_load`` yields one partition per tile; building the frame that way
    here keeps the fixture honest about the invariant under test.
    """
    parts = [dask_geopandas.from_geopandas(_egi_gdf({tile: count}), npartitions=1)
             for tile, count in tile_pixel_counts.items()]
    return dd.concat(parts) if len(parts) > 1 else parts[0]


def _h3_dataset_dir(base, index_level=6, partition_level=3, sidecar_level=None,
                    columns=None):
    """Write a minimal simplified H3 dataset: one parquet + sidecar.

    The parquet basename is the H3 partition cell, matching what ``gh3_export``
    writes — which is what makes the filename a usable ground truth for the
    partition level.
    """
    import h3

    os.makedirs(base, exist_ok=True)
    gdf = _h3_gdf(level=index_level, n=8)
    part_cell = h3.cell_to_parent(gdf.index[0], partition_level)
    gdf = gdf.copy()
    gdf[f'h3_{partition_level:02d}'] = part_cell
    if columns:
        for name, values in columns.items():
            gdf[name] = values

    gdf.to_parquet(os.path.join(base, f'{part_cell}.parquet'))

    meta = {
        'index_type': 'h3',
        'index_level': index_level,
        'partition_ids': [part_cell],
        'file_format': 'parquet',
    }
    if sidecar_level is not None:
        meta['h3_partition_level'] = sidecar_level
    with open(os.path.join(base, 'gedih3_dataset.json'), 'w') as fh:
        json.dump(meta, fh)

    return base, part_cell


@pytest.fixture
def egi_caplog(caplog):
    """caplog wired to the egi.raster logger — the 'gedih3' root logger sets
    propagate=False, so records never reach caplog's handler on their own."""
    lg = logging.getLogger('gedih3.egi.raster')
    lg.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger='gedih3.egi.raster'):
            yield caplog
    finally:
        lg.removeHandler(caplog.handler)


# =============================================================================
# gh3_rasterize — frame input
# =============================================================================

class TestGh3RasterizeFrames:

    def test_egi_tiles_are_named_by_outer_tile(self, tmp_dir):
        out = os.path.join(tmp_dir, 'egi_tiles')
        paths = gh3.gh3_rasterize(_egi_ddf({(100, 50): 5, (101, 50): 3}), out)

        assert len(paths) == 2
        assert all(os.path.exists(p) for p in paths)
        # Names come from the egi12_id attr, not a positional 'tile_0' fallback.
        names = sorted(os.path.basename(p) for p in paths)
        assert names == sorted([f'{_egi12_id(100, 50)}.tif',
                                f'{_egi12_id(101, 50)}.tif'])

    def test_egi_output_stays_in_ease_grid(self, tmp_dir):
        import rioxarray

        out = os.path.join(tmp_dir, 'egi_merged')   # no .tif suffix on purpose
        result = gh3.gh3_rasterize(_egi_ddf({(100, 50): 5}), out, merge=True)

        assert isinstance(result, str) and result.endswith('.tif')
        assert os.path.exists(result)
        assert rioxarray.open_rasterio(result).rio.crs.to_epsg() == 6933

    def test_h3_frame_tiled(self, tmp_dir):
        import rioxarray

        out = os.path.join(tmp_dir, 'h3_tiles')
        ddf = dask_geopandas.from_geopandas(_h3_gdf(), npartitions=1)
        paths = gh3.gh3_rasterize(ddf, out)

        assert len(paths) >= 1
        assert all(os.path.exists(p) for p in paths)
        assert rioxarray.open_rasterio(paths[0]).rio.crs.to_epsg() == 4326

    def test_returned_paths_are_flat_and_exclude_the_vrt(self, tmp_dir):
        out = os.path.join(tmp_dir, 'flat')
        paths = gh3.gh3_rasterize(_egi_ddf({(100, 50): 5, (101, 50): 3}), out)

        assert all(',' not in p for p in paths)
        assert all(p.endswith('.tif') for p in paths)
        assert not any(p.endswith('.vrt') for p in paths)

    def test_index_type_override(self, tmp_dir):
        gdf = _egi_gdf({(100, 50): 4})
        gdf.index.name = None      # detection has nothing to go on
        ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)

        paths = gh3.gh3_rasterize(ddf, os.path.join(tmp_dir, 'ovr'),
                                  index_type='egi')
        assert len(paths) == 1 and os.path.exists(paths[0])

    def test_undeterminable_index_type_raises(self, tmp_dir):
        gdf = _egi_gdf({(100, 50): 4})
        gdf.index.name = None
        ddf = dask_geopandas.from_geopandas(gdf, npartitions=1)

        with pytest.raises(GediValidationError, match='index type'):
            gh3.gh3_rasterize(ddf, os.path.join(tmp_dir, 'nope'))

    def test_query_with_a_frame_raises(self, tmp_dir):
        ddf = _egi_ddf({(100, 50): 4})
        with pytest.raises(GediValidationError, match='query='):
            gh3.gh3_rasterize(ddf, os.path.join(tmp_dir, 'q'), query='val > 1')


class TestSubTwelvePartitionNaming:
    """Partitions finer than level 12 must not collide on output filename.

    Several level-N (N < 12) partitions nest in one level-12 outer tile. Naming
    tiles after the outer tile made them all write to the same path, so every
    partition but the last was silently replaced. Tiles are now named after the
    partition, the way the H3 rasterizer already did.
    """

    PART = 10

    def _sub12_ddf(self, with_part_col):
        """Two distinct level-10 partitions inside ONE level-12 outer tile."""
        from gedih3 import egi
        from gedih3.egi.config import LIMITS, OUTER_RES

        res = egi.get_resolution(LEVEL)
        x0 = LIMITS['lon_w'] + 100 * OUTER_RES
        y0 = LIMITS['lat_s'] + 50 * OUTER_RES

        parts = []
        for offset in (2, 100):
            xs = x0 + res * (np.arange(3) + offset + 0.5)
            ys = y0 + res * (np.arange(3) + offset + 0.5)
            hashes = np.array([egi.to_hash(x, y, level=LEVEL) for x, y in zip(xs, ys)],
                              dtype=np.uint64)
            data = {'val': np.arange(3, dtype=float) + offset}
            if with_part_col:
                data[f'egi{self.PART}'] = [int(v) for v in egi.to_parent(hashes, self.PART)]
            gdf = gpd.GeoDataFrame(
                data, geometry=gpd.points_from_xy(xs, ys), crs='EPSG:6933',
                index=pd.Index(hashes, name=f'egi{LEVEL:02d}'))
            parts.append(dask_geopandas.from_geopandas(gdf, npartitions=1))
        return dd.concat(parts)

    @staticmethod
    def _finite_pixels(paths):
        import rioxarray
        return sum(int(np.isfinite(rioxarray.open_rasterio(p).values).sum())
                   for p in paths)

    def test_partition_column_gives_each_partition_its_own_tile(self, tmp_dir):
        out = os.path.join(tmp_dir, 'sub12_col')
        paths = gh3.gh3_rasterize(self._sub12_ddf(True), out)

        assert len(set(paths)) == 2
        assert self._finite_pixels(paths) == 6      # nothing dropped

    def test_explicit_partition_level_is_enough(self, tmp_dir):
        out = os.path.join(tmp_dir, 'sub12_explicit')
        paths = gh3.gh3_rasterize(self._sub12_ddf(False), out,
                                  partition_level=self.PART)

        assert len(set(paths)) == 2
        assert self._finite_pixels(paths) == 6

    def test_unresolvable_collision_is_reported(self, tmp_dir, caplog):
        """Nothing identifies the partitions — the loss is unavoidable, but
        it must never be silent."""
        out = os.path.join(tmp_dir, 'sub12_blind')

        lg = logging.getLogger('gedih3.gh3driver')
        lg.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.ERROR, logger='gedih3.gh3driver'):
                paths = gh3.gh3_rasterize(self._sub12_ddf(False), out)
        finally:
            lg.removeHandler(caplog.handler)

        assert len(set(paths)) == 1 and len(paths) == 2
        assert 'were overwritten' in caplog.text

    def test_level_12_partitioning_keeps_the_tile_id(self, tmp_dir):
        """The standard case is unchanged: files named by the level-12 tile."""
        out = os.path.join(tmp_dir, 'lvl12')
        paths = gh3.gh3_rasterize(_egi_ddf({(100, 50): 5, (101, 50): 3}), out)

        assert sorted(os.path.basename(p) for p in paths) == sorted(
            [f'{_egi12_id(100, 50)}.tif', f'{_egi12_id(101, 50)}.tif'])


# =============================================================================
# gh3_rasterize — plain (non-dask) frame
# =============================================================================

class TestGh3RasterizeNonDask:

    def test_plain_egi_frame_merge(self, tmp_dir):
        out = os.path.join(tmp_dir, 'plain.tif')
        result = gh3.gh3_rasterize(_egi_gdf({(100, 50): 5}), out, merge=True)

        assert result == out and os.path.exists(out)

    def test_multi_tile_plain_frame_keeps_the_majority_and_warns(self, tmp_dir,
                                                                 egi_caplog):
        """A frame is treated as one partition: one tile, one raster.

        Merging must not silently discard tiles — the majority wins and the
        violation is reported.
        """
        out = os.path.join(tmp_dir, 'multi.tif')
        gh3.gh3_rasterize(_egi_gdf({(100, 50): 5, (101, 50): 2}), out, merge=True)

        assert os.path.exists(out)
        assert 'spans 2 outer tiles' in egi_caplog.text


# =============================================================================
# gh3_rasterize — dataset directory input
# =============================================================================

class TestGh3RasterizePathInput:

    def test_h3_dataset_dir_tiled(self, tmp_dir):
        base, part_cell = _h3_dataset_dir(os.path.join(tmp_dir, 'ds'))
        out = os.path.join(tmp_dir, 'ds_tiles')

        paths = gh3.gh3_rasterize(base, out)

        assert len(paths) >= 1
        assert all(os.path.exists(p) for p in paths)

    def test_partition_level_comes_from_the_sidecar(self, tmp_dir, monkeypatch):
        seen = {}

        def spy(gdf, columns=None, **kwargs):
            seen.update(kwargs)
            return pd.Series(dtype=object)

        monkeypatch.setattr('gedih3.raster.rasterize_h3_partition', spy)
        base, _ = _h3_dataset_dir(os.path.join(tmp_dir, 'ds'),
                                  partition_level=3, sidecar_level=3)

        gh3.gh3_rasterize(base, os.path.join(tmp_dir, 'o'))
        assert seen.get('partition_level') == 3

    def test_stale_sidecar_loses_to_the_filenames(self, tmp_dir, monkeypatch, caplog):
        seen = {}

        def spy(gdf, columns=None, **kwargs):
            seen.update(kwargs)
            return pd.Series(dtype=object)

        monkeypatch.setattr('gedih3.raster.rasterize_h3_partition', spy)
        # Filenames are level-3 cells; the sidecar claims 5.
        base, _ = _h3_dataset_dir(os.path.join(tmp_dir, 'ds'),
                                  partition_level=3, sidecar_level=5)

        lg = logging.getLogger('gedih3.gh3driver')
        lg.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.WARNING, logger='gedih3.gh3driver'):
                gh3.gh3_rasterize(base, os.path.join(tmp_dir, 'o'))
        finally:
            lg.removeHandler(caplog.handler)

        assert seen.get('partition_level') == 3
        assert 'disagrees with filenames' in caplog.text

    def test_h3_database_is_rejected(self, tmp_dir):
        db = os.path.join(tmp_dir, 'db')
        os.makedirs(db, exist_ok=True)
        with open(os.path.join(db, 'gedih3_build_log.json'), 'w') as fh:
            json.dump({'h3_resolution_level': 12, 'h3_partition_level': 3}, fh)

        with pytest.raises(GediValidationError, match='gh3_aggregate'):
            gh3.gh3_rasterize(db, os.path.join(tmp_dir, 'o'))

    def test_column_wildcards_are_expanded(self, tmp_dir, monkeypatch):
        seen = {}

        def spy(gdf, columns=None, **kwargs):
            seen['columns'] = columns
            return pd.Series(dtype=object)

        monkeypatch.setattr('gedih3.raster.rasterize_h3_partition', spy)
        base, _ = _h3_dataset_dir(
            os.path.join(tmp_dir, 'ds'),
            columns={'agbd_mean': np.arange(8, dtype=float),
                     'agbd_sd': np.arange(8, dtype=float),
                     'other': np.arange(8, dtype=float)},
        )

        gh3.gh3_rasterize(base, os.path.join(tmp_dir, 'o'), columns=['agbd_*'])
        assert sorted(seen['columns']) == ['agbd_mean', 'agbd_sd']


# =============================================================================
# gh3_to_raster dispatch
# =============================================================================

class TestGh3ToRasterDispatch:

    def test_egi_frame_returns_ease_grid_dataset(self):
        xras = gh3.gh3_to_raster(_egi_gdf({(100, 50): 5}))

        assert isinstance(xras, xr.Dataset)
        assert xras.rio.crs.to_epsg() == 6933
        assert 'val' in xras.data_vars

    def test_h3_frame_unchanged(self):
        xras = gh3.gh3_to_raster(_h3_gdf())

        assert isinstance(xras, xr.Dataset)
        assert xras.rio.crs.to_epsg() == 4326

    def test_multi_tile_egi_frame_raises(self):
        # geodf_to_raster rasterizes exactly one tile and refuses to guess.
        with pytest.raises(GediRasterizationError, match='outer tile'):
            gh3.gh3_to_raster(_egi_gdf({(100, 50): 5, (101, 50): 2}))

    def test_unindexed_frame_raises(self):
        gdf = gpd.GeoDataFrame(
            {'val': [1.0, 2.0]},
            geometry=gpd.points_from_xy([0.0, 1.0], [0.0, 1.0]), crs='EPSG:4326')
        with pytest.raises(GediValidationError, match='index type'):
            gh3.gh3_to_raster(gdf)


# =============================================================================
# Export surface
# =============================================================================

def test_gh3_rasterize_is_exported():
    assert 'gh3_rasterize' in gh3.__all__
    assert callable(gh3.gh3_rasterize)
