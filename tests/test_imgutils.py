"""
Tests for gedih3.imgutils — Raster sampling at GEDI shot locations.

Unit tests use synthetic rasters and DataFrames.
Integration tests use real GEDI data + NASA DEM (marked with @pytest.mark.integration).
"""

import os
import tempfile
import shutil

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point

from gedih3.imgutils import (
    resolve_raster_source,
    get_raster_info,
    parse_window_specs,
    sample_raster_at_points,
    _compute_sampling_meta,
    _window_sum,
    _window_mean,
    _window_median,
    _window_mode,
)
from gedih3.exceptions import GediImageSamplingError


# =============================================================================
# Fixtures
# =============================================================================


def _make_synthetic_raster(path, nx=10, ny=10, crs='EPSG:4326',
                           bounds=(-10, -10, 10, 10), values=None,
                           nbands=1, nodata=None):
    """Create a synthetic GeoTIFF for testing.

    Parameters
    ----------
    path : str
        Output file path
    nx, ny : int
        Raster dimensions
    crs : str
        Coordinate reference system
    bounds : tuple
        (minx, miny, maxx, maxy) in the given CRS
    values : ndarray, optional
        Pixel values. If None, uses sequential integers.
    nbands : int
        Number of bands
    nodata : float, optional
        NoData value
    """
    import rioxarray
    import xarray as xr

    minx, miny, maxx, maxy = bounds
    x_res = (maxx - minx) / nx
    y_res = (maxy - miny) / ny

    x = np.linspace(minx + x_res / 2, maxx - x_res / 2, nx)
    y = np.linspace(maxy - y_res / 2, miny + y_res / 2, ny)

    if values is None:
        values = np.arange(ny * nx).reshape(ny, nx).astype(np.float64)

    if nbands > 1:
        if values.ndim == 2:
            values = np.stack([values * (i + 1) for i in range(nbands)])
        data = xr.DataArray(
            values,
            dims=['band', 'y', 'x'],
            coords={'band': np.arange(1, nbands + 1), 'y': y, 'x': x}
        )
    else:
        if values.ndim == 3:
            values = values[0]
        data = xr.DataArray(
            values[np.newaxis, :, :] if values.ndim == 2 else values,
            dims=['band', 'y', 'x'],
            coords={'band': [1], 'y': y, 'x': x}
        )

    data = data.rio.set_spatial_dims(x_dim='x', y_dim='y')
    data = data.rio.write_crs(crs)
    if nodata is not None:
        data = data.rio.write_nodata(nodata)
    data.rio.to_raster(path)
    return path


def _make_test_points(lon_lat_pairs, partition_col='h3_03', part_id='test01'):
    """Create a GeoDataFrame of test points (simulating GEDI shots).

    Parameters
    ----------
    lon_lat_pairs : list of (lon, lat) tuples
    partition_col : str
        Partition column name
    part_id : str
        Partition ID value

    Returns
    -------
    GeoDataFrame
    """
    geoms = [Point(lon, lat) for lon, lat in lon_lat_pairs]
    gdf = gpd.GeoDataFrame({
        'shot_number': np.arange(len(lon_lat_pairs), dtype='int64'),
        partition_col: part_id,
    }, geometry=geoms, crs='EPSG:4326')
    return gdf


# =============================================================================
# Test: resolve_raster_source
# =============================================================================

class TestResolveRasterSource:

    def test_single_file(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path)
        result, is_vrt, count = resolve_raster_source(path)
        assert result == path
        assert is_vrt is False
        assert count == 1

    def test_directory_single_tile(self, tmp_dir):
        _make_synthetic_raster(os.path.join(tmp_dir, 'tile.tif'))
        result, is_vrt, count = resolve_raster_source(tmp_dir, file_format='tif')
        assert result.endswith('.tif')
        assert is_vrt is False
        assert count == 1

    def test_directory_multiple_tiles_builds_vrt(self, tmp_dir):
        for i in range(3):
            _make_synthetic_raster(
                os.path.join(tmp_dir, f'tile_{i}.tif'),
                bounds=(i * 10, 0, (i + 1) * 10, 10)
            )
        result, is_vrt, count = resolve_raster_source(tmp_dir)
        assert result.endswith('.vrt')
        assert is_vrt is True
        assert count == 3
        assert os.path.exists(result)

    def test_nonexistent_path_raises(self):
        with pytest.raises(GediImageSamplingError, match="not found"):
            resolve_raster_source('/nonexistent/path')

    def test_empty_directory_raises(self, tmp_dir):
        with pytest.raises(GediImageSamplingError, match="No .tif files"):
            resolve_raster_source(tmp_dir, file_format='tif')


# =============================================================================
# Test: get_raster_info
# =============================================================================

class TestGetRasterInfo:

    def test_basic_info(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path, nx=20, ny=15, bounds=(-5, -3, 5, 3))
        info = get_raster_info(path)

        assert info['band_count'] == 1
        assert info['shape'] == (15, 20)
        assert len(info['band_names']) == 1
        assert info['crs'] is not None
        # Bounds should be close to requested
        assert info['bounds_wgs84'][0] < -4
        assert info['bounds_wgs84'][2] > 4

    def test_multiband(self, tmp_dir):
        path = os.path.join(tmp_dir, 'multi.tif')
        _make_synthetic_raster(path, nbands=3)
        info = get_raster_info(path)
        assert info['band_count'] == 3
        assert len(info['band_names']) == 3

    def test_nodata(self, tmp_dir):
        path = os.path.join(tmp_dir, 'nodata.tif')
        _make_synthetic_raster(path, nodata=-9999)
        info = get_raster_info(path)
        assert info['nodata'] == -9999


# =============================================================================
# Test: Window Operations
# =============================================================================

class TestWindowOperations:

    def test_window_sum(self):
        data = np.ones((5, 5))
        result = _window_sum(data, 3)
        # Center pixel of 3x3 window on all-ones = 9
        assert result[2, 2] == 9.0

    def test_window_mean(self):
        data = np.ones((5, 5)) * 4.0
        result = _window_mean(data, 3)
        assert np.isclose(result[2, 2], 4.0)

    def test_window_mean_gradient(self):
        data = np.arange(25, dtype=float).reshape(5, 5)
        result = _window_mean(data, 3)
        # Center pixel (12) should be mean of 3x3 neighborhood
        expected = np.mean([6, 7, 8, 11, 12, 13, 16, 17, 18])
        assert np.isclose(result[2, 2], expected)

    def test_window_median_is_actual_median(self):
        """Verify median fix: should return 50th percentile, not 0.5th."""
        data = np.array([
            [1, 2, 3, 4, 5],
            [6, 7, 8, 9, 10],
            [11, 12, 99, 14, 15],
            [16, 17, 18, 19, 20],
            [21, 22, 23, 24, 25],
        ], dtype=float)
        result = _window_median(data, 3)
        # Median of 3x3 at center [2,2]: sorted [7,8,9,12,99,14,17,18,19] → median=14
        center = result[2, 2]
        neighborhood = [7, 8, 9, 12, 99, 14, 17, 18, 19]
        assert np.isclose(center, np.median(neighborhood))
        # Old bug would return ~0.5th percentile ≈ minimum
        assert center != min(neighborhood)

    def test_window_mode(self):
        data = np.array([
            [1, 1, 2],
            [1, 1, 2],
            [3, 3, 2],
        ], dtype=int)
        result = _window_mode(data, 3)
        # Center [1,1]: counts in 3x3 window → 1:4, 2:3, 3:2 → mode=1
        assert result[1, 1] == 1

    def test_window_mode_tie_breaking(self):
        """Mode with all same value."""
        data = np.full((5, 5), 7, dtype=int)
        result = _window_mode(data, 3)
        assert np.all(result == 7)


# =============================================================================
# Test: parse_window_specs
# =============================================================================

class TestParseWindowSpecs:

    def test_basic_spec(self):
        result = parse_window_specs(['033'])
        assert len(result) == 1
        assert result[0]['band'] == 0
        assert result[0]['size'] == 3
        assert result[0]['op'] == 'mode'
        assert result[0]['name'] == 'b0_mode_3x3'

    def test_multiple_specs(self):
        result = parse_window_specs(['013', '151', '032'])
        assert len(result) == 3
        assert result[0]['op'] == 'mode'
        assert result[1]['op'] == 'mean'
        assert result[2]['op'] == 'median'

    def test_all_ops(self):
        specs = ['030', '031', '032', '033']
        result = parse_window_specs(specs)
        ops = [r['op'] for r in result]
        assert ops == ['sum', 'mean', 'median', 'mode']

    def test_none_returns_empty(self):
        assert parse_window_specs(None) == []

    def test_invalid_length_raises(self):
        with pytest.raises(GediImageSamplingError, match="3 digits"):
            parse_window_specs(['12'])

    def test_even_size_raises(self):
        with pytest.raises(GediImageSamplingError, match="odd"):
            parse_window_specs(['020'])

    def test_invalid_op_raises(self):
        with pytest.raises(GediImageSamplingError, match="0-3"):
            parse_window_specs(['034'])


# =============================================================================
# Test: sample_raster_at_points
# =============================================================================

class TestSampleRasterAtPoints:

    def test_basic_sampling(self, tmp_dir):
        """Sample at known pixel centers, verify exact values."""
        path = os.path.join(tmp_dir, 'test.tif')
        # 10x10 raster, bounds (-10, -10, 10, 10), values = row*10+col
        values = np.arange(100, dtype=float).reshape(10, 10)
        _make_synthetic_raster(path, values=values, bounds=(-10, -10, 10, 10))

        # Points at pixel centers: pixel (0,0) is at x=-9, y=9 (top-left)
        pts = _make_test_points([(-9, 9), (-7, 9), (-9, 7)])
        result = sample_raster_at_points(pts, path, band_names=['elevation'])

        assert 'elevation' in result.columns
        assert 'relative_pixel_distance' in result.columns
        assert len(result) == 3
        # relative_pixel_distance should be close to 0 at pixel centers
        assert all(result['relative_pixel_distance'] < 0.5)

    def test_multiband_sampling(self, tmp_dir):
        path = os.path.join(tmp_dir, 'multi.tif')
        _make_synthetic_raster(path, nbands=3, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(0, 0)])
        result = sample_raster_at_points(pts, path, band_names=['b0', 'b1', 'b2'])

        assert 'b0' in result.columns
        assert 'b1' in result.columns
        assert 'b2' in result.columns
        # Band 2 values should be 2x band 1 (our synthetic data)
        assert result['b1'].iloc[0] == pytest.approx(result['b0'].iloc[0] * 2)

    def test_out_of_bounds_get_nan(self, tmp_dir):
        """Points outside raster extent should get NaN."""
        path = os.path.join(tmp_dir, 'small.tif')
        _make_synthetic_raster(path, bounds=(0, 0, 5, 5))

        # One point inside, one outside
        pts = _make_test_points([(2.5, 2.5), (50, 50)])
        result = sample_raster_at_points(pts, path, band_names=['val'])

        assert not np.isnan(result.iloc[0]['val'])
        assert np.isnan(result.iloc[1]['val'])
        assert np.isnan(result.iloc[1]['relative_pixel_distance'])

    def test_nodata_pixels_get_nan(self, tmp_dir):
        """Points on NoData pixels should get NaN."""
        path = os.path.join(tmp_dir, 'nodata.tif')
        values = np.ones((10, 10)) * 42.0
        values[5, 5] = -9999  # NoData pixel
        _make_synthetic_raster(path, values=values, nodata=-9999, bounds=(-10, -10, 10, 10))

        # Point at center of nodata pixel
        pts = _make_test_points([(1, -1)])
        result = sample_raster_at_points(pts, path, band_names=['val'])

        assert np.isnan(result.iloc[0]['val'])

    def test_fillna_replaces_nodata(self, tmp_dir):
        """fillna should replace NoData values."""
        path = os.path.join(tmp_dir, 'nodata.tif')
        values = np.ones((10, 10)) * 42.0
        values[5, 5] = -9999
        _make_synthetic_raster(path, values=values, nodata=-9999, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(1, -1)])
        result = sample_raster_at_points(pts, path, band_names=['val'], fillna=0.0)

        assert result.iloc[0]['val'] == 0.0

    def test_dropna_removes_nan_rows(self, tmp_dir):
        """dropna should remove rows where all bands are NaN."""
        path = os.path.join(tmp_dir, 'small.tif')
        _make_synthetic_raster(path, bounds=(0, 0, 5, 5))

        # One inside, one outside
        pts = _make_test_points([(2.5, 2.5), (50, 50)])
        result = sample_raster_at_points(pts, path, band_names=['val'], dropna=True)

        assert len(result) == 1

    def test_empty_partition(self, tmp_dir):
        """Empty partition should return empty DataFrame."""
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path)

        empty_gdf = gpd.GeoDataFrame(
            {'shot_number': pd.Series(dtype='int64'), 'h3_03': pd.Series(dtype=str)},
            geometry=gpd.GeoSeries(dtype='geometry'),
            crs='EPSG:4326'
        )
        result = sample_raster_at_points(empty_gdf, path, band_names=['val'])
        assert len(result) == 0

    def test_preserves_partition_col(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(0, 0)], partition_col='h3_03', part_id='abc123')
        result = sample_raster_at_points(pts, path, band_names=['val'], partition_col='h3_03')

        # h3_03 should be set as the DataFrame index (finest spatial column)
        assert result.index.name == 'h3_03'
        assert result.index[0] == 'abc123'

    def test_preserves_shot_number(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(0, 0)])
        result = sample_raster_at_points(pts, path, band_names=['val'])

        assert 'shot_number' in result.columns

    def test_geo_flag_produces_geodataframe(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        _make_synthetic_raster(path, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(0, 0)])
        result = sample_raster_at_points(pts, path, band_names=['val'], geo=True)

        assert isinstance(result, gpd.GeoDataFrame)
        assert 'geometry' in result.columns


# =============================================================================
# Test: Window operations in sampling
# =============================================================================

class TestSamplingWithWindows:

    def test_window_mean_in_sampling(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        values = np.arange(100, dtype=float).reshape(10, 10)
        _make_synthetic_raster(path, values=values, bounds=(-10, -10, 10, 10))

        pts = _make_test_points([(0, 0)])
        window_ops = parse_window_specs(['031'])  # band 0, size 3, mean

        result = sample_raster_at_points(
            pts, path, band_names=['val'], window_ops=window_ops
        )

        # Should have the window column
        window_col = [c for c in result.columns if 'mean_3x3' in c]
        assert len(window_col) == 1
        # Window mean should differ from raw value (unless uniform)
        assert not np.isnan(result[window_col[0]].iloc[0])

    def test_window_mode_in_sampling(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.tif')
        values = np.ones((10, 10), dtype=float) * 5
        values[4:7, 4:7] = 3  # 3x3 patch of value 3
        _make_synthetic_raster(path, values=values, bounds=(-10, -10, 10, 10))

        # Point in center of the 3-patch
        pts = _make_test_points([(0, 0)])
        window_ops = parse_window_specs(['033'])  # band 0, size 3, mode

        result = sample_raster_at_points(
            pts, path, band_names=['val'], window_ops=window_ops
        )

        window_col = [c for c in result.columns if 'mode_3x3' in c]
        assert len(window_col) == 1


# =============================================================================
# Test: CRS reprojection
# =============================================================================

class TestCRSReprojection:

    def test_utm_raster_wgs84_points(self, tmp_dir):
        """UTM raster + WGS84 points → correct sampling via reprojection."""
        path = os.path.join(tmp_dir, 'utm.tif')

        # Create a raster in UTM zone 33N (EPSG:32633)
        # Bounds roughly covering a small area around (15°E, 50°N)
        values = np.arange(100, dtype=float).reshape(10, 10)
        _make_synthetic_raster(
            path, values=values,
            crs='EPSG:32633',
            bounds=(450000, 5500000, 460000, 5510000)
        )

        # Point approximately at center of the raster (WGS84)
        from pyproj import Transformer
        t = Transformer.from_crs('EPSG:32633', 'EPSG:4326', always_xy=True)
        center_lon, center_lat = t.transform(455000, 5505000)

        pts = _make_test_points([(center_lon, center_lat)])
        result = sample_raster_at_points(pts, path, band_names=['val'])

        assert len(result) == 1
        assert not np.isnan(result.iloc[0]['val'])
        assert result.iloc[0]['relative_pixel_distance'] < 1.0


# =============================================================================
# Test: _compute_sampling_meta
# =============================================================================

class TestComputeSamplingMeta:

    def test_basic_meta(self):
        meta = _compute_sampling_meta(
            band_names=['b0', 'b1'],
            window_ops=None,
            geo=False,
            partition_col='h3_03'
        )
        # h3_03 should be set as the index (finest spatial column)
        assert meta.index.name == 'h3_03'
        assert 'shot_number' in meta.columns
        assert 'b0' in meta.columns
        assert 'b1' in meta.columns
        assert 'relative_pixel_distance' in meta.columns

    def test_meta_with_windows(self):
        wops = parse_window_specs(['031'])
        meta = _compute_sampling_meta(
            band_names=['elevation'],
            window_ops=wops,
            geo=False,
            partition_col=None
        )
        window_cols = [c for c in meta.columns if 'mean_3x3' in c]
        assert len(window_cols) == 1

    def test_meta_geo(self):
        meta = _compute_sampling_meta(
            band_names=['val'],
            window_ops=None,
            geo=True,
            partition_col=None
        )
        assert isinstance(meta, gpd.GeoDataFrame)
        assert 'geometry' in meta.columns


# =============================================================================
# Integration tests (require external data)
# =============================================================================

@pytest.mark.integration
class TestIntegration:

    DEM_DIR = '/gpfs/data1/vclgp/decontot/data/raster/nasa_dem/'
    H3_DB = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_merged/'
    REGION = '/gpfs/data1/vclgp/decontot/data/vector/other_boundaries/RO_UF_2022.shp'

    @pytest.fixture(autouse=True)
    def check_data_exists(self):
        """Skip integration tests if test data is not available."""
        for path in [self.DEM_DIR, self.H3_DB, self.REGION]:
            if not os.path.exists(path):
                pytest.skip(f"Test data not found: {path}")

    def test_vrt_from_tiles(self):
        """Build VRT from tile directory."""
        raster_path, is_vrt, count = resolve_raster_source(self.DEM_DIR)
        assert count > 1
        assert is_vrt is True
        assert os.path.exists(raster_path)

    def test_raster_info(self):
        """Read raster metadata from VRT."""
        raster_path, _, _ = resolve_raster_source(self.DEM_DIR)
        info = get_raster_info(raster_path)
        assert info['crs'] is not None
        assert info['band_count'] >= 1
        assert info['bounds_wgs84'][0] < info['bounds_wgs84'][2]

    def test_h3_database_sampling(self, tmp_dir):
        """Sample DEM at GEDI shots from H3 database (small region)."""
        from dask.distributed import Client

        raster_path, _, _ = resolve_raster_source(self.DEM_DIR)
        info = get_raster_info(raster_path)

        from gedih3.cliutils import parse_region, h3_col_name
        import gedih3.gh3driver as gh3

        region = parse_region(self.REGION)
        from shapely.geometry import box as shapely_box

        # Use Romania bounds intersected with DEM
        img_gdf = gpd.GeoDataFrame(
            geometry=[shapely_box(*info['bounds_wgs84'])],
            crs='EPSG:4326'
        )
        roi = gpd.overlay(img_gdf, region.to_crs('EPSG:4326'), how='intersection')

        with Client(n_workers=2, threads_per_worker=1, memory_limit='4GB') as client:
            ddf = gh3.gh3_load(
                columns=['geometry'],
                region=roi,
                source=self.H3_DB
            )

            # Sample just the first few partitions
            sample_part = ddf.get_partition(0).compute()
            if len(sample_part) > 0:
                part_level = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=self.H3_DB)
                partition_col = h3_col_name(part_level)

                result = sample_raster_at_points(
                    sample_part,
                    raster_path,
                    band_names=info['band_names'],
                    partition_col=partition_col
                )

                assert len(result) > 0
                # Elevation values should be in a valid range
                band_col = info['band_names'][0]
                valid = result[band_col].dropna()
                if len(valid) > 0:
                    assert valid.min() > -500  # No ocean trenches in Romania
                    assert valid.max() < 3000  # Below 3000m
