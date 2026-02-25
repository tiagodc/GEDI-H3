"""
Tests for gedih3.vecutils -- vector polygon spatial join utilities.

Tests use synthetic shapefiles and GeoDataFrames to validate spatial join
operations without requiring external data.
"""

import os
import tempfile
import shutil

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point, Polygon, box

from gedih3.vecutils import (
    resolve_vector_source,
    get_vector_info,
    load_vector,
    join_polygons_to_points,
    _compute_join_meta,
    _empty_join_result,
    _VECTOR_CACHE,
)
from gedih3.exceptions import GediSpatialJoinError


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def clear_vector_cache():
    """Clear worker-level vector cache between tests."""
    _VECTOR_CACHE.clear()
    yield
    _VECTOR_CACHE.clear()


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp(prefix="vecutils_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_polygons(tmp_dir):
    """Create a GeoJSON file with two non-overlapping polygons."""
    polys = gpd.GeoDataFrame({
        'region_name': ['RegionA', 'RegionB'],
        'region_code': [1, 2],
        'area_km2': [100.0, 200.0],
    }, geometry=[
        box(-51, 0, -50.5, 0.5),   # RegionA: western half
        box(-50.5, 0, -50, 0.5),   # RegionB: eastern half
    ], crs='EPSG:4326')
    path = os.path.join(tmp_dir, 'regions.geojson')
    polys.to_file(path, driver='GeoJSON')
    return path


@pytest.fixture
def sample_points_gdf():
    """GeoDataFrame of points: some inside RegionA, some inside RegionB, one outside."""
    points = gpd.GeoDataFrame({
        'h3_03': ['abc'] * 5,
        'shot_number': np.arange(5, dtype=np.int64),
        'agbd_l4a': [10.0, 20.0, 30.0, 40.0, 50.0],
    }, geometry=[
        Point(-50.8, 0.25),   # inside RegionA
        Point(-50.7, 0.25),   # inside RegionA
        Point(-50.3, 0.25),   # inside RegionB
        Point(-50.2, 0.25),   # inside RegionB
        Point(-49.0, 5.0),    # outside both
    ], crs='EPSG:4326')
    return points


# =============================================================================
# Test: resolve_vector_source
# =============================================================================

class TestResolveVectorSource:

    def test_single_file(self, sample_polygons):
        path, count = resolve_vector_source(sample_polygons)
        assert path == sample_polygons
        assert count == 1

    def test_directory_with_single_file(self, tmp_dir, sample_polygons):
        path, count = resolve_vector_source(tmp_dir)
        assert os.path.isfile(path)
        assert count == 1

    def test_nonexistent_path_raises(self):
        with pytest.raises(GediSpatialJoinError, match="not found"):
            resolve_vector_source('/nonexistent/path/to/shapefile.shp')

    def test_empty_directory_raises(self, tmp_dir):
        empty_dir = os.path.join(tmp_dir, 'empty')
        os.makedirs(empty_dir)
        with pytest.raises(GediSpatialJoinError, match="No vector files"):
            resolve_vector_source(empty_dir)

    def test_directory_with_format_filter(self, tmp_dir, sample_polygons):
        # Create a shapefile too (sample_polygons is geojson)
        gdf = gpd.read_file(sample_polygons)
        gdf.to_file(os.path.join(tmp_dir, 'other.shp'))
        # Filter to only geojson
        path, count = resolve_vector_source(tmp_dir, file_format='geojson')
        assert path.endswith('.geojson')


# =============================================================================
# Test: get_vector_info
# =============================================================================

class TestGetVectorInfo:

    def test_basic_metadata(self, sample_polygons):
        info = get_vector_info(sample_polygons)
        assert 'crs' in info
        assert 'bounds_wgs84' in info
        assert 'columns' in info
        assert 'feature_count' in info
        assert 'geometry_type' in info
        assert info['feature_count'] == 2
        assert 'region_name' in info['columns']
        assert 'region_code' in info['columns']

    def test_bounds_wgs84(self, sample_polygons):
        info = get_vector_info(sample_polygons)
        bounds = info['bounds_wgs84']
        assert len(bounds) == 4
        # Bounds should roughly match our polygon extents
        assert bounds[0] <= -50.5  # west
        assert bounds[2] >= -50.0  # east

    def test_projected_crs_bounds_reprojected(self, tmp_dir):
        """Verify that bounds from a projected CRS are reprojected to WGS84."""
        polys = gpd.GeoDataFrame({
            'name': ['test'],
        }, geometry=[box(0, 0, 100000, 100000)], crs='EPSG:32618')
        path = os.path.join(tmp_dir, 'projected.gpkg')
        polys.to_file(path, driver='GPKG')
        info = get_vector_info(path)
        bounds = info['bounds_wgs84']
        # Bounds should be in WGS84 range
        assert -180 <= bounds[0] <= 180
        assert -90 <= bounds[1] <= 90


# =============================================================================
# Test: load_vector
# =============================================================================

class TestLoadVector:

    def test_basic_load(self, sample_polygons):
        gdf = load_vector(sample_polygons)
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) == 2
        assert gdf.crs.to_epsg() == 4326

    def test_column_filtering(self, sample_polygons):
        gdf = load_vector(sample_polygons, columns=['region_name'])
        assert 'region_name' in gdf.columns
        assert 'region_code' not in gdf.columns
        assert 'geometry' in gdf.columns

    def test_missing_column_raises(self, sample_polygons):
        with pytest.raises(GediSpatialJoinError, match="Columns not found"):
            load_vector(sample_polygons, columns=['nonexistent_col'])

    def test_crs_reprojection(self, tmp_dir):
        """Load a projected file and verify reprojection to WGS84."""
        polys = gpd.GeoDataFrame({
            'name': ['test'],
        }, geometry=[box(500000, 0, 600000, 100000)], crs='EPSG:32618')
        path = os.path.join(tmp_dir, 'projected.gpkg')
        polys.to_file(path, driver='GPKG')
        gdf = load_vector(path, to_crs=4326)
        assert gdf.crs.to_epsg() == 4326

    def test_invalid_geometry_type_raises(self, tmp_dir):
        """Lines should be rejected."""
        from shapely.geometry import LineString
        lines = gpd.GeoDataFrame({
            'name': ['line1'],
        }, geometry=[LineString([(0, 0), (1, 1)])], crs='EPSG:4326')
        path = os.path.join(tmp_dir, 'lines.geojson')
        lines.to_file(path, driver='GeoJSON')
        with pytest.raises(GediSpatialJoinError, match="unsupported geometry"):
            load_vector(path)


# =============================================================================
# Test: join_polygons_to_points
# =============================================================================

class TestJoinPolygonsToPoints:

    def test_left_join_keeps_all_points(self, sample_polygons, sample_points_gdf):
        result = join_polygons_to_points(
            sample_points_gdf,
            sample_polygons,
            join_columns=['region_name', 'region_code'],
            predicate='within',
            how='left',
        )
        # Left join keeps all 5 points
        assert len(result) == 5
        assert 'region_name' in result.columns
        assert 'region_code' in result.columns

    def test_inner_join_drops_unmatched(self, sample_polygons, sample_points_gdf):
        result = join_polygons_to_points(
            sample_points_gdf,
            sample_polygons,
            join_columns=['region_name'],
            predicate='within',
            how='inner',
        )
        # Inner join drops the point outside both regions
        assert len(result) == 4

    def test_column_prefix(self, sample_polygons, sample_points_gdf):
        result = join_polygons_to_points(
            sample_points_gdf,
            sample_polygons,
            join_columns=['region_name'],
            prefix='eco_',
        )
        assert 'eco_region_name' in result.columns
        assert 'region_name' not in result.columns

    def test_intersects_predicate(self, sample_polygons, sample_points_gdf):
        result = join_polygons_to_points(
            sample_points_gdf,
            sample_polygons,
            join_columns=['region_name'],
            predicate='intersects',
        )
        # intersects should match same points as within for points
        assert len(result) >= 4

    def test_empty_input(self, sample_polygons):
        empty = gpd.GeoDataFrame({
            'h3_03': pd.Series(dtype='object'),
            'shot_number': pd.Series(dtype='int64'),
            'agbd_l4a': pd.Series(dtype='float64'),
        }, geometry=gpd.GeoSeries(dtype='geometry'), crs='EPSG:4326')
        result = join_polygons_to_points(
            empty,
            sample_polygons,
            join_columns=['region_name'],
        )
        assert len(result) == 0

    def test_preserves_shot_number(self, sample_polygons, sample_points_gdf):
        result = join_polygons_to_points(
            sample_points_gdf,
            sample_polygons,
            join_columns=['region_name'],
        )
        assert 'shot_number' in result.columns

    def test_column_conflict_raises(self, tmp_dir, sample_points_gdf):
        """Column name conflict without prefix should raise."""
        # Create polygon with 'agbd_l4a' column (same as points)
        polys = gpd.GeoDataFrame({
            'agbd_l4a': [999.0],
        }, geometry=[box(-52, -1, -49, 2)], crs='EPSG:4326')
        path = os.path.join(tmp_dir, 'conflict.geojson')
        polys.to_file(path, driver='GeoJSON')
        with pytest.raises(GediSpatialJoinError, match="Column name conflicts"):
            join_polygons_to_points(
                sample_points_gdf,
                path,
                join_columns=['agbd_l4a'],
            )

    def test_prefix_applied_to_polygon_columns(self, tmp_dir, sample_points_gdf):
        """Verify prefix is applied to polygon attribute columns."""
        polys = gpd.GeoDataFrame({
            'biome': ['tropical'],
            'eco_id': [42],
        }, geometry=[box(-52, -1, -49, 2)], crs='EPSG:4326')
        path = os.path.join(tmp_dir, 'biome.geojson')
        polys.to_file(path, driver='GeoJSON')
        result = join_polygons_to_points(
            sample_points_gdf,
            path,
            join_columns=['biome', 'eco_id'],
            prefix='poly_',
        )
        assert 'poly_biome' in result.columns
        assert 'poly_eco_id' in result.columns
        assert 'biome' not in result.columns

    def test_no_geometry_column_raises(self, sample_polygons):
        """DataFrame without geometry should raise."""
        df = pd.DataFrame({
            'h3_03': ['abc'],
            'shot_number': np.array([1], dtype=np.int64),
        })
        with pytest.raises(GediSpatialJoinError, match="no geometry"):
            join_polygons_to_points(df, sample_polygons, join_columns=['region_name'])


# =============================================================================
# Test: _compute_join_meta
# =============================================================================

class TestComputeJoinMeta:

    def test_basic_schema(self):
        result = _compute_join_meta(
            join_columns=['region_name', 'region_code'],
            polygon_dtypes={'region_name': 'object', 'region_code': 'int64'},
            prefix=None,
            geo=False,
            partition_col='h3_03',
        )
        assert isinstance(result, pd.DataFrame)
        assert 'region_name' in result.columns
        assert 'region_code' in result.columns
        assert 'shot_number' in result.columns
        assert len(result) == 0

    def test_with_prefix(self):
        result = _compute_join_meta(
            join_columns=['region_name'],
            polygon_dtypes={'region_name': 'object'},
            prefix='eco_',
            geo=False,
            partition_col=None,
        )
        assert 'eco_region_name' in result.columns
        assert 'region_name' not in result.columns

    def test_with_geo(self):
        result = _compute_join_meta(
            join_columns=['name'],
            polygon_dtypes={'name': 'object'},
            prefix=None,
            geo=True,
            partition_col=None,
        )
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'geometry' in result.columns


# =============================================================================
# Test: _empty_join_result
# =============================================================================

class TestEmptyJoinResult:

    def test_basic_empty(self):
        result = _empty_join_result(
            join_columns=['name'],
            prefix=None,
            geo=False,
            partition_col=None,
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'name' in result.columns
        assert 'shot_number' in result.columns

    def test_with_prefix(self):
        result = _empty_join_result(
            join_columns=['name'],
            prefix='eco_',
            geo=False,
            partition_col=None,
        )
        assert 'eco_name' in result.columns

    def test_with_geo(self):
        result = _empty_join_result(
            join_columns=['name'],
            prefix=None,
            geo=True,
            partition_col=None,
        )
        assert isinstance(result, gpd.GeoDataFrame)

    def test_with_partition_col(self):
        result = _empty_join_result(
            join_columns=['name'],
            prefix=None,
            geo=False,
            partition_col='h3_03',
        )
        # h3_03 should be the index
        assert result.index.name == 'h3_03' or 'h3_03' in result.columns
