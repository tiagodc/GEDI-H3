"""
Tests for gedih3.cliutils -- column filtering, naming, and coordinate utilities.

Focuses on the data processing helper functions that are used throughout
the CLI tools and library code.
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point

from gedih3.cliutils import (
    is_internal_column,
    filter_data_columns,
    get_numeric_columns,
    get_rasterizable_columns,
    get_aggregatable_columns,
    filter_raster_columns,
    h3_col_name,
    find_coordinate_column,
)


# =============================================================================
# Test: is_internal_column
# =============================================================================

class TestIsInternalColumn:

    def test_h3_partition_columns(self):
        assert is_internal_column('h3_03') is True
        assert is_internal_column('h3_06') is True
        assert is_internal_column('h3_12') is True
        assert is_internal_column('h3_15') is True
        assert is_internal_column('h3_00') is True

    def test_egi_index_columns(self):
        assert is_internal_column('egi06') is True
        assert is_internal_column('egi01') is True
        assert is_internal_column('egi12') is True

    def test_egi_coordinate_columns(self):
        assert is_internal_column('_egi_x') is True
        assert is_internal_column('_egi_y') is True

    def test_shot_number_columns(self):
        assert is_internal_column('shot_number') is True
        assert is_internal_column('shot_number_l2a') is True
        assert is_internal_column('shot_number_l4a') is True

    def test_data_columns_not_internal(self):
        assert is_internal_column('agbd_l4a') is False
        assert is_internal_column('rh_098_l2a') is False
        assert is_internal_column('quality_flag_l2a') is False
        assert is_internal_column('lat_lowestmode_l2a') is False
        assert is_internal_column('lon_lowestmode_l2a') is False

    def test_geometry_not_internal(self):
        assert is_internal_column('geometry') is False

    def test_datetime_not_internal(self):
        assert is_internal_column('datetime') is False

    def test_similar_but_not_internal(self):
        # Patterns that look similar but should not match
        assert is_internal_column('h3_resolution') is False  # not h3_XX pattern
        assert is_internal_column('egi_level') is False  # not egiXX pattern
        assert is_internal_column('my_egi_x') is False  # no leading underscore
        assert is_internal_column('h3') is False  # missing _XX suffix


# =============================================================================
# Test: filter_data_columns
# =============================================================================

class TestFilterDataColumns:

    def test_mixed_list(self):
        cols = ['h3_03', 'agbd_l4a', 'rh_098_l2a', 'shot_number_l2a', 'egi06']
        result = filter_data_columns(cols)
        assert result == ['agbd_l4a', 'rh_098_l2a']

    def test_no_internal_columns(self):
        cols = ['agbd_l4a', 'rh_098_l2a', 'quality_flag_l2a']
        result = filter_data_columns(cols)
        assert result == cols

    def test_all_internal_columns(self):
        cols = ['h3_03', 'h3_12', 'shot_number', 'egi06']
        result = filter_data_columns(cols)
        assert result == []

    def test_geometry_excluded_by_default(self):
        cols = ['agbd_l4a', 'geometry']
        result = filter_data_columns(cols)
        assert result == ['agbd_l4a']

    def test_geometry_included_when_requested(self):
        cols = ['agbd_l4a', 'geometry']
        result = filter_data_columns(cols, exclude_geometry=False)
        assert 'geometry' in result

    def test_empty_list(self):
        assert filter_data_columns([]) == []

    def test_preserves_order(self):
        cols = ['z_col', 'agbd_l4a', 'a_col']
        result = filter_data_columns(cols)
        assert result == ['z_col', 'agbd_l4a', 'a_col']


# =============================================================================
# Test: get_numeric_columns
# =============================================================================

class TestGetNumericColumns:

    def test_with_sample_gdf(self, sample_gdf):
        result = get_numeric_columns(sample_gdf)
        # Should include numeric data columns but not internal ones
        assert 'agbd_l4a' in result
        assert 'rh_098_l2a' in result
        assert 'quality_flag_l2a' in result
        assert 'lat_lowestmode_l2a' in result
        assert 'lon_lowestmode_l2a' in result
        # Should NOT include internal columns
        assert 'shot_number_l2a' not in result
        assert 'h3_03' not in result

    def test_with_dask_gdf(self, sample_ddf):
        result = get_numeric_columns(sample_ddf)
        assert 'agbd_l4a' in result
        assert 'rh_098_l2a' in result
        assert 'shot_number_l2a' not in result

    def test_include_internal(self, sample_gdf):
        result = get_numeric_columns(sample_gdf, exclude_internal=False)
        assert 'shot_number_l2a' in result
        assert 'agbd_l4a' in result

    def test_no_numeric_columns(self):
        df = pd.DataFrame({'name': ['a', 'b'], 'category': ['x', 'y']})
        result = get_numeric_columns(df)
        assert result == []

    def test_mixed_types(self):
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.0, 2.0, 3.0],
            'str_col': ['a', 'b', 'c'],
            'h3_03': ['abc', 'def', 'ghi'],  # internal but string
        })
        result = get_numeric_columns(df)
        assert 'int_col' in result
        assert 'float_col' in result
        assert 'str_col' not in result
        assert 'h3_03' not in result


# =============================================================================
# Test: get_rasterizable_columns
# =============================================================================

class TestGetRasterizableColumns:

    def test_basic(self, sample_gdf):
        result = get_rasterizable_columns(sample_gdf)
        assert 'agbd_l4a' in result
        assert 'rh_098_l2a' in result
        assert 'shot_number_l2a' not in result
        assert 'h3_03' not in result

    def test_with_dask(self, sample_ddf):
        result = get_rasterizable_columns(sample_ddf)
        assert 'agbd_l4a' in result
        assert 'shot_number_l2a' not in result

    def test_no_numeric(self):
        df = pd.DataFrame({'label': ['a', 'b']})
        result = get_rasterizable_columns(df)
        assert result == []


# =============================================================================
# Test: get_aggregatable_columns
# =============================================================================

class TestGetAggregatableColumns:

    def test_with_pandas_df(self, sample_gdf):
        result = get_aggregatable_columns(sample_gdf)
        assert 'agbd_l4a' in result
        assert 'rh_098_l2a' in result
        assert 'shot_number_l2a' not in result
        assert 'h3_03' not in result

    def test_with_dask_df(self, sample_ddf):
        result = get_aggregatable_columns(sample_ddf)
        assert 'agbd_l4a' in result
        assert 'shot_number_l2a' not in result

    def test_excludes_egi_columns(self):
        df = pd.DataFrame({
            'agbd_l4a': [1.0, 2.0],
            'egi06': np.array([100, 200], dtype=np.uint64),
            '_egi_x': [1.0, 2.0],
            '_egi_y': [3.0, 4.0],
        })
        result = get_aggregatable_columns(df)
        assert result == ['agbd_l4a']

    def test_empty_df(self):
        df = pd.DataFrame()
        result = get_aggregatable_columns(df)
        assert result == []


# =============================================================================
# Test: filter_raster_columns
# =============================================================================

class TestFilterRasterColumns:

    def test_with_explicit_columns(self):
        gdf = gpd.GeoDataFrame({
            'agbd_l4a': [1.0],
            'egi06': np.array([100], dtype=np.uint64),
            'h3_03': ['abc'],
        }, geometry=[Point(0, 0)], crs='EPSG:4326')
        result = filter_raster_columns(['agbd_l4a', 'egi06', 'h3_03'], gdf)
        assert result == ['agbd_l4a']

    def test_with_none_auto_detects(self):
        gdf = gpd.GeoDataFrame({
            'agbd_l4a': [1.0, 2.0],
            'rh_098_l2a': [10.0, 20.0],
            'label': ['a', 'b'],
            'shot_number': np.array([1, 2], dtype=np.int64),
        }, geometry=[Point(0, 0), Point(1, 1)], crs='EPSG:4326')
        result = filter_raster_columns(None, gdf)
        assert 'agbd_l4a' in result
        assert 'rh_098_l2a' in result
        assert 'label' not in result
        assert 'shot_number' not in result

    def test_excludes_geometry(self):
        gdf = gpd.GeoDataFrame({
            'agbd_l4a': [1.0],
        }, geometry=[Point(0, 0)], crs='EPSG:4326')
        result = filter_raster_columns(['agbd_l4a', 'geometry'], gdf)
        assert result == ['agbd_l4a']

    def test_all_internal_returns_none(self):
        gdf = gpd.GeoDataFrame({
            'h3_03': ['abc'],
            'shot_number': np.array([1], dtype=np.int64),
        }, geometry=[Point(0, 0)], crs='EPSG:4326')
        result = filter_raster_columns(['h3_03', 'shot_number'], gdf)
        assert result is None

    def test_excludes_index_column(self):
        gdf = gpd.GeoDataFrame({
            'agbd_l4a': [1.0, 2.0],
            'tile_id': ['a', 'b'],
        }, geometry=[Point(0, 0), Point(1, 1)], crs='EPSG:4326')
        gdf = gdf.set_index('tile_id')
        result = filter_raster_columns(['agbd_l4a', 'tile_id'], gdf)
        assert result == ['agbd_l4a']


# =============================================================================
# Test: h3_col_name
# =============================================================================

class TestH3ColName:

    def test_single_digit_level(self):
        assert h3_col_name(3) == 'h3_03'
        assert h3_col_name(0) == 'h3_00'
        assert h3_col_name(6) == 'h3_06'
        assert h3_col_name(9) == 'h3_09'

    def test_double_digit_level(self):
        assert h3_col_name(10) == 'h3_10'
        assert h3_col_name(12) == 'h3_12'
        assert h3_col_name(15) == 'h3_15'

    def test_consistent_format(self):
        # All names should be 5 characters: h3_XX
        for level in range(0, 16):
            name = h3_col_name(level)
            assert len(name) == 5
            assert name.startswith('h3_')


# =============================================================================
# Test: find_coordinate_column
# =============================================================================

class TestFindCoordinateColumn:

    def test_exact_match(self):
        cols = ['lon_lowestmode', 'lat_lowestmode', 'agbd']
        assert find_coordinate_column(cols, 'lon_lowestmode') == 'lon_lowestmode'

    def test_suffix_match(self):
        cols = ['lon_lowestmode_l2a', 'lat_lowestmode_l2a', 'agbd_l4a']
        assert find_coordinate_column(cols, 'lon_lowestmode') == 'lon_lowestmode_l2a'

    def test_no_match_returns_none(self):
        cols = ['agbd_l4a', 'rh_098_l2a']
        assert find_coordinate_column(cols, 'lon_lowestmode') is None

    def test_prefers_l2a_suffix(self):
        cols = ['lon_lowestmode_l2a', 'lon_lowestmode_l4a']
        assert find_coordinate_column(cols, 'lon_lowestmode') == 'lon_lowestmode_l2a'

    def test_single_match_non_l2a(self):
        cols = ['lon_lowestmode_l4a', 'agbd_l4a']
        assert find_coordinate_column(cols, 'lon_lowestmode') == 'lon_lowestmode_l4a'

    def test_simple_column_names(self):
        cols = ['lon', 'lat', 'value']
        assert find_coordinate_column(cols, 'lon') == 'lon'

    def test_empty_columns_list(self):
        assert find_coordinate_column([], 'lon') is None

    def test_partial_prefix_match(self):
        # 'lon' should match 'lon_lowestmode_l2a' since it starts with 'lon'
        cols = ['lon_lowestmode_l2a']
        assert find_coordinate_column(cols, 'lon') == 'lon_lowestmode_l2a'
