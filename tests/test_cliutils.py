"""
Tests for gedih3.cliutils -- column filtering, naming, coordinate utilities,
and CLI argument parsing functions.

Focuses on the data processing helper functions that are used throughout
the CLI tools and library code.
"""

import argparse
import json
import os
import tempfile
import shutil

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
    parse_egi_levels,
    parse_aggregation,
    parse_region,
    safe_query,
    detect_dataset_format,
    _expand_percentile_specs,
    cli_exception_handler,
    resolve_product_vars,
)
from gedih3.exceptions import GediValidationError


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


# =============================================================================
# Test: parse_egi_levels
# =============================================================================

class TestParseEgiLevels:

    def test_single_level(self):
        assert parse_egi_levels('6') == (6, 12)

    def test_level_with_partition(self):
        assert parse_egi_levels('1:12') == (1, 12)

    def test_level_with_custom_partition(self):
        assert parse_egi_levels('6:10') == (6, 10)

    def test_none_returns_none(self):
        assert parse_egi_levels(None) is None

    def test_min_level(self):
        assert parse_egi_levels('1') == (1, 12)

    def test_max_level(self):
        assert parse_egi_levels('12') == (12, 12)

    def test_zero_level_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be 1-12"):
            parse_egi_levels('0')

    def test_thirteen_level_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be 1-12"):
            parse_egi_levels('13')

    def test_non_integer_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            parse_egi_levels('abc')

    def test_non_integer_pair_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be integers"):
            parse_egi_levels('a:b')

    def test_partition_less_than_level_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be >= level"):
            parse_egi_levels('6:3')

    def test_too_many_colons_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="level:partition"):
            parse_egi_levels('1:2:3')


# =============================================================================
# Test: parse_aggregation
# =============================================================================

class TestParseAggregation:

    def test_single_function(self):
        assert parse_aggregation('mean') == 'mean'

    def test_single_function_std(self):
        assert parse_aggregation('std') == 'std'

    def test_list_of_functions(self):
        result = parse_aggregation("['mean', 'std', 'count']")
        assert result == ['mean', 'std', 'count']

    def test_dict_of_functions(self):
        result = parse_aggregation("{'col': ['mean', 'std']}")
        assert result == {'col': ['mean', 'std']}

    def test_percentile_single(self):
        result = parse_aggregation('p25')
        assert callable(result)
        assert result.__name__ == 'p25'

    def test_percentile_in_list(self):
        result = parse_aggregation("['mean', 'p50', 'p95']")
        assert result[0] == 'mean'
        assert callable(result[1])
        assert callable(result[2])
        assert result[1].__name__ == 'p50'
        assert result[2].__name__ == 'p95'

    def test_json_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(['mean', 'std'], f)
            f.flush()
            result = parse_aggregation(f.name)
        os.unlink(f.name)
        assert result == ['mean', 'std']

    def test_text_file_single_line(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('mean\n')
            f.flush()
            result = parse_aggregation(f.name)
        os.unlink(f.name)
        assert result == 'mean'

    def test_text_file_multi_line(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('mean\nstd\ncount\n')
            f.flush()
            result = parse_aggregation(f.name)
        os.unlink(f.name)
        assert result == ['mean', 'std', 'count']

    def test_text_file_with_comments(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('# aggregation funcs\nmean\n# skip this\nstd\n')
            f.flush()
            result = parse_aggregation(f.name)
        os.unlink(f.name)
        assert result == ['mean', 'std']

    def test_invalid_literal_raises(self):
        with pytest.raises(GediValidationError, match="Invalid aggregation"):
            parse_aggregation("[mean, std]")  # missing quotes

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write('# just comments\n')
            f.flush()
            with pytest.raises(GediValidationError, match="empty"):
                parse_aggregation(f.name)
        os.unlink(f.name)


# =============================================================================
# Test: _expand_percentile_specs
# =============================================================================

class TestExpandPercentileSpecs:

    def test_string_percentile(self):
        result = _expand_percentile_specs('p25')
        assert callable(result)
        assert result.__name__ == 'p25'

    def test_string_non_percentile(self):
        assert _expand_percentile_specs('mean') == 'mean'

    def test_list_with_percentiles(self):
        result = _expand_percentile_specs(['mean', 'p50', 'p95'])
        assert result[0] == 'mean'
        assert callable(result[1])
        assert callable(result[2])

    def test_dict_with_percentiles(self):
        result = _expand_percentile_specs({'col': ['mean', 'p50']})
        assert result['col'][0] == 'mean'
        assert callable(result['col'][1])

    def test_dict_with_single_value(self):
        result = _expand_percentile_specs({'col': 'p75'})
        assert callable(result['col'])

    def test_none_passthrough(self):
        assert _expand_percentile_specs(None) is None

    def test_numeric_passthrough(self):
        assert _expand_percentile_specs(42) == 42


# =============================================================================
# Test: parse_region
# =============================================================================

class TestParseRegion:

    def test_none_returns_none(self):
        assert parse_region(None) is None

    def test_bbox_string(self):
        result = parse_region("-51,0,-50,1")
        # Should return a GeoDataFrame or similar spatial object
        assert result is not None

    def test_shapefile_path(self):
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, 'test.shp')
            gdf = gpd.GeoDataFrame(
                {'id': [1]},
                geometry=[Point(-50.5, 0.5)],
                crs='EPSG:4326'
            )
            gdf.to_file(path)
            result = parse_region(path)
            assert result is not None
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_invalid_bbox_raises(self):
        with pytest.raises(GediValidationError):
            parse_region("not,valid,bbox,here")

    def test_invalid_format_raises(self):
        with pytest.raises(GediValidationError):
            parse_region("invalidregionspec")


# =============================================================================
# Test: safe_query
# =============================================================================

class TestSafeQuery:

    def test_normal_query(self):
        df = pd.DataFrame({'agbd_l4a': [10.0, 20.0, 30.0], 'flag': [1, 0, 1]})
        result = safe_query(df, 'agbd_l4a > 15')
        assert len(result) == 2

    def test_empty_query_returns_original(self):
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = safe_query(df, '')
        assert len(result) == 3

    def test_none_query_returns_original(self):
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = safe_query(df, None)
        assert len(result) == 3

    def test_columns_with_slashes(self):
        df = pd.DataFrame({
            'rx/energy': [1.0, 2.0, 3.0],
            'agbd': [10.0, 20.0, 30.0],
        })
        result = safe_query(df, '`rx/energy` > 1.5')
        assert len(result) == 2
        # Verify original column names preserved
        assert 'rx/energy' in result.columns


# =============================================================================
# Test: detect_dataset_format
# =============================================================================

class TestDetectDatasetFormat:

    def test_parquet_directory(self):
        tmpdir = tempfile.mkdtemp()
        try:
            # Create a parquet file
            df = pd.DataFrame({'a': [1, 2]})
            df.to_parquet(os.path.join(tmpdir, 'data.parquet'))
            result = detect_dataset_format(tmpdir)
            assert result == 'parquet'
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_with_metadata_file(self):
        tmpdir = tempfile.mkdtemp()
        try:
            meta = {'file_format': 'parquet'}
            with open(os.path.join(tmpdir, 'gedih3_dataset.json'), 'w') as f:
                json.dump(meta, f)
            result = detect_dataset_format(tmpdir)
            assert result == 'parquet'
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_empty_dir_defaults_parquet(self):
        tmpdir = tempfile.mkdtemp()
        try:
            result = detect_dataset_format(tmpdir)
            assert result == 'parquet'
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# =============================================================================
# Test: cli_exception_handler
# =============================================================================

class TestCliExceptionHandler:

    def test_normal_execution(self):
        args = argparse.Namespace(verbose=0)
        with cli_exception_handler(args):
            pass  # no error

    def test_exception_causes_exit(self):
        args = argparse.Namespace(verbose=0)
        with pytest.raises(SystemExit) as exc_info:
            with cli_exception_handler(args):
                raise ValueError("test error")
        assert exc_info.value.code == 1

    def test_keyboard_interrupt_causes_exit(self):
        args = argparse.Namespace(verbose=0)
        with pytest.raises(SystemExit) as exc_info:
            with cli_exception_handler(args):
                raise KeyboardInterrupt()
        assert exc_info.value.code == 130


# =============================================================================
# Test: resolve_product_vars
# =============================================================================

def _make_product_args(**overrides):
    """Build a Namespace mimicking parsed add_product_args() output."""
    defaults = dict(detail_level=None, l1b=None, l2a=None, l2b=None, l4a=None, l4c=None)
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestResolveProductVars:

    def test_detail_level_excludes_l1b(self):
        """--detail-level default should include L2A/L2B/L4A/L4C but NOT L1B."""
        result = resolve_product_vars(_make_product_args(detail_level='default'))
        assert 'L1B' not in result
        for prod in ('L2A', 'L2B', 'L4A', 'L4C'):
            assert result[prod] == ['default']

    def test_detail_level_minimal_excludes_l1b(self):
        result = resolve_product_vars(_make_product_args(detail_level='min'))
        assert 'L1B' not in result
        assert result['L2A'] == ['min']

    def test_detail_level_all_excludes_l1b(self):
        result = resolve_product_vars(_make_product_args(detail_level='all'))
        assert 'L1B' not in result
        assert result['L4A'] == ['all']

    def test_detail_level_with_l1b_bare(self):
        """--detail-level min -l1b (bare) should include L1B as all."""
        result = resolve_product_vars(_make_product_args(detail_level='min', l1b=[]))
        assert result['L1B'] == ['all']
        assert result['L2A'] == ['min']

    def test_detail_level_with_l1b_vars(self):
        """--detail-level all -l1b rxwaveform should include L1B with specified vars."""
        result = resolve_product_vars(_make_product_args(detail_level='all', l1b=['rxwaveform']))
        assert result['L1B'] == ['rxwaveform']
        assert result['L2A'] == ['all']

    def test_detail_level_with_l2a_raises(self):
        """--detail-level default -l2a should raise GediValidationError."""
        with pytest.raises(GediValidationError):
            resolve_product_vars(_make_product_args(detail_level='default', l2a=['rh']))

    def test_detail_level_with_l4c_raises(self):
        """--detail-level default -l4c should raise GediValidationError."""
        with pytest.raises(GediValidationError):
            resolve_product_vars(_make_product_args(detail_level='default', l4c=[]))

    def test_per_product_mode(self):
        """-l2a default -l4a minimal should work without --detail-level."""
        result = resolve_product_vars(_make_product_args(l2a=['default'], l4a=['minimal']))
        assert result == {'L2A': ['default'], 'L4A': ['minimal']}

    def test_per_product_with_l1b(self):
        """-l1b -l2a default should both be included."""
        result = resolve_product_vars(_make_product_args(l1b=[], l2a=['default']))
        assert result['L1B'] == ['all']
        assert result['L2A'] == ['default']

    def test_no_flags_returns_empty(self):
        """No flags at all should return empty dict."""
        result = resolve_product_vars(_make_product_args())
        assert result == {}
