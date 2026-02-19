"""
Tests for gedih3.validation -- parameter validation functions.

Covers all public validation functions with valid inputs, invalid inputs,
edge cases, and boundary conditions.
"""

import os
import json
import tempfile
import shutil

import pytest
import h3

from gedih3.validation import (
    validate_h3_resolution,
    validate_h3_params,
    validate_h3_cell,
    validate_egi_level,
    validate_product,
    validate_product_vars,
    validate_file_exists,
    validate_directory_exists,
    validate_database_path,
    validate_coordinates,
    validate_bbox,
)
from gedih3.exceptions import (
    H3ValidationError,
    EGIValidationError,
    GediProductError,
    GediVariableError,
    GediFileError,
    GediDatabaseNotFoundError,
)


# =============================================================================
# Test: validate_h3_resolution
# =============================================================================

class TestValidateH3Resolution:

    def test_valid_min(self):
        assert validate_h3_resolution(0) == 0

    def test_valid_mid(self):
        assert validate_h3_resolution(6) == 6

    def test_valid_max(self):
        assert validate_h3_resolution(15) == 15

    def test_all_valid_levels(self):
        for level in range(0, 16):
            assert validate_h3_resolution(level) == level

    def test_negative_raises(self):
        with pytest.raises(H3ValidationError, match="between 0 and 15"):
            validate_h3_resolution(-1)

    def test_above_max_raises(self):
        with pytest.raises(H3ValidationError, match="between 0 and 15"):
            validate_h3_resolution(16)

    def test_string_raises(self):
        with pytest.raises(H3ValidationError, match="must be an integer"):
            validate_h3_resolution("abc")

    def test_none_raises(self):
        with pytest.raises(H3ValidationError, match="must be an integer"):
            validate_h3_resolution(None)

    def test_float_raises(self):
        with pytest.raises(H3ValidationError, match="must be an integer"):
            validate_h3_resolution(6.0)

    def test_custom_param_name_in_error(self):
        with pytest.raises(H3ValidationError, match="partition"):
            validate_h3_resolution(-1, param_name='partition')

    def test_error_attributes(self):
        with pytest.raises(H3ValidationError) as exc_info:
            validate_h3_resolution(20)
        assert exc_info.value.param_name == 'resolution'
        assert exc_info.value.value == 20


# =============================================================================
# Test: validate_h3_params
# =============================================================================

class TestValidateH3Params:

    def test_valid_pair_12_3(self):
        assert validate_h3_params(12, 3) == (12, 3)

    def test_valid_pair_6_3(self):
        assert validate_h3_params(6, 3) == (6, 3)

    def test_valid_equal_levels(self):
        assert validate_h3_params(6, 6) == (6, 6)

    def test_valid_min_pair(self):
        assert validate_h3_params(0, 0) == (0, 0)

    def test_valid_max_pair(self):
        assert validate_h3_params(15, 15) == (15, 15)

    def test_partition_greater_than_res_raises(self):
        with pytest.raises(H3ValidationError, match="partition level"):
            validate_h3_params(3, 12)

    def test_invalid_res_raises(self):
        with pytest.raises(H3ValidationError):
            validate_h3_params(16, 3)

    def test_invalid_partition_raises(self):
        with pytest.raises(H3ValidationError):
            validate_h3_params(12, -1)

    def test_string_res_raises(self):
        with pytest.raises(H3ValidationError, match="must be an integer"):
            validate_h3_params("12", 3)


# =============================================================================
# Test: validate_h3_cell
# =============================================================================

class TestValidateH3Cell:

    def test_valid_cell_res3(self):
        cell = h3.latlng_to_cell(0, 0, 3)
        assert validate_h3_cell(cell) == cell

    def test_valid_cell_res12(self):
        cell = h3.latlng_to_cell(45.0, -90.0, 12)
        assert validate_h3_cell(cell) == cell

    def test_valid_cell_with_expected_res(self):
        cell = h3.latlng_to_cell(0, 0, 6)
        assert validate_h3_cell(cell, expected_res=6) == cell

    def test_wrong_expected_res_raises(self):
        cell = h3.latlng_to_cell(0, 0, 6)
        with pytest.raises(H3ValidationError, match="has resolution 6, expected 3"):
            validate_h3_cell(cell, expected_res=3)

    def test_invalid_string_raises(self):
        with pytest.raises(H3ValidationError, match="Invalid H3 cell"):
            validate_h3_cell("not_a_cell")

    def test_empty_string_raises(self):
        with pytest.raises(H3ValidationError, match="Invalid H3 cell"):
            validate_h3_cell("")

    def test_non_string_raises(self):
        with pytest.raises(H3ValidationError, match="must be a string"):
            validate_h3_cell(12345)

    def test_none_raises(self):
        with pytest.raises(H3ValidationError, match="must be a string"):
            validate_h3_cell(None)


# =============================================================================
# Test: validate_egi_level
# =============================================================================

class TestValidateEgiLevel:

    def test_valid_min(self):
        assert validate_egi_level(1) == 1

    def test_valid_mid(self):
        assert validate_egi_level(6) == 6

    def test_valid_max(self):
        assert validate_egi_level(12) == 12

    def test_all_valid_levels(self):
        for level in range(1, 13):
            assert validate_egi_level(level) == level

    def test_zero_raises(self):
        with pytest.raises(EGIValidationError, match="between 1 and 12"):
            validate_egi_level(0)

    def test_thirteen_raises(self):
        with pytest.raises(EGIValidationError, match="between 1 and 12"):
            validate_egi_level(13)

    def test_negative_raises(self):
        with pytest.raises(EGIValidationError, match="between 1 and 12"):
            validate_egi_level(-1)

    def test_float_raises(self):
        with pytest.raises(EGIValidationError, match="must be an integer"):
            validate_egi_level(6.0)

    def test_string_raises(self):
        with pytest.raises(EGIValidationError, match="must be an integer"):
            validate_egi_level("6")

    def test_none_raises(self):
        with pytest.raises(EGIValidationError, match="must be an integer"):
            validate_egi_level(None)

    def test_custom_param_name(self):
        with pytest.raises(EGIValidationError, match="partition"):
            validate_egi_level(0, param_name='partition')

    def test_error_attributes(self):
        with pytest.raises(EGIValidationError) as exc_info:
            validate_egi_level(0)
        assert exc_info.value.param_name == 'level'
        assert exc_info.value.value == 0


# =============================================================================
# Test: validate_product
# =============================================================================

class TestValidateProduct:

    def test_valid_uppercase(self):
        assert validate_product('L2A') == 'L2A'

    def test_valid_lowercase(self):
        assert validate_product('l4a') == 'L4A'

    def test_valid_mixed_case(self):
        assert validate_product('L2b') == 'L2B'

    def test_all_valid_products(self):
        for p in ['L1B', 'L2A', 'L2B', 'L3', 'L4A', 'L4B', 'L4C']:
            assert validate_product(p) == p

    def test_invalid_product_raises(self):
        with pytest.raises(GediProductError, match="Invalid GEDI product"):
            validate_product('L5A')

    def test_empty_string_raises(self):
        with pytest.raises(GediProductError, match="Invalid GEDI product"):
            validate_product('')

    def test_non_string_raises(self):
        with pytest.raises(GediProductError, match="must be a string"):
            validate_product(42)

    def test_none_raises(self):
        with pytest.raises(GediProductError, match="must be a string"):
            validate_product(None)


# =============================================================================
# Test: validate_product_vars
# =============================================================================

class TestValidateProductVars:

    def test_valid_dict_with_list(self):
        result = validate_product_vars({'L2A': ['rh', 'quality_flag']})
        assert result == {'L2A': ['rh', 'quality_flag']}

    def test_valid_dict_with_string(self):
        result = validate_product_vars({'L4A': 'agbd'})
        assert result == {'L4A': ['agbd']}

    def test_valid_dict_with_none(self):
        result = validate_product_vars({'L2A': None})
        assert result == {'L2A': None}

    def test_normalizes_product_case(self):
        result = validate_product_vars({'l2a': ['rh']})
        assert 'L2A' in result

    def test_multiple_products(self):
        result = validate_product_vars({'L2A': ['rh'], 'L4A': ['agbd']})
        assert 'L2A' in result
        assert 'L4A' in result

    def test_invalid_product_raises(self):
        with pytest.raises(GediProductError, match="Invalid GEDI product"):
            validate_product_vars({'L5A': ['var']})

    def test_non_dict_raises(self):
        with pytest.raises(GediProductError, match="must be a dictionary"):
            validate_product_vars("L2A")

    def test_non_string_vars_raises(self):
        with pytest.raises(GediVariableError, match="must be strings"):
            validate_product_vars({'L2A': [1, 2, 3]})

    def test_invalid_var_type_raises(self):
        with pytest.raises(GediVariableError, match="must be a string, list, or None"):
            validate_product_vars({'L2A': 42})


# =============================================================================
# Test: validate_file_exists
# =============================================================================

class TestValidateFileExists:

    def test_existing_file(self, tmp_dir):
        path = os.path.join(tmp_dir, 'test.txt')
        with open(path, 'w') as f:
            f.write('test')
        assert validate_file_exists(path) == path

    def test_missing_file_raises(self):
        with pytest.raises(GediFileError, match="not found"):
            validate_file_exists('/nonexistent/path/to/file.txt')

    def test_custom_file_type_in_error(self):
        with pytest.raises(GediFileError, match="HDF5 file"):
            validate_file_exists('/nonexistent/file.h5', file_type='HDF5 file')

    def test_directory_passes(self, tmp_dir):
        # validate_file_exists uses os.path.exists, which is True for dirs too
        assert validate_file_exists(tmp_dir) == tmp_dir


# =============================================================================
# Test: validate_directory_exists
# =============================================================================

class TestValidateDirectoryExists:

    def test_existing_directory(self, tmp_dir):
        assert validate_directory_exists(tmp_dir) == tmp_dir

    def test_missing_directory_raises(self):
        with pytest.raises(GediFileError, match="Directory not found"):
            validate_directory_exists('/nonexistent/directory')

    def test_create_missing_directory(self, tmp_dir):
        new_dir = os.path.join(tmp_dir, 'new_subdir')
        assert not os.path.exists(new_dir)
        result = validate_directory_exists(new_dir, create=True)
        assert result == new_dir
        assert os.path.isdir(new_dir)

    def test_file_not_directory_raises(self, tmp_dir):
        path = os.path.join(tmp_dir, 'a_file.txt')
        with open(path, 'w') as f:
            f.write('test')
        with pytest.raises(GediFileError, match="not a directory"):
            validate_directory_exists(path)

    def test_create_nested(self, tmp_dir):
        nested = os.path.join(tmp_dir, 'a', 'b', 'c')
        validate_directory_exists(nested, create=True)
        assert os.path.isdir(nested)


# =============================================================================
# Test: validate_database_path
# =============================================================================

class TestValidateDatabasePath:

    def test_valid_database_with_h3_dirs(self, tmp_dir):
        # Create fake H3 partition directory
        h3_dir = os.path.join(tmp_dir, 'h3_03=abc123')
        os.makedirs(h3_dir)
        assert validate_database_path(tmp_dir) == tmp_dir

    def test_valid_database_with_parquet_files(self, tmp_dir):
        # Create a fake parquet file
        pq_path = os.path.join(tmp_dir, 'data.parquet')
        with open(pq_path, 'w') as f:
            f.write('fake')
        assert validate_database_path(tmp_dir) == tmp_dir

    def test_nonexistent_path_raises(self):
        with pytest.raises(GediDatabaseNotFoundError, match="not found"):
            validate_database_path('/nonexistent/database')

    def test_file_not_dir_raises(self, tmp_dir):
        path = os.path.join(tmp_dir, 'a_file.txt')
        with open(path, 'w') as f:
            f.write('test')
        with pytest.raises(GediDatabaseNotFoundError, match="not a directory"):
            validate_database_path(path)

    def test_empty_directory_raises(self, tmp_dir):
        empty = os.path.join(tmp_dir, 'empty_db')
        os.makedirs(empty)
        with pytest.raises(GediDatabaseNotFoundError, match="empty or invalid"):
            validate_database_path(empty)


# =============================================================================
# Test: validate_coordinates
# =============================================================================

class TestValidateCoordinates:

    def test_valid_origin(self):
        assert validate_coordinates(0, 0) == (0, 0)

    def test_valid_extremes(self):
        assert validate_coordinates(90, 180) == (90, 180)
        assert validate_coordinates(-90, -180) == (-90, -180)

    def test_valid_float(self):
        assert validate_coordinates(45.123, -73.456) == (45.123, -73.456)

    def test_lat_too_high_raises(self):
        with pytest.raises(ValueError, match="Latitude"):
            validate_coordinates(91, 0)

    def test_lat_too_low_raises(self):
        with pytest.raises(ValueError, match="Latitude"):
            validate_coordinates(-91, 0)

    def test_lon_too_high_raises(self):
        with pytest.raises(ValueError, match="Longitude"):
            validate_coordinates(0, 181)

    def test_lon_too_low_raises(self):
        with pytest.raises(ValueError, match="Longitude"):
            validate_coordinates(0, -181)

    def test_boundary_lat(self):
        assert validate_coordinates(90, 0) == (90, 0)
        assert validate_coordinates(-90, 0) == (-90, 0)

    def test_boundary_lon(self):
        assert validate_coordinates(0, 180) == (0, 180)
        assert validate_coordinates(0, -180) == (0, -180)


# =============================================================================
# Test: validate_bbox
# =============================================================================

class TestValidateBbox:

    def test_valid_bbox(self):
        result = validate_bbox([-50, 0, -49, 1])
        assert result == (-50, 0, -49, 1)

    def test_valid_bbox_tuple(self):
        result = validate_bbox((-180, -90, 180, 90))
        assert result == (-180, -90, 180, 90)

    def test_valid_global_bbox(self):
        result = validate_bbox([-180, -90, 180, 90])
        assert result == (-180, -90, 180, 90)

    def test_antimeridian_crossing_allowed(self):
        # West > East is valid for antimeridian crossing
        result = validate_bbox([170, -10, -170, 10])
        assert result == (170, -10, -170, 10)

    def test_south_greater_than_north_raises(self):
        with pytest.raises(ValueError, match="South.*must be <= North"):
            validate_bbox([-50, 10, -49, 0])

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError, match="4 elements"):
            validate_bbox([-50, 0, -49])

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="4 elements"):
            validate_bbox([])

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="list or tuple"):
            validate_bbox("invalid")

    def test_out_of_range_lat_raises(self):
        with pytest.raises(ValueError, match="Latitude"):
            validate_bbox([-50, -100, -49, 1])

    def test_out_of_range_lon_raises(self):
        with pytest.raises(ValueError, match="Longitude"):
            validate_bbox([-200, 0, -49, 1])

    def test_five_elements_raises(self):
        with pytest.raises(ValueError, match="4 elements"):
            validate_bbox([-50, 0, -49, 1, 5])

    def test_zero_size_bbox_valid(self):
        # South == North is allowed (degenerate bbox)
        result = validate_bbox([0, 0, 1, 0])
        assert result == (0, 0, 1, 0)
