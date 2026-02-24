"""
Tests for gedih3.egi.core and gedih3.egi.config -- EGI hash functions.

Focuses on functions NOT already covered by test_egi_comprehensive.py:
- get_level: extract level from hash
- get_scale: get pixel size from hash
- pixels_per_tile: compute tile dimensions
- validate_hash: check hash well-formedness
- hasher: construct hash from components (scalar and array)

Also tests egi.config utility functions:
- validate_level
- get_resolution
- get_level_from_resolution
- egi_col_name
"""

import numpy as np
import pytest

from gedih3.egi.config import (
    EGI_CRS,
    LIMITS,
    OUTER_LEVEL,
    OUTER_RES,
    RESOLUTIONS,
    egi_col_name,
    get_level_from_resolution,
    get_resolution,
    validate_level,
)
from gedih3.egi.core import (
    from_hash,
    get_level,
    get_scale,
    hasher,
    pixels_per_tile,
    to_hash,
    to_parent,
    validate_hash,
)

# =============================================================================
# Test: get_level
# =============================================================================


class TestGetLevel:
    def test_known_levels(self):
        """Hash at each level should decode to that level."""
        x, y = 0.0, 0.0  # EPSG:6933 origin area
        for level in range(1, 13):
            h = to_hash(x, y, level)
            assert int(get_level(h)) == level

    def test_scalar_hash(self):
        h = to_hash(1000000.0, 1000000.0, 6)
        result = get_level(h)
        assert int(result) == 6

    def test_array_of_hashes(self):
        x = np.array([0.0, 1000000.0, -5000000.0])
        y = np.array([0.0, 2000000.0, -3000000.0])
        hashes = to_hash(x, y, level=6)
        levels = get_level(hashes)
        assert np.all(levels == 6)

    def test_mixed_levels_array(self):
        h1 = to_hash(0.0, 0.0, 1)
        h6 = to_hash(0.0, 0.0, 6)
        h12 = to_hash(0.0, 0.0, 12)
        hashes = np.array([h1, h6, h12], dtype=np.uint64)
        levels = get_level(hashes)
        expected = np.array([1, 6, 12])
        np.testing.assert_array_equal(levels, expected)


# =============================================================================
# Test: get_scale
# =============================================================================


class TestGetScale:
    def test_level6_scale(self):
        """Level 6 should have ~1000m pixel size."""
        h = to_hash(0.0, 0.0, 6)
        scale = get_scale(h)
        assert scale == RESOLUTIONS[6]

    def test_level1_scale(self):
        """Level 1 should have ~1m pixel size."""
        h = to_hash(0.0, 0.0, 1)
        scale = get_scale(h)
        assert scale == RESOLUTIONS[1]

    def test_level12_scale(self):
        """Level 12 should have ~160km pixel size."""
        h = to_hash(0.0, 0.0, 12)
        scale = get_scale(h)
        assert scale == RESOLUTIONS[12]

    def test_array_of_hashes(self):
        x = np.array([0.0, 1000000.0])
        y = np.array([0.0, 2000000.0])
        hashes = to_hash(x, y, level=6)
        scales = get_scale(hashes)
        np.testing.assert_array_equal(scales, [RESOLUTIONS[6], RESOLUTIONS[6]])

    def test_all_levels_match_resolutions_table(self):
        """get_scale for each level should match the RESOLUTIONS table."""
        for level in range(1, 13):
            h = to_hash(0.0, 0.0, level)
            assert get_scale(h) == RESOLUTIONS[level]


# =============================================================================
# Test: pixels_per_tile
# =============================================================================


class TestPixelsPerTile:
    def test_with_level_input(self):
        """Level input (1-12) should return OUTER_RES / scale."""
        for level in range(1, 13):
            expected = OUTER_RES / RESOLUTIONS[level]
            assert pixels_per_tile(level) == expected

    def test_with_hash_input(self):
        """Hash input should give same result as level input."""
        for level in range(1, 13):
            h = to_hash(0.0, 0.0, level)
            assert pixels_per_tile(h) == pixels_per_tile(level)

    def test_level6_is_near_integer(self):
        """Level 6 (~1km) should have approximately integer pixels per tile."""
        ppt = pixels_per_tile(6)
        assert abs(ppt - round(ppt)) < 1e-3

    def test_level12_is_one(self):
        """Level 12 (coarsest = OUTER_RES) should have 1 pixel per tile."""
        ppt = pixels_per_tile(12)
        assert ppt == 1.0

    def test_finer_levels_have_more_pixels(self):
        """Finer levels should have more pixels per tile."""
        for i in range(1, 12):
            assert pixels_per_tile(i) >= pixels_per_tile(i + 1)


# =============================================================================
# Test: validate_hash
# =============================================================================


class TestValidateHash:
    def test_valid_hash_scalar(self):
        h = to_hash(0.0, 0.0, 6)
        assert validate_hash(h) is True

    def test_valid_hash_all_levels(self):
        for level in range(1, 13):
            h = to_hash(1000000.0, 1000000.0, level)
            assert validate_hash(h) is True

    def test_invalid_hash_zero(self):
        # Level 0 is invalid (EGI uses 1-12)
        assert validate_hash(np.uint64(0)) is False

    def test_invalid_hash_level_too_high(self):
        # Construct a hash with level 13
        fake_hash = np.uint64(13) * np.uint64(1e18)
        assert validate_hash(fake_hash) is False

    def test_valid_array(self):
        hashes = np.array(
            [
                to_hash(0.0, 0.0, 1),
                to_hash(0.0, 0.0, 6),
                to_hash(0.0, 0.0, 12),
            ],
            dtype=np.uint64,
        )
        assert validate_hash(hashes) is True

    def test_invalid_array_mixed(self):
        valid = to_hash(0.0, 0.0, 6)
        invalid = np.uint64(0)
        hashes = np.array([valid, invalid], dtype=np.uint64)
        assert validate_hash(hashes) is False


# =============================================================================
# Test: hasher (component assembly)
# =============================================================================


class TestHasher:
    def test_scalar_inputs(self):
        """Scalar inputs should produce a uint64."""
        h = hasher(6, 100, 45, 500, 300)
        assert isinstance(h, (np.uint64, np.generic))
        # Verify level is encoded correctly
        assert int(get_level(h)) == 6

    def test_roundtrip_through_from_hash(self):
        """Components -> hasher -> from_hash should return the same components."""
        level, px_o, py_o, px_i, py_i = 6, 100, 45, 500, 300
        h = hasher(level, px_o, py_o, px_i, py_i)
        out_level, out_scale, out_px_o, out_py_o, out_px_i, out_py_i = from_hash(h)

        assert int(out_level) == level
        assert int(out_px_o) == px_o
        assert int(out_py_o) == py_o
        assert int(out_px_i) == px_i
        assert int(out_py_i) == py_i

    def test_array_inputs(self):
        """Array inputs should produce array of uint64."""
        levels = np.array([6, 6], dtype=np.uint64)
        px_o = np.array([100, 150], dtype=np.uint16)
        py_o = np.array([45, 50], dtype=np.uint16)
        px_i = np.array([500, 600], dtype=np.uint32)
        py_i = np.array([300, 400], dtype=np.uint32)

        hashes = hasher(levels, px_o, py_o, px_i, py_i)
        assert len(hashes) == 2
        assert hashes.dtype == np.uint64

    def test_zero_inner_coordinates(self):
        """Zero inner coordinates should produce valid hash."""
        h = hasher(1, 0, 0, 0, 0)
        level, scale, px_o, py_o, px_i, py_i = from_hash(h)
        assert int(level) == 1
        assert int(px_o) == 0
        assert int(py_o) == 0
        assert int(px_i) == 0
        assert int(py_i) == 0

    def test_max_outer_tile_coordinates(self):
        """Maximum outer tile coordinates should encode correctly."""
        # Maximum values: px_outer up to 215, py_outer up to 90
        h = hasher(6, 215, 90, 0, 0)
        _, _, px_o, py_o, _, _ = from_hash(h)
        assert int(px_o) == 215
        assert int(py_o) == 90


# =============================================================================
# Test: egi.config utility functions
# =============================================================================


class TestEgiConfig:
    def test_validate_level_valid(self):
        for level in range(1, 13):
            validate_level(level)  # Should not raise

    def test_validate_level_zero_raises(self):
        with pytest.raises(ValueError, match="between 1 and 12"):
            validate_level(0)

    def test_validate_level_thirteen_raises(self):
        with pytest.raises(ValueError, match="between 1 and 12"):
            validate_level(13)

    def test_validate_level_negative_raises(self):
        with pytest.raises(ValueError, match="between 1 and 12"):
            validate_level(-5)

    def test_get_resolution_all_levels(self):
        for level in range(1, 13):
            res = get_resolution(level)
            assert res == RESOLUTIONS[level]
            assert res > 0

    def test_get_resolution_invalid_raises(self):
        with pytest.raises(ValueError):
            get_resolution(0)

    def test_get_resolution_monotonically_increasing(self):
        """Higher levels should have larger pixel sizes."""
        prev = 0
        for level in range(1, 13):
            res = get_resolution(level)
            assert res > prev
            prev = res

    def test_get_level_from_resolution_all_levels(self):
        for level, res in RESOLUTIONS.items():
            assert get_level_from_resolution(res) == level

    def test_get_level_from_resolution_near_match(self):
        """Slightly off resolutions should still match within tolerance."""
        res6 = RESOLUTIONS[6]
        # 0.5% deviation should match with default 1% tolerance
        assert get_level_from_resolution(res6 * 1.005) == 6

    def test_get_level_from_resolution_no_match(self):
        with pytest.raises(ValueError, match="No EGI level matches"):
            get_level_from_resolution(12345.6789)

    def test_egi_col_name(self):
        assert egi_col_name(1) == "egi01"
        assert egi_col_name(6) == "egi06"
        assert egi_col_name(12) == "egi12"

    def test_outer_res_equals_level12(self):
        """OUTER_RES should equal the resolution at level 12."""
        assert OUTER_RES == RESOLUTIONS[12]

    def test_outer_level_is_12(self):
        assert OUTER_LEVEL == 12

    def test_egi_crs_is_6933(self):
        assert EGI_CRS == 6933

    def test_limits_are_symmetric(self):
        """EPSG:6933 bounds should be symmetric around x=0."""
        assert abs(LIMITS["lon_w"] + LIMITS["lon_e"]) < 1e-6
        assert abs(LIMITS["lat_s"] + LIMITS["lat_n"]) < 1e-6

    def test_resolutions_table_complete(self):
        """All 12 levels should be present."""
        assert len(RESOLUTIONS) == 12
        for level in range(1, 13):
            assert level in RESOLUTIONS


# =============================================================================
# Test: to_hash and from_hash roundtrip (scalar, focused edge cases)
# =============================================================================


class TestToHashEdgeCases:
    def test_origin(self):
        """Coordinates near EPSG:6933 origin should hash correctly."""
        h = to_hash(0.0, 0.0, 6)
        assert validate_hash(h) is True
        assert int(get_level(h)) == 6

    def test_extreme_west(self):
        """Hash near western boundary of EPSG:6933."""
        x = LIMITS["lon_w"] + 1.0  # 1 meter inside
        y = 0.0
        h = to_hash(x, y, 6)
        assert validate_hash(h) is True

    def test_extreme_east(self):
        """Hash near eastern boundary of EPSG:6933."""
        x = LIMITS["lon_e"] - 1.0  # 1 meter inside
        y = 0.0
        h = to_hash(x, y, 6)
        assert validate_hash(h) is True

    def test_extreme_south(self):
        """Hash near southern boundary of EPSG:6933."""
        x = 0.0
        y = LIMITS["lat_s"] + 1.0
        h = to_hash(x, y, 6)
        assert validate_hash(h) is True

    def test_extreme_north(self):
        """Hash near northern boundary of EPSG:6933."""
        x = 0.0
        y = LIMITS["lat_n"] - 1.0
        h = to_hash(x, y, 6)
        assert validate_hash(h) is True

    def test_numpy_array_broadcast(self):
        """to_hash should support numpy array inputs."""
        x = np.array([0.0, 1000.0, -1000.0])
        y = np.array([0.0, 1000.0, -1000.0])
        hashes = to_hash(x, y, level=6)
        assert len(hashes) == 3
        assert hashes.dtype == np.uint64

    def test_invalid_level_raises(self):
        with pytest.raises(ValueError, match="between 1 and 12"):
            to_hash(0.0, 0.0, 0)

    def test_to_parent_same_level(self):
        """to_parent at the same level should return the same hash."""
        h = to_hash(0.0, 0.0, 6)
        parent = to_parent(h, 6)
        assert h == parent

    def test_to_parent_finer_raises(self):
        """to_parent to a finer level should raise."""
        h = to_hash(0.0, 0.0, 6)
        with pytest.raises(ValueError, match="Cannot convert to finer"):
            to_parent(h, 1)
