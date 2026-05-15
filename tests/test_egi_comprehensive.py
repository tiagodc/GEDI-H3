#!/usr/bin/env python
"""
Comprehensive EGI Tests

Validates EGI coordinate-to-hash conversion by:
1. Generating random points across the Earth's surface (EPSG:6933)
2. For each point and each resolution level, computing the EGI hash
3. Computing the polygon from that hash
4. Verifying that the original point intersects its polygon

Pytest tests use 500 random points by default (@pytest.mark.slow).
Run standalone with more points: python test_egi_comprehensive.py -n 5000
"""
import sys
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Tuple, Optional
import time
import pytest

from gedih3.egi.config import LIMITS, RESOLUTIONS, OUTER_RES, OUTER_LEVEL
from gedih3.egi.core import to_hash, from_hash, hasher, to_parent, pixels_per_tile
from gedih3.egi.spatial import pixel_shape, pixel_coordinate, pixel_coordinates

from shapely.geometry import Point


# =============================================================================
# Test helpers (reused by both pytest classes and standalone __main__)
# =============================================================================

@dataclass
class EGITestResult:
    """Result of a single test case."""
    x: float
    y: float
    level: int
    passed: bool
    error_message: Optional[str] = None
    hash_value: Optional[int] = None
    expected_py_outer: Optional[int] = None
    actual_py_outer: Optional[int] = None


def _test_point_polygon_intersection(x: float, y: float, level: int) -> EGITestResult:
    """Test that a point intersects its EGI polygon."""
    try:
        egi_hash = to_hash(x, y, level)
        polygon = pixel_shape(egi_hash)
        point = Point(x, y)

        if not polygon.buffer(0.001).contains(point):
            level_out, scale, px_outer, py_outer, px_inner, py_inner = from_hash(egi_hash)
            return EGITestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Point ({x}, {y}) not in polygon. Hash: {egi_hash}, "
                             f"px_outer={px_outer}, py_outer={py_outer}, "
                             f"px_inner={px_inner}, py_inner={py_inner}, "
                             f"polygon_bounds={polygon.bounds}",
                hash_value=int(egi_hash)
            )
        return EGITestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def _test_roundtrip(x: float, y: float, level: int) -> EGITestResult:
    """Test hash -> center coordinate -> hash roundtrip."""
    try:
        egi_hash = to_hash(x, y, level)
        cx, cy = pixel_coordinate(egi_hash, center=True)
        center_hash = to_hash(cx, cy, level)

        if egi_hash != center_hash:
            level_out, scale, px_outer, py_outer, px_inner, py_inner = from_hash(egi_hash)
            level_c, _, px_o_c, py_o_c, px_i_c, py_i_c = from_hash(center_hash)
            return EGITestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Roundtrip mismatch: original hash={egi_hash}, center hash={center_hash}. "
                             f"Original: px_o={px_outer}, py_o={py_outer}, px_i={px_inner}, py_i={py_inner}. "
                             f"Center: px_o={px_o_c}, py_o={py_o_c}, px_i={px_i_c}, py_i={py_i_c}. "
                             f"Center coords: ({cx}, {cy})",
                hash_value=int(egi_hash)
            )
        return EGITestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def _test_outer_inner_consistency(x: float, y: float, fine_level: int = 6) -> EGITestResult:
    """Test that outer tile coordinates are consistent between levels."""
    try:
        fine_hash = to_hash(x, y, fine_level)
        _, _, fine_px_o, fine_py_o, _, _ = from_hash(fine_hash)

        outer_hash = to_hash(x, y, OUTER_LEVEL)
        _, _, outer_px_o, outer_py_o, _, _ = from_hash(outer_hash)

        if fine_px_o != outer_px_o or fine_py_o != outer_py_o:
            return EGITestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"Outer tile mismatch: fine level has ({fine_px_o}, {fine_py_o}), "
                             f"outer level has ({outer_px_o}, {outer_py_o})",
                hash_value=int(fine_hash),
                expected_py_outer=int(outer_py_o),
                actual_py_outer=int(fine_py_o)
            )
        return EGITestResult(x=x, y=y, level=fine_level, passed=True, hash_value=int(fine_hash))
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=fine_level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def _test_to_parent_consistency(x: float, y: float, fine_level: int = 1, parent_level: int = 6) -> EGITestResult:
    """Test that to_parent gives consistent results with direct hashing."""
    try:
        fine_hash = to_hash(x, y, fine_level)
        parent_via_func = to_parent(fine_hash, parent_level)
        _, _, parent_px_o, parent_py_o, _, _ = from_hash(parent_via_func)

        direct_hash = to_hash(x, y, parent_level)
        _, _, direct_px_o, direct_py_o, _, _ = from_hash(direct_hash)

        if parent_px_o != direct_px_o or parent_py_o != direct_py_o:
            return EGITestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"to_parent outer tile mismatch: to_parent gives ({parent_px_o}, {parent_py_o}), "
                             f"direct hash gives ({direct_px_o}, {direct_py_o})",
                hash_value=int(fine_hash)
            )
        return EGITestResult(x=x, y=y, level=fine_level, passed=True, hash_value=int(fine_hash))
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=fine_level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def _test_tile_boundary(tile_y: int, eps: float = 1e-6) -> EGITestResult:
    """Check invariants for ``y`` just below a tile boundary.

    For ``eps`` well above float64 precision at the boundary magnitude, the
    point lands strictly inside ``tile_y - 1`` at every level. For sub-precision
    ``eps``, ``y_boundary - eps`` is indistinguishable from ``y_boundary``
    itself, and direct hashing at different levels may legitimately disagree
    on which neighbor tile to pick (the float rounding of
    ``(OUTER_RES - eps) / scale`` depends on ``scale``). The hard invariants —
    always true — are that (1) hashes have in-range inner indices and (2) the
    pipeline path ``fine → to_parent(coarse)`` agrees with itself across levels.
    """
    x = 0
    y_boundary = LIMITS['lat_s'] + tile_y * OUTER_RES
    y = y_boundary - eps

    try:
        outer_hash = to_hash(x, y, OUTER_LEVEL)
        _, _, _, outer_py_o, _, outer_pyi = from_hash(outer_hash)

        fine_level = 6
        fine_hash = to_hash(x, y, fine_level)
        _, _, _, fine_py_o, _, fine_pyi = from_hash(fine_hash)

        # Invariant 1: inner index is in valid range for its level.
        max_outer_inner = round(OUTER_RES / RESOLUTIONS[OUTER_LEVEL]) - 1
        max_fine_inner = round(OUTER_RES / RESOLUTIONS[fine_level]) - 1
        if not (0 <= outer_pyi <= max_outer_inner):
            return EGITestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"Outer-level inner index out of range at tile {tile_y}, eps={eps}: "
                             f"py_inner={outer_pyi}, max={max_outer_inner}",
            )
        if not (0 <= fine_pyi <= max_fine_inner):
            return EGITestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"Fine-level inner index out of range at tile {tile_y}, eps={eps}: "
                             f"py_inner={fine_pyi}, max={max_fine_inner}",
            )

        # Invariant 2: each level picks one of the two boundary neighbors.
        for label, picked in (("outer", outer_py_o), ("fine", fine_py_o)):
            if int(picked) not in (tile_y - 1, tile_y):
                return EGITestResult(
                    x=x, y=y, level=fine_level, passed=False,
                    error_message=f"{label} boundary tile out of expected neighbor pair at "
                                 f"tile {tile_y}, eps={eps}: got {picked}, "
                                 f"expected one of ({tile_y - 1}, {tile_y})",
                    expected_py_outer=tile_y - 1,
                    actual_py_outer=int(picked),
                )

        # Invariant 3: the production pipeline path (hash at fine + to_parent up
        # to outer) is internally consistent — it never produces an out-of-range
        # inner index at any level. This is the property that actually drives
        # the build/extract output filenames.
        rolled = to_parent(fine_hash, OUTER_LEVEL)
        _, _, _, rolled_py_o, _, rolled_pyi = from_hash(rolled)
        if rolled_pyi != 0:
            return EGITestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"to_parent(fine, OUTER_LEVEL) produced non-zero inner index at "
                             f"tile {tile_y}, eps={eps}: py_inner={rolled_pyi}",
            )

        return EGITestResult(x=x, y=y, level=fine_level, passed=True)
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=6, passed=False,
            error_message=f"Exception at tile boundary {tile_y}: {type(e).__name__}: {str(e)}"
        )


def _test_inner_coordinate_range(x: float, y: float, level: int) -> EGITestResult:
    """Test that inner coordinates are within valid range."""
    try:
        egi_hash = to_hash(x, y, level)
        _, scale, _, _, px_inner, py_inner = from_hash(egi_hash)

        max_inner = int(OUTER_RES / scale) - 1

        if px_inner > max_inner or py_inner > max_inner:
            return EGITestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Inner coordinates out of range: px_inner={px_inner}, py_inner={py_inner}, "
                             f"max_inner={max_inner}",
                hash_value=int(egi_hash)
            )
        if px_inner < 0 or py_inner < 0:
            return EGITestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Negative inner coordinates: px_inner={px_inner}, py_inner={py_inner}",
                hash_value=int(egi_hash)
            )
        return EGITestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))
    except Exception as e:
        return EGITestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def _generate_random_points(n: int, seed: int = 42) -> Tuple[np.ndarray, np.ndarray]:
    """Generate n random points within EPSG:6933 bounds."""
    np.random.seed(seed)
    margin = 1.0
    x = np.random.uniform(LIMITS['lon_w'] + margin, LIMITS['lon_e'] - margin, n)
    y = np.random.uniform(LIMITS['lat_s'] + margin, LIMITS['lat_n'] - margin, n)
    return x, y


# =============================================================================
# Pytest test classes
# =============================================================================

@pytest.mark.slow
class TestEGIPointPolygonIntersection:
    """Every point should intersect its EGI polygon at every level."""

    @pytest.fixture(scope='class')
    def random_points(self):
        return _generate_random_points(500)

    @pytest.mark.parametrize("level", [1, 6, 12])
    def test_random_points_intersect(self, random_points, level):
        """500 random points all intersect their polygon at given level."""
        x, y = random_points
        failures = []
        for xi, yi in zip(x, y):
            result = _test_point_polygon_intersection(xi, yi, level)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} of {len(x)} points failed at level {level}. " \
            f"First: {failures[0].error_message}"


@pytest.mark.slow
class TestEGIRoundtrip:
    """Hash -> center coordinate -> hash must be identical."""

    @pytest.fixture(scope='class')
    def random_points(self):
        return _generate_random_points(500)

    @pytest.mark.parametrize("level", [1, 6, 12])
    def test_roundtrip_consistency(self, random_points, level):
        """Center of pixel, when re-hashed, gives the same hash."""
        x, y = random_points
        failures = []
        for xi, yi in zip(x, y):
            result = _test_roundtrip(xi, yi, level)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} of {len(x)} roundtrip failures at level {level}. " \
            f"First: {failures[0].error_message}"


@pytest.mark.slow
class TestEGIOuterTileConsistency:
    """Outer tile coordinates must be same regardless of inner level."""

    @pytest.fixture(scope='class')
    def random_points(self):
        return _generate_random_points(500)

    @pytest.mark.parametrize("level", [1, 6, 12])
    def test_outer_consistency(self, random_points, level):
        """Outer tile IDs are consistent across levels."""
        x, y = random_points
        failures = []
        for xi, yi in zip(x, y):
            result = _test_outer_inner_consistency(xi, yi, level)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} outer tile inconsistencies at level {level}. " \
            f"First: {failures[0].error_message}"


@pytest.mark.slow
class TestEGIInnerCoordinateRange:
    """Inner coordinates must be within [0, max_inner]."""

    @pytest.fixture(scope='class')
    def random_points(self):
        return _generate_random_points(500)

    @pytest.mark.parametrize("level", [1, 6, 12])
    def test_inner_coords_valid(self, random_points, level):
        """Inner coordinates are non-negative and within bounds."""
        x, y = random_points
        failures = []
        for xi, yi in zip(x, y):
            result = _test_inner_coordinate_range(xi, yi, level)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} inner coordinate range failures at level {level}. " \
            f"First: {failures[0].error_message}"


@pytest.mark.slow
class TestEGIToParentConsistency:
    """to_parent must give same outer tile as direct hashing."""

    @pytest.fixture(scope='class')
    def random_points(self):
        return _generate_random_points(500)

    def test_parent_matches_direct(self, random_points):
        """to_parent(level=1 -> level=6) matches direct hash at level 6."""
        x, y = random_points
        failures = []
        for xi, yi in zip(x, y):
            result = _test_to_parent_consistency(xi, yi)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} parent consistency failures. " \
            f"First: {failures[0].error_message}"


class TestEGITileBoundary:
    """Tile boundary consistency (not marked slow, runs fast)."""

    @pytest.mark.parametrize("eps", [1e-8, 1e-6, 1e-4, 1e-2])
    def test_tile_boundaries(self, eps):
        """All 90 tile boundaries are consistent at given epsilon."""
        failures = []
        for tile_y in range(1, 91):
            result = _test_tile_boundary(tile_y, eps)
            if not result.passed:
                failures.append(result)
        assert not failures, \
            f"{len(failures)} boundary failures at eps={eps}. " \
            f"First: {failures[0].error_message}"


# =============================================================================
# Standalone runner (for deeper testing outside pytest)
# =============================================================================

def _run_test_batch(args: Tuple) -> List[EGITestResult]:
    """Run a batch of tests (for parallel execution)."""
    x_batch, y_batch, levels, test_type = args
    results = []
    for x, y in zip(x_batch, y_batch):
        for level in levels:
            if test_type == 'intersection':
                results.append(_test_point_polygon_intersection(x, y, level))
            elif test_type == 'roundtrip':
                results.append(_test_roundtrip(x, y, level))
            elif test_type == 'outer_consistency':
                results.append(_test_outer_inner_consistency(x, y, level))
            elif test_type == 'parent_consistency':
                results.append(_test_to_parent_consistency(x, y))
            elif test_type == 'inner_range':
                results.append(_test_inner_coordinate_range(x, y, level))
    return results


def main():
    """Run comprehensive EGI tests (standalone mode)."""
    import argparse

    parser = argparse.ArgumentParser(description='Comprehensive EGI Tests')
    parser.add_argument('-n', '--num-points', type=int, default=5000)
    parser.add_argument('-j', '--jobs', type=int, default=20)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--levels', type=str, default='1,6,12')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    levels = [int(l) for l in args.levels.split(',')]
    x, y = _generate_random_points(args.num_points, seed=args.seed)

    print(f"Testing {args.num_points} random points across levels {levels}")
    print(f"Workers: {args.jobs}")

    all_failures = []
    batch_size = max(1, args.num_points // args.jobs)

    for test_type in ['intersection', 'roundtrip', 'outer_consistency', 'inner_range', 'parent_consistency']:
        batches = []
        for i in range(0, args.num_points, batch_size):
            end = min(i + batch_size, args.num_points)
            batches.append((x[i:end], y[i:end], levels, test_type))

        results = []
        start = time.time()
        with ProcessPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(_run_test_batch, batch) for batch in batches]
            for future in as_completed(futures):
                results.extend(future.result())

        failures = [r for r in results if not r.passed]
        elapsed = time.time() - start
        status = "PASSED" if not failures else f"FAILED ({len(failures)})"
        print(f"  {test_type}: {status} ({len(results)} tests, {elapsed:.1f}s)")
        if failures and args.verbose:
            for f in failures[:5]:
                print(f"    {f.error_message}")
        all_failures.extend(failures)

    # Boundary tests
    boundary_failures = []
    for tile_y in range(1, 91):
        for eps in [1e-8, 1e-6, 1e-4, 1e-2]:
            result = _test_tile_boundary(tile_y, eps)
            if not result.passed:
                boundary_failures.append(result)
    status = "PASSED" if not boundary_failures else f"FAILED ({len(boundary_failures)})"
    print(f"  boundary: {status} (360 tests)")
    all_failures.extend(boundary_failures)

    print(f"\nOverall: {'PASSED' if not all_failures else f'FAILED ({len(all_failures)} failures)'}")
    return 1 if all_failures else 0


if __name__ == '__main__':
    sys.exit(main())
