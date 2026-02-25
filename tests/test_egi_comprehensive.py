#!/usr/bin/env python
"""
Comprehensive EGI Tests

This test suite validates EGI coordinate-to-hash conversion by:
1. Generating random points across the Earth's surface (EPSG:6933)
2. For each point and each resolution level, computing the EGI hash
3. Computing the polygon from that hash
4. Verifying that the original point intersects its polygon

If EGI is correctly implemented, every point should always intersect
its corresponding EGI polygon at every resolution level.
"""
import sys
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Tuple, Optional
import time

# This module is a standalone test script (run via __main__), not a pytest module.
# The test_* functions take explicit parameters and are called from main().
# Tell pytest to skip collection entirely.
import pytest
pytestmark = pytest.mark.skip(reason="Standalone script, not a pytest module — run via: python tests/test_egi_comprehensive.py")

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from gedih3.egi.config import LIMITS, RESOLUTIONS, OUTER_RES, OUTER_LEVEL
from gedih3.egi.core import to_hash, from_hash, hasher, to_parent, pixels_per_tile
from gedih3.egi.spatial import pixel_shape, pixel_coordinate, pixel_coordinates

from shapely.geometry import Point


@dataclass
class TestResult:
    """Result of a single test case."""
    x: float
    y: float
    level: int
    passed: bool
    error_message: Optional[str] = None
    hash_value: Optional[int] = None
    expected_py_outer: Optional[int] = None
    actual_py_outer: Optional[int] = None


def test_point_polygon_intersection(x: float, y: float, level: int) -> TestResult:
    """
    Test that a point intersects its EGI polygon.

    Parameters
    ----------
    x, y : float
        Coordinates in EPSG:6933
    level : int
        EGI resolution level (1-12)

    Returns
    -------
    TestResult
        Result indicating pass/fail and any error details
    """
    try:
        # Compute EGI hash for the point
        egi_hash = to_hash(x, y, level)

        # Get the polygon for this hash
        polygon = pixel_shape(egi_hash)

        # Create a point and check intersection
        point = Point(x, y)

        # The point should be within or on the boundary of the polygon
        # Use buffer(0.001) to handle floating-point edge cases
        if not polygon.buffer(0.001).contains(point):
            # More detailed error info
            level_out, scale, px_outer, py_outer, px_inner, py_inner = from_hash(egi_hash)
            return TestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Point ({x}, {y}) not in polygon. Hash: {egi_hash}, "
                             f"px_outer={px_outer}, py_outer={py_outer}, "
                             f"px_inner={px_inner}, py_inner={py_inner}, "
                             f"polygon_bounds={polygon.bounds}",
                hash_value=int(egi_hash)
            )

        return TestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))

    except Exception as e:
        return TestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def test_roundtrip(x: float, y: float, level: int) -> TestResult:
    """
    Test hash -> center coordinate -> hash roundtrip.

    The center of a pixel, when hashed, should give the same hash.
    """
    try:
        # Compute EGI hash for the point
        egi_hash = to_hash(x, y, level)

        # Get the center coordinate
        cx, cy = pixel_coordinate(egi_hash, center=True)

        # Hash the center coordinate
        center_hash = to_hash(cx, cy, level)

        # They should match
        if egi_hash != center_hash:
            level_out, scale, px_outer, py_outer, px_inner, py_inner = from_hash(egi_hash)
            level_c, _, px_o_c, py_o_c, px_i_c, py_i_c = from_hash(center_hash)
            return TestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Roundtrip mismatch: original hash={egi_hash}, center hash={center_hash}. "
                             f"Original: px_o={px_outer}, py_o={py_outer}, px_i={px_inner}, py_i={py_inner}. "
                             f"Center: px_o={px_o_c}, py_o={py_o_c}, px_i={px_i_c}, py_i={py_i_c}. "
                             f"Center coords: ({cx}, {cy})",
                hash_value=int(egi_hash)
            )

        return TestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))

    except Exception as e:
        return TestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def test_outer_inner_consistency(x: float, y: float, fine_level: int = 6) -> TestResult:
    """
    Test that the outer tile coordinates are consistent between levels.

    A point's outer tile coordinates should be the same regardless of the
    inner resolution level.
    """
    try:
        # Compute hash at fine level
        fine_hash = to_hash(x, y, fine_level)
        _, _, fine_px_o, fine_py_o, _, _ = from_hash(fine_hash)

        # Compute hash at outer level (level 12)
        outer_hash = to_hash(x, y, OUTER_LEVEL)
        _, _, outer_px_o, outer_py_o, _, _ = from_hash(outer_hash)

        # Outer tile coordinates should match
        if fine_px_o != outer_px_o or fine_py_o != outer_py_o:
            return TestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"Outer tile mismatch: fine level has ({fine_px_o}, {fine_py_o}), "
                             f"outer level has ({outer_px_o}, {outer_py_o})",
                hash_value=int(fine_hash),
                expected_py_outer=int(outer_py_o),
                actual_py_outer=int(fine_py_o)
            )

        return TestResult(x=x, y=y, level=fine_level, passed=True, hash_value=int(fine_hash))

    except Exception as e:
        return TestResult(
            x=x, y=y, level=fine_level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def test_to_parent_consistency(x: float, y: float, fine_level: int = 1, parent_level: int = 6) -> TestResult:
    """
    Test that to_parent gives consistent results with direct hashing.

    For a point, the parent hash computed via to_parent should have the same
    outer tile coordinates as a direct hash at that level.
    """
    try:
        # Compute fine hash
        fine_hash = to_hash(x, y, fine_level)

        # Convert to parent using to_parent
        parent_via_func = to_parent(fine_hash, parent_level)
        _, _, parent_px_o, parent_py_o, _, _ = from_hash(parent_via_func)

        # Compute parent hash directly
        direct_hash = to_hash(x, y, parent_level)
        _, _, direct_px_o, direct_py_o, _, _ = from_hash(direct_hash)

        # Outer tile coordinates should match
        if parent_px_o != direct_px_o or parent_py_o != direct_py_o:
            return TestResult(
                x=x, y=y, level=fine_level, passed=False,
                error_message=f"to_parent outer tile mismatch: to_parent gives ({parent_px_o}, {parent_py_o}), "
                             f"direct hash gives ({direct_px_o}, {direct_py_o})",
                hash_value=int(fine_hash)
            )

        return TestResult(x=x, y=y, level=fine_level, passed=True, hash_value=int(fine_hash))

    except Exception as e:
        return TestResult(
            x=x, y=y, level=fine_level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def test_tile_boundary(tile_y: int, eps: float = 1e-6) -> TestResult:
    """
    Test consistency at a specific tile boundary.
    """
    x = 0  # Fixed x coordinate
    y_boundary = LIMITS['lat_s'] + tile_y * OUTER_RES

    # Test just below boundary
    y = y_boundary - eps

    try:
        outer_hash = to_hash(x, y, OUTER_LEVEL)
        _, _, _, outer_py_o, _, _ = from_hash(outer_hash)

        fine_hash = to_hash(x, y, 6)
        _, _, _, fine_py_o, _, _ = from_hash(fine_hash)

        # Just below tile boundary N should be in tile N-1
        expected_tile = tile_y - 1

        if outer_py_o != expected_tile or fine_py_o != expected_tile:
            return TestResult(
                x=x, y=y, level=6, passed=False,
                error_message=f"Boundary test at tile {tile_y}, eps={eps}: "
                             f"expected tile {expected_tile}, outer={outer_py_o}, fine={fine_py_o}",
                expected_py_outer=expected_tile,
                actual_py_outer=int(fine_py_o)
            )

        return TestResult(x=x, y=y, level=6, passed=True)

    except Exception as e:
        return TestResult(
            x=x, y=y, level=6, passed=False,
            error_message=f"Exception at tile boundary {tile_y}: {type(e).__name__}: {str(e)}"
        )


def test_inner_coordinate_range(x: float, y: float, level: int) -> TestResult:
    """
    Test that inner coordinates are within valid range.
    """
    try:
        egi_hash = to_hash(x, y, level)
        _, scale, _, _, px_inner, py_inner = from_hash(egi_hash)

        max_inner = int(OUTER_RES / scale) - 1

        if px_inner > max_inner or py_inner > max_inner:
            return TestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Inner coordinates out of range: px_inner={px_inner}, py_inner={py_inner}, "
                             f"max_inner={max_inner}",
                hash_value=int(egi_hash)
            )

        if px_inner < 0 or py_inner < 0:
            return TestResult(
                x=x, y=y, level=level, passed=False,
                error_message=f"Negative inner coordinates: px_inner={px_inner}, py_inner={py_inner}",
                hash_value=int(egi_hash)
            )

        return TestResult(x=x, y=y, level=level, passed=True, hash_value=int(egi_hash))

    except Exception as e:
        return TestResult(
            x=x, y=y, level=level, passed=False,
            error_message=f"Exception: {type(e).__name__}: {str(e)}"
        )


def generate_random_points(n: int, seed: int = 42) -> Tuple[np.ndarray, np.ndarray]:
    """Generate n random points within EPSG:6933 bounds."""
    np.random.seed(seed)

    # Add some margin to avoid exact boundary issues
    margin = 1.0  # 1 meter margin

    x = np.random.uniform(LIMITS['lon_w'] + margin, LIMITS['lon_e'] - margin, n)
    y = np.random.uniform(LIMITS['lat_s'] + margin, LIMITS['lat_n'] - margin, n)

    return x, y


def generate_boundary_points() -> List[Tuple[float, float]]:
    """Generate points at/near tile boundaries for focused testing."""
    points = []

    # Test several tile boundaries in Y direction
    for tile_y in range(1, 91):  # All possible tile boundaries
        y_boundary = LIMITS['lat_s'] + tile_y * OUTER_RES
        x = 0  # Fixed x

        # Points at various distances from boundary
        for eps in [1e-10, 1e-8, 1e-6, 1e-4, 1e-2, 1.0, 10.0, 100.0]:
            points.append((x, y_boundary - eps))  # Just below
            points.append((x, y_boundary + eps))  # Just above

    return points


def run_test_batch(args: Tuple) -> List[TestResult]:
    """Run a batch of tests (for parallel execution)."""
    x_batch, y_batch, levels, test_type = args
    results = []

    for x, y in zip(x_batch, y_batch):
        for level in levels:
            if test_type == 'intersection':
                results.append(test_point_polygon_intersection(x, y, level))
            elif test_type == 'roundtrip':
                results.append(test_roundtrip(x, y, level))
            elif test_type == 'outer_consistency':
                results.append(test_outer_inner_consistency(x, y, level))
            elif test_type == 'parent_consistency':
                results.append(test_to_parent_consistency(x, y))
            elif test_type == 'inner_range':
                results.append(test_inner_coordinate_range(x, y, level))

    return results


def main():
    """Run comprehensive EGI tests."""
    import argparse

    parser = argparse.ArgumentParser(description='Comprehensive EGI Tests')
    parser.add_argument('-n', '--num-points', type=int, default=5000,
                       help='Number of random points to test (default: 5000)')
    parser.add_argument('-j', '--jobs', type=int, default=20,
                       help='Number of parallel workers (default: 20)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed (default: 42)')
    parser.add_argument('--levels', type=str, default='1,6,12',
                       help='Comma-separated levels to test (default: 1,6,12)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output (show all failures)')
    args = parser.parse_args()

    levels = [int(l) for l in args.levels.split(',')]

    print("=" * 70)
    print(" EGI Comprehensive Test Suite")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  EGI_RES6 (base resolution): {RESOLUTIONS[6]}")
    print(f"  OUTER_RES (tile size): {OUTER_RES}")
    print(f"  LIMITS: lat_s={LIMITS['lat_s']}, lat_n={LIMITS['lat_n']}")
    print(f"  LIMITS: lon_w={LIMITS['lon_w']}, lon_e={LIMITS['lon_e']}")
    print(f"  Pixels per tile at level 6: {pixels_per_tile(6)}")
    print(f"  Test levels: {levels}")
    print(f"  Number of random points: {args.num_points}")
    print(f"  Parallel workers: {args.jobs}")
    print()

    all_failures = []

    # =========================================================================
    # Test 1: Tile Boundary Consistency
    # =========================================================================
    print("-" * 70)
    print("Test 1: Tile Boundary Consistency")
    print("-" * 70)

    boundary_failures = []
    for tile_y in range(1, 91):
        # Use eps >= 1e-8 because smaller values are below float64 precision
        # at these coordinate magnitudes (~7 million meters)
        for eps in [1e-8, 1e-6, 1e-4, 1e-2]:
            result = test_tile_boundary(tile_y, eps)
            if not result.passed:
                boundary_failures.append(result)

    if boundary_failures:
        print(f"  FAILED: {len(boundary_failures)} boundary tests failed")
        if args.verbose:
            for f in boundary_failures[:10]:
                print(f"    {f.error_message}")
        all_failures.extend(boundary_failures)
    else:
        print(f"  PASSED: All tile boundary tests passed")
    print()

    # =========================================================================
    # Test 2: Point-Polygon Intersection (Random Points)
    # =========================================================================
    print("-" * 70)
    print("Test 2: Point-Polygon Intersection (Random Points)")
    print("-" * 70)

    x, y = generate_random_points(args.num_points, seed=args.seed)

    # Split into batches for parallel execution
    batch_size = max(1, args.num_points // args.jobs)
    batches = []
    for i in range(0, args.num_points, batch_size):
        end = min(i + batch_size, args.num_points)
        batches.append((x[i:end], y[i:end], levels, 'intersection'))

    intersection_results = []
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(run_test_batch, batch) for batch in batches]
        for future in as_completed(futures):
            intersection_results.extend(future.result())

    elapsed = time.time() - start_time
    failures = [r for r in intersection_results if not r.passed]

    print(f"  Total tests: {len(intersection_results)}")
    print(f"  Passed: {len(intersection_results) - len(failures)}")
    print(f"  Failed: {len(failures)}")
    print(f"  Time: {elapsed:.2f}s")

    if failures:
        all_failures.extend(failures)
        if args.verbose:
            print("  Sample failures:")
            for f in failures[:5]:
                print(f"    {f.error_message}")
    print()

    # =========================================================================
    # Test 3: Roundtrip Consistency (hash -> center -> hash)
    # =========================================================================
    print("-" * 70)
    print("Test 3: Roundtrip Consistency")
    print("-" * 70)

    batches = []
    for i in range(0, args.num_points, batch_size):
        end = min(i + batch_size, args.num_points)
        batches.append((x[i:end], y[i:end], levels, 'roundtrip'))

    roundtrip_results = []
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(run_test_batch, batch) for batch in batches]
        for future in as_completed(futures):
            roundtrip_results.extend(future.result())

    elapsed = time.time() - start_time
    failures = [r for r in roundtrip_results if not r.passed]

    print(f"  Total tests: {len(roundtrip_results)}")
    print(f"  Passed: {len(roundtrip_results) - len(failures)}")
    print(f"  Failed: {len(failures)}")
    print(f"  Time: {elapsed:.2f}s")

    if failures:
        all_failures.extend(failures)
        if args.verbose:
            print("  Sample failures:")
            for f in failures[:5]:
                print(f"    {f.error_message}")
    print()

    # =========================================================================
    # Test 4: Outer Tile Consistency Across Levels
    # =========================================================================
    print("-" * 70)
    print("Test 4: Outer Tile Consistency Across Levels")
    print("-" * 70)

    batches = []
    for i in range(0, args.num_points, batch_size):
        end = min(i + batch_size, args.num_points)
        batches.append((x[i:end], y[i:end], levels, 'outer_consistency'))

    consistency_results = []
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(run_test_batch, batch) for batch in batches]
        for future in as_completed(futures):
            consistency_results.extend(future.result())

    elapsed = time.time() - start_time
    failures = [r for r in consistency_results if not r.passed]

    print(f"  Total tests: {len(consistency_results)}")
    print(f"  Passed: {len(consistency_results) - len(failures)}")
    print(f"  Failed: {len(failures)}")
    print(f"  Time: {elapsed:.2f}s")

    if failures:
        all_failures.extend(failures)
        if args.verbose:
            print("  Sample failures:")
            for f in failures[:5]:
                print(f"    {f.error_message}")
    print()

    # =========================================================================
    # Test 5: Inner Coordinate Range Validity
    # =========================================================================
    print("-" * 70)
    print("Test 5: Inner Coordinate Range Validity")
    print("-" * 70)

    batches = []
    for i in range(0, args.num_points, batch_size):
        end = min(i + batch_size, args.num_points)
        batches.append((x[i:end], y[i:end], levels, 'inner_range'))

    range_results = []
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(run_test_batch, batch) for batch in batches]
        for future in as_completed(futures):
            range_results.extend(future.result())

    elapsed = time.time() - start_time
    failures = [r for r in range_results if not r.passed]

    print(f"  Total tests: {len(range_results)}")
    print(f"  Passed: {len(range_results) - len(failures)}")
    print(f"  Failed: {len(failures)}")
    print(f"  Time: {elapsed:.2f}s")

    if failures:
        all_failures.extend(failures)
        if args.verbose:
            print("  Sample failures:")
            for f in failures[:5]:
                print(f"    {f.error_message}")
    print()

    # =========================================================================
    # Test 6: Boundary Points (Focused Testing)
    # =========================================================================
    print("-" * 70)
    print("Test 6: Boundary Points (Focused Testing)")
    print("-" * 70)

    boundary_points = generate_boundary_points()
    print(f"  Testing {len(boundary_points)} boundary points...")

    boundary_results = []
    for x_pt, y_pt in boundary_points:
        for level in levels:
            boundary_results.append(test_point_polygon_intersection(x_pt, y_pt, level))
            boundary_results.append(test_outer_inner_consistency(x_pt, y_pt, level))

    failures = [r for r in boundary_results if not r.passed]

    print(f"  Total tests: {len(boundary_results)}")
    print(f"  Passed: {len(boundary_results) - len(failures)}")
    print(f"  Failed: {len(failures)}")

    if failures:
        all_failures.extend(failures)
        if args.verbose:
            print("  Sample failures:")
            for f in failures[:10]:
                print(f"    {f.error_message}")
    print()

    # =========================================================================
    # Summary
    # =========================================================================
    print("=" * 70)
    print(" Summary")
    print("=" * 70)

    if all_failures:
        print(f"OVERALL: FAILED - {len(all_failures)} total failures")

        # Analyze failure patterns
        outer_mismatches = [f for f in all_failures if f.expected_py_outer is not None]
        if outer_mismatches:
            print(f"\nOuter tile mismatches: {len(outer_mismatches)}")
            # Group by expected/actual
            mismatch_patterns = {}
            for f in outer_mismatches:
                key = (f.expected_py_outer, f.actual_py_outer)
                mismatch_patterns[key] = mismatch_patterns.get(key, 0) + 1
            for (expected, actual), count in sorted(mismatch_patterns.items())[:10]:
                print(f"  Expected py_outer={expected}, got {actual}: {count} occurrences")

        return 1
    else:
        print("OVERALL: PASSED - All tests passed!")
        return 0


if __name__ == '__main__':
    sys.exit(main())
