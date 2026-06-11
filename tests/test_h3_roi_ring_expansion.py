"""Regression tests for ROI x H3 partition selection under child overhang.

H3 children are not geometrically contained in their parents: a shot stored
under partition ``cell_to_parent(latlng_to_cell(lon, lat, 12), 3)`` can sit
up to ~0.14-0.16 x the parent edge length (~8-10 km at L3) outside that
partition's polygon — i.e. physically inside a *neighbor's* polygon. A ROI
placed in that overhang band intersects the neighbor's polygon only, so an
exact polygon intersection silently skips the partition that actually stores
the shots. Observed in production on the EGI path (~5% of one L3 partition's
shots; see ``egi_h3_intersection``) and fixed there with ring-1 grid_disk
expansion; these tests cover the same fix in ``intersect_h3_geometries``
(used by gh3_load, the build spatial filters, and the resume-coverage check)
and ``geoseries_to_filter`` (DuckDB path).
"""
import h3
import pytest
from shapely.geometry import Point, box

from gedih3.h3utils import fix_h3_geometry, intersect_h3_geometries

PART_RES = 3
INDEX_RES = 12

# Cells around the suite's usual Amazon test region plus their ring-1
# neighbors — enough boundary length that the deterministic sampling below
# always finds overhang points.
_SEED_CELLS = sorted(h3.grid_disk(h3.latlng_to_cell(0.5, -50.5, PART_RES), 1))


def _find_overhang_point(min_storage_distance_deg=0.01):
    """Find (point, polygon_cell, storage_cell) where the point lies inside
    ``polygon_cell``'s polygon but its hierarchy partition is the different
    cell ``storage_cell``, and the point is at least
    ``min_storage_distance_deg`` away from ``storage_cell``'s polygon (so a
    small ROI around the point cannot touch it geometrically).
    """
    for cell in _SEED_CELLS:
        poly = fix_h3_geometry(cell)
        centroid = poly.centroid
        boundary = poly.exterior
        for eps in (0.02, 0.04, 0.06, 0.08):
            for i in range(32):
                bp = boundary.interpolate(i / 32, normalized=True)
                px = bp.x + (centroid.x - bp.x) * eps
                py = bp.y + (centroid.y - bp.y) * eps
                p = Point(px, py)
                if not poly.contains(p):
                    continue
                storage = h3.cell_to_parent(
                    h3.latlng_to_cell(py, px, INDEX_RES), PART_RES
                )
                if storage == cell:
                    continue
                if fix_h3_geometry(storage).distance(p) < min_storage_distance_deg:
                    continue
                return p, cell, storage
    return None


@pytest.fixture(scope='module')
def overhang_case():
    found = _find_overhang_point()
    assert found is not None, (
        "No parent/child overhang point found in the seed cells — either the "
        "h3 library changed its subdivision geometry or the sampling needs "
        "more seeds. The ring-expansion fix relies on this phenomenon."
    )
    return found


class TestH3ExpandRing:
    """Direct tests for the shared expansion primitive (used by
    intersect_h3_geometries, egi_h3_intersection and geoseries_to_filter)."""

    def test_unrestricted_expansion_is_grid_disk_union(self):
        cell = h3.latlng_to_cell(0.5, -50.5, PART_RES)
        from gedih3.h3utils import h3_expand_ring
        assert h3_expand_ring([cell]) == sorted(h3.grid_disk(cell, 1))

    def test_valid_set_restricts_but_keeps_input_cells(self):
        from gedih3.h3utils import h3_expand_ring
        cell = h3.latlng_to_cell(0.5, -50.5, PART_RES)
        nbr = next(c for c in h3.grid_disk(cell, 1) if c != cell)
        assert h3_expand_ring([cell], valid={nbr}) == sorted([cell, nbr])


class TestOverhangPhenomenon:
    def test_point_in_one_polygon_stored_in_another(self, overhang_case):
        """Documents the root cause: hierarchy partition != polygon partition."""
        p, polygon_cell, storage_cell = overhang_case
        assert storage_cell != polygon_cell
        assert fix_h3_geometry(polygon_cell).contains(p)
        assert not fix_h3_geometry(storage_cell).contains(p)
        # The fix's sufficiency bound: the storage cell is always an
        # immediate neighbor of the polygon cell.
        assert storage_cell in h3.grid_disk(polygon_cell, 1)


class TestIntersectH3GeometriesExpansion:
    def test_legacy_exact_intersection_misses_storage_partition(self, overhang_case):
        """The pre-fix behavior (expand_ring=0): a small ROI in the overhang
        band selects the polygon cell but not the partition storing its shots."""
        p, polygon_cell, storage_cell = overhang_case
        roi = box(p.x - 0.001, p.y - 0.001, p.x + 0.001, p.y + 0.001)
        # Emulate a database whose partition list includes the storage cell
        # (it must — that's where the shots live).
        candidates = sorted(h3.grid_disk(polygon_cell, 1))

        legacy = intersect_h3_geometries(roi, h3_ids=candidates, expand_ring=0)
        assert polygon_cell in legacy
        assert storage_cell not in legacy, (
            "precondition drift: the ROI now touches the storage cell's "
            "polygon — the overhang finder should have excluded this point"
        )

    def test_default_ring1_includes_storage_partition(self, overhang_case):
        p, polygon_cell, storage_cell = overhang_case
        roi = box(p.x - 0.001, p.y - 0.001, p.x + 0.001, p.y + 0.001)
        candidates = sorted(h3.grid_disk(polygon_cell, 1))

        fixed = intersect_h3_geometries(roi, h3_ids=candidates)
        assert polygon_cell in fixed
        assert storage_cell in fixed

    def test_expansion_is_superset_of_exact(self):
        roi = box(-50.6, 0.4, -50.4, 0.6)
        exact = intersect_h3_geometries(roi, h3_ids=_SEED_CELLS, expand_ring=0)
        expanded = intersect_h3_geometries(roi, h3_ids=_SEED_CELLS)
        assert set(exact) <= set(expanded)

    def test_expansion_restricted_to_candidate_ids(self):
        """Ring neighbors absent from h3_ids (e.g. partitions that don't
        exist in the database) must not be invented — mirrors the
        valid_partitions filter in egi_h3_intersection."""
        roi = box(-50.6, 0.4, -50.4, 0.6)
        exact = intersect_h3_geometries(roi, h3_ids=_SEED_CELLS, expand_ring=0)
        only_hit = exact[:1]
        result = intersect_h3_geometries(roi, h3_ids=only_hit)
        assert result == only_hit

    def test_global_candidates_expand_too(self):
        """h3_ids=None (build path): expansion applies against the full grid."""
        roi = box(-50.6, 0.4, -50.4, 0.6)
        exact = intersect_h3_geometries(roi, res=PART_RES, expand_ring=0)
        expanded = intersect_h3_geometries(roi, res=PART_RES)
        ring = set()
        for c in exact:
            ring.update(h3.grid_disk(c, 1))
        assert set(expanded) == ring


class TestGeoseriesToFilterExpansion:
    def test_filter_includes_ring1_neighbors(self):
        import geopandas as gpd
        from gedih3.sqlutils import geoseries_to_filter

        cell = h3.latlng_to_cell(0.5, -50.5, PART_RES)
        lat, lng = h3.cell_to_latlng(cell)
        small = gpd.GeoSeries(
            [box(lng - 0.01, lat - 0.01, lng + 0.01, lat + 0.01)], crs=4326
        )

        legacy = geoseries_to_filter(small, expand_ring=0)
        fixed = geoseries_to_filter(small)
        assert cell in legacy and cell in fixed
        for nbr in h3.grid_disk(cell, 1):
            assert nbr in fixed
        missing_in_legacy = [
            n for n in h3.grid_disk(cell, 1) if n not in legacy
        ]
        assert missing_in_legacy, (
            "expected the unexpanded filter to omit at least one ring-1 "
            "neighbor for a polygon deep inside a single cell"
        )
