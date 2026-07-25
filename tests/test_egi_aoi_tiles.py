"""Region selection in ``egi.aoi_tiles``.

Two properties pinned here:

1. The sindex-based selection (replacing a serial per-tile ``.apply`` over
   all 19,656 global tiles) returns exactly the tiles the brute-force
   intersection returns.
2. The region is densified before reprojection to EPSG:6933 — a straight
   lon/lat edge bows in ``y = A*sin(lat)``, and the chord of a sparse-vertex
   polygon used to cut inside the true boundary and silently drop tiles.
"""

import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import Point, Polygon, box

from gedih3 import egi


def test_sindex_selection_matches_bruteforce():
    region = gpd.GeoDataFrame(geometry=[box(-52, -2, -49, 2)], crs=4326)
    got = egi.aoi_tiles(region)

    all_tiles = egi.aoi_tiles()
    reg = region.to_crs(4326)
    reg = reg.set_geometry(reg.geometry.segmentize(0.1)).to_crs(egi.EGI_CRS_STRING)
    expected = all_tiles[all_tiles.geometry.apply(lambda x: reg.intersects(x).any())]

    assert sorted(got.index) == sorted(expected.index)
    assert len(got) > 0


def test_sparse_polygon_region_is_densified():
    """Every tile holding a point of the strip's centerline must be selected.

    Without densification the vertex-only reprojection of this 60-degree
    quad missed 3 of the 37 centerline tiles (verified against the
    pre-fix implementation).
    """
    quad = Polygon([(0, 0), (10, 60), (10.6, 60), (0.6, 0)])
    region = gpd.GeoDataFrame(geometry=[quad], crs=4326)

    ts = np.linspace(0.05, 0.95, 40)
    centerline = [(10 * t + 0.3, 60 * t) for t in ts]
    pts = gpd.GeoSeries([Point(x, y) for x, y in centerline],
                        crs=4326).to_crs(egi.EGI_CRS_STRING)
    needed = {int(egi.to_hash(float(p.x), float(p.y), level=12)) for p in pts}

    selected = set(int(i) for i in egi.aoi_tiles(region).index)
    assert needed.issubset(selected)


def test_region_without_crs_raises():
    region = gpd.GeoDataFrame(geometry=[box(-52, -2, -49, 2)])
    with pytest.raises(ValueError):
        egi.aoi_tiles(region)
