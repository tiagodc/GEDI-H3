"""
Tests that geodf_to_raster and build_vrt preserve the canonical EGI pixel size.

Regression for the edge-tile resolution bug:
- Tiles at the eastern CRS boundary were produced with a smaller x_cell because
  pixel_shape() clamps `right` to LIMITS['lon_e'] and from_bounds() derived the
  pixel size from the shorter width.
- gdal.BuildVRT (default resolution='average') then averaged all tile pixel
  sizes, pulling the VRT to ~1000.677 m instead of 1000.895 m.
"""
import os
import tempfile
import numpy as np
import pytest
import geopandas as gpd
from shapely.geometry import Point

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_egi_gdf(px_outer: int, py_outer: int, level: int = 6,
                  n_pixels: int = 10) -> gpd.GeoDataFrame:
    """
    Build a minimal EGI-indexed GeoDataFrame for one outer tile.

    Places `n_pixels` pixels scattered within the tile at px_inner/py_inner
    in [5, pixels_per_tile-5) so we avoid the very edge of the tile.
    """
    from gedih3.egi.core import hasher, pixels_per_tile as _ppt
    from gedih3.egi.spatial import pixel_coordinate

    ppt = int(_ppt(level))
    rng = np.random.default_rng(42)
    px_inner = rng.integers(5, ppt - 5, size=n_pixels).astype(np.uint32)
    py_inner = rng.integers(5, ppt - 5, size=n_pixels).astype(np.uint32)

    hashes = hasher(level, px_outer, py_outer, px_inner, py_inner)

    # Geometry: point at pixel centre (EPSG:6933, then convert to 4326)
    xs, ys = [], []
    for h in hashes:
        cx, cy = pixel_coordinate(np.uint64(h), center=True)
        xs.append(cx)
        ys.append(cy)

    gdf = gpd.GeoDataFrame(
        {'value': rng.random(n_pixels).astype(np.float32)},
        index=hashes,
        geometry=[Point(x, y) for x, y in zip(xs, ys)],
        crs='EPSG:6933',
    )
    return gdf


def _x_cell(xras) -> float:
    """Return the x pixel size from an xarray Dataset's rioxarray transform."""
    return float(xras.rio.transform().a)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGeodfToRasterPixelSize:
    """geodf_to_raster must always produce x_cell == RESOLUTIONS[level]."""

    def test_interior_tile_pixel_size(self):
        """Interior tile (px_outer=100, py_outer=50) → correct x_cell."""
        from gedih3 import egi
        from gedih3.egi.config import RESOLUTIONS

        gdf = _make_egi_gdf(px_outer=100, py_outer=50)
        xras = egi.geodf_to_raster(gdf, columns=['value'])

        assert abs(_x_cell(xras) - RESOLUTIONS[6]) < 1e-3, (
            f"Interior tile x_cell={_x_cell(xras):.9f}, expected {RESOLUTIONS[6]:.9f}"
        )

    def test_eastern_edge_tile_pixel_size(self):
        """Eastern edge tile (px_outer=216) → same x_cell as interior tile."""
        from gedih3 import egi
        from gedih3.egi.config import RESOLUTIONS, LIMITS, OUTER_RES

        # Confirm this is actually an edge tile (right edge is clamped)
        edge_px_outer = int((LIMITS['lon_e'] - LIMITS['lon_w']) // OUTER_RES)
        gdf = _make_egi_gdf(px_outer=edge_px_outer, py_outer=50)
        xras = egi.geodf_to_raster(gdf, columns=['value'])

        assert abs(_x_cell(xras) - RESOLUTIONS[6]) < 1e-3, (
            f"Edge tile x_cell={_x_cell(xras):.9f}, expected {RESOLUTIONS[6]:.9f}"
        )

    def test_interior_and_edge_tile_same_pixel_size(self):
        """Interior and edge tiles must produce the same x_cell."""
        from gedih3 import egi
        from gedih3.egi.config import LIMITS, OUTER_RES

        edge_px_outer = int((LIMITS['lon_e'] - LIMITS['lon_w']) // OUTER_RES)

        interior = egi.geodf_to_raster(_make_egi_gdf(px_outer=100, py_outer=50),
                                        columns=['value'])
        edge = egi.geodf_to_raster(_make_egi_gdf(px_outer=edge_px_outer, py_outer=50),
                                    columns=['value'])

        assert abs(_x_cell(interior) - _x_cell(edge)) < 1e-6, (
            f"Interior x_cell={_x_cell(interior):.9f} != edge x_cell={_x_cell(edge):.9f}"
        )


def _vrt_backends():
    """Return the VRT writers available here, as (id, callable) pairs.

    ``build_vrt_xml`` is always present; the ``osgeo`` path only on installs
    that carry the GDAL Python bindings (conda / HPC, not plain pip).
    """
    from gedih3.raster.export import build_vrt, build_vrt_xml

    backends = [pytest.param(build_vrt_xml, id='xml')]
    try:
        import osgeo.gdal  # noqa: F401
    except ImportError:
        pass
    else:
        backends.append(pytest.param(build_vrt, id='osgeo'))
    return backends


class TestBuildVRTResolution:
    """build_vrt must not average edge-tile pixel sizes into the VRT."""

    @pytest.mark.parametrize('writer', _vrt_backends())
    def test_vrt_resolution_matches_tiles(self, tmp_path, writer):
        """VRT built from interior + edge tiles keeps the canonical resolution."""
        import rasterio
        from gedih3 import egi
        from gedih3.egi.config import RESOLUTIONS, LIMITS, OUTER_RES

        edge_px_outer = int((LIMITS['lon_e'] - LIMITS['lon_w']) // OUTER_RES)
        tile_pairs = [
            (100, 50),           # interior
            (edge_px_outer, 50), # eastern edge
        ]

        tif_files = []
        for px_o, py_o in tile_pairs:
            gdf = _make_egi_gdf(px_outer=px_o, py_outer=py_o)
            xras = egi.geodf_to_raster(gdf, columns=['value'])
            out = str(tmp_path / f"tile_{px_o}_{py_o}.tif")
            xras.rio.to_raster(out)
            tif_files.append(out)

        vrt_path = str(tmp_path / "mosaic.vrt")
        writer(tif_files, vrt_path)

        # Read back through rasterio so the assertion does not itself depend
        # on the osgeo bindings.
        with rasterio.open(vrt_path) as src:
            vrt_xres = src.transform.a

        assert abs(vrt_xres - RESOLUTIONS[6]) < 1e-3, (
            f"VRT x_cell={vrt_xres:.9f}, expected {RESOLUTIONS[6]:.9f}. "
            "Edge tiles are contaminating the VRT resolution."
        )
