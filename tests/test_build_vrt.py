"""
Tests for the VRT mosaic writers in gedih3.raster.export.

``build_vrt`` prefers the ``osgeo.gdal`` bindings and falls back to the
rasterio-only XML writer ``build_vrt_xml``. The GDAL bindings are not
pip-installable without a version-matched system libgdal, so pip-only
installs always take the fallback — these tests pin the fallback's output to
be equivalent to what ``gdal.BuildVRT`` produces wherever both are available.
"""

import os

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from gedih3.exceptions import GediRasterizationError, GediImageSamplingError
from gedih3.raster.export import build_vrt, build_vrt_xml, build_vrt_safe

try:
    import osgeo.gdal  # noqa: F401
    HAS_OSGEO = True
except ImportError:
    HAS_OSGEO = False

requires_osgeo = pytest.mark.skipif(not HAS_OSGEO, reason="GDAL Python bindings not installed")


# =============================================================================
# Fixtures
# =============================================================================

# Tile layouts exercising the mosaic geometry: contiguous, gapped, ragged
# (partial edge tiles), and diagonal (large empty quadrants).
LAYOUTS = {
    'row':     [(0, 0, 16, 16), (16, 0, 16, 16)],
    'grid':    [(0, 0, 16, 16), (16, 0, 16, 16), (0, 16, 16, 16), (16, 16, 16, 16)],
    'gap':     [(0, 0, 16, 16), (48, 0, 16, 16)],
    'ragged':  [(0, 0, 16, 16), (16, 0, 7, 16), (0, 16, 16, 9)],
    'diagonal': [(0, 0, 16, 16), (32, 32, 16, 16)],
}

XRES = YRES = 0.01
ORIGIN_X, ORIGIN_Y = -51.0, 1.0


def _make_tiles(tmp_path, layout, count=1, dtype='float32', nodata=-9999.0,
                crs='EPSG:4326', xres=XRES, yres=YRES):
    """Write the named tile layout as GeoTIFFs; return their paths."""
    paths = []
    for i, (col, row, width, height) in enumerate(LAYOUTS[layout] if isinstance(layout, str) else layout):
        path = str(tmp_path / f'tile_{i}.tif')
        transform = from_origin(ORIGIN_X + col * xres, ORIGIN_Y - row * yres, xres, yres)
        with rasterio.open(path, 'w', driver='GTiff', width=width, height=height,
                           count=count, dtype=dtype, crs=crs, transform=transform,
                           nodata=nodata) as dst:
            for band in range(1, count + 1):
                arr = (np.arange(width * height).reshape(height, width)
                       + i * 1000 + band * 100).astype(dtype)
                arr[0, 0] = nodata          # embed a nodata pixel per tile
                dst.write(arr, band)
        paths.append(path)
    return paths


def _summarize(path):
    """Read a raster into a comparable summary dict."""
    with rasterio.open(path) as src:
        return {
            'shape': (src.count, src.height, src.width),
            'transform': tuple(round(v, 12) for v in src.transform[:6]),
            'crs': src.crs,
            'dtypes': src.dtypes,
            'nodata': src.nodata,
            'bounds': tuple(round(v, 12) for v in src.bounds),
            'data': src.read(masked=True),
        }


# =============================================================================
# Test: XML writer matches the GDAL bindings
# =============================================================================

class TestXMLMatchesGDAL:
    """build_vrt_xml must be indistinguishable from gdal.BuildVRT on read."""

    @requires_osgeo
    @pytest.mark.parametrize('layout', sorted(LAYOUTS))
    def test_backends_agree(self, tmp_path, layout):
        tifs = _make_tiles(tmp_path, layout,
                           count=3 if layout == 'grid' else 1,
                           dtype='int16' if layout == 'gap' else 'float32',
                           nodata=-32768 if layout == 'gap' else -9999.0)

        from osgeo import gdal
        gdal.UseExceptions()
        gdal_vrt = str(tmp_path / 'gdal.vrt')
        ds = gdal.Open(tifs[0])
        gt = ds.GetGeoTransform()
        ds = None
        gdal.BuildVRT(gdal_vrt, tifs,
                      options=gdal.BuildVRTOptions(resolution='user',
                                                   xRes=gt[1], yRes=abs(gt[5])))

        xml_vrt = build_vrt_xml(tifs, str(tmp_path / 'xml.vrt'))

        ref, got = _summarize(gdal_vrt), _summarize(xml_vrt)
        for key in ('shape', 'transform', 'crs', 'dtypes', 'nodata', 'bounds'):
            assert got[key] == ref[key], f"{key}: gdal={ref[key]} xml={got[key]}"
        np.testing.assert_array_equal(got['data'].mask, ref['data'].mask)
        np.testing.assert_array_equal(got['data'].filled(0), ref['data'].filled(0))


# =============================================================================
# Test: XML writer standalone correctness
# =============================================================================

class TestXMLWriter:
    """Properties the fallback must hold on its own, with no osgeo present."""

    @pytest.mark.parametrize('layout', sorted(LAYOUTS))
    def test_mosaic_covers_union_of_tiles(self, tmp_path, layout):
        tifs = _make_tiles(tmp_path, layout)
        vrt = build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))

        lefts, rights, bottoms, tops = [], [], [], []
        for t in tifs:
            with rasterio.open(t) as src:
                lefts.append(src.bounds.left)
                rights.append(src.bounds.right)
                bottoms.append(src.bounds.bottom)
                tops.append(src.bounds.top)

        with rasterio.open(vrt) as src:
            assert src.bounds.left == pytest.approx(min(lefts))
            assert src.bounds.right == pytest.approx(max(rights))
            assert src.bounds.bottom == pytest.approx(min(bottoms))
            assert src.bounds.top == pytest.approx(max(tops))

    def test_pixel_values_land_at_source_coordinates(self, tmp_path):
        """Every source pixel must be readable at its own map coordinate."""
        tifs = _make_tiles(tmp_path, 'gap')
        vrt = build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))

        with rasterio.open(vrt) as mosaic:
            for tif in tifs:
                with rasterio.open(tif) as src:
                    expected = src.read(1, masked=True)
                    for row, col in [(3, 3), (8, 11), (15, 15)]:
                        x, y = src.xy(row, col)
                        got = list(mosaic.sample([(x, y)], masked=True))[0][0]
                        assert got == expected[row, col]

    def test_resolution_pinned_to_first_tile(self, tmp_path):
        """A finer-pixelled later tile must not drag the mosaic resolution."""
        coarse = _make_tiles(tmp_path, [(0, 0, 16, 16)])
        fine_dir = tmp_path / 'fine'
        fine_dir.mkdir()
        fine = _make_tiles(fine_dir, [(0, 0, 16, 16)], xres=XRES / 4, yres=YRES / 4)

        vrt = build_vrt_xml(coarse + fine, str(tmp_path / 'm.vrt'))
        with rasterio.open(vrt) as src:
            assert src.transform.a == pytest.approx(XRES)
            assert abs(src.transform.e) == pytest.approx(YRES)

    def test_multiband_preserved(self, tmp_path):
        tifs = _make_tiles(tmp_path, 'grid', count=3)
        vrt = build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))
        with rasterio.open(vrt) as src:
            assert src.count == 3

    def test_nodata_preserved(self, tmp_path):
        tifs = _make_tiles(tmp_path, 'row', nodata=-9999.0)
        vrt = build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))
        with rasterio.open(vrt) as src:
            assert src.nodata == -9999.0
            assert src.read(1, masked=True).mask.any()

    def test_source_paths_are_relative(self, tmp_path):
        """Relative SourceFilename keeps the mosaic valid if the dir moves."""
        tifs = _make_tiles(tmp_path, 'row')
        vrt = build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))
        assert str(tmp_path) not in open(vrt).read()

        moved = tmp_path.parent / 'moved'
        os.rename(tmp_path, moved)
        with rasterio.open(str(moved / 'm.vrt')) as src:
            assert src.read(1, masked=True).count() > 0

    def test_written_atomically(self, tmp_path):
        """No .tmp residue is left behind by a successful write."""
        tifs = _make_tiles(tmp_path, 'row')
        build_vrt_xml(tifs, str(tmp_path / 'm.vrt'))
        assert not [f for f in os.listdir(tmp_path) if f.endswith('.tmp')]


# =============================================================================
# Test: preconditions — refuse rather than emit a wrong mosaic
# =============================================================================

class TestXMLWriterPreconditions:

    def test_mixed_crs_raises(self, tmp_path):
        a = _make_tiles(tmp_path, [(0, 0, 16, 16)])
        other = tmp_path / 'other'
        other.mkdir()
        b = _make_tiles(other, [(0, 0, 16, 16)], crs='EPSG:3857')
        with pytest.raises(GediRasterizationError, match='single CRS'):
            build_vrt_xml(a + b, str(tmp_path / 'm.vrt'))

    def test_rotated_raster_raises(self, tmp_path):
        path = str(tmp_path / 'rot.tif')
        rotated = rasterio.Affine(XRES, 0.002, ORIGIN_X, 0.002, -YRES, ORIGIN_Y)
        with rasterio.open(path, 'w', driver='GTiff', width=8, height=8, count=1,
                           dtype='float32', crs='EPSG:4326', transform=rotated) as dst:
            dst.write(np.zeros((8, 8), dtype='float32'), 1)
        with pytest.raises(GediRasterizationError, match='rotated'):
            build_vrt_xml([path], str(tmp_path / 'm.vrt'))

    def test_empty_input_raises(self, tmp_path):
        with pytest.raises(GediRasterizationError, match='at least one'):
            build_vrt_xml([], str(tmp_path / 'm.vrt'))


# =============================================================================
# Test: build_vrt dispatch and the non-fatal wrapper
# =============================================================================

class TestDispatchAndSafety:

    def test_build_vrt_falls_back_without_osgeo(self, tmp_path, monkeypatch):
        """A missing osgeo import must transparently take the XML path."""
        import builtins
        real_import = builtins.__import__

        def blocked(name, *args, **kwargs):
            if name.startswith('osgeo'):
                raise ImportError('no osgeo')
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, '__import__', blocked)

        tifs = _make_tiles(tmp_path, 'row')
        vrt = build_vrt(tifs, str(tmp_path / 'm.vrt'))
        with rasterio.open(vrt) as src:
            assert src.count == 1

    def test_build_vrt_safe_returns_path_on_success(self, tmp_path):
        tifs = _make_tiles(tmp_path, 'row')
        assert build_vrt_safe(tifs, str(tmp_path / 'm.vrt')) is not None

    def test_build_vrt_safe_warns_and_returns_none(self, tmp_path):
        """A failed mosaic must not discard the finished tiles.

        A missing first tile fails identically under both backends — unlike a
        CRS mismatch, which gdal.BuildVRT merely warns about and skips.
        """
        tifs = _make_tiles(tmp_path, 'row')
        broken = [str(tmp_path / 'does_not_exist.tif')] + tifs

        # gedih3 loggers set propagate=False, so caplog's root handler cannot
        # see them — attach directly to the emitting logger instead.
        import logging
        records = []
        handler = logging.Handler()
        handler.emit = records.append
        module_logger = logging.getLogger('gedih3.raster.export')
        module_logger.addHandler(handler)
        try:
            result = build_vrt_safe(broken, str(tmp_path / 'm.vrt'))
        finally:
            module_logger.removeHandler(handler)

        assert result is None
        assert any('Could not build VRT mosaic' in r.getMessage() for r in records)
        assert all(os.path.exists(t) for t in tifs)


# =============================================================================
# Test: gh3_from_img keeps mosaic failures fatal
# =============================================================================

class TestImgUtilsMosaicIsFatal:
    """resolve_raster_source has no fallback — the VRT is the sampling source."""

    def test_mosaic_failure_becomes_sampling_error(self, tmp_path, monkeypatch):
        """Whatever the backend raises must surface as GediImageSamplingError."""
        import gedih3.raster.export as export_mod
        from gedih3.imgutils import resolve_raster_source

        _make_tiles(tmp_path, 'row')
        monkeypatch.setattr(export_mod, 'build_vrt',
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom')))

        with pytest.raises(GediImageSamplingError, match='Could not mosaic'):
            resolve_raster_source(str(tmp_path))
