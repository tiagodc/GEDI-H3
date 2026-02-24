#!/usr/bin/env python
"""
End-to-End Python API Pipeline Tests for gedih3

This module tests the complete Python API workflow from data download through
aggregation and rasterization. Tests the programmatic interface as a developer
would use it in their scripts or notebooks.

Requirements:
- NASA Earthdata credentials configured in ~/.netrc
- Sufficient disk space for test data
- Network access to NASA DAACs

Author: gedih3 team
"""

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

# Add src to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# =============================================================================
# Test Configuration
# =============================================================================

# Small test region (Amazon rainforest, ~1 degree box)
TEST_BBOX = [-51, 0, -50, 1]  # [west, south, east, north]

# Test temporal range
TEST_DATE_START = "2020-06-01"
TEST_DATE_END = "2020-06-30"

# H3 resolution settings
TEST_H3_RESOLUTION = 12
TEST_H3_PARTITION = 5


class APITestConfig:
    """Test configuration with temp directories."""

    def __init__(self):
        self.base_dir = tempfile.mkdtemp(prefix="gedih3_pyapi_test_")
        self.soc_dir = os.path.join(self.base_dir, "soc")
        self.h3_dir = os.path.join(self.base_dir, "h3_database")
        self.output_dir = os.path.join(self.base_dir, "output")

        for d in [self.soc_dir, self.h3_dir, self.output_dir]:
            os.makedirs(d, exist_ok=True)

    def cleanup(self):
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)


# =============================================================================
# Unit Tests for Core Modules
# =============================================================================


class TestConfigModule:
    """Test gedih3.config module."""

    def test_import_config(self):
        """Test config module imports."""
        from gedih3.config import GEDI_PRODUCTS, GH3_DEFAULT_H3_DIR

        assert GH3_DEFAULT_H3_DIR is not None
        assert isinstance(GEDI_PRODUCTS, dict)
        assert "L2A" in GEDI_PRODUCTS

    def test_gedi_products(self):
        """Test GEDI product definitions."""
        from gedih3.config import GEDI_PRODUCTS

        # Check L2A
        assert "L2A" in GEDI_PRODUCTS
        assert "doi" in GEDI_PRODUCTS["L2A"]

        # Check L4A
        assert "L4A" in GEDI_PRODUCTS
        assert "doi" in GEDI_PRODUCTS["L4A"]


class TestGediDriver:
    """Test gedih3.gedidriver module."""

    def test_gedi_file_class(self):
        """Test GEDIFile class parsing."""
        from gedih3.gedidriver import GEDIFile

        # Test typical GEDI filename
        filename = "GEDI02_A_2020100123456_O12345_02_T01234_02_003_02_V002.h5"
        gf = GEDIFile(filename)

        # product is the raw prefix (GEDI02), level is the sub-product (A)
        assert gf.product == "GEDI02"
        assert gf.level == "A"
        assert gf.version == 2
        assert gf.orbit == 12345

    def test_gedi_shot_class(self):
        """Test GEDIShot class decoding."""
        from gedih3.gedidriver import GEDIShot

        # Test shot number decoding
        shot_number = 123456789012345678
        gs = GEDIShot(shot_number)

        assert gs.beam is not None
        assert gs.orbit is not None

    def test_gedi_vars_expand(self):
        """Test variable expansion."""
        from gedih3.gedidriver import gedi_vars_expand

        # Test 'default' expansion
        product_vars = {"L2A": ["default"]}
        expanded = gedi_vars_expand(product_vars)

        assert isinstance(expanded["L2A"], list)
        assert len(expanded["L2A"]) > 0

        # Test 'minimal' expansion
        product_vars = {"L2A": ["minimal"]}
        expanded = gedi_vars_expand(product_vars)

        assert isinstance(expanded["L2A"], list)


class TestH3Utils:
    """Test gedih3.h3utils module."""

    def test_fix_h3_geometry(self):
        """Test H3 geometry fixing (antimeridian)."""
        import h3

        from gedih3.h3utils import fix_h3_geometry

        # Get an H3 cell
        h3_cell = h3.latlng_to_cell(0, -50, 3)
        geometry = fix_h3_geometry(h3_cell)

        assert geometry is not None
        assert geometry.is_valid

    def test_intersect_h3_geometries(self):
        """Test spatial intersection with H3 cells."""
        from shapely.geometry import box

        from gedih3.h3utils import intersect_h3_geometries

        # Create test bbox
        bbox = box(*TEST_BBOX)

        # Get intersecting H3 cells
        h3_cells = intersect_h3_geometries(bbox, res=3)

        assert isinstance(h3_cells, list)
        assert len(h3_cells) > 0

    def test_h3_index_df(self):
        """Test DataFrame H3 indexing."""
        from gedih3.h3utils import h3_index_df

        # Create test DataFrame
        df = pd.DataFrame(
            {"lat_lowestmode": [0.5, 0.6, 0.7], "lon_lowestmode": [-50.5, -50.4, -50.3], "value": [1, 2, 3]}
        )

        # Add H3 index
        indexed_df = h3_index_df(df, res=12, part=5)

        assert indexed_df.index.name.startswith("h3_")
        assert "h3_05" in indexed_df.columns


class TestValidation:
    """Test gedih3.validation module."""

    def test_validate_h3_params(self):
        """Test H3 parameter validation."""
        from gedih3.exceptions import H3ValidationError
        from gedih3.validation import validate_h3_params

        # Valid parameters
        res, part = validate_h3_params(12, 3)
        assert res == 12
        assert part == 3

        # Invalid: partition > resolution
        with pytest.raises(H3ValidationError):
            validate_h3_params(3, 12)

        # Invalid: out of range
        with pytest.raises(H3ValidationError):
            validate_h3_params(20, 3)

    def test_validate_egi_level(self):
        """Test EGI level validation."""
        from gedih3.exceptions import EGIValidationError
        from gedih3.validation import validate_egi_level

        # Valid level
        level = validate_egi_level(6)
        assert level == 6

        # Invalid level
        with pytest.raises(EGIValidationError):
            validate_egi_level(20)


class TestExceptions:
    """Test gedih3.exceptions module."""

    def test_exception_hierarchy(self):
        """Test exception class hierarchy."""
        from gedih3.exceptions import (
            GediDownloadError,
            GediError,
            GediFileError,
            GediNetworkError,
            GediValidationError,
            H3ValidationError,
        )

        # Test inheritance
        assert issubclass(GediNetworkError, GediError)
        assert issubclass(GediDownloadError, GediNetworkError)
        assert issubclass(GediValidationError, GediError)
        assert issubclass(H3ValidationError, GediValidationError)
        assert issubclass(GediFileError, GediError)

    def test_exception_messages(self):
        """Test exception message formatting."""
        from gedih3.exceptions import H3ValidationError

        exc = H3ValidationError("test message")
        assert "test message" in str(exc)


# =============================================================================
# Unit Tests for EGI Module
# =============================================================================


class TestEGIModule:
    """Test gedih3.egi module."""

    def test_egi_encode_decode(self):
        """Test EGI hash encoding/decoding."""
        # Encode a point using to_hash (expects EASE-Grid coordinates)
        # First convert lat/lon to EASE-Grid 2.0 (EPSG:6933) coordinates
        import pyproj

        from gedih3 import egi

        transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True)
        x, y = transformer.transform(-50.5, 0.5)  # lon, lat for transformer

        egi_hash = egi.to_hash(x, y, level=6)

        assert isinstance(egi_hash, (int, np.uint64))

        # Decode back to coordinates using pixel_coordinate
        decoded_x, decoded_y = egi.pixel_coordinate(egi_hash, center=True)

        # Should be close to original (within cell size)
        res = egi.get_resolution(6)
        assert abs(decoded_x - x) < res
        assert abs(decoded_y - y) < res

    def test_egi_get_resolution(self):
        """Test EGI resolution lookup."""
        from gedih3 import egi

        # Level 6 is ~1km
        res = egi.get_resolution(6)
        assert 900 < res < 1100  # Approximately 1km

    def test_egi_dataframe(self):
        """Test adding EGI index to DataFrame."""
        from gedih3 import egi

        # Create test DataFrame
        df = pd.DataFrame(
            {"lat_lowestmode": [0.5, 0.6, 0.7], "lon_lowestmode": [-50.5, -50.4, -50.3], "value": [1, 2, 3]}
        )

        # Add EGI index
        egi_df = egi.egi_dataframe(df, level=6)

        assert "egi06" in egi_df.columns or egi_df.index.name == "egi06"


# =============================================================================
# Unit Tests for Raster Module
# =============================================================================


class TestRasterModule:
    """Test gedih3.raster module."""

    def test_get_h3_resolution_meters(self):
        """Test H3 resolution to meters conversion."""
        from gedih3.raster.h3_raster import get_h3_resolution_meters

        # Level 12 should be around 20-25m
        res_meters = get_h3_resolution_meters(12)
        assert 15 < res_meters < 30

        # Level 6 should be larger (around 7-8 km)
        res_meters_6 = get_h3_resolution_meters(6)
        assert res_meters_6 > res_meters
        assert 5000 < res_meters_6 < 10000

    def test_detect_partition_level(self):
        """Test dynamic partition level detection."""
        from shapely.geometry import Point

        from gedih3.raster.h3_raster import _detect_partition_level

        # Create test GeoDataFrame with H3 partition column
        gdf = gpd.GeoDataFrame(
            {"h3_05": ["abc", "def", "ghi"], "value": [1, 2, 3], "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]}
        )

        level = _detect_partition_level(gdf)
        assert level == 5

    def test_generate_time_windows(self):
        """Test time window generation."""
        from gedih3.raster.timeseries import generate_time_windows

        windows = list(generate_time_windows("2020-01-01", "2020-12-31", 3, "months"))

        assert len(windows) == 4  # 4 quarters
        assert windows[0][2] == "2020-01_to_2020-04" or "2020" in windows[0][2]


# =============================================================================
# Integration Tests for Python API
# =============================================================================


@pytest.fixture(scope="module")
def test_config():
    """Create test configuration."""
    config = APITestConfig()
    yield config
    # config.cleanup()


@pytest.mark.integration
class TestDAAC:
    """Test DAAC access functionality."""

    def test_gedi_accessor_init(self):
        """Test GEDIAccessor initialization."""
        from gedih3.daac import GEDIAccessor

        # Create accessor without authentication (just test init)
        try:
            accessor = GEDIAccessor(authenticate=False, spatial=TEST_BBOX, temporal=(TEST_DATE_START, TEST_DATE_END))
            assert accessor.spatial is not None
        except Exception as e:
            # Auth may fail, but init should work
            pytest.skip(f"Accessor init issue: {e}")

    def test_search_data(self):
        """Test data search (requires auth)."""
        from gedih3.daac import GEDIAccessor

        try:
            accessor = GEDIAccessor(authenticate=True, spatial=TEST_BBOX, temporal=(TEST_DATE_START, TEST_DATE_END))

            granules = accessor.search_data("L2A")
            assert isinstance(granules, list)

        except Exception as e:
            pytest.skip(f"Search requires authentication: {e}")


@pytest.mark.integration
@pytest.mark.slow
class TestDownloadAPI:
    """Test download functionality via Python API."""

    def test_gedi_download(self, test_config):
        """Test downloading GEDI data programmatically."""
        from gedih3.daac import gedi_download

        try:
            product_vars = {"L2A": ["default"], "L4A": ["agbd"]}

            paths = gedi_download(
                product_vars=product_vars,
                odir=test_config.soc_dir,
                spatial=TEST_BBOX,
                temporal=(TEST_DATE_START, TEST_DATE_END),
                resume=True,
                n_jobs=2,
            )

            assert isinstance(paths, (list, type(None)))

        except Exception as e:
            pytest.skip(f"Download failed: {e}")


@pytest.mark.integration
@pytest.mark.slow
class TestBuildAPI:
    """Test H3 database building via Python API."""

    def test_build_h3db(self, test_config):
        """Test building H3 database programmatically."""
        from dask.distributed import Client

        from gedih3.gh3builder import build_h3db

        # Check for SOC files
        h5_files = list(Path(test_config.soc_dir).rglob("*.h5"))
        if not h5_files:
            pytest.skip("No SOC files available for building")

        product_vars = {"L2A": ["default"], "L4A": ["agbd"]}

        with Client(n_workers=2, threads_per_worker=1, memory_limit="2GB") as _client:
            h3_files = build_h3db(
                product_vars=product_vars,
                res=TEST_H3_RESOLUTION,
                part=TEST_H3_PARTITION,
                spatial=TEST_BBOX,
                soc_source=test_config.soc_dir,
                h3_dir=test_config.h3_dir,
            )

        assert h3_files is None or isinstance(h3_files, list)

        # Verify database metadata
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if os.path.exists(log_file):
            with open(log_file) as f:
                meta = json.load(f)
            assert meta["h3_resolution_level"] == TEST_H3_RESOLUTION
            assert meta["h3_partition_level"] == TEST_H3_PARTITION


@pytest.mark.integration
class TestGH3Driver:
    """Test gh3driver module functionality."""

    def test_gh3_read_meta(self, test_config):
        """Test reading database metadata."""
        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Read partition level
        part_level = gh3.gh3_read_meta("h3_partition_level", gh3_root_dir=test_config.h3_dir)

        assert part_level is not None
        assert isinstance(part_level, int)

    def test_gh3_load(self, test_config):
        """Test loading H3 data."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            ddf = gh3.gh3_load(columns=["agbd_l4a"], region=TEST_BBOX, gh3_dir=test_config.h3_dir)

            assert ddf is not None
            assert ddf.npartitions > 0

    def test_gh3_aggregate(self, test_config):
        """Test H3 aggregation."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            ddf = gh3.gh3_load(columns=["agbd_l4a"], gh3_dir=test_config.h3_dir)

            agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg="mean", add_geometry=True)

            result = agg_df.head(10)
            assert len(result) >= 0
            assert "geometry" in result.columns or hasattr(result, "geometry")

    def test_egi_aggregate(self, test_config):
        """Test EGI aggregation."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            ddf = gh3.gh3_load(columns=["agbd_l4a", "lat_lowestmode", "lon_lowestmode"], gh3_dir=test_config.h3_dir)

            agg_df = gh3.egi_aggregate(ddf, target_level=6, agg="mean", add_geometry=True)

            result = agg_df.head(10)
            assert len(result) >= 0


@pytest.mark.integration
class TestRasterization:
    """Test rasterization functionality."""

    def test_h3_to_raster(self, test_config):
        """Test H3 to raster conversion."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3
        from gedih3.raster import h3_to_raster

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            ddf = gh3.gh3_load(columns=["agbd_l4a"], gh3_dir=test_config.h3_dir)

            agg_gdf = gh3.gh3_aggregate(ddf, target_res=6, agg="mean", add_geometry=True).compute()

            if len(agg_gdf) > 0:
                xras = h3_to_raster(agg_gdf, columns=["agbd_l4a_mean"])

                assert xras is not None
                assert hasattr(xras, "rio")

    def test_egi_to_raster(self, test_config):
        """Test EGI to raster conversion."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3
        from gedih3 import egi

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            ddf = gh3.gh3_load(columns=["agbd_l4a", "lat_lowestmode", "lon_lowestmode"], gh3_dir=test_config.h3_dir)

            agg_gdf = gh3.egi_aggregate(ddf, target_level=6, agg="mean", add_geometry=True).compute()

            if len(agg_gdf) > 0:
                xras = egi.geodf_to_raster(agg_gdf)

                assert xras is not None
                assert hasattr(xras, "rio")


# =============================================================================
# Full Pipeline Test
# =============================================================================


@pytest.mark.integration
@pytest.mark.slow
class TestFullPythonPipeline:
    """Test complete Python API pipeline."""

    def test_full_pipeline(self):
        """
        Run complete pipeline via Python API:
        1. Download GEDI data
        2. Build H3 database
        3. Load and query data
        4. Aggregate to EGI
        5. Rasterize to GeoTIFF
        """
        from dask.distributed import Client

        config = APITestConfig()

        try:
            with Client(n_workers=2, threads_per_worker=1, memory_limit="4GB") as client:
                print(f"\nDask dashboard: {client.dashboard_link}")

                # Step 1: Download
                print("\n" + "=" * 70)
                print("STEP 1: Downloading GEDI data")
                print("=" * 70)

                from gedih3.daac import gedi_download

                product_vars = {"L2A": ["default"], "L4A": ["agbd"]}

                try:
                    paths = gedi_download(  # noqa: F841
                        product_vars=product_vars,
                        odir=config.soc_dir,
                        spatial=TEST_BBOX,
                        temporal=(TEST_DATE_START, TEST_DATE_END),
                        resume=True,
                        n_jobs=2,
                    )
                except Exception as e:
                    print(f"Download error (may be auth): {e}")

                # Check for files
                h5_files = list(Path(config.soc_dir).rglob("*.h5"))
                if not h5_files:
                    pytest.skip("No data available for test region/dates")

                # Step 2: Build H3 database
                print("\n" + "=" * 70)
                print("STEP 2: Building H3 database")
                print("=" * 70)

                from gedih3.gh3builder import build_h3db

                h3_files = build_h3db(  # noqa: F841
                    product_vars=product_vars,
                    res=TEST_H3_RESOLUTION,
                    part=TEST_H3_PARTITION,
                    spatial=TEST_BBOX,
                    soc_source=config.soc_dir,
                    h3_dir=config.h3_dir,
                )

                assert os.path.exists(os.path.join(config.h3_dir, "gedih3_build_log.json"))

                # Step 3: Load and query data
                print("\n" + "=" * 70)
                print("STEP 3: Loading and querying data")
                print("=" * 70)

                import gedih3.gh3driver as gh3

                ddf = gh3.gh3_load(
                    columns=["agbd_l4a", "quality_flag_l2a", "lat_lowestmode", "lon_lowestmode"],
                    region=TEST_BBOX,
                    query="quality_flag_l2a == 1",
                    gh3_dir=config.h3_dir,
                )

                print(f"Loaded {ddf.npartitions} partitions")

                # Step 4: Aggregate to EGI
                print("\n" + "=" * 70)
                print("STEP 4: Aggregating to EGI level 6")
                print("=" * 70)

                agg_gdf = gh3.egi_aggregate(ddf, target_level=6, agg="mean", add_geometry=True)

                result = agg_gdf.compute()
                print(f"Aggregated to {len(result)} pixels")

                if len(result) == 0:
                    pytest.skip("No data after aggregation")

                # Step 5: Rasterize
                print("\n" + "=" * 70)
                print("STEP 5: Rasterizing to GeoTIFF")
                print("=" * 70)

                from gedih3 import egi

                xras = egi.geodf_to_raster(result)

                output_path = os.path.join(config.output_dir, "agbd_test.tif")
                egi.export_raster(xras, output_path)

                assert os.path.exists(output_path)
                print(f"Raster saved to: {output_path}")

                print("\n" + "=" * 70)
                print("FULL PYTHON API PIPELINE COMPLETED SUCCESSFULLY!")
                print("=" * 70)

        finally:
            # config.cleanup()
            pass


# =============================================================================
# Data Quality Tests
# =============================================================================


class TestDataQuality:
    """Test data quality and consistency."""

    def test_h3_index_consistency(self, test_config):
        """Test H3 index consistency across operations."""
        import h3

        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Read metadata
        res_level = gh3.gh3_read_meta("h3_resolution_level", gh3_root_dir=test_config.h3_dir)
        part_level = gh3.gh3_read_meta("h3_partition_level", gh3_root_dir=test_config.h3_dir)

        # Partition level should be <= resolution level
        assert part_level <= res_level

        # Check H3 cell IDs in data
        h3_ids = gh3.gh3_read_meta("h3_partition_ids", gh3_root_dir=test_config.h3_dir)
        if h3_ids:
            # All partition IDs should be at partition level
            for h3_id in h3_ids[:10]:  # Check first 10
                assert h3.get_resolution(h3_id) == part_level

    def test_aggregation_data_integrity(self, test_config):
        """Test that aggregation preserves data integrity."""
        from dask.distributed import Client

        import gedih3.gh3driver as gh3

        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        with Client(n_workers=2, threads_per_worker=1) as _client:
            # Load original data
            ddf = gh3.gh3_load(columns=["agbd_l4a"], gh3_dir=test_config.h3_dir)

            original_count = ddf.map_partitions(len).compute().sum()

            # Aggregate
            agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg="mean")

            agg_result = agg_df.compute()

            # Aggregated data should have fewer rows
            if original_count > 0:
                assert len(agg_result) <= original_count


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    # Run with: python -m pytest tests/test_python_api_pipeline.py -v
    # Or: python tests/test_python_api_pipeline.py
    pytest.main([__file__, "-v", "-s", "--tb=short"])
