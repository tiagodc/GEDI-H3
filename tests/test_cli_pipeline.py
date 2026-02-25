#!/usr/bin/env python
"""
End-to-End CLI Pipeline Tests for gedih3

This module tests the complete CLI workflow from data download through
aggregation and rasterization. Tests use subprocess to call CLI tools
as a user would from the command line.

Requirements:
- NASA Earthdata credentials configured in ~/.netrc
- Sufficient disk space for test data
- Network access to NASA DAACs

Author: gedih3 team
"""

import os
import sys
import json
import shutil
import tempfile
import subprocess
import pytest
from pathlib import Path

# Add src to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


# =============================================================================
# Test Configuration
# =============================================================================

# Small test region (Amazon rainforest, ~1 degree box)
# Note: Use equals syntax (--region=) when bbox starts with negative to avoid argparse issues
TEST_REGION = "-51,0,-50,1"
TEST_REGION_ARG = f"--region={TEST_REGION}"  # Use equals syntax to handle leading negative

# Test temporal range (small window for faster tests)
TEST_DATE_START = "2020-06-01"
TEST_DATE_END = "2020-06-30"

# H3 resolution settings
TEST_H3_RESOLUTION = 12  # Index level
TEST_H3_PARTITION = 5    # Partition level (higher = smaller partitions for testing)

# Dask settings for tests (reduced for faster execution)
TEST_CORES = 2
TEST_MEMORY = 4  # GB per worker


class CLITestConfig:
    """Test configuration that creates temp directories."""

    def __init__(self):
        self.base_dir = tempfile.mkdtemp(prefix="gedih3_test_")
        self.soc_dir = os.path.join(self.base_dir, "soc")
        self.h3_dir = os.path.join(self.base_dir, "h3_database")
        self.extract_dir = os.path.join(self.base_dir, "extracted")
        self.aggregate_dir = os.path.join(self.base_dir, "aggregated")
        self.raster_dir = os.path.join(self.base_dir, "rasters")

        # Create directories
        for d in [self.soc_dir, self.h3_dir, self.extract_dir,
                  self.aggregate_dir, self.raster_dir]:
            os.makedirs(d, exist_ok=True)

    def cleanup(self):
        """Remove all test directories."""
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)


def run_cli(cmd: list, timeout: int = 600) -> subprocess.CompletedProcess:
    """
    Run a CLI command and return the result.

    Parameters
    ----------
    cmd : list
        Command and arguments as list
    timeout : int
        Timeout in seconds

    Returns
    -------
    subprocess.CompletedProcess
        Result of the command execution
    """
    print(f"\n{'='*70}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*70}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if result.stdout:
        print("STDOUT:", result.stdout[:2000])
    if result.stderr:
        print("STDERR:", result.stderr[:2000])

    return result


# =============================================================================
# Unit Tests for CLI Argument Parsing
# =============================================================================

class TestCLIArguments:
    """Test CLI argument parsing for all tools."""

    def test_gh3_download_help(self):
        """Test gh3_download --help works."""
        result = run_cli(["gh3_download", "--help"])
        assert result.returncode == 0
        assert "Download GEDI data" in result.stdout or "download" in result.stdout.lower()

    def test_gh3_build_help(self):
        """Test gh3_build --help works."""
        result = run_cli(["gh3_build", "--help"])
        assert result.returncode == 0
        assert "Build H3-indexed" in result.stdout or "build" in result.stdout.lower()

    def test_gh3_extract_help(self):
        """Test gh3_extract --help works."""
        result = run_cli(["gh3_extract", "--help"])
        assert result.returncode == 0
        assert "Extract" in result.stdout or "extract" in result.stdout.lower()

    def test_gh3_aggregate_help(self):
        """Test gh3_aggregate --help works."""
        result = run_cli(["gh3_aggregate", "--help"])
        assert result.returncode == 0
        assert "aggregate" in result.stdout.lower() or "aggregat" in result.stdout.lower()

    def test_gh3_rasterize_help(self):
        """Test gh3_rasterize --help works."""
        result = run_cli(["gh3_rasterize", "--help"])
        assert result.returncode == 0
        assert "rasterize" in result.stdout.lower() or "GeoTIFF" in result.stdout

    def test_gh3_list_variables_help(self):
        """Test gh3_list_variables --help works."""
        result = run_cli(["gh3_list_variables", "--help"])
        assert result.returncode == 0

    def test_gh3_list_resolutions_help(self):
        """Test gh3_list_resolutions --help works."""
        result = run_cli(["gh3_list_resolutions", "--help"])
        assert result.returncode == 0


class TestGH3ListVariables:
    """Test gh3_list_variables tool."""

    def test_list_l2a_variables(self):
        """Test listing L2A variables."""
        result = run_cli(["gh3_list_variables", "-p", "L2A"])
        assert result.returncode == 0
        # Should contain common L2A variables
        assert "rh" in result.stdout.lower() or "height" in result.stdout.lower()

    def test_list_l4a_variables(self):
        """Test listing L4A variables."""
        result = run_cli(["gh3_list_variables", "-p", "L4A"])
        assert result.returncode == 0
        # Should contain AGBD
        assert "agbd" in result.stdout.lower()

    def test_list_variables_with_grep(self):
        """Test grep filtering of variables."""
        result = run_cli(["gh3_list_variables", "-p", "L2A", "-g", "quality"])
        assert result.returncode == 0
        # Should filter to quality-related variables
        assert "quality" in result.stdout.lower()


class TestGH3ListResolutions:
    """Test gh3_list_resolutions tool."""

    def test_list_h3_resolutions(self):
        """Test listing H3 resolutions."""
        result = run_cli(["gh3_list_resolutions"])
        assert result.returncode == 0
        # Should show resolution table
        assert "Res" in result.stdout or "Level" in result.stdout

    def test_list_egi_resolutions(self):
        """Test listing EGI resolutions."""
        result = run_cli(["gh3_list_resolutions", "-egi"])
        assert result.returncode == 0
        assert "EGI" in result.stdout or "EASE" in result.stdout

    def test_specific_resolution(self):
        """Test showing specific resolution."""
        result = run_cli(["gh3_list_resolutions", "-r", "6"])
        assert result.returncode == 0


# =============================================================================
# Integration Tests for CLI Pipeline
# =============================================================================

@pytest.fixture(scope="module")
def test_config():
    """Create test configuration with temp directories."""
    config = CLITestConfig()
    yield config
    # Cleanup after tests (comment out to inspect results)
    # config.cleanup()


@pytest.mark.integration
@pytest.mark.slow
class TestCLIDownload:
    """Test gh3_download CLI tool."""

    def test_download_with_region(self, test_config):
        """Test downloading GEDI data for a small region."""
        cmd = [
            "gh3_download",
            TEST_REGION_ARG,
            "-d0", TEST_DATE_START,
            "-d1", TEST_DATE_END,
            "-l2a", "default",
            "-l4a", "agbd",
            "-o", test_config.soc_dir,
            "-N", str(TEST_CORES),
            "-M", str(TEST_MEMORY),
            "-v"
        ]

        result = run_cli(cmd, timeout=1800)  # 30 min timeout for download

        # Check that download completed (may have 0 files if no data in region)
        assert result.returncode == 0, f"Download failed: {result.stderr}"

        # Check that log file was created
        log_file = os.path.join(test_config.soc_dir, "gedih3_download_log.json")
        if os.path.exists(log_file):
            with open(log_file) as f:
                log_data = json.load(f)
            assert log_data.get('status') in ['COMPLETED', 'DOWNLOADING']

    def test_download_resume(self, test_config):
        """Test resuming a download."""
        cmd = [
            "gh3_download",
            TEST_REGION_ARG,
            "-l2a", "default",
            "-o", test_config.soc_dir,
            "--resume",
            "-N", str(TEST_CORES),
            "-Q"  # Quiet mode
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0


@pytest.mark.integration
@pytest.mark.slow
class TestCLIBuild:
    """Test gh3_build CLI tool."""

    def test_build_h3_database(self, test_config):
        """Test building H3 database from SOC files."""
        # Skip if no SOC files exist
        soc_files = list(Path(test_config.soc_dir).rglob("*.h5"))
        if not soc_files:
            pytest.skip("No SOC files available for building")

        cmd = [
            "gh3_build",
            TEST_REGION_ARG,
            "-l2a", "default",
            "-l4a", "agbd",
            "-h3r", str(TEST_H3_RESOLUTION),
            "-h3p", str(TEST_H3_PARTITION),
            "-i", test_config.soc_dir,
            "-o", test_config.h3_dir,
            "-N", str(TEST_CORES),
            "-M", str(TEST_MEMORY),
            "-v"
        ]

        result = run_cli(cmd, timeout=1800)

        assert result.returncode == 0, f"Build failed: {result.stderr}"

        # Check that database was created
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        assert os.path.exists(log_file), "Build log not created"

        # Verify log contents
        with open(log_file) as f:
            log_data = json.load(f)

        assert log_data.get('status') == 'COMPLETED'
        assert log_data.get('h3_resolution_level') == TEST_H3_RESOLUTION
        assert log_data.get('h3_partition_level') == TEST_H3_PARTITION

    def test_build_with_custom_h3_levels(self, test_config):
        """Test building with custom H3 resolution levels."""
        custom_h3_dir = os.path.join(test_config.base_dir, "h3_custom")
        os.makedirs(custom_h3_dir, exist_ok=True)

        soc_files = list(Path(test_config.soc_dir).rglob("*.h5"))
        if not soc_files:
            pytest.skip("No SOC files available for building")

        # Use different partition/index levels
        cmd = [
            "gh3_build",
            TEST_REGION_ARG,
            "-l2a", "minimal",
            "-h3r", "10",  # Coarser index
            "-h3p", "4",   # Different partition
            "-i", test_config.soc_dir,
            "-o", custom_h3_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=1200)

        if result.returncode == 0:
            # Verify custom levels in metadata
            log_file = os.path.join(custom_h3_dir, "gedih3_build_log.json")
            if os.path.exists(log_file):
                with open(log_file) as f:
                    log_data = json.load(f)
                assert log_data.get('h3_resolution_level') == 10
                assert log_data.get('h3_partition_level') == 4


@pytest.mark.integration
class TestCLIExtract:
    """Test gh3_extract CLI tool."""

    def test_extract_with_region(self, test_config):
        """Test extracting data from H3 database."""
        # Check if database exists
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        cmd = [
            "gh3_extract",
            "-d", test_config.h3_dir,
            TEST_REGION_ARG,
            "-l2a", "rh",
            "-l4a", "agbd",
            "-o", test_config.extract_dir,
            "-N", str(TEST_CORES),
            "-v"
        ]

        result = run_cli(cmd, timeout=600)

        assert result.returncode == 0, f"Extract failed: {result.stderr}"

        # Check output files exist
        output_files = list(Path(test_config.extract_dir).rglob("*.parquet"))
        assert len(output_files) > 0, "No output files created"

    def test_extract_with_quality_filter(self, test_config):
        """Test extraction with quality filtering."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        quality_dir = os.path.join(test_config.base_dir, "extract_quality")
        os.makedirs(quality_dir, exist_ok=True)

        cmd = [
            "gh3_extract",
            "-d", test_config.h3_dir,
            "-l4a", "agbd",
            "-y",  # Quality filter
            "-o", quality_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0


@pytest.mark.integration
class TestCLIAggregate:
    """Test gh3_aggregate CLI tool."""

    def test_aggregate_h3(self, test_config):
        """Test H3 aggregation."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        cmd = [
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-h3", "6",  # Aggregate to level 6
            "-l4a", "agbd",
            "-a", "mean",
            "-o", test_config.aggregate_dir,
            "-N", str(TEST_CORES),
            "-v"
        ]

        result = run_cli(cmd, timeout=600)

        assert result.returncode == 0, f"Aggregate failed: {result.stderr}"

        # Check output
        output_files = list(Path(test_config.aggregate_dir).rglob("*.parquet"))
        assert len(output_files) > 0, "No aggregated files created"

    def test_aggregate_egi(self, test_config):
        """Test EGI (EASE Grid) aggregation."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        egi_dir = os.path.join(test_config.base_dir, "aggregate_egi")
        os.makedirs(egi_dir, exist_ok=True)

        cmd = [
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-egi", "6",  # EGI level 6 (~1km)
            "-l4a", "agbd",
            "-a", "mean",
            "-o", egi_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0

    def test_aggregate_merged_metadata(self, test_config):
        """Test that merged aggregation output writes gedih3_dataset.json metadata."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        merged_dir = os.path.join(test_config.base_dir, "aggregate_merged")
        os.makedirs(merged_dir, exist_ok=True)

        cmd = [
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-egi", "6",
            "-l4a", "agbd",
            "-a", "mean",
            "-m",  # Merged output
            "-o", merged_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0, f"Merged aggregate failed: {result.stderr}"

        # Verify metadata file was created
        meta_file = os.path.join(merged_dir, "gedih3_dataset.json")
        assert os.path.exists(meta_file), "gedih3_dataset.json not created for merged output"

        # Verify metadata contents
        with open(meta_file) as f:
            meta = json.load(f)

        assert meta.get('index_type') == 'egi', f"Expected index_type='egi', got '{meta.get('index_type')}'"
        assert meta.get('index_level') == 6, f"Expected index_level=6, got {meta.get('index_level')}"


@pytest.mark.integration
class TestCLIRasterize:
    """Test gh3_rasterize CLI tool.

    gh3_rasterize requires pre-aggregated datasets (from gh3_aggregate).
    Each test first creates an aggregated dataset, then rasterizes it.
    """

    def test_rasterize_h3(self, test_config):
        """Test rasterization of H3-aggregated dataset."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Step 1: Create H3 aggregated dataset
        h3_agg_dir = os.path.join(test_config.base_dir, "raster_h3_agg")
        os.makedirs(h3_agg_dir, exist_ok=True)

        agg_result = run_cli([
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-h3", "6",
            "-l4a", "agbd",
            "-a", "mean",
            "-o", h3_agg_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ], timeout=600)
        assert agg_result.returncode == 0, f"H3 aggregation failed: {agg_result.stderr}"

        # Step 2: Rasterize the aggregated dataset
        cmd = [
            "gh3_rasterize",
            "-d", h3_agg_dir,
            "-o", test_config.raster_dir,
            "-N", str(TEST_CORES),
            "-v"
        ]

        result = run_cli(cmd, timeout=600)

        assert result.returncode == 0, f"Rasterize failed: {result.stderr}"

        # Check output
        output_files = list(Path(test_config.raster_dir).rglob("*.tif"))
        assert len(output_files) > 0, "No raster files created"

    def test_rasterize_egi(self, test_config):
        """Test rasterization of EGI-aggregated dataset."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Step 1: Create EGI aggregated dataset
        egi_agg_dir = os.path.join(test_config.base_dir, "raster_egi_agg")
        os.makedirs(egi_agg_dir, exist_ok=True)

        agg_result = run_cli([
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-egi", "6",
            "-l4a", "agbd",
            "-a", "mean",
            "-o", egi_agg_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ], timeout=600)
        assert agg_result.returncode == 0, f"EGI aggregation failed: {agg_result.stderr}"

        # Step 2: Rasterize the aggregated dataset
        egi_raster_dir = os.path.join(test_config.base_dir, "raster_egi")
        os.makedirs(egi_raster_dir, exist_ok=True)

        cmd = [
            "gh3_rasterize",
            "-d", egi_agg_dir,
            "-o", egi_raster_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0

    def test_rasterize_merged(self, test_config):
        """Test merged raster output from pre-aggregated dataset."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Step 1: Create EGI aggregated dataset
        merged_agg_dir = os.path.join(test_config.base_dir, "raster_merged_agg")
        os.makedirs(merged_agg_dir, exist_ok=True)

        agg_result = run_cli([
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-egi", "6",
            "-l4a", "agbd",
            "-a", "mean",
            "-o", merged_agg_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ], timeout=600)
        assert agg_result.returncode == 0, f"EGI aggregation failed: {agg_result.stderr}"

        # Step 2: Rasterize with merge
        merged_raster = os.path.join(test_config.base_dir, "merged_raster.tif")

        cmd = [
            "gh3_rasterize",
            "-d", merged_agg_dir,
            "-m",  # Merge
            "-o", merged_raster,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)

        if result.returncode == 0:
            assert os.path.exists(merged_raster), "Merged raster not created"

    def test_rasterize_timeseries(self, test_config):
        """Test rasterization of time-series aggregated dataset.

        gh3_rasterize auto-detects time-series subdirectories containing
        gedih3_dataset.json metadata files.
        """
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        # Step 1: Create time-series aggregated dataset
        ts_agg_dir = os.path.join(test_config.base_dir, "raster_ts_agg")
        os.makedirs(ts_agg_dir, exist_ok=True)

        agg_result = run_cli([
            "gh3_aggregate",
            "-d", test_config.h3_dir,
            "-egi", "6",
            "-l4a", "agbd",
            "-a", "mean",
            "-ti", "6", "-tu", "months",
            "-t0", TEST_DATE_START, "-t1", TEST_DATE_END,
            "-o", ts_agg_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ], timeout=600)
        assert agg_result.returncode == 0, f"Time-series aggregation failed: {agg_result.stderr}"

        # Verify time-series subdirectories were created
        window_dirs = [d for d in Path(ts_agg_dir).iterdir()
                       if d.is_dir() and (d / "gedih3_dataset.json").exists()]
        assert len(window_dirs) > 0, "No time-series window directories created"

        # Step 2: Rasterize the time-series dataset
        ts_raster_dir = os.path.join(test_config.base_dir, "raster_ts")
        os.makedirs(ts_raster_dir, exist_ok=True)

        cmd = [
            "gh3_rasterize",
            "-d", ts_agg_dir,
            "-o", ts_raster_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0, f"Time-series rasterize failed: {result.stderr}"

        # Check output has subdirectories with tif files
        output_tifs = list(Path(ts_raster_dir).rglob("*.tif"))
        assert len(output_tifs) > 0, "No raster files created from time-series"


# =============================================================================
# Full Pipeline Test
# =============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestFullCLIPipeline:
    """Test complete CLI pipeline from download to rasterization."""

    def test_full_pipeline(self):
        """
        Run complete pipeline:
        1. Download GEDI data
        2. Build H3 database
        3. Extract data
        4. Aggregate data
        5. Rasterize to GeoTIFF
        """
        config = CLITestConfig()

        try:
            # Step 1: Download
            print("\n" + "="*70)
            print("STEP 1: Downloading GEDI data")
            print("="*70)

            download_cmd = [
                "gh3_download",
                TEST_REGION_ARG,
                "-d0", TEST_DATE_START,
                "-d1", TEST_DATE_END,
                "-l2a", "default",
                "-l4a", "agbd",
                "-o", config.soc_dir,
                "-N", str(TEST_CORES),
                "-v"
            ]

            result = run_cli(download_cmd, timeout=1800)
            assert result.returncode == 0, "Download step failed"

            # Check for downloaded files
            h5_files = list(Path(config.soc_dir).rglob("*.h5"))
            if not h5_files:
                pytest.skip("No data available for test region/dates")

            # Step 2: Build
            print("\n" + "="*70)
            print("STEP 2: Building H3 database")
            print("="*70)

            build_cmd = [
                "gh3_build",
                TEST_REGION_ARG,
                "-l2a", "default",
                "-l4a", "agbd",
                "-h3r", str(TEST_H3_RESOLUTION),
                "-h3p", str(TEST_H3_PARTITION),
                "-i", config.soc_dir,
                "-o", config.h3_dir,
                "-N", str(TEST_CORES),
                "-v"
            ]

            result = run_cli(build_cmd, timeout=1800)
            assert result.returncode == 0, "Build step failed"

            # Verify database
            log_file = os.path.join(config.h3_dir, "gedih3_build_log.json")
            assert os.path.exists(log_file), "Database not created"

            # Step 3: Extract
            print("\n" + "="*70)
            print("STEP 3: Extracting data")
            print("="*70)

            extract_cmd = [
                "gh3_extract",
                "-d", config.h3_dir,
                "-l4a", "agbd",
                "-y",  # Quality filter
                "-o", config.extract_dir,
                "-N", str(TEST_CORES),
                "-v"
            ]

            result = run_cli(extract_cmd, timeout=600)
            assert result.returncode == 0, "Extract step failed"

            # Step 4: Aggregate
            print("\n" + "="*70)
            print("STEP 4: Aggregating data")
            print("="*70)

            aggregate_cmd = [
                "gh3_aggregate",
                "-d", config.h3_dir,
                "-egi", "6",
                "-l4a", "agbd",
                "-a", "mean",
                "-o", config.aggregate_dir,
                "-N", str(TEST_CORES),
                "-v"
            ]

            result = run_cli(aggregate_cmd, timeout=600)
            assert result.returncode == 0, "Aggregate step failed"

            # Step 5: Rasterize (uses pre-aggregated dataset from Step 4)
            print("\n" + "="*70)
            print("STEP 5: Rasterizing data")
            print("="*70)

            raster_output = os.path.join(config.raster_dir, "agbd.tif")
            rasterize_cmd = [
                "gh3_rasterize",
                "-d", config.aggregate_dir,
                "-m",  # Merged output
                "-o", raster_output,
                "-N", str(TEST_CORES),
                "-v"
            ]

            result = run_cli(rasterize_cmd, timeout=600)
            assert result.returncode == 0, "Rasterize step failed"

            # Verify final output
            assert os.path.exists(raster_output), "Final raster not created"

            print("\n" + "="*70)
            print("FULL PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*70)

        finally:
            # Cleanup (comment out to inspect results)
            # config.cleanup()
            pass


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestCLIErrorHandling:
    """Test CLI error handling."""

    def test_download_missing_product(self):
        """Test download with no product specified."""
        result = run_cli([
            "gh3_download",
            TEST_REGION_ARG,
            "-o", "/tmp/test"
        ])
        # Should fail because no product selected
        assert result.returncode != 0

    def test_build_missing_source(self):
        """Test build with non-existent source directory."""
        result = run_cli([
            "gh3_build",
            "-l2a", "default",
            "-i", "/nonexistent/path",
            "-o", "/tmp/test"
        ])
        assert result.returncode != 0

    def test_extract_missing_database(self):
        """Test extract with non-existent database."""
        result = run_cli([
            "gh3_extract",
            "-d", "/nonexistent/database",
            "-l2a", "rh",
            "-o", "/tmp/test"
        ])
        assert result.returncode != 0

    def test_aggregate_missing_level(self):
        """Test aggregate without specifying target level."""
        result = run_cli([
            "gh3_aggregate",
            "-d", "/nonexistent/database",
            "-l4a", "agbd",
            "-o", "/tmp/test"
        ])
        # Should fail because neither -h3 nor -egi specified
        assert result.returncode != 0

    def test_rasterize_no_metadata(self):
        """Test gh3_rasterize with directory lacking dataset metadata."""
        with tempfile.TemporaryDirectory() as empty_dir:
            result = run_cli([
                "gh3_rasterize",
                "-d", empty_dir,
                "-o", os.path.join(empty_dir, "output"),
                "-Q"
            ])
            # Should fail because no gedih3_dataset.json and no valid subdirectories
            assert result.returncode != 0


@pytest.mark.integration
class TestCLIExtractEGIShuffle:
    """Test --egi-shuffle flag in gh3_extract."""

    def test_extract_egi_shuffle(self, test_config):
        """Test EGI extraction with shuffle-based loading."""
        log_file = os.path.join(test_config.h3_dir, "gedih3_build_log.json")
        if not os.path.exists(log_file):
            pytest.skip("H3 database not built")

        shuffle_dir = os.path.join(test_config.base_dir, "extract_egi_shuffle")
        os.makedirs(shuffle_dir, exist_ok=True)

        cmd = [
            "gh3_extract",
            "-d", test_config.h3_dir,
            "-l4a", "agbd",
            "-egi", "1:12",
            "--egi-shuffle",
            "-o", shuffle_dir,
            "-N", str(TEST_CORES),
            "-Q"
        ]

        result = run_cli(cmd, timeout=600)
        assert result.returncode == 0, f"EGI shuffle extract failed: {result.stderr}"

        # Check output files exist
        output_files = list(Path(shuffle_dir).rglob("*.parquet"))
        assert len(output_files) > 0, "No output files from EGI shuffle extract"


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == '__main__':
    # Run with: python -m pytest tests/test_cli_pipeline.py -v
    # Or: python tests/test_cli_pipeline.py
    pytest.main([__file__, '-v', '-s', '--tb=short'])
