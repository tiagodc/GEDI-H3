"""
Integration tests for the gedih3 CLI and Python API pipeline.

Uses the existing tutorial database at tmp/gedih3_tutorial/h3_database/.
All tests are marked @pytest.mark.integration and skip if DB not found.

Groups:
  A: Extract (CLI)
  B: Aggregate from DB (CLI)
  C: Aggregate from extracted dataset (CLI)
  D: Rasterize (CLI)
  E: Aggregate + Rasterize combined (CLI)
  F: Ancillary data tools (CLI)
  G: Update tool (CLI)
  H: Python API parity
  I: End-to-end download + build (slow, requires NASA creds)
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

# =============================================================================
# Configuration
# =============================================================================

REPO_ROOT = Path(__file__).parent.parent
TUTORIAL_DB = REPO_ROOT / "tmp" / "gedih3_tutorial" / "h3_database"
TEST_REGION = "-51,0,-50,1"
TEST_REGION_ARG = f"--region={TEST_REGION}"

# Python executable from the conda env
PYTHON = sys.executable

# Check if tutorial database exists
HAS_TUTORIAL_DB = (TUTORIAL_DB / "gedih3_build_log.json").exists()

skip_no_db = pytest.mark.skipif(not HAS_TUTORIAL_DB, reason=f"Tutorial database not found at {TUTORIAL_DB}")


def run_cli(cmd, timeout=300):
    """Run CLI command via Python -m to ensure correct environment."""
    env = os.environ.copy()
    env["PATH"] = os.path.dirname(PYTHON) + ":" + env.get("PATH", "")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return result


def assert_cli_success(result, cmd_name=""):
    """Assert CLI command succeeded, print stderr on failure."""
    if result.returncode != 0:
        msg = f"CLI command failed: {cmd_name}\nSTDERR: {result.stderr}\nSTDOUT: {result.stdout}"
        pytest.fail(msg)


def has_parquet_files(directory):
    """Check if directory contains parquet files."""
    return len(list(Path(directory).rglob("*.parquet"))) > 0


def has_tif_files(directory):
    """Check if directory contains tif files."""
    return len(list(Path(directory).rglob("*.tif"))) > 0


def has_dataset_meta(directory):
    """Check if directory contains gedih3_dataset.json."""
    return (Path(directory) / "gedih3_dataset.json").exists()


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def tmp_output():
    """Temporary output directory with cleanup."""
    d = tempfile.mkdtemp(prefix="gedih3_inttest_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# =============================================================================
# Group A: Extract (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestExtractCLI:
    def test_h3_extract_basic(self, tmp_output):
        """Extract L4A agbd from DB."""
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_extract basic")
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_h3_extract_with_region(self, tmp_output):
        """Extract with spatial filter."""
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                TEST_REGION_ARG,
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_extract with region")
        assert has_parquet_files(tmp_output)

    def test_egi_extract(self, tmp_output):
        """Extract with EGI indexing."""
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-egi",
                "1:12",
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_extract EGI")
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_extract_metadata_contents(self, tmp_output):
        """Verify metadata JSON has expected keys."""
        run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        meta_path = Path(tmp_output) / "gedih3_dataset.json"
        assert meta_path.exists()
        with open(meta_path) as f:
            meta = json.load(f)
        assert "columns" in meta or "h3_columns" in meta or "source" in meta


# =============================================================================
# Group B: Aggregate from DB (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestAggregateCLI:
    def test_h3_aggregate(self, tmp_output):
        """H3 aggregation from DB."""
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                str(TUTORIAL_DB),
                "-h3",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_aggregate H3")
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_egi_aggregate(self, tmp_output):
        """EGI aggregation from DB."""
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                str(TUTORIAL_DB),
                "-egi",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-o",
                tmp_output,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_aggregate EGI")
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_merged_aggregate(self, tmp_output):
        """Merged aggregation output."""
        out_dir = os.path.join(tmp_output, "merged_out")
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                str(TUTORIAL_DB),
                "-h3",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-m",
                "-o",
                out_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_aggregate merged")
        # gh3_export with merge=True writes merged file at output.parquet
        # alongside a metadata directory at output/
        merged_file = out_dir + ".parquet"
        assert os.path.isfile(merged_file) or has_parquet_files(out_dir)


# =============================================================================
# Group C: Aggregate from extracted dataset (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestAggregateFromExtractCLI:
    def test_chain_extract_then_aggregate(self, tmp_output):
        """Extract with geometry → Aggregate chain."""
        extract_dir = os.path.join(tmp_output, "extracted")
        agg_dir = os.path.join(tmp_output, "aggregated")
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(agg_dir, exist_ok=True)

        # Step 1: Extract with geometry (needed for downstream aggregation)
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-g",
                "-o",
                extract_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "extract step")
        assert has_parquet_files(extract_dir)

        # Step 2: Aggregate from extracted
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                extract_dir,
                "-h3",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-o",
                agg_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "aggregate from extract")
        assert has_parquet_files(agg_dir)


# =============================================================================
# Group D: Rasterize (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestRasterizeCLI:
    @pytest.fixture
    def egi_agg_dir(self, tmp_output):
        """Create an EGI aggregated dataset for rasterization tests."""
        agg_dir = os.path.join(tmp_output, "egi_agg")
        os.makedirs(agg_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                str(TUTORIAL_DB),
                "-egi",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-o",
                agg_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "egi aggregate for rasterize")
        return agg_dir

    def test_tiled_rasterize(self, tmp_output, egi_agg_dir):
        """Tiled raster output."""
        raster_dir = os.path.join(tmp_output, "rasters")
        os.makedirs(raster_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_rasterize",
                "-d",
                egi_agg_dir,
                "-o",
                raster_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_rasterize tiled")
        assert has_tif_files(raster_dir)

    def test_merged_rasterize(self, tmp_output, egi_agg_dir):
        """Merged raster output."""
        raster_path = os.path.join(tmp_output, "merged.tif")
        result = run_cli(
            [
                "gh3_rasterize",
                "-d",
                egi_agg_dir,
                "-m",
                "-o",
                raster_path,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_rasterize merged")
        assert os.path.exists(raster_path)

    def test_rasterize_variable_selection(self, tmp_output, egi_agg_dir):
        """Rasterize with variable selection using actual column name."""
        # Read actual columns from the aggregated data
        pq_files = list(Path(egi_agg_dir).glob("*.parquet"))
        if not pq_files:
            pytest.skip("No parquet files in egi_agg_dir")
        df = pd.read_parquet(pq_files[0])
        # Find a numeric column that contains 'agbd'
        agbd_cols = [c for c in df.columns if "agbd" in c and df[c].dtype in ("float64", "float32")]
        if not agbd_cols:
            pytest.skip(f"No agbd column found. Columns: {list(df.columns)}")

        raster_dir = os.path.join(tmp_output, "rasters_sel")
        os.makedirs(raster_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_rasterize",
                "-d",
                egi_agg_dir,
                "-l",
                agbd_cols[0],
                "-o",
                raster_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_rasterize variable selection")
        assert has_tif_files(raster_dir)


# =============================================================================
# Group E: Aggregate + Rasterize combined (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestAggregateRasterizeCLI:
    def test_aggregate_with_rasterize_flag(self, tmp_output):
        """gh3_aggregate with -R flag produces raster output."""
        out_dir = os.path.join(tmp_output, "agg_raster")
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                str(TUTORIAL_DB),
                "-egi",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-R",
                "--compress",
                "LZW",
                "-o",
                out_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_aggregate -R")
        # -R flag produces raster-only output (no parquet files)
        assert has_tif_files(out_dir)


# =============================================================================
# Group F: Ancillary data tools (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestAncillaryToolsCLI:
    def test_from_img(self, tmp_output):
        """gh3_from_img with synthetic raster."""
        import xarray as xr

        # Create a synthetic raster covering the study area
        raster_path = os.path.join(tmp_output, "dem.tif")
        x = np.linspace(-51, -50, 100)
        y = np.linspace(0, 1, 100)
        data = np.random.uniform(50, 500, (1, 100, 100)).astype(np.float32)
        ds = xr.DataArray(data, dims=["band", "y", "x"], coords={"x": x, "y": y, "band": [1]})
        ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y")
        ds = ds.rio.write_crs("EPSG:4326")
        ds.rio.to_raster(raster_path)

        out_dir = os.path.join(tmp_output, "img_out")
        os.makedirs(out_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_from_img",
                "-i",
                raster_path,
                "-d",
                str(TUTORIAL_DB),
                "-b",
                "elevation",
                "-o",
                out_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_from_img")
        assert has_parquet_files(out_dir)

    def test_from_polygon(self, tmp_output):
        """gh3_from_polygon with synthetic shapefile."""
        # Create a polygon covering the study area
        poly_path = os.path.join(tmp_output, "regions.geojson")
        gdf = gpd.GeoDataFrame(
            {
                "region_name": ["Amazon"],
                "biome_code": [1],
            },
            geometry=[box(-51, 0, -50, 1)],
            crs="EPSG:4326",
        )
        gdf.to_file(poly_path, driver="GeoJSON")

        out_dir = os.path.join(tmp_output, "poly_out")
        os.makedirs(out_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_from_polygon",
                "-i",
                poly_path,
                "-c",
                "region_name",
                "biome_code",
                "-d",
                str(TUTORIAL_DB),
                "-o",
                out_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_from_polygon")
        assert has_parquet_files(out_dir)


# =============================================================================
# Group G: Update tool (CLI)
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestUpdateCLI:
    def test_update_adds_columns(self, tmp_output):
        """Extract L4A with geometry, then update with L2A rh_098."""
        extract_dir = os.path.join(tmp_output, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        # Step 1: Extract L4A with geometry (needed for update spatial matching)
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-g",
                "-o",
                extract_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "extract for update")

        # Step 2: Update with L2A rh_098
        result = run_cli(
            [
                "gh3_update",
                "-d",
                extract_dir,
                "-l2a",
                "rh_098",
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "gh3_update")

        # Verify new column is present
        pq_files = list(Path(extract_dir).glob("*.parquet"))
        assert len(pq_files) > 0
        df = pd.read_parquet(pq_files[0])
        rh_cols = [c for c in df.columns if "rh_098" in c]
        assert len(rh_cols) > 0, f"rh_098 column not found. Columns: {list(df.columns)}"


# =============================================================================
# Group H: Python API parity
# =============================================================================


@pytest.mark.integration
@skip_no_db
class TestPythonAPIParity:
    def test_gh3_load(self):
        """Load data via Python API."""
        import gedih3.gh3driver as gh3

        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=["agbd_l4a"])
        assert ddf is not None
        sample = ddf.head(10)
        assert len(sample) > 0
        assert "agbd_l4a" in sample.columns

    def test_gh3_load_with_region(self):
        """Load data with spatial filter."""
        import gedih3.gh3driver as gh3

        ddf = gh3.gh3_load(
            source=str(TUTORIAL_DB),
            columns=["agbd_l4a"],
            region=[-51, 0, -50, 1],
        )
        sample = ddf.head(10)
        assert len(sample) > 0

    def test_gh3_aggregate_api(self):
        """Aggregate via Python API."""
        import gedih3.gh3driver as gh3

        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=["agbd_l4a"])
        agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg="mean")
        assert agg_df is not None
        result = agg_df.compute() if hasattr(agg_df, "compute") else agg_df
        assert len(result) > 0
        agg_cols = [c for c in result.columns if "agbd" in c]
        assert len(agg_cols) > 0

    def test_egi_aggregate_api(self):
        """EGI aggregation via Python API."""
        import gedih3.egi as egi
        import gedih3.gh3driver as gh3

        # Load with geometry for EGI coordinate extraction
        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=["agbd_l4a", "geometry"])
        df = ddf.compute()

        # Add EGI index
        egi_df = egi.egi_dataframe(df, level=6)
        assert egi_df is not None
        assert len(egi_df) > 0

    def test_load_dataset_eager(self, tmp_output):
        """Load extracted dataset eagerly (lazy=False)."""
        import gedih3.gh3driver as gh3

        # First extract with geometry so output is geoparquet
        extract_dir = os.path.join(tmp_output, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-g",
                "-o",
                extract_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "extract for load_dataset")

        # Load via Python API (eager)
        gdf = gh3.gh3_load(source=extract_dir, lazy=False)
        assert gdf is not None
        assert len(gdf) > 0

    def test_load_dataset_lazy(self, tmp_output):
        """Load extracted dataset lazily (Dask)."""
        import gedih3.gh3driver as gh3

        extract_dir = os.path.join(tmp_output, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        run_cli(
            [
                "gh3_extract",
                "-d",
                str(TUTORIAL_DB),
                "-l4a",
                "agbd",
                "-y",
                "-g",
                "-o",
                extract_dir,
                "-N",
                "2",
                "-Q",
            ]
        )

        ddf = gh3.gh3_load(source=extract_dir, lazy=True)
        assert ddf is not None
        sample = ddf.head(5)
        assert len(sample) > 0


# =============================================================================
# Group I: End-to-end download + build (slow)
# =============================================================================


@pytest.mark.integration
@pytest.mark.slow
class TestEndToEndPipeline:
    """Full pipeline from download to rasterization.

    Marked slow — skip with: pytest -m "not slow"
    Requires NASA Earthdata credentials.
    """

    @pytest.fixture(scope="class")
    def pipeline_dir(self):
        d = tempfile.mkdtemp(prefix="gedih3_e2e_")
        yield d
        shutil.rmtree(d, ignore_errors=True)

    def test_download(self, pipeline_dir):
        """Download 1 month of data for small region."""
        try:
            import earthaccess

            earthaccess.login()
        except Exception:
            pytest.skip("NASA Earthdata credentials not available")

        soc_dir = os.path.join(pipeline_dir, "soc")
        os.makedirs(soc_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_download",
                TEST_REGION_ARG,
                "-d0",
                "2020-01-01",
                "-d1",
                "2020-01-31",
                "-l2a",
                "default",
                "-l4a",
                "default",
                "-N",
                "2",
                "-Q",
                "-o",
                soc_dir,
            ],
            timeout=600,
        )
        if result.returncode != 0:
            pytest.skip(f"Download failed: {result.stderr[:200]}")

    def test_build(self, pipeline_dir):
        """Build H3 database from downloaded files."""
        soc_dir = os.path.join(pipeline_dir, "soc")
        h3_dir = os.path.join(pipeline_dir, "h3_database")
        if not os.path.isdir(soc_dir) or not os.listdir(soc_dir):
            pytest.skip("No downloaded data available")

        os.makedirs(h3_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_build",
                TEST_REGION_ARG,
                "-d0",
                "2020-01-01",
                "-d1",
                "2020-01-31",
                "-l2a",
                "default",
                "-l4a",
                "default",
                "-h3r",
                "12",
                "-h3p",
                "3",
                "-N",
                "2",
                "-Q",
                "-i",
                soc_dir,
                "-o",
                h3_dir,
            ],
            timeout=600,
        )
        assert_cli_success(result, "gh3_build")
        assert (Path(h3_dir) / "gedih3_build_log.json").exists()

    def test_extract_from_new_db(self, pipeline_dir):
        """Extract from newly built DB."""
        h3_dir = os.path.join(pipeline_dir, "h3_database")
        if not (Path(h3_dir) / "gedih3_build_log.json").exists():
            pytest.skip("No built database available")

        extract_dir = os.path.join(pipeline_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_extract",
                "-d",
                h3_dir,
                "-l4a",
                "agbd",
                "-y",
                "-o",
                extract_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "extract from new DB")
        assert has_parquet_files(extract_dir)

    def test_aggregate_from_new_db(self, pipeline_dir):
        """Aggregate from newly built DB."""
        h3_dir = os.path.join(pipeline_dir, "h3_database")
        if not (Path(h3_dir) / "gedih3_build_log.json").exists():
            pytest.skip("No built database available")

        agg_dir = os.path.join(pipeline_dir, "aggregated")
        os.makedirs(agg_dir, exist_ok=True)
        result = run_cli(
            [
                "gh3_aggregate",
                "-d",
                h3_dir,
                "-h3",
                "6",
                "-l4a",
                "agbd",
                "-a",
                "mean",
                "-o",
                agg_dir,
                "-N",
                "2",
                "-Q",
            ]
        )
        assert_cli_success(result, "aggregate from new DB")
        assert has_parquet_files(agg_dir)
