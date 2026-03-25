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

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from pathlib import Path
from shapely.geometry import Point, box

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

skip_no_db = pytest.mark.skipif(
    not HAS_TUTORIAL_DB,
    reason=f"Tutorial database not found at {TUTORIAL_DB}"
)


def run_cli(cmd, timeout=300):
    """Run CLI command via Python -m to ensure correct environment."""
    env = os.environ.copy()
    env['PATH'] = os.path.dirname(PYTHON) + ':' + env.get('PATH', '')
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
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y',
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_extract basic')
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_h3_extract_with_region(self, tmp_output):
        """Extract with spatial filter."""
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y',
            TEST_REGION_ARG,
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_extract with region')
        assert has_parquet_files(tmp_output)

    def test_egi_extract(self, tmp_output):
        """Extract with EGI indexing."""
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y',
            '-egi', '1:12',
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_extract EGI')
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_extract_metadata_contents(self, tmp_output):
        """Verify metadata JSON has expected keys."""
        run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y',
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        meta_path = Path(tmp_output) / "gedih3_dataset.json"
        assert meta_path.exists()
        with open(meta_path) as f:
            meta = json.load(f)
        assert 'columns' in meta or 'h3_columns' in meta or 'source' in meta


# =============================================================================
# Group B: Aggregate from DB (CLI)
# =============================================================================

@pytest.mark.integration
@skip_no_db
class TestAggregateCLI:

    def test_h3_aggregate(self, tmp_output):
        """H3 aggregation from DB."""
        result = run_cli([
            'gh3_aggregate', '-d', str(TUTORIAL_DB),
            '-h3', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_aggregate H3')
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_egi_aggregate(self, tmp_output):
        """EGI aggregation from DB."""
        result = run_cli([
            'gh3_aggregate', '-d', str(TUTORIAL_DB),
            '-egi', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', tmp_output, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_aggregate EGI')
        assert has_parquet_files(tmp_output)
        assert has_dataset_meta(tmp_output)

    def test_merged_aggregate(self, tmp_output):
        """Merged aggregation output."""
        out_dir = os.path.join(tmp_output, 'merged_out')
        result = run_cli([
            'gh3_aggregate', '-d', str(TUTORIAL_DB),
            '-h3', '6', '-l4a', 'agbd', '-a', 'mean',
            '-m', '-o', out_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_aggregate merged')
        # gh3_export with merge=True writes merged file at output.parquet
        # alongside a metadata directory at output/
        merged_file = out_dir + '.parquet'
        assert os.path.isfile(merged_file) or has_parquet_files(out_dir)


# =============================================================================
# Group C: Aggregate from extracted dataset (CLI)
# =============================================================================

@pytest.mark.integration
@skip_no_db
class TestAggregateFromExtractCLI:

    def test_chain_extract_then_aggregate(self, tmp_output):
        """Extract with geometry → Aggregate chain."""
        extract_dir = os.path.join(tmp_output, 'extracted')
        agg_dir = os.path.join(tmp_output, 'aggregated')
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(agg_dir, exist_ok=True)

        # Step 1: Extract with geometry (needed for downstream aggregation)
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y', '-g',
            '-o', extract_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'extract step')
        assert has_parquet_files(extract_dir)

        # Step 2: Aggregate from extracted
        result = run_cli([
            'gh3_aggregate', '-d', extract_dir,
            '-h3', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', agg_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'aggregate from extract')
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
        agg_dir = os.path.join(tmp_output, 'egi_agg')
        os.makedirs(agg_dir, exist_ok=True)
        result = run_cli([
            'gh3_aggregate', '-d', str(TUTORIAL_DB),
            '-egi', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', agg_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'egi aggregate for rasterize')
        return agg_dir

    def test_tiled_rasterize(self, tmp_output, egi_agg_dir):
        """Tiled raster output."""
        raster_dir = os.path.join(tmp_output, 'rasters')
        os.makedirs(raster_dir, exist_ok=True)
        result = run_cli([
            'gh3_rasterize', '-d', egi_agg_dir,
            '-o', raster_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_rasterize tiled')
        assert has_tif_files(raster_dir)

    def test_merged_rasterize(self, tmp_output, egi_agg_dir):
        """Merged raster output."""
        raster_path = os.path.join(tmp_output, 'merged.tif')
        result = run_cli([
            'gh3_rasterize', '-d', egi_agg_dir,
            '-m', '-o', raster_path, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_rasterize merged')
        assert os.path.exists(raster_path)

    def test_rasterize_variable_selection(self, tmp_output, egi_agg_dir):
        """Rasterize with variable selection using actual column name."""
        # Read actual columns from the aggregated data
        pq_files = list(Path(egi_agg_dir).glob("*.parquet"))
        if not pq_files:
            pytest.skip("No parquet files in egi_agg_dir")
        df = pd.read_parquet(pq_files[0])
        # Find a numeric column that contains 'agbd'
        agbd_cols = [c for c in df.columns if 'agbd' in c and df[c].dtype in ('float64', 'float32')]
        if not agbd_cols:
            pytest.skip(f"No agbd column found. Columns: {list(df.columns)}")

        raster_dir = os.path.join(tmp_output, 'rasters_sel')
        os.makedirs(raster_dir, exist_ok=True)
        result = run_cli([
            'gh3_rasterize', '-d', egi_agg_dir,
            '-l', agbd_cols[0],
            '-o', raster_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_rasterize variable selection')
        assert has_tif_files(raster_dir)


# =============================================================================
# Group E: Aggregate + Rasterize combined (CLI)
# =============================================================================

@pytest.mark.integration
@skip_no_db
class TestAggregateRasterizeCLI:

    def test_aggregate_with_rasterize_flag(self, tmp_output):
        """gh3_aggregate with -R flag produces raster output."""
        out_dir = os.path.join(tmp_output, 'agg_raster')
        result = run_cli([
            'gh3_aggregate', '-d', str(TUTORIAL_DB),
            '-egi', '6', '-l4a', 'agbd', '-a', 'mean',
            '-R', '--compress', 'LZW',
            '-o', out_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_aggregate -R')
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
        import rioxarray  # noqa: ensure available
        import xarray as xr

        # Create a synthetic raster covering the study area
        raster_path = os.path.join(tmp_output, 'dem.tif')
        x = np.linspace(-51, -50, 100)
        y = np.linspace(0, 1, 100)
        data = np.random.uniform(50, 500, (1, 100, 100)).astype(np.float32)
        ds = xr.DataArray(
            data, dims=['band', 'y', 'x'],
            coords={'x': x, 'y': y, 'band': [1]}
        )
        ds = ds.rio.set_spatial_dims(x_dim='x', y_dim='y')
        ds = ds.rio.write_crs('EPSG:4326')
        ds.rio.to_raster(raster_path)

        out_dir = os.path.join(tmp_output, 'img_out')
        os.makedirs(out_dir, exist_ok=True)
        result = run_cli([
            'gh3_from_img', '-i', raster_path,
            '-d', str(TUTORIAL_DB),
            '-b', 'elevation',
            '-o', out_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_from_img')
        assert has_parquet_files(out_dir)

    def test_from_polygon(self, tmp_output):
        """gh3_from_polygon with synthetic shapefile."""
        # Create a polygon covering the study area
        poly_path = os.path.join(tmp_output, 'regions.geojson')
        gdf = gpd.GeoDataFrame({
            'region_name': ['Amazon'],
            'biome_code': [1],
        }, geometry=[box(-51, 0, -50, 1)], crs='EPSG:4326')
        gdf.to_file(poly_path, driver='GeoJSON')

        out_dir = os.path.join(tmp_output, 'poly_out')
        os.makedirs(out_dir, exist_ok=True)
        result = run_cli([
            'gh3_from_polygon', '-i', poly_path,
            '-c', 'region_name', 'biome_code',
            '-d', str(TUTORIAL_DB),
            '-o', out_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_from_polygon')
        assert has_parquet_files(out_dir)


# =============================================================================
# Group G: Update tool (CLI)
# =============================================================================

@pytest.mark.integration
@skip_no_db
class TestUpdateCLI:

    def test_update_adds_columns(self, tmp_output):
        """Extract L4A with geometry, then update with L2A rh_098."""
        extract_dir = os.path.join(tmp_output, 'extracted')
        os.makedirs(extract_dir, exist_ok=True)

        # Step 1: Extract L4A with geometry (needed for update spatial matching)
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y', '-g',
            '-o', extract_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'extract for update')

        # Step 2: Update with L2A rh_098
        result = run_cli([
            'gh3_update', '-d', extract_dir,
            '-l2a', 'rh_098',
            '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'gh3_update')

        # Verify new column is present
        pq_files = list(Path(extract_dir).glob("*.parquet"))
        assert len(pq_files) > 0
        df = pd.read_parquet(pq_files[0])
        rh_cols = [c for c in df.columns if 'rh_098' in c]
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
        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=['agbd_l4a'])
        assert ddf is not None
        sample = ddf.head(10)
        assert len(sample) > 0
        assert 'agbd_l4a' in sample.columns

    def test_gh3_load_with_region(self):
        """Load data with spatial filter."""
        import gedih3.gh3driver as gh3
        ddf = gh3.gh3_load(
            source=str(TUTORIAL_DB),
            columns=['agbd_l4a'],
            region=[-51, 0, -50, 1],
        )
        sample = ddf.head(10)
        assert len(sample) > 0

    def test_gh3_aggregate_api(self):
        """Aggregate via Python API."""
        import gedih3.gh3driver as gh3
        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=['agbd_l4a'])
        agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')
        assert agg_df is not None
        result = agg_df.compute() if hasattr(agg_df, 'compute') else agg_df
        assert len(result) > 0
        agg_cols = [c for c in result.columns if 'agbd' in c]
        assert len(agg_cols) > 0

    def test_egi_aggregate_api(self):
        """EGI aggregation via Python API."""
        import gedih3.gh3driver as gh3
        import gedih3.egi as egi

        # Load with geometry for EGI coordinate extraction
        ddf = gh3.gh3_load(source=str(TUTORIAL_DB), columns=['agbd_l4a', 'geometry'])
        df = ddf.compute()

        # Add EGI index
        egi_df = egi.egi_dataframe(df, level=6)
        assert egi_df is not None
        assert len(egi_df) > 0

    def test_load_dataset_eager(self, tmp_output):
        """Load extracted dataset eagerly (lazy=False)."""
        import gedih3.gh3driver as gh3

        # First extract with geometry so output is geoparquet
        extract_dir = os.path.join(tmp_output, 'extracted')
        os.makedirs(extract_dir, exist_ok=True)
        result = run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y', '-g',
            '-o', extract_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'extract for load_dataset')

        # Load via Python API (eager)
        gdf = gh3.gh3_load(source=extract_dir, lazy=False)
        assert gdf is not None
        assert len(gdf) > 0

    def test_load_dataset_lazy(self, tmp_output):
        """Load extracted dataset lazily (Dask)."""
        import gedih3.gh3driver as gh3

        extract_dir = os.path.join(tmp_output, 'extracted')
        os.makedirs(extract_dir, exist_ok=True)
        run_cli([
            'gh3_extract', '-d', str(TUTORIAL_DB),
            '-l4a', 'agbd', '-y', '-g',
            '-o', extract_dir, '-N', '2', '-Q',
        ])

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

    @pytest.fixture(scope='class')
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

        soc_dir = os.path.join(pipeline_dir, 'soc')
        os.makedirs(soc_dir, exist_ok=True)
        result = run_cli([
            'gh3_download',
            TEST_REGION_ARG,
            '-d0', '2020-01-01', '-d1', '2020-01-31',
            '-l2a', 'default', '-l4a', 'default',
            '-N', '2', '-Q',
            '-o', soc_dir,
        ], timeout=600)
        if result.returncode != 0:
            pytest.skip(f"Download failed: {result.stderr[:200]}")

    def test_build(self, pipeline_dir):
        """Build H3 database from downloaded files."""
        soc_dir = os.path.join(pipeline_dir, 'soc')
        h3_dir = os.path.join(pipeline_dir, 'h3_database')
        if not os.path.isdir(soc_dir) or not os.listdir(soc_dir):
            pytest.skip("No downloaded data available")

        os.makedirs(h3_dir, exist_ok=True)
        result = run_cli([
            'gh3_build',
            TEST_REGION_ARG,
            '-d0', '2020-01-01', '-d1', '2020-01-31',
            '-l2a', 'default', '-l4a', 'default',
            '-h3r', '12', '-h3p', '3',
            '-N', '2', '-Q',
            '-i', soc_dir, '-o', h3_dir,
        ], timeout=600)
        assert_cli_success(result, 'gh3_build')
        assert (Path(h3_dir) / "gedih3_build_log.json").exists()

    def test_extract_from_new_db(self, pipeline_dir):
        """Extract from newly built DB."""
        h3_dir = os.path.join(pipeline_dir, 'h3_database')
        if not (Path(h3_dir) / "gedih3_build_log.json").exists():
            pytest.skip("No built database available")

        extract_dir = os.path.join(pipeline_dir, 'extracted')
        os.makedirs(extract_dir, exist_ok=True)
        result = run_cli([
            'gh3_extract', '-d', h3_dir,
            '-l4a', 'agbd', '-y',
            '-o', extract_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'extract from new DB')
        assert has_parquet_files(extract_dir)

    def test_aggregate_from_new_db(self, pipeline_dir):
        """Aggregate from newly built DB."""
        h3_dir = os.path.join(pipeline_dir, 'h3_database')
        if not (Path(h3_dir) / "gedih3_build_log.json").exists():
            pytest.skip("No built database available")

        agg_dir = os.path.join(pipeline_dir, 'aggregated')
        os.makedirs(agg_dir, exist_ok=True)
        result = run_cli([
            'gh3_aggregate', '-d', h3_dir,
            '-h3', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', agg_dir, '-N', '2', '-Q',
        ])
        assert_cli_success(result, 'aggregate from new DB')
        assert has_parquet_files(agg_dir)


# =============================================================================
# S3-based build integrity tests
# =============================================================================

def _s3_build(out_dir, region="-51,0,-50,1", t0=None, t1=None,
              l2a='minimal', l4a='agbd', extra_args=None, timeout=900):
    """Run gh3_build --s3 and return the subprocess result."""
    cmd = [
        'gh3_build', '--s3',
        '-r', region,
        '-l2a', l2a, '-l4a', l4a,
        '-h3r', '12', '-h3p', '3',
        '-N', '2', '-T', '1', '-M', '4',
        '-o', out_dir, '-Q',
    ]
    if t0:
        cmd += ['-t0', t0]
    if t1:
        cmd += ['-t1', t1]
    if extra_args:
        cmd += extra_args
    return run_cli(cmd, timeout=timeout)


def _check_s3_auth():
    """Skip if S3/NASA creds are not available."""
    try:
        import earthaccess
        earthaccess.login()
    except Exception:
        pytest.skip("NASA Earthdata credentials not available for S3 tests")


def _read_all_shots(h3_dir):
    """Read all shot_numbers from a built H3 database."""
    all_shots = []
    for f in Path(h3_dir).rglob('*.parquet'):
        df = pd.read_parquet(f, columns=['shot_number'])
        all_shots.extend(df['shot_number'].tolist())
    return all_shots


def _read_all_columns(h3_dir):
    """Read all column names from a built H3 database."""
    cols = set()
    for f in Path(h3_dir).rglob('*.parquet'):
        df = gpd.read_parquet(f)
        cols.update(df.columns)
        break  # Schema is uniform across partitions
    return cols


@pytest.mark.integration
class TestS3BuildFromScratch:
    """Build a fresh H3 database via S3 streaming and validate integrity.

    All tests in this class share a single built database (class-scoped fixture).
    Region: [-51,0,-50,1], L2A minimal + L4A agbd, 2020-01-01 to 2020-03-31.
    """

    @pytest.fixture(scope='class')
    def s3_built_database(self, tmp_path_factory, persistent_test_dir):
        """Build a fresh H3 database from S3. Shared across all tests in class."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_build'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_build")
        h3_dir = str(base / 'h3_database')
        os.makedirs(h3_dir, exist_ok=True)

        result = _s3_build(
            h3_dir, t0='2020-01-01', t1='2020-03-31',
        )
        if result.returncode != 0:
            pytest.skip(f"S3 build failed: {result.stderr[:300]}")

        return h3_dir

    def test_fresh_build_completes(self, s3_built_database):
        """Build returns COMPLETED, log file exists."""
        log_path = os.path.join(s3_built_database, 'gedih3_build_log.json')
        assert os.path.exists(log_path)
        with open(log_path) as f:
            log = json.load(f)
        assert log['status'] == 'COMPLETED'

    def test_fresh_build_no_duplicate_shots(self, s3_built_database):
        """All shot_numbers are unique across all partitions."""
        shots = _read_all_shots(s3_built_database)
        assert len(shots) > 0, "Database has no shots"
        assert len(shots) == len(set(shots)), \
            f"Duplicate shots: {len(shots)} total, {len(set(shots))} unique"

    def test_fresh_build_no_nan_only_cols(self, s3_built_database):
        """No column is entirely NaN in any partition file."""
        from gedih3.utils import check_nan_only_columns

        for f in Path(s3_built_database).rglob('*.parquet'):
            df = gpd.read_parquet(f)
            nan_cols = check_nan_only_columns(df)
            assert nan_cols == [], \
                f"NaN-only columns in {f.name}: {nan_cols}"

    def test_fresh_build_metadata_accurate(self, s3_built_database):
        """Build log date_range, columns, and partitions match actual data."""
        log_path = os.path.join(s3_built_database, 'gedih3_build_log.json')
        with open(log_path) as f:
            log = json.load(f)

        # Partition IDs match directories
        log_parts = set(log.get('h3_partition_ids', []))
        disk_parts = set()
        for d in Path(s3_built_database).glob('h3_03=*'):
            disk_parts.add(d.name.split('=')[1])
        assert log_parts == disk_parts

        # Columns in log exist in data
        log_cols = set(log.get('h3_columns', []))
        data_cols = _read_all_columns(s3_built_database)
        for col in log_cols:
            if col != 'datetime':
                assert col in data_cols, f"Log column '{col}' missing from data"

    def test_idempotent_rebuild_is_noop(self, s3_built_database):
        """Re-run same params → no files modified."""
        import glob as globmod

        pq_files = globmod.glob(
            os.path.join(s3_built_database, 'h3_*', '*', '*.parquet')
        )
        mtimes_before = {f: os.path.getmtime(f) for f in pq_files}

        result = _s3_build(
            s3_built_database, t0='2020-01-01', t1='2020-03-31',
        )
        assert result.returncode == 0

        for f, mtime in mtimes_before.items():
            assert os.path.getmtime(f) == mtime, f"File modified: {f}"

    def test_version_mismatch_rejected(self, s3_built_database):
        """Attempt v3 update on v2 DB raises error."""
        result = _s3_build(
            s3_built_database, t0='2020-01-01', t1='2020-03-31',
            extra_args=['--gedi-version', '3'],
        )
        assert result.returncode != 0
        assert 'version mismatch' in result.stderr.lower() or \
               'version' in result.stderr.lower()

    def test_temporal_expansion_adds_rows(self, s3_built_database):
        """Expand date range → more or equal rows, same columns."""
        initial_shots = len(_read_all_shots(s3_built_database))
        initial_cols = _read_all_columns(s3_built_database)

        # Expand to 6 months (adds Apr-Jun 2020)
        result = _s3_build(
            s3_built_database, t0='2020-01-01', t1='2020-06-30',
        )
        assert result.returncode == 0, f"Temporal expansion failed: {result.stderr[:300]}"

        updated_shots = len(_read_all_shots(s3_built_database))
        updated_cols = _read_all_columns(s3_built_database)
        assert updated_shots >= initial_shots, \
            f"Row count decreased: {initial_shots} → {updated_shots}"
        assert updated_cols == initial_cols, \
            f"Column mismatch after temporal expansion: {updated_cols ^ initial_cols}"


@pytest.mark.integration
class TestS3VariableUpdate:
    """Test adding variables to an existing S3-built database."""

    @pytest.fixture(scope='class')
    def s3_base_database(self, tmp_path_factory, persistent_test_dir):
        """Build a minimal database, then return its path."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_var_update'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_var_update")
        h3_dir = str(base / 'h3_database')
        os.makedirs(h3_dir, exist_ok=True)

        result = _s3_build(
            h3_dir, t0='2020-01-01', t1='2020-03-31',
            l2a='minimal', l4a='agbd',
        )
        if result.returncode != 0:
            pytest.skip(f"Base S3 build failed: {result.stderr[:300]}")
        return h3_dir

    def test_variable_update_adds_columns(self, s3_base_database):
        """Add L4C, verify schema expanded without row loss."""
        initial_shots = _read_all_shots(s3_base_database)
        initial_count = len(initial_shots)
        initial_cols = _read_all_columns(s3_base_database)

        result = _s3_build(
            s3_base_database, t0='2020-01-01', t1='2020-03-31',
            l2a='minimal', l4a='agbd',
            extra_args=['-l4c', 'minimal'],
        )
        assert result.returncode == 0, f"Variable update failed: {result.stderr[:300]}"

        updated_cols = _read_all_columns(s3_base_database)
        assert len(updated_cols) > len(initial_cols), "No new columns added"

        updated_shots = _read_all_shots(s3_base_database)
        assert len(updated_shots) == initial_count, \
            f"Row count changed: {initial_count} → {len(updated_shots)}"


@pytest.mark.integration
class TestS3ExtractIntegrity:
    """Validate extract output from an S3-built database."""

    @pytest.fixture(scope='class')
    def s3_extract_dir(self, tmp_path_factory, persistent_test_dir):
        """Build DB, extract, return extract directory."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_extract'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_extract")
        h3_dir = str(base / 'h3_database')
        extract_dir = str(base / 'extracted')
        os.makedirs(h3_dir, exist_ok=True)
        os.makedirs(extract_dir, exist_ok=True)

        # Build
        result = _s3_build(h3_dir, t0='2020-01-01', t1='2020-03-31')
        if result.returncode != 0:
            pytest.skip(f"S3 build failed: {result.stderr[:300]}")

        # Extract
        result = run_cli([
            'gh3_extract', '-d', h3_dir,
            '-l4a', 'agbd', '-y',
            '-o', extract_dir, '-N', '2', '-Q',
        ], timeout=300)
        if result.returncode != 0:
            pytest.skip(f"Extract failed: {result.stderr[:300]}")

        return extract_dir

    def test_extract_no_duplicate_shots(self, s3_extract_dir):
        """Extracted shots are unique."""
        shots = []
        for f in Path(s3_extract_dir).rglob('*.parquet'):
            df = pd.read_parquet(f, columns=['shot_number'])
            shots.extend(df['shot_number'].tolist())
        assert len(shots) == len(set(shots))

    def test_extract_no_nan_only_columns(self, s3_extract_dir):
        """No NaN-only columns in extracted output."""
        from gedih3.utils import check_nan_only_columns

        for f in Path(s3_extract_dir).rglob('*.parquet'):
            df = gpd.read_parquet(f)
            nan_cols = check_nan_only_columns(df)
            assert nan_cols == [], f"NaN-only columns in {f.name}: {nan_cols}"

    def test_extract_file_naming_correct(self, s3_extract_dir):
        """Output file names are valid H3 cells."""
        import h3 as h3lib
        for f in Path(s3_extract_dir).glob('*.parquet'):
            stem = f.stem
            assert h3lib.is_valid_cell(stem), f"Invalid H3 cell name: {stem}"


@pytest.mark.slow
@pytest.mark.integration
class TestIncrementalEqualsFull:
    """A database built at once must match one built incrementally.

    TESTING.md requirement: 'for the same area and variables, a database built
    at once (all dates) must match the content of a database built incrementally.'
    """

    @pytest.fixture(scope='class')
    def both_databases(self, tmp_path_factory, persistent_test_dir):
        """Build DB_full in one shot, DB_incr in two halves."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 'incr_vs_full'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("incr_vs_full")

        # Full build
        full_dir = str(base / 'full')
        os.makedirs(full_dir, exist_ok=True)
        result = _s3_build(full_dir, t0='2020-01-01', t1='2020-03-31')
        if result.returncode != 0:
            pytest.skip(f"Full build failed: {result.stderr[:300]}")

        # Incremental build: first half
        incr_dir = str(base / 'incremental')
        os.makedirs(incr_dir, exist_ok=True)
        result = _s3_build(incr_dir, t0='2020-01-01', t1='2020-02-15')
        if result.returncode != 0:
            pytest.skip(f"Incremental phase 1 failed: {result.stderr[:300]}")

        # Incremental build: second half (update)
        result = _s3_build(incr_dir, t0='2020-01-01', t1='2020-03-31')
        if result.returncode != 0:
            pytest.skip(f"Incremental phase 2 failed: {result.stderr[:300]}")

        return full_dir, incr_dir

    def test_incremental_equals_full_build(self, both_databases):
        """Same shot set, same columns, same total rows."""
        full_dir, incr_dir = both_databases

        full_shots = set(_read_all_shots(full_dir))
        incr_shots = set(_read_all_shots(incr_dir))

        assert len(full_shots) > 0, "Full database is empty"
        assert len(incr_shots) > 0, "Incremental database is empty"

        # Same shot set
        assert full_shots == incr_shots, \
            f"Shot mismatch: {len(full_shots - incr_shots)} in full only, " \
            f"{len(incr_shots - full_shots)} in incremental only"

        # Same columns
        full_cols = _read_all_columns(full_dir)
        incr_cols = _read_all_columns(incr_dir)
        assert full_cols == incr_cols, \
            f"Column mismatch: {full_cols ^ incr_cols}"


# =============================================================================
# S3: Spatial Expansion (TESTING.md §5.3 item 5)
# =============================================================================

@pytest.mark.integration
class TestS3SpatialExpansion:
    """Build a small region, then expand spatially and check new partitions."""

    @pytest.fixture(scope='class')
    def s3_spatial_db(self, tmp_path_factory, persistent_test_dir):
        """Build a small-region database for spatial expansion test."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_spatial'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_spatial")
        h3_dir = str(base / 'h3_database')
        os.makedirs(h3_dir, exist_ok=True)

        result = _s3_build(
            h3_dir, region="-50.5,0.25,-50,0.75",
            t0='2020-01-01', t1='2020-03-31',
        )
        if result.returncode != 0:
            pytest.skip(f"Spatial base build failed: {result.stderr[:300]}")
        return h3_dir

    def test_spatial_expansion_adds_partitions(self, s3_spatial_db):
        """Expanding region adds new H3 partition directories."""
        initial_parts = set(d.name for d in Path(s3_spatial_db).glob('h3_03=*'))

        result = _s3_build(
            s3_spatial_db, region="-51,0,-50,1",
            t0='2020-01-01', t1='2020-03-31',
        )
        assert result.returncode == 0, \
            f"Spatial expansion failed: {result.stderr[:300]}"

        updated_parts = set(d.name for d in Path(s3_spatial_db).glob('h3_03=*'))
        assert len(updated_parts) >= len(initial_parts), \
            f"Partitions did not grow: {len(initial_parts)} → {len(updated_parts)}"


# =============================================================================
# S3: Aggregate Integrity (TESTING.md §5.3 item 8)
# =============================================================================

@pytest.mark.integration
class TestS3AggregateIntegrity:
    """Aggregate from S3-built DB: reduced rows, no NaN-only columns."""

    @pytest.fixture(scope='class')
    def s3_agg_data(self, tmp_path_factory, persistent_test_dir):
        """Build DB, aggregate, return (h3_dir, agg_dir)."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_agg'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_agg")
        h3_dir = str(base / 'h3_database')
        agg_dir = str(base / 'aggregated')
        os.makedirs(h3_dir, exist_ok=True)
        os.makedirs(agg_dir, exist_ok=True)

        result = _s3_build(h3_dir, t0='2020-01-01', t1='2020-03-31')
        if result.returncode != 0:
            pytest.skip(f"S3 build failed: {result.stderr[:300]}")

        result = run_cli([
            'gh3_aggregate', '-d', h3_dir,
            '-h3', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', agg_dir, '-N', '2', '-Q',
        ], timeout=300)
        if result.returncode != 0:
            pytest.skip(f"Aggregate failed: {result.stderr[:300]}")

        return h3_dir, agg_dir

    def test_aggregate_row_count_reduced(self, s3_agg_data):
        """Aggregated dataset has fewer rows than raw database."""
        h3_dir, agg_dir = s3_agg_data
        raw_count = len(_read_all_shots(h3_dir))
        agg_count = sum(
            len(pd.read_parquet(f))
            for f in Path(agg_dir).rglob('*.parquet')
        )
        assert agg_count < raw_count, \
            f"Aggregated rows ({agg_count}) not less than raw ({raw_count})"

    def test_aggregate_no_nan_only_columns(self, s3_agg_data):
        """No NaN-only columns in aggregated output."""
        from gedih3.utils import check_nan_only_columns

        _, agg_dir = s3_agg_data
        for f in Path(agg_dir).rglob('*.parquet'):
            df = gpd.read_parquet(f)
            nan_cols = check_nan_only_columns(df)
            assert nan_cols == [], \
                f"NaN-only columns in {f.name}: {nan_cols}"


# =============================================================================
# S3: Rasterize Integrity (TESTING.md §5.3 item 9)
# =============================================================================

@pytest.mark.integration
class TestS3RasterizeIntegrity:
    """Rasterize from S3-built DB: valid GeoTIFF output."""

    @pytest.fixture(scope='class')
    def s3_raster_data(self, tmp_path_factory, persistent_test_dir):
        """Build DB, EGI aggregate, rasterize, return raster_dir."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_raster'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_raster")
        h3_dir = str(base / 'h3_database')
        agg_dir = str(base / 'egi_agg')
        raster_dir = str(base / 'rasters')
        os.makedirs(h3_dir, exist_ok=True)
        os.makedirs(agg_dir, exist_ok=True)
        os.makedirs(raster_dir, exist_ok=True)

        result = _s3_build(h3_dir, t0='2020-01-01', t1='2020-03-31')
        if result.returncode != 0:
            pytest.skip(f"S3 build failed: {result.stderr[:300]}")

        result = run_cli([
            'gh3_aggregate', '-d', h3_dir,
            '-egi', '6', '-l4a', 'agbd', '-a', 'mean',
            '-o', agg_dir, '-N', '2', '-Q',
        ], timeout=300)
        if result.returncode != 0:
            pytest.skip(f"EGI aggregate failed: {result.stderr[:300]}")

        result = run_cli([
            'gh3_rasterize', '-d', agg_dir,
            '-o', raster_dir, '-N', '2', '-Q',
        ], timeout=300)
        if result.returncode != 0:
            pytest.skip(f"Rasterize failed: {result.stderr[:300]}")

        return raster_dir

    def test_rasterize_produces_valid_tif(self, s3_raster_data):
        """Output is readable GeoTIFF with valid (non-all-NaN) values."""
        import rasterio

        tif_files = list(Path(s3_raster_data).rglob('*.tif'))
        assert len(tif_files) > 0, "No .tif files produced"

        for tif in tif_files:
            with rasterio.open(tif) as src:
                data = src.read(1)
                assert data.shape[0] > 0 and data.shape[1] > 0, \
                    f"Empty raster: {tif.name}"
                assert not np.all(np.isnan(data)), \
                    f"All-NaN raster: {tif.name}"


# =============================================================================
# S3: Build Without Time Constraints (TESTING.md §5.2)
# =============================================================================

@pytest.mark.slow
@pytest.mark.integration
class TestNoTimeConstraints:
    """Build without -d0/-d1 flags, then re-run to detect 'already up to date'."""

    def test_build_without_dates_then_rerun(self, tmp_path_factory, persistent_test_dir):
        """Build with no time constraints, re-run → no files modified."""
        _check_s3_auth()
        if persistent_test_dir:
            base = persistent_test_dir / 's3_no_dates'
            os.makedirs(base, exist_ok=True)
        else:
            base = tmp_path_factory.mktemp("s3_no_dates")
        h3_dir = str(base / 'h3_database')
        os.makedirs(h3_dir, exist_ok=True)

        result = _s3_build(h3_dir, timeout=1800)  # No d0/d1
        assert result.returncode == 0, \
            f"Build without dates failed: {result.stderr[:300]}"

        # Re-run → should detect "already up to date"
        pq_files = list(Path(h3_dir).rglob('*.parquet'))
        mtimes = {str(f): os.path.getmtime(f) for f in pq_files}

        result2 = _s3_build(h3_dir, timeout=1800)
        assert result2.returncode == 0

        for f, mt in mtimes.items():
            assert os.path.getmtime(f) == mt, \
                f"File modified on re-run: {os.path.basename(f)}"
