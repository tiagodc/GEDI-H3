"""
Benchmark: Direct download+subset vs S3 subset for GEDI files.

Compares two approaches for obtaining subsetted GEDI HDF5 data:
  A) Download full file, then subset locally
  B) Open via S3, subset directly from remote handle

Requires NASA Earthdata credentials and network access.
"""

import os
import time
import tempfile
import shutil

import pytest
import h5py

pytestmark = pytest.mark.integration


@pytest.fixture
def earthaccess_session():
    """Authenticate with NASA Earthdata. Skip if credentials unavailable."""
    try:
        import earthaccess
        auth = earthaccess.login()
        if not auth.authenticated:
            pytest.skip("NASA Earthdata authentication failed")
        return earthaccess
    except Exception as e:
        pytest.skip(f"earthaccess unavailable: {e}")


@pytest.fixture
def l4a_granule(earthaccess_session):
    """Search for a single L4A granule in a small region."""
    ea = earthaccess_session
    results = ea.search_data(
        short_name="GEDI04_A",
        bounding_box=(-51, 0, -50, 1),
        temporal=("2020-06-01", "2020-06-30"),
        count=1,
    )
    if not results:
        pytest.skip("No L4A granules found for test region/period")
    return results[0]


SUBSET_VARS = ["shot_number", "agbd", "lat_lowestmode", "lon_lowestmode"]


def _get_h5_info(filepath):
    """Return dict with dataset count and total rows from first beam."""
    info = {"datasets": 0, "rows": 0}
    with h5py.File(filepath, "r") as f:
        def count(name, obj):
            if isinstance(obj, h5py.Dataset):
                info["datasets"] += 1
        f.visititems(count)
        # Get row count from first available beam
        for key in f.keys():
            if key.startswith("BEAM"):
                sn = f.get(f"{key}/shot_number")
                if sn is not None:
                    info["rows"] = sn.shape[0]
                break
    return info


def test_s3_vs_download_benchmark(earthaccess_session, l4a_granule, tmp_dir):
    """Compare wall-clock time: download+subset vs S3 subset."""
    ea = earthaccess_session
    from gedih3.gedidriver import gedi_subset

    # --- Approach A: Full download, then local subset ---
    dl_dir = os.path.join(tmp_dir, "download")
    os.makedirs(dl_dir)

    t0 = time.perf_counter()
    downloaded = ea.download(l4a_granule, dl_dir)
    t_download = time.perf_counter() - t0

    assert downloaded, "Download returned no files"
    full_path = downloaded[0]
    full_size = os.path.getsize(full_path)

    subset_a = os.path.join(tmp_dir, "subset_a.h5")
    t0 = time.perf_counter()
    result_a = gedi_subset(full_path, subset_a, SUBSET_VARS)
    t_local_subset = time.perf_counter() - t0

    assert result_a is not None, "Local subset produced no output"
    size_a = os.path.getsize(subset_a)

    # --- Approach B: S3 open, then remote subset ---
    t0 = time.perf_counter()
    s3_files = ea.open([l4a_granule])
    t_s3_open = time.perf_counter() - t0

    assert s3_files, "S3 open returned no file handles"
    s3_handle = s3_files[0]

    subset_b = os.path.join(tmp_dir, "subset_b.h5")
    t0 = time.perf_counter()
    result_b = gedi_subset(s3_handle, subset_b, SUBSET_VARS)
    t_s3_subset = time.perf_counter() - t0

    assert result_b is not None, "S3 subset produced no output"
    size_b = os.path.getsize(subset_b)

    # --- Validate outputs match ---
    info_a = _get_h5_info(subset_a)
    info_b = _get_h5_info(subset_b)

    assert info_a["datasets"] == info_b["datasets"], (
        f"Dataset count mismatch: local={info_a['datasets']} vs s3={info_b['datasets']}"
    )
    assert info_a["rows"] == info_b["rows"], (
        f"Row count mismatch: local={info_a['rows']} vs s3={info_b['rows']}"
    )

    # --- Report ---
    t_approach_a = t_download + t_local_subset
    t_approach_b = t_s3_open + t_s3_subset

    print("\n" + "=" * 60)
    print("S3 ETL BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Full file size:        {full_size / 1e6:.1f} MB")
    print(f"Subset file size:      {size_a / 1e6:.1f} MB ({size_a/full_size*100:.1f}%)")
    print(f"Datasets in subset:    {info_a['datasets']}")
    print(f"Rows per beam:         {info_a['rows']}")
    print()
    print(f"Approach A (download + local subset):")
    print(f"  Download:            {t_download:.1f}s")
    print(f"  Local subset:        {t_local_subset:.1f}s")
    print(f"  Total:               {t_approach_a:.1f}s")
    print()
    print(f"Approach B (S3 open + remote subset):")
    print(f"  S3 open:             {t_s3_open:.1f}s")
    print(f"  S3 subset:           {t_s3_subset:.1f}s")
    print(f"  Total:               {t_approach_b:.1f}s")
    print()
    if t_approach_b > 0:
        ratio = t_approach_a / t_approach_b
        print(f"Speedup (A/B):         {ratio:.1f}x")
    print("=" * 60)
