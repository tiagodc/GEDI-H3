"""Tests for the package-wide parallelism primitives in
``gedih3.parallel`` — three walker functions (SOC year/doy, H3 DB
``h3_NN=*``, flat dir) plus the manifest freshness smoke check.

Verifies the always-parallel contract carried over from the doctor:

* Without a registered Client, walkers raise ``GediError`` from the
  inner ``parallel_map`` (no serial fallback).
* With a registered Client, every leaf is enumerated and the full
  sorted list comes back on the driver.
* Worker exceptions abort the walk — no partial manifest.
* The freshness smoke check correctly distinguishes a fresh manifest
  from one written before a producer-tree mutation.
* The ``doctor.parallel`` re-export of ``parallel_map`` is the same
  object as the top-level one (backwards-compat shim).
"""

import os
import time
from pathlib import Path

import pytest

from gedih3.exceptions import GediError


# ---- fixtures --------------------------------------------------------------


@pytest.fixture
def _local_dask_client():
    """Tiny in-process LocalCluster — same pattern as
    test_doctor_parallel.py. Threads to keep the test cheap."""
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1,
        processes=False,
        dashboard_address=None,
        silence_logs='ERROR',
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


@pytest.fixture
def synthetic_soc_tree(tmp_path):
    """SOC year/doy/*.h5 tree. Three years × two doys × five files
    each = 30 GEDI HDF5 files (empty; the walker never opens them)."""
    paths = []
    for year in ('2020', '2021', '2022'):
        for doy in ('001', '200'):
            doy_dir = tmp_path / year / doy
            doy_dir.mkdir(parents=True)
            for i in range(5):
                p = doy_dir / f'GEDI02_A_{year}{doy}010101_O{i:05d}_01_T00001_02_003_01_V003.h5'
                p.touch()
                paths.append(str(p))
    # Also drop an SGS file to verify exclude filtering.
    sgs = tmp_path / '2020' / '001' / 'GEDI02_A_SGS_skipme_V003_SGS.h5'
    sgs.touch()
    return tmp_path, sorted(paths)


@pytest.fixture
def synthetic_h3_db(tmp_path):
    """H3 DB with ``h3_03=<cell>/`` partition dirs containing parquet
    files at both flat and year-nested levels. Workers should pick up
    both."""
    paths = []
    for cell in ('830e41fffffffff', '830e43fffffffff'):
        p_dir = tmp_path / f'h3_03={cell}'
        p_dir.mkdir()
        # flat parquet
        f1 = p_dir / 'gedi.parquet'
        f1.touch()
        paths.append(str(f1))
        # year-nested parquet
        y = p_dir / 'year=2020'
        y.mkdir()
        f2 = y / 'gedi.parquet'
        f2.touch()
        paths.append(str(f2))
    # Non-h3 sibling at the root must be ignored by the walker.
    (tmp_path / 'gedih3_build_log.json').touch()
    return tmp_path, sorted(paths)


# ---- always-parallel contract: no Client → raise ---------------------------


def test_walk_soc_parallel_requires_client(synthetic_soc_tree):
    """Walker has no serial fallback — without a registered Client the
    inner parallel_map raises GediError."""
    from gedih3.parallel import walk_soc_parallel
    soc_dir, _ = synthetic_soc_tree
    with pytest.raises(GediError, match='dask.distributed Client'):
        walk_soc_parallel(str(soc_dir))


def test_walk_h3db_parallel_requires_client(synthetic_h3_db):
    from gedih3.parallel import walk_h3db_parallel
    db_dir, _ = synthetic_h3_db
    with pytest.raises(GediError, match='dask.distributed Client'):
        walk_h3db_parallel(str(db_dir))


# ---- walker correctness with a Client --------------------------------------


def test_walk_soc_parallel_finds_every_file(_local_dask_client, synthetic_soc_tree):
    """All .h5 files matching the pattern come back, sorted, and the
    SGS file the exclude filter targets is dropped at the worker."""
    from gedih3.parallel import walk_soc_parallel
    soc_dir, expected = synthetic_soc_tree
    result = walk_soc_parallel(str(soc_dir), pattern='GEDI*.h5',
                               exclude=['*_SGS.h5'])
    assert result == expected, "Walker must return the canonical sorted file list"


def test_walk_soc_parallel_no_exclude_includes_sgs(_local_dask_client, synthetic_soc_tree):
    """Without the exclude filter, the SGS file is included."""
    from gedih3.parallel import walk_soc_parallel
    soc_dir, expected = synthetic_soc_tree
    result = walk_soc_parallel(str(soc_dir), pattern='GEDI*.h5')
    # 30 base files + 1 SGS
    assert len(result) == 31
    assert any(p.endswith('_SGS.h5') for p in result)


def test_walk_h3db_parallel_finds_flat_and_nested(_local_dask_client, synthetic_h3_db):
    """Worker recursive glob picks up parquet at any depth under each
    partition; non-partition siblings at the root are ignored by the
    driver-side enumeration."""
    from gedih3.parallel import walk_h3db_parallel
    db_dir, expected = synthetic_h3_db
    result = walk_h3db_parallel(str(db_dir), pattern='*.parquet')
    assert result == expected


def test_walk_flat_parallel_is_single_dir(tmp_path):
    """Flat walker is single-dir scandir — no Client needed (single
    leaf, no fan-out). Provided for API symmetry with the other two."""
    from gedih3.parallel import walk_flat_parallel
    (tmp_path / 'a.parquet').touch()
    (tmp_path / 'b.parquet').touch()
    (tmp_path / 'skip.json').touch()
    result = walk_flat_parallel(str(tmp_path), pattern='*.parquet')
    assert [os.path.basename(p) for p in result] == ['a.parquet', 'b.parquet']


def test_walk_soc_parallel_aborts_on_worker_failure(_local_dask_client, synthetic_soc_tree, monkeypatch):
    """Per the R2 contract a partial manifest is worse than a slow
    re-walk — any worker exception must surface as a hard abort, not
    a partially-populated return value."""
    from gedih3 import parallel as parallel_mod
    soc_dir, _ = synthetic_soc_tree

    real = parallel_mod._scan_doy_dir
    fail_counter = {'n': 0}

    def _flaky(doy, *, pattern, exclude=None):
        fail_counter['n'] += 1
        if fail_counter['n'] == 2:
            raise OSError("simulated GPFS metadata server error")
        return real(doy, pattern=pattern, exclude=exclude)

    monkeypatch.setattr(parallel_mod, '_scan_doy_dir', _flaky)
    with pytest.raises(GediError, match='aborting walk'):
        parallel_mod.walk_soc_parallel(str(soc_dir))


# ---- impure-task contract: no key-cached stale results ----------------------


def _read_marker(path):
    with open(path) as f:
        return f.read()


def _echo(x):
    return x


def test_parallel_map_resubmission_sees_live_state(_local_dask_client, tmp_path):
    """Identical (fn, args) submitted twice must re-read the filesystem,
    not return dask's key-cached result. Regression for the CI-observed
    soc_health race: write_soc_manifest walked a dir, a file was added,
    and _enumerate_soc_files' identical resubmission raced the async
    release of the first walk's futures — with dask's default pure=True
    the scheduler handed back the stale walk and the new file was
    invisible."""
    from gedih3.parallel import parallel_map

    marker = tmp_path / 'state.txt'
    marker.write_text('before')
    first = list(parallel_map([str(marker)], _read_marker))
    marker.write_text('after')
    second = list(parallel_map([str(marker)], _read_marker))

    assert first[0][1] == 'before'
    assert second[0][1] == 'after', (
        "parallel_map returned a key-cached stale result — client.map "
        "must be called with pure=False"
    )


def test_parallel_map_submits_pure_false(_local_dask_client):
    """Direct contract: both dispatch branches (unbatched and batched)
    mark their tasks impure so dask never key-caches them."""
    from gedih3.parallel import parallel_map

    captured = []
    real_map = _local_dask_client.map

    def spy(*args, **kwargs):
        captured.append(kwargs.get('pure'))
        return real_map(*args, **kwargs)

    _local_dask_client.map = spy
    try:
        list(parallel_map(['a', 'b'], _echo))                 # unbatched
        list(parallel_map(['a', 'b'], _echo, batch_size=1))   # batched
    finally:
        del _local_dask_client.map

    assert captured == [False, False]


# ---- doctor.parallel re-export identity ------------------------------------


def test_doctor_parallel_reexport_identity():
    """The doctor's parallel_map import must be the same object as the
    new top-level one — back-compat shim is a no-op, not a wrapper."""
    from gedih3.parallel import parallel_map as a
    from gedih3.doctor.parallel import parallel_map as b
    assert a is b


# ---- manifest freshness smoke check ----------------------------------------


def test_check_manifest_freshness_fresh(tmp_path):
    """Manifest mtime ≥ root mtime → fresh; returns True with no log."""
    from gedih3.parallel import check_manifest_freshness
    manifest = tmp_path / '_manifest.txt'
    manifest.write_text("foo.parquet\n")
    # Touch manifest after the root so its mtime is newer.
    os.utime(str(manifest), (time.time() + 10, time.time() + 10))
    assert check_manifest_freshness(str(manifest), str(tmp_path)) is True


def test_check_manifest_freshness_stale_returns_false(tmp_path):
    """Root mtime newer than manifest → False. The loud ERROR log is a
    side effect (verified by pytest's own captured-stdout, which it
    grabs before caplog/capsys/capfd can intercept it — same caveat as
    test_soc_discovery.py:73-75). The function's contract is the
    return value; the message is for the user."""
    from gedih3.parallel import check_manifest_freshness
    manifest = tmp_path / '_manifest.txt'
    manifest.write_text("foo.parquet\n")
    old = time.time() - 60
    os.utime(str(manifest), (old, old))
    (tmp_path / 'new_file.h5').touch()
    assert check_manifest_freshness(
        str(manifest), str(tmp_path),
        remedy='gh3_doctor --check soc_health --fix',
    ) is False


def test_check_manifest_freshness_missing_files_returns_true(tmp_path):
    """Missing manifest or root → existing fallback paths handle it;
    the check returns True (don't add a second error log on top of the
    missing-manifest path's own behavior)."""
    from gedih3.parallel import check_manifest_freshness
    # Neither path exists.
    assert check_manifest_freshness(
        str(tmp_path / 'no_such_manifest.txt'),
        str(tmp_path / 'no_such_root'),
    ) is True


def test_check_manifest_freshness_raises_when_requested(tmp_path):
    from gedih3.parallel import check_manifest_freshness
    manifest = tmp_path / '_manifest.txt'
    manifest.write_text("\n")
    old = time.time() - 60
    os.utime(str(manifest), (old, old))
    (tmp_path / 'x.h5').touch()
    with pytest.raises(GediError, match='older than'):
        check_manifest_freshness(
            str(manifest), str(tmp_path), raise_on_stale=True,
        )


# ---- end-to-end producer refresh: write_soc_manifest + _read_manifest ------


def test_write_soc_manifest_with_prebuilt_files_skips_walk(synthetic_soc_tree, tmp_path):
    """When the caller hands a pre-computed file list, the writer must
    not invoke the parallel walker (verified indirectly: no Client is
    needed)."""
    from gedih3.gedidriver import write_soc_manifest
    soc_dir, expected = synthetic_soc_tree
    # files= is supplied → no Client, no walk
    n = write_soc_manifest(str(soc_dir), files=expected)
    assert n == len(expected)
    # Manifest file exists at the SOC root
    from gedih3.config import SOC_MANIFEST_FILENAME
    assert (Path(soc_dir) / SOC_MANIFEST_FILENAME).exists()


def test_write_soc_manifest_walks_when_no_files(_local_dask_client, synthetic_soc_tree):
    """Without ``files=``, the writer kicks the parallel walker; result
    matches the prebuilt-list path."""
    from gedih3.gedidriver import write_soc_manifest, _read_soc_manifest
    soc_dir, expected = synthetic_soc_tree
    n = write_soc_manifest(str(soc_dir))
    # All 30 V003 + the V003_SGS one — SGS pattern matches 'GEDI*.h5'
    # because the writer applies no exclude filter (that's caller-side).
    assert n == len(expected) + 1
    # Manifest is reachable via _read_soc_manifest.
    read_back = _read_soc_manifest(str(soc_dir))
    assert read_back is not None
    assert len(read_back) == n


def test_generate_manifest_dispatches_by_tree_shape(_local_dask_client, synthetic_h3_db):
    """``tree_shape='h3db'`` finds parquet under every h3_NN=*
    partition (flat + year-nested)."""
    from gedih3.utils import generate_manifest, _read_manifest
    db_dir, expected = synthetic_h3_db
    generate_manifest(str(db_dir), pattern='*.parquet', tree_shape='h3db')
    rel = _read_manifest(str(db_dir))
    assert rel is not None
    abs_paths = sorted(os.path.join(str(db_dir), r) for r in rel)
    assert abs_paths == expected


def test_generate_manifest_flat_shape(tmp_path):
    """``tree_shape='flat'`` for ``gh3_extract``/``gh3_aggregate``
    output dirs — single scandir, no Client needed."""
    from gedih3.utils import generate_manifest, _read_manifest
    (tmp_path / 'a.parquet').touch()
    (tmp_path / 'b.parquet').touch()
    generate_manifest(str(tmp_path), pattern='*.parquet', tree_shape='flat')
    rel = _read_manifest(str(tmp_path))
    assert sorted(rel) == ['a.parquet', 'b.parquet']
