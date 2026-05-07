"""Tests for the doctor's distributed work helper (parallel.py).

Verifies that:
  * The serial fallback (no dask client) works and emits exception
    objects in-band rather than crashing.
  * When a dask client is registered, results are produced identically
    to the serial path (order may differ — completion order vs. input
    order).
  * The O(1) emptiness primitives (``partition_is_empty``,
    ``year_dir_is_empty``, ``list_year_dirs``) match the legacy
    recursive-glob behavior on the same inputs.
"""

import os

import pytest

from gedih3.doctor.parallel import (
    parallel_map,
    partition_is_empty,
    list_year_dirs,
    year_dir_is_empty,
)


# ---- shared worker fns (top-level so dask can pickle them) ------------------

def _double(x):
    return x * 2


def _explode(x):
    raise RuntimeError(f"boom on {x}")


def _scan_path(path):
    return os.listdir(path)


def _add_kw(x, *, increment=1):
    return x + increment


# ---- serial path ------------------------------------------------------------

def test_parallel_map_serial_basic():
    """No dask client registered → serial loop, in-order results."""
    out = list(parallel_map([1, 2, 3], _double))
    assert out == [(1, 2), (2, 4), (3, 6)]


def test_parallel_map_serial_empty_input_yields_nothing():
    out = list(parallel_map([], _double))
    assert out == []


def test_parallel_map_serial_exception_is_in_band():
    """A worker raising must yield (item, exception) instead of crashing."""
    out = list(parallel_map([1, 2], _explode))
    assert len(out) == 2
    for it, res in out:
        assert isinstance(res, RuntimeError)
        assert str(it) in str(res)


def test_parallel_map_serial_broadcast_kwargs():
    out = list(parallel_map([10, 20, 30], _add_kw, increment=5))
    assert out == [(10, 15), (20, 25), (30, 35)]


# ---- parallel path (with a real LocalCluster) ------------------------------

@pytest.fixture
def _local_dask_client():
    """Spin up a tiny in-process LocalCluster + Client for parallel-path tests."""
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1,
        processes=False,                # threads keep the test cheap
        dashboard_address=None,
        silence_logs='ERROR',
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def test_parallel_map_parallel_path_returns_full_set(_local_dask_client):
    """With a registered client: every item gets a result; order may differ."""
    items = list(range(8))
    results = list(parallel_map(items, _double))
    # Every input item appears exactly once (order undefined in the
    # parallel path — completion order, not input order).
    seen_items = sorted(it for it, _ in results)
    assert seen_items == items
    seen_pairs = {it: r for it, r in results}
    assert seen_pairs == {i: i * 2 for i in items}


def test_parallel_map_parallel_path_exceptions_in_band(_local_dask_client):
    """Per-task failures surface as exception objects in the result tuples."""
    out = list(parallel_map([1, 2, 3], _explode))
    assert len(out) == 3
    for _, res in out:
        assert isinstance(res, Exception)


def test_parallel_map_parallel_path_broadcast_kwargs(_local_dask_client):
    out = list(parallel_map([10, 20, 30], _add_kw, increment=5))
    pairs = {it: r for it, r in out}
    assert pairs == {10: 15, 20: 25, 30: 35}


def test_parallel_map_serial_and_parallel_agree(_local_dask_client):
    """Same inputs + same fn → same set of (item, result) pairs both ways."""
    items = list(range(20))
    parallel_pairs = sorted(parallel_map(items, _double))
    # Build a fresh client-less map by closing the client temporarily
    _local_dask_client.close()  # forces get_dask_client() back to None
    serial_pairs = list(parallel_map(items, _double))
    assert parallel_pairs == serial_pairs


# ---- O(1) emptiness primitives --------------------------------------------

def test_partition_is_empty_true_on_empty_dir(tmp_path):
    p = tmp_path / 'h3_03=abc'
    p.mkdir()
    assert partition_is_empty(str(p)) is True


def test_partition_is_empty_false_with_top_level_parquet(tmp_path):
    p = tmp_path / 'h3_03=abc'
    p.mkdir()
    (p / 'data.parquet').write_bytes(b'x')
    assert partition_is_empty(str(p)) is False


def test_partition_is_empty_false_with_nested_parquet(tmp_path):
    p = tmp_path / 'h3_03=abc'
    y = p / 'year=2020'
    y.mkdir(parents=True)
    (y / 'data.parquet').write_bytes(b'x')
    assert partition_is_empty(str(p)) is False


def test_partition_is_empty_true_with_only_metadata(tmp_path):
    p = tmp_path / 'h3_03=abc'
    p.mkdir()
    (p / 'partition_meta.json').write_text('{}')
    assert partition_is_empty(str(p)) is True


def test_partition_is_empty_handles_nonexistent_dir():
    """Defensive: missing directory returns True (treated as empty)."""
    assert partition_is_empty('/nonexistent/path/that/should/not/exist') is True


def test_list_year_dirs_returns_only_subdirs(tmp_path):
    p = tmp_path / 'h3_03=abc'
    p.mkdir()
    (p / 'year=2020').mkdir()
    (p / 'year=2021').mkdir()
    (p / 'partition_meta.json').write_text('{}')
    out = list_year_dirs(str(p))
    assert len(out) == 2
    for d in out:
        assert d.endswith(os.sep)


def test_year_dir_is_empty_true_when_no_parquets(tmp_path):
    y = tmp_path / 'year=2020'
    y.mkdir()
    assert year_dir_is_empty(str(y)) is True


def test_year_dir_is_empty_false_when_parquet_present(tmp_path):
    y = tmp_path / 'year=2020'
    y.mkdir()
    (y / 'data.parquet').write_bytes(b'x')
    assert year_dir_is_empty(str(y)) is False
