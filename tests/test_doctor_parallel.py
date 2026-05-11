"""Tests for the doctor's distributed work helper (parallel.py).

Verifies the always-parallel contract:
  * Without a registered Client, ``parallel_map`` raises ``GediError``
    with a clear "wrap your code in Client(...)" message.
  * With a registered Client, every item gets a ``(item, result)``
    tuple in completion order; per-task exceptions are surfaced
    in-band.
  * Batched dispatch covers the very-large-input case
    (``len(items) > batch_size`` ⇒ one task per chunk).
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


# ---- always-parallel contract: requires a registered Client ----------------

def test_parallel_map_raises_when_no_client_registered():
    """Without a registered dask Client, parallel_map must refuse to
    run rather than silently fall back to a serial implementation —
    the always-parallel contract eliminates code-path branching."""
    from gedih3.exceptions import GediError
    with pytest.raises(GediError, match='dask.distributed Client'):
        list(parallel_map([1, 2, 3], _double))


def test_parallel_map_empty_input_yields_nothing_without_client():
    """Empty input is a no-op even without a Client — the function
    short-circuits before checking for the Client. Avoids a
    pointless cluster requirement when callers iterate over a
    possibly-empty list."""
    out = list(parallel_map([], _double))
    assert out == []


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


def test_parallel_map_returns_full_set(_local_dask_client):
    """With a registered client: every item gets a result; order may differ."""
    items = list(range(8))
    results = list(parallel_map(items, _double))
    # Every input item appears exactly once (order undefined in the
    # parallel path — completion order, not input order).
    seen_items = sorted(it for it, _ in results)
    assert seen_items == items
    seen_pairs = {it: r for it, r in results}
    assert seen_pairs == {i: i * 2 for i in items}


def test_parallel_map_exceptions_in_band(_local_dask_client):
    """Per-task failures surface as exception objects in the result tuples."""
    out = list(parallel_map([1, 2, 3], _explode))
    assert len(out) == 3
    for _, res in out:
        assert isinstance(res, Exception)


def test_parallel_map_broadcast_kwargs(_local_dask_client):
    out = list(parallel_map([10, 20, 30], _add_kw, increment=5))
    pairs = {it: r for it, r in out}
    assert pairs == {10: 15, 20: 25, 30: 35}


# ---- batched dispatch (S1 — soc_health task explosion fix) -----------------

def test_parallel_map_batched_returns_full_set(_local_dask_client):
    """Batched parallel path: every item gets a result; order may differ."""
    items = list(range(50))
    results = list(parallel_map(items, _double, batch_size=10))
    seen = sorted(it for it, _ in results)
    assert seen == items
    pairs = {it: r for it, r in results}
    assert pairs == {i: i * 2 for i in items}


def test_parallel_map_batched_preserves_in_band_exceptions(_local_dask_client):
    """Per-item exceptions surface in the result tuple, not the batch's."""
    out = list(parallel_map([1, 2, 3, 4], _explode, batch_size=2))
    assert len(out) == 4
    for it, res in out:
        assert isinstance(res, RuntimeError)


def test_parallel_map_batched_below_threshold_uses_unbatched(_local_dask_client):
    """If len(items) <= batch_size, behave like the unbatched path."""
    items = [10, 20, 30]
    results = list(parallel_map(items, _double, batch_size=10))
    pairs = {it: r for it, r in results}
    assert pairs == {10: 20, 20: 40, 30: 60}


def test_parallel_map_batched_with_broadcast_kwargs(_local_dask_client):
    """Broadcast kwargs flow through the chunk worker via closure."""
    out = list(parallel_map([1, 2, 3, 4, 5, 6], _add_kw,
                            batch_size=3, increment=10))
    pairs = {it: r for it, r in out}
    assert pairs == {1: 11, 2: 12, 3: 13, 4: 14, 5: 15, 6: 16}


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


# ---- streaming memory pillar (v0.8.x lessons) -----------------------------
#
# These tests assert that the per-file scans don't fall back to
# ``pd.read_parquet(path, columns=...)`` (which materializes the full
# requested column set) by monkey-patching that call to raise.

def test_count_duplicates_does_not_materialize_full_column(tmp_path, monkeypatch):
    """parquet_health._scan_one_file must stream shot_number via
    ``pq.ParquetFile.iter_batches`` and walk per-RG min/max — never
    ``pd.read_parquet(columns=['shot_number'])`` on the whole file."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gedih3.doctor.diagnoses.parquet_health import _scan_one_file

    # Build a 100k-row parquet with 5 row groups of 20k each, no dups.
    df = pd.DataFrame({
        'shot_number': np.arange(100_000, dtype=np.int64),
        'agbd_l4a': np.random.uniform(0, 300, 100_000),
    })
    path = str(tmp_path / 'big.parquet')
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=20_000)

    # Trip a tripwire if anyone tries the full-file ``pd.read_parquet``
    # path. The streaming implementation only uses pyarrow.parquet.
    def _no_full_read(*a, **kw):
        raise AssertionError(
            "pd.read_parquet was called — _count_duplicates must stream via iter_batches"
        )
    monkeypatch.setattr(pd, 'read_parquet', _no_full_read)

    info = _scan_one_file(path)
    assert info['corrupt'] is False
    assert info['unreadable_shot_number'] is False
    assert info['duplicates'] == 0


def test_count_duplicates_detects_intra_row_group_duplicates(tmp_path):
    """Streaming implementation must still find within-row-group dups."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gedih3.doctor.diagnoses.parquet_health import _scan_one_file

    # 10 rows, two of them duplicates (shot 0 appears twice).
    df = pd.DataFrame({
        'shot_number': np.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8], dtype=np.int64),
    })
    path = str(tmp_path / 'dup.parquet')
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=10)
    info = _scan_one_file(path)
    assert info['duplicates'] == 1


def test_count_duplicates_detects_cross_row_group_duplicates(tmp_path):
    """Cross-row-group duplicates are real bugs and must be counted
    exactly. Earlier we used per-RG min/max overlap as a proxy, but
    overlap is the *normal* post-merge state of GEDI partitions (whose
    row groups come from different granules with interleaving
    shot_numbers) — so the proxy produced false positives on every
    correctly-built partition. The fix is exact global value_counts."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gedih3.doctor.diagnoses.parquet_health import _scan_one_file

    # Two RGs, both covering the same range, with REAL duplicates: each
    # of {1,2,3,4,5} appears in both groups.
    df = pd.DataFrame({
        'shot_number': np.array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5], dtype=np.int64),
    })
    path = str(tmp_path / 'real_dups.parquet')
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=5)
    info = _scan_one_file(path)
    assert info['duplicates'] == 5


def test_count_duplicates_no_false_positive_on_overlapping_ranges(tmp_path):
    """Regression test: a cleanly-merged partition whose row groups
    cover overlapping shot_number ranges (because they come from
    different granules) but contain NO actual duplicates must NOT be
    flagged as having duplicates. Earlier the cross_rg_overlap proxy
    fired here, producing 44k false positives on a real continental
    database."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gedih3.doctor.diagnoses.parquet_health import _scan_one_file

    # Two RGs, ranges overlap (1..10 vs 5..15), but every shot_number
    # is globally unique.
    df = pd.DataFrame({
        'shot_number': np.array(
            [1, 2, 3, 4, 6, 7, 8, 9, 11, 12,
             5, 10, 13, 14, 15, 16, 17, 18, 19, 20],
            dtype=np.int64,
        ),
    })
    path = str(tmp_path / 'overlap_no_dups.parquet')
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=10)
    info = _scan_one_file(path)
    assert info['duplicates'] == 0


def test_per_granule_null_counts_streams_via_iter_batches(tmp_path, monkeypatch):
    """inspect.per_granule_null_counts must iterate row groups via
    ``pq.ParquetFile.iter_batches`` — NOT pull the full multi-column
    dataframe via ``pd.read_parquet(columns=...)``."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gedih3.doctor.inspect import per_granule_null_counts

    n = 50_000
    df = pd.DataFrame({
        'shot_number': np.arange(n, dtype=np.int64),
        'root_file_l2a': [
            'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5'
        ] * n,
        'agbd_l4a': np.where(np.arange(n) % 3 == 0, np.nan, 1.0),
    })
    path = str(tmp_path / 'partition.parquet')
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=10_000)

    def _no_full_read(*a, **kw):
        raise AssertionError(
            "pd.read_parquet was called — per_granule_null_counts must "
            "iter_batches via pyarrow"
        )
    monkeypatch.setattr(pd, 'read_parquet', _no_full_read)

    out = per_granule_null_counts(path, {'L4A': ['agbd_l4a']})
    # One granule key, one product, ~n/3 nulls.
    assert len(out) == 1
    granule_key = next(iter(out))
    assert out[granule_key]['L4A'] == sum(1 for i in range(n) if i % 3 == 0)
