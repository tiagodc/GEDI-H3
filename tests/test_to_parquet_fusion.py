"""
Smoke test: confirm `optimize_graph=True` (task fusion) does not change the
number of parquet files emitted by `to_parquet(partition_on=...)`.

Background: gh3builder._write_partitioned previously passed
`persist(optimize_graph=False)` because a much older run had reportedly
reduced output cardinality. Per dask/dask#8445 + #8487 fusion only collapses
linear chains within a single dataframe partition — it cannot merge data
across partitions (that would require a shuffle). The hive-partitioned write
contract is "one file per (input partition × leaf directory it touches)",
regardless of fusion.

This test asserts that contract by writing the same multi-partition dataframe
twice — once with fusion off, once with fusion on — and comparing the parquet
file trees. Identical trees → safe to flip the flag in production.
"""
import os
import pathlib

import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
import pyarrow.dataset as pa_ds

# Reuse the same in-process LocalCluster pattern as test_doctor_parallel.py
# so the smoke test is hermetic — no dependency on a running scheduler.
@pytest.fixture
def _local_dask_client():
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


def _make_ddf(n_partitions: int = 6, rows_per_partition: int = 200) -> dd.DataFrame:
    """Multi-partition ddf with two hive columns ('h3_03', 'year') that each
    partition spans broadly — the worst case for hive fan-out and the only
    case where fusion could in principle 'merge' files (it cannot)."""
    rng = np.random.default_rng(0)
    frames = []
    for i in range(n_partitions):
        df = pd.DataFrame({
            'value': rng.standard_normal(rows_per_partition),
            # Every partition spans the full set of h3 cells and years, so the
            # cartesian fan-out is maximal. If fusion ever silently merged data
            # across input partitions, the per-leaf file count would drop.
            'h3_03': rng.integers(0, 4, rows_per_partition),
            'year':  rng.integers(2019, 2022, rows_per_partition),
        })
        frames.append(df)
    pdf = pd.concat(frames, ignore_index=True)
    return dd.from_pandas(pdf, npartitions=n_partitions)


def _list_parquet(root: str) -> list[str]:
    return sorted(
        str(p.relative_to(root))
        for p in pathlib.Path(root).rglob('*.parquet')
    )


def _read_tree(root: str) -> pd.DataFrame:
    """Read a hive-partitioned tree back into one pandas frame, via pyarrow.

    Deliberately *not* ``dd.read_parquet``. Dask's arrow engine rebuilds hive
    partition columns with ``pd.Categorical(categories=<discovered keys>)``
    (``dask/dataframe/io/parquet/arrow.py``), which raises "Categorical
    categories must be unique" whenever pyarrow's discovered partition
    dictionary contains a repeat. That made this test intermittently fail in
    the minimum-versions CI job, and the code is unchanged in current dask, so
    there is no floor to raise past it.

    The dependency was incidental: this test is about *file cardinality* under
    graph fusion, and the round-trip below only needs to prove no rows were
    lost or merged. Both trees are read through this same function, so any
    dtype quirk cancels out of the comparison.
    """
    table = pa_ds.dataset(root, partitioning='hive', format='parquet').to_table()
    df = table.to_pandas()
    # Hive columns come back as dictionary-encoded; normalise so the two trees
    # compare on values rather than on categorical encoding.
    for col in ('h3_03', 'year'):
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype(df[col].cat.categories.dtype)
    return df.sort_values('value').reset_index(drop=True)


def test_fusion_preserves_partition_on_output_cardinality(_local_dask_client, tmp_path):
    """optimize_graph=True must emit the same parquet tree as optimize_graph=False
    when partition_on is in play. Guards the change in gh3builder._write_partitioned
    against the 'fusion merged my partitions' fear documented in CLAUDE.md."""
    ddf = _make_ddf()

    out_off = tmp_path / 'opt_off'
    out_on  = tmp_path / 'opt_on'

    kwargs = dict(
        write_index=True,
        partition_on=['h3_03', 'year'],
        write_metadata_file=False,
        compute=False,
    )

    t_off = ddf.to_parquet(str(out_off), **kwargs).persist(optimize_graph=False)
    t_off.compute()

    t_on = ddf.to_parquet(str(out_on), **kwargs).persist()  # default optimize_graph=True
    t_on.compute()

    files_off = _list_parquet(str(out_off))
    files_on  = _list_parquet(str(out_on))

    # Same count, same hive directory layout, same per-leaf file count.
    assert len(files_off) == len(files_on), (
        f"Fusion changed total file count: optimize_graph=False -> {len(files_off)} "
        f"files, optimize_graph=True -> {len(files_on)} files"
    )

    leaves_off = sorted({os.path.dirname(p) for p in files_off})
    leaves_on  = sorted({os.path.dirname(p) for p in files_on})
    assert leaves_off == leaves_on, (
        f"Fusion changed hive leaf set:\n  off: {leaves_off}\n  on:  {leaves_on}"
    )

    # Per-leaf file count must match (this is the 'fewer files per partition'
    # symptom the user remembered — proving it does not occur).
    from collections import Counter
    per_leaf_off = Counter(os.path.dirname(p) for p in files_off)
    per_leaf_on  = Counter(os.path.dirname(p) for p in files_on)
    assert per_leaf_off == per_leaf_on, (
        f"Fusion changed per-leaf file count:\n  off: {per_leaf_off}\n  on:  {per_leaf_on}"
    )

    # And the data round-trips identically (no rows lost / merged).
    rt_off = _read_tree(str(out_off))
    rt_on  = _read_tree(str(out_on))
    assert len(rt_off) == len(ddf), "rows lost on the optimize_graph=False write"
    pd.testing.assert_frame_equal(rt_off, rt_on, check_like=True)
