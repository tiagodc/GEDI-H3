"""Tests for granule reconciliation on resume.

Verifies that ``_reconcile_granules_from_disk`` correctly identifies granules
already represented in tmp/partitions/ and database/ and flips their build-log
status to INDEXED, so a stage-1 rerun does not redo extraction work.
"""
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import make_partition_dir, make_build_log


def _write_tmp_fragment(parent, h3_part, year, granule_path, basename='part.0.parquet', n=10):
    """Create a tmp/partitions/h3_<p>=<id>/year=<yyyy>/<basename> fragment.

    The fragment carries a ``root_file_l2a`` column whose value is parseable
    by GEDIFile naming convention so the reconciliation helper can recover
    (orbit, granule, track) without opening the source HDF5.
    """
    leaf_dir = os.path.join(parent, f'h3_03={h3_part}', f'year={year}')
    os.makedirs(leaf_dir, exist_ok=True)
    path = os.path.join(leaf_dir, basename)
    table = pa.table({
        'shot_number': pa.array(np.arange(n, dtype=np.uint64)),
        'root_file_l2a': pa.array([granule_path] * n),
        'agbd_l4a': pa.array(np.random.uniform(0, 300, n)),
    })
    pq.write_table(table, path)
    return path


def _gedi_basename(orbit, granule, track, version=2):
    return (
        f"GEDI02_A_2020001000000_O{orbit:05d}_{granule:02d}_T{track:05d}"
        f"_02_003_02_V{version:03d}.h5"
    )


def _logger_with_pending(h3_dir, granules):
    from gedih3.logger import H3BuildLogger
    make_build_log(
        h3_dir,
        status='PARTITIONING',
        granules=[{'orbit': g[0], 'granule': g[1], 'track': g[2], 'status': 'PENDING'}
                  for g in granules],
        h3_partition_ids=[],
    )
    return H3BuildLogger(product_vars=None, dir=h3_dir)


class TestReconcileFromTmpFragments:
    def test_flips_pending_to_indexed_from_tmp(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        granules = [(99, 1, 5), (100, 2, 6), (101, 3, 7)]

        for orb, gran, trk in granules[:2]:
            _write_tmp_fragment(
                tmp_partitions, h3_part='830001fffffffff', year='2020',
                granule_path=f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.BEAM0000.parquet',
            )

        h3_logger = _logger_with_pending(h3_dir, granules)

        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        assert n_flipped == 2
        statuses = {(g['orbit'], g['granule'], g['track']): g['status']
                    for g in h3_logger.granule_info}
        assert statuses[(99, 1, 5)] == 'INDEXED'
        assert statuses[(100, 2, 6)] == 'INDEXED'
        assert statuses[(101, 3, 7)] == 'PENDING'

    def test_idempotent(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)
        granules = [(50, 1, 1)]
        _write_tmp_fragment(
            tmp_partitions, '830001fffffffff', '2020',
            f'/soc/{_gedi_basename(50, 1, 1)}',
        )
        h3_logger = _logger_with_pending(h3_dir, granules)

        first = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        second = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        assert first == 1
        assert second == 0  # already INDEXED, nothing to flip
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'

    def test_empty_tmp_no_flip(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)
        h3_logger = _logger_with_pending(h3_dir, [(1, 1, 1)])

        n_flipped = _reconcile_granules_from_disk(
            h3_dir, h3_logger, tmp_dir=os.path.join(tmp_dir, 'nonexistent'),
        )
        assert n_flipped == 0
        assert h3_logger.granule_info[0]['status'] == 'PENDING'

    def test_corrupt_fragment_skipped(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        # Good fragment for granule (10,1,1)
        _write_tmp_fragment(
            tmp_partitions, '830001fffffffff', '2020',
            f'/soc/{_gedi_basename(10, 1, 1)}',
        )
        # Corrupt fragment for granule (20,2,2) — not a parquet
        leaf = os.path.join(tmp_partitions, 'h3_03=830002fffffffff', 'year=2020')
        os.makedirs(leaf, exist_ok=True)
        with open(os.path.join(leaf, 'corrupt.parquet'), 'wb') as f:
            f.write(b'not a parquet file')

        h3_logger = _logger_with_pending(h3_dir, [(10, 1, 1), (20, 2, 2)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        assert n_flipped == 1
        statuses = {(g['orbit'], g['granule'], g['track']): g['status']
                    for g in h3_logger.granule_info}
        assert statuses[(10, 1, 1)] == 'INDEXED'
        assert statuses[(20, 2, 2)] == 'PENDING'  # corrupt → skipped


class TestReconcileFromDatabase:
    def test_finalized_partition_metadata_marks_indexed(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)

        # Build a finalized partition with metadata listing two granules
        granules = [
            {'orbit': 7, 'granule': 1, 'track': 3},
            {'orbit': 8, 'granule': 2, 'track': 4},
        ]
        make_partition_dir(h3_dir, h3_part='830001fffffffff', year='2020',
                           granules=granules)

        h3_logger = _logger_with_pending(h3_dir, [(7, 1, 3), (8, 2, 4), (9, 3, 5)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=None)

        assert n_flipped == 2
        statuses = {(g['orbit'], g['granule'], g['track']): g['status']
                    for g in h3_logger.granule_info}
        assert statuses[(7, 1, 3)] == 'INDEXED'
        assert statuses[(8, 2, 4)] == 'INDEXED'
        assert statuses[(9, 3, 5)] == 'PENDING'


class TestReconcileScalability:
    def test_sequential_fallback_no_dask(self, tmp_dir, monkeypatch):
        """Helper works without a Dask client (sequential scan path)."""
        from gedih3.gh3builder import _reconcile_granules_from_disk

        # Force get_dask_client to return None
        import gedih3.gh3builder as gh
        monkeypatch.setattr(gh, 'get_dask_client', lambda: None)

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)
        granules = [(101, 1, 7), (102, 2, 8)]
        for orb, gran, trk in granules:
            _write_tmp_fragment(
                tmp_partitions, '830001fffffffff', '2020',
                f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.BEAM0000.parquet',
            )

        h3_logger = _logger_with_pending(h3_dir, granules)
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        assert n_flipped == 2


class TestReconcileShortCircuits:
    """Optimisations that avoid the disk scan when the result is predictable."""

    def test_skips_disk_scan_when_log_has_no_pending(self, tmp_dir, monkeypatch):
        """If every granule in the log is already INDEXED, do not glob the tmp tree."""
        from gedih3.gh3builder import _reconcile_granules_from_disk
        from gedih3.logger import H3BuildLogger
        import gedih3.gh3builder as gh

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        make_build_log(
            h3_dir, status='PARTITIONING', h3_partition_ids=[],
            granules=[
                {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
                {'orbit': 2, 'granule': 2, 'track': 2, 'status': 'INDEXED'},
            ],
        )
        h3_logger = H3BuildLogger(product_vars=None, dir=h3_dir)

        # Sentinel that fails if anyone walks tmp_partitions / h3_dir for files.
        called = {'n': 0}
        real_glob = gh.glob.glob

        def _spy(pattern, *a, **kw):
            called['n'] += 1
            return real_glob(pattern, *a, **kw)

        monkeypatch.setattr(gh.glob, 'glob', _spy)

        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        assert n_flipped == 0
        assert called['n'] == 0, "early-out should skip every glob in the function body"


class TestProcessH3Partition:
    """Direct unit test of the per-partition worker helper."""

    def test_returns_unique_granule_ids_via_threadpool(self, tmp_dir):
        from gedih3.gh3builder import _process_h3_partition

        partition_dir = os.path.join(tmp_dir, 'h3_03=830001fffffffff')
        # Three fragments in two different years; two share a granule.
        for year, (orb, gran, trk), beam in [
            ('2020', (200, 1, 5), 'BEAM0000'),
            ('2020', (200, 1, 5), 'BEAM0001'),
            ('2021', (201, 2, 6), 'BEAM0000'),
        ]:
            year_dir = os.path.join(partition_dir, f'year={year}')
            os.makedirs(year_dir, exist_ok=True)
            path = os.path.join(year_dir, f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}.parquet')
            tab = pa.table({
                'shot_number': pa.array(np.arange(5, dtype=np.uint64)),
                'root_file_l2a': pa.array([f'/soc/{_gedi_basename(orb, gran, trk)}'] * 5),
            })
            pq.write_table(tab, path)

        ids = _process_h3_partition(partition_dir)
        assert ids == {(200, 1, 5), (201, 2, 6)}

    def test_returns_empty_set_for_missing_dir(self, tmp_dir):
        from gedih3.gh3builder import _process_h3_partition
        # Nonexistent directory must not raise.
        assert _process_h3_partition(os.path.join(tmp_dir, 'does_not_exist')) == set()


class TestGranuleIdParse:
    def test_parses_standard_filename(self):
        from gedih3.gh3builder import _granule_id_from_l2a_path
        path = (
            '/soc/2020/001/'
            'GEDI02_A_2020001000000_O12345_67_T98765_02_003_02_V002.h5'
        )
        assert _granule_id_from_l2a_path(path) == (12345, 67, 98765)

    def test_returns_none_on_garbage(self):
        from gedih3.gh3builder import _granule_id_from_l2a_path
        assert _granule_id_from_l2a_path('not a gedi file') is None
        assert _granule_id_from_l2a_path('') is None
        assert _granule_id_from_l2a_path(None) is None
