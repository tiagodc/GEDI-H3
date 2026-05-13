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


def _write_all_beams(parent, h3_part, year, orbit, granule, track):
    """Helper: write one fragment per GEDI beam for a granule (simulates a
    fully-extracted granule). Matches the post-v0.8.0 naming convention so
    ``_FRAGMENT_BASENAME_RE`` parses both granule and beam."""
    from gedih3.config import GEDI_BEAMS
    for beam in GEDI_BEAMS:
        _write_tmp_fragment(
            parent, h3_part=h3_part, year=year,
            granule_path=f'/soc/{_gedi_basename(orbit, granule, track)}',
            basename=f'O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}.parquet',
        )


class TestReconcileFromTmpFragments:
    def test_flips_pending_to_indexed_from_tmp(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        granules = [(99, 1, 5), (100, 2, 6), (101, 3, 7)]

        # Write the full 8-beam set for the first two granules — the third
        # has nothing on disk and must remain PENDING.
        for orb, gran, trk in granules[:2]:
            _write_all_beams(tmp_partitions, '830001fffffffff', '2020', orb, gran, trk)

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

        # Good fragment for granule (10,1,1) — legacy ``part.0.parquet``
        # name, so reconcile flips via the _LEGACY_BEAM_SENTINEL path.
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


class TestPartialGranuleSafety:
    """Regression coverage: a granule with only some beams on disk must NOT
    be flipped INDEXED. Without per-beam tracking the reconcile would mark
    it complete and the missing beams' shots would be silently dropped on
    the next stage-1 run (skip filter excludes the granule)."""

    def test_partial_beam_set_not_flipped(self, tmp_dir):
        """3 of 8 beams written → granule stays PENDING for re-extraction."""
        from gedih3.gh3builder import _reconcile_granules_from_disk
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 42, 7, 13
        for beam in GEDI_BEAMS[:3]:  # only 3 of 8 beams
            _write_tmp_fragment(
                tmp_partitions, '830001fffffffff', '2020',
                f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}.parquet',
            )

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        assert n_flipped == 0
        assert h3_logger.granule_info[0]['status'] == 'PENDING', (
            "Partial granule incorrectly flipped to INDEXED — missing beams' "
            "shots would be silently dropped on the next stage-1 run."
        )

    def test_partial_then_complete_recovery(self, tmp_dir):
        """After re-extraction fills in the missing beams, the next reconcile
        pass flips the granule. This is the resume-after-resume happy path."""
        from gedih3.gh3builder import _reconcile_granules_from_disk
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 42, 7, 13
        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])

        # First pass: only some beams present → no flip.
        for beam in GEDI_BEAMS[:3]:
            _write_tmp_fragment(
                tmp_partitions, '830001fffffffff', '2020',
                f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}.parquet',
            )
        assert _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions) == 0
        assert h3_logger.granule_info[0]['status'] == 'PENDING'

        # Second pass: remaining beams arrive → flip happens.
        for beam in GEDI_BEAMS[3:]:
            _write_tmp_fragment(
                tmp_partitions, '830001fffffffff', '2020',
                f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}.parquet',
            )
        assert _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions) == 1
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'

    def test_complete_beam_set_across_multiple_h3_dirs(self, tmp_dir):
        """Realistic case: one granule's beams span several h3 partitions.
        Reconcile must aggregate beam sets across all h3_* dirs, not check
        completeness per-h3-dir."""
        from gedih3.gh3builder import _reconcile_granules_from_disk
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 99, 9, 99
        # Split the 8 beams across 3 different h3 partition dirs to confirm
        # the reconcile aggregates correctly.
        h3_parts = ['830001fffffffff', '830002fffffffff', '830003fffffffff']
        for i, beam in enumerate(GEDI_BEAMS):
            _write_tmp_fragment(
                tmp_partitions, h3_parts[i % 3], '2020',
                f'/soc/{_gedi_basename(orb, gran, trk)}',
                basename=f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}.parquet',
            )

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        assert n_flipped == 1
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'

    def test_finalized_partition_metadata_flips_without_beam_check(self, tmp_dir):
        """Granules listed in a finalized partition's ``.metadata.json`` are
        merged-and-complete by construction, so the beam-coverage check is
        bypassed for them (Pass A in _reconcile_granules_from_disk)."""
        from gedih3.gh3builder import _reconcile_granules_from_disk
        from gedih3.config import PARTITION_META_FILENAME

        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)

        # Write a synthetic finalized metadata JSON naming granule (5,5,5).
        orb, gran, trk = 5, 5, 5
        part_dir = os.path.join(h3_dir, 'h3_03=830001fffffffff')
        os.makedirs(part_dir)
        meta_path = os.path.join(part_dir, f'h3_03=830001fffffffff{PARTITION_META_FILENAME}')
        with open(meta_path, 'w') as f:
            json.dump({'granules': [{'orbit': orb, 'granule': gran, 'track': trk}]}, f)

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger)

        assert n_flipped == 1
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'


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
        # Write the full GEDI_BEAMS set per granule so the reconcile sees
        # them as complete (otherwise the partial-beam guard keeps them
        # PENDING — see TestPartialGranuleSafety).
        for orb, gran, trk in granules:
            _write_all_beams(tmp_partitions, '830001fffffffff', '2020', orb, gran, trk)

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

    def test_filename_fast_path(self, tmp_dir, monkeypatch):
        """v0.8.0+ filenames let us skip parquet I/O entirely — granule
        IDs come from the basename. Verify by spying on
        _granule_ids_in_fragment to assert it isn't called."""
        from gedih3.gh3builder import _process_h3_partition
        import gedih3.gh3builder as gh

        partition_dir = os.path.join(tmp_dir, 'h3_03=830001fffffffff')
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

        called = {'n': 0}
        real = gh._granule_ids_in_fragment
        monkeypatch.setattr(gh, '_granule_ids_in_fragment',
                            lambda p: (called.__setitem__('n', called['n'] + 1) or real(p)))

        # New contract: returns ``{(orbit,granule,track): set(beams)}`` so
        # the reconcile can detect partial-write granules.
        result = _process_h3_partition(partition_dir)
        assert result == {
            (200, 1, 5): {'BEAM0000', 'BEAM0001'},
            (201, 2, 6): {'BEAM0000'},
        }
        assert called['n'] == 0, "fast-path filenames must not trigger parquet metadata reads"

    def test_legacy_filename_fallback(self, tmp_dir):
        """Legacy 'part.NNN.parquet' fragments fall through to the parquet-
        metadata-read path. Beam isn't recoverable from the filename, so the
        legacy fallback tags the granule with ``_LEGACY_BEAM_SENTINEL`` —
        the reconcile then treats those granules as complete (preserving
        pre-v0.8.0 resume semantics)."""
        from gedih3.gh3builder import _process_h3_partition, _LEGACY_BEAM_SENTINEL

        partition_dir = os.path.join(tmp_dir, 'h3_03=830002fffffffff')
        for year, (orb, gran, trk), part_idx in [
            ('2020', (300, 1, 5), 1),
            ('2020', (301, 2, 6), 2),
        ]:
            year_dir = os.path.join(partition_dir, f'year={year}')
            os.makedirs(year_dir, exist_ok=True)
            path = os.path.join(year_dir, f'part.{part_idx}.parquet')
            tab = pa.table({
                'shot_number': pa.array(np.arange(5, dtype=np.uint64)),
                'root_file_l2a': pa.array([f'/soc/{_gedi_basename(orb, gran, trk)}'] * 5),
            })
            pq.write_table(tab, path)

        result = _process_h3_partition(partition_dir)
        assert result == {
            (300, 1, 5): {_LEGACY_BEAM_SENTINEL},
            (301, 2, 6): {_LEGACY_BEAM_SENTINEL},
        }

    def test_returns_empty_dict_for_missing_dir(self, tmp_dir):
        from gedih3.gh3builder import _process_h3_partition
        assert _process_h3_partition(os.path.join(tmp_dir, 'does_not_exist')) == {}


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
