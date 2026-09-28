"""Tests for the merge-resume shortcut and merge robustness.

Covers:
- ``_detect_merge_resume_signal`` returns the right signal for L1 and L2
  detection paths, and ``None`` otherwise.
- ``_merge_and_finalize`` skips empty tmp partition dirs without raising.
- ``h3_merge_files`` falls back to a fresh merge when an existing dest
  parquet is unreadable (corrupt) instead of aborting the whole merge phase.
"""
import json
import os
import types

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ---------------------------------------------------------------------------
# _detect_merge_resume_signal — pure logic, no I/O
# ---------------------------------------------------------------------------

class TestDetectMergeResumeSignal:
    def _logger(self, prev_status, granule_info=None):
        # Minimal stand-in — only previous_status and granule_info are read.
        return types.SimpleNamespace(
            previous_status=prev_status,
            granule_info=granule_info or [],
        )

    def test_l1_status_merging(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        signal = _detect_merge_resume_signal(self._logger('MERGING'), tmp_dir)
        assert signal == 'log status MERGING'

    def test_l2_progress_file_with_content(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        progress = os.path.join(tmp_dir, '_merge_progress.txt')
        with open(progress, 'w') as f:
            f.write('/some/h3_a/year=2020\n')
        signal = _detect_merge_resume_signal(self._logger('PROCESSING'), tmp_dir)
        assert signal == 'merge progress file present'

    def test_l2_progress_file_empty_returns_none(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        # Empty file (0 bytes or only blank lines) is not a valid signal.
        progress = os.path.join(tmp_dir, '_merge_progress.txt')
        with open(progress, 'w') as f:
            f.write('\n   \n')
        signal = _detect_merge_resume_signal(self._logger('PROCESSING'), tmp_dir)
        assert signal is None

    def test_no_signal_default(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        signal = _detect_merge_resume_signal(self._logger('PROCESSING'), tmp_dir)
        assert signal is None

    def test_l1_takes_precedence_over_l2(self, tmp_dir):
        """L1 short-circuit: if status is MERGING, return that even if a
        progress file also exists."""
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        with open(os.path.join(tmp_dir, '_merge_progress.txt'), 'w') as f:
            f.write('/some/h3/year=2020\n')
        signal = _detect_merge_resume_signal(self._logger('MERGING'), tmp_dir)
        assert signal == 'log status MERGING'

    def test_merge_failed_veto_blocks_l1(self, tmp_dir):
        """If any granule has status MERGE_FAILED, the shortcut must be
        suppressed even when L1 fires — the merge-only path has no way
        to re-extract them, and looping merge-only against the same
        corrupt fragments would never converge."""
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        granule_info = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'MERGE_FAILED'},
        ]
        signal = _detect_merge_resume_signal(
            self._logger('MERGING', granule_info=granule_info), tmp_dir,
        )
        assert signal is None

    def test_merge_failed_veto_blocks_l2(self, tmp_dir):
        """Same veto applies to the progress-file fallback signal."""
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        with open(os.path.join(tmp_dir, '_merge_progress.txt'), 'w') as f:
            f.write('/some/h3_a/year=2020\n')
        granule_info = [
            {'orbit': 5, 'granule': 1, 'track': 9, 'status': 'MERGE_FAILED'},
        ]
        signal = _detect_merge_resume_signal(
            self._logger('PROCESSING', granule_info=granule_info), tmp_dir,
        )
        assert signal is None

    def test_no_veto_when_only_indexed(self, tmp_dir):
        """A log with only INDEXED granules must NOT block the shortcut —
        nothing needs re-extraction so the merge-only fast path is correct."""
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        granule_info = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'INDEXED'},
        ]
        signal = _detect_merge_resume_signal(
            self._logger('MERGING', granule_info=granule_info), tmp_dir,
        )
        assert signal == 'log status MERGING'

    # ── Veto: pending new product/variable work ────────────────────────
    #
    # Reproduced live: adding a product (e.g. -l4a) to an existing
    # COMPLETED database, in both local-SOC and -dl modes, hit a stale
    # _merge_progress.txt left over from the database's original build in
    # the same tmp directory. L2 misread it as "this request's merge
    # already started", skipped Stage 1/2 extract entirely, and
    # _merge_and_finalize found nothing new to merge — reporting SUCCESS
    # and recording the product as added while not one of its columns was
    # ever written to any partition file.

    def test_pending_new_work_vetoes_l1(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        signal = _detect_merge_resume_signal(
            self._logger('MERGING'), tmp_dir, has_pending_new_work=True,
        )
        assert signal is None

    def test_pending_new_work_vetoes_l2(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        with open(os.path.join(tmp_dir, '_merge_progress.txt'), 'w') as f:
            f.write('/some/h3_a/year=2020\n')
        signal = _detect_merge_resume_signal(
            self._logger('PROCESSING'), tmp_dir, has_pending_new_work=True,
        )
        assert signal is None

    def test_no_pending_new_work_does_not_veto(self, tmp_dir):
        """Default (has_pending_new_work=False) preserves prior behavior —
        a plain crash-resume with no new product/variable request still
        takes the shortcut."""
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        signal = _detect_merge_resume_signal(self._logger('MERGING'), tmp_dir)
        assert signal == 'log status MERGING'


# ---------------------------------------------------------------------------
# _merge_and_finalize — empty tmp dirs are skipped
# ---------------------------------------------------------------------------

def _write_minimal_partition(parent, h3_part, year, n=5):
    """Create a tmp partition with one valid parquet fragment.

    Schema mirrors the real stage-1 output enough that
    ``h3_write_metadata`` (called inside ``h3_merge_files``) succeeds:
    ``shot_number`` + ``root_file_l2a`` + ``datetime`` are the columns
    it reads.
    """
    leaf = os.path.join(parent, f'h3_03={h3_part}', f'year={year}')
    os.makedirs(leaf, exist_ok=True)
    path = os.path.join(leaf, 'part.0.parquet')
    granule_path = (
        f'/soc/GEDI02_A_{year}001000000_O00077_03_T00099_02_003_02_V002.h5'
    )
    df = pd.DataFrame({
        'shot_number': np.arange(n, dtype=np.uint64),
        'root_file_l2a': [granule_path] * n,
        'datetime': pd.to_datetime(['2020-01-01'] * n),
        'agbd_l4a': np.random.uniform(0, 300, n),
    })
    pq.write_table(pa.Table.from_pandas(df), path)
    return leaf, path


@pytest.mark.integration
class TestMergeAndFinalizeSkipsEmptyDirs:
    """Small integration test — needs a real Dask client because
    _merge_and_finalize submits each partition as a Dask task."""

    def test_empty_year_dir_is_skipped(self, tmp_dir):
        from dask.distributed import Client
        from gedih3.gh3builder import _merge_and_finalize
        parquet_dir = os.path.join(tmp_dir, 'tmp', 'partitions')
        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)

        # One real partition with content.
        _write_minimal_partition(parquet_dir, '830001fffffffff', '2020')
        # One empty year dir (no parquets inside).
        empty_leaf = os.path.join(parquet_dir, 'h3_03=830002fffffffff', 'year=2021')
        os.makedirs(empty_leaf)

        with Client(n_workers=2, threads_per_worker=1, processes=False):
            h3_files = _merge_and_finalize(parquet_dir, h3_dir)

        # Returns the list of merged h3 parquet files.
        assert any('830001fffffffff' in f for f in h3_files)
        # Empty partition didn't get merged.
        assert not any('830002fffffffff' in f for f in h3_files)


# ---------------------------------------------------------------------------
# h3_merge_files — corrupt dest is recovered
# ---------------------------------------------------------------------------

class TestCorruptDestFallback:
    def test_corrupt_existing_dest_is_overwritten(self, tmp_dir):
        """A corrupt parquet at the merge destination should not abort the
        merge — it should be discarded and the tmp fragments merged fresh."""
        from gedih3.gh3builder import h3_merge_files

        # h3_merge_files expects in_dir at <tmp_root>/h3_*/year=*/ with a
        # trailing slash (it's what glob.glob('.../*/*/') returns).
        in_dir = os.path.join(tmp_dir, 'h3_03=830001fffffffff', 'year=2020') + '/'
        out_dir = os.path.join(tmp_dir, 'database')

        # Valid fragment in the input dir (schema matches stage-1 enough
        # for h3_write_metadata to succeed after the merge).
        os.makedirs(in_dir, exist_ok=True)
        in_path = os.path.join(in_dir, 'part.0.parquet')
        granule_path = (
            '/soc/GEDI02_A_2020001000000_O00077_03_T00099_02_003_02_V002.h5'
        )
        df = pd.DataFrame({
            'shot_number': np.arange(7, dtype=np.uint64),
            'root_file_l2a': [granule_path] * 7,
            'datetime': pd.to_datetime(['2020-01-01'] * 7),
            'agbd_l4a': np.random.uniform(0, 300, 7),
        })
        pq.write_table(pa.Table.from_pandas(df), in_path)

        # Corrupt destination (looks like a parquet file but contains
        # garbage). Place it at the path h3_merge_files would target.
        odir = os.path.join(out_dir, 'h3_03=830001fffffffff', 'year=2020')
        os.makedirs(odir, exist_ok=True)
        corrupt_dest = os.path.join(odir, '830001fffffffff.2020.0.parquet')
        with open(corrupt_dest, 'wb') as f:
            f.write(b'PAR1\x00\x00not a real parquet\x00')

        result = h3_merge_files(in_dir, out_dir, rm_src=False, replace=False)

        assert result == corrupt_dest
        # Result file is now a real, readable parquet — the corrupt one was
        # detected and overwritten with the freshly-merged content.
        meta = pq.ParquetFile(corrupt_dest).metadata
        assert meta.num_rows == 7


# ---------------------------------------------------------------------------
# _cleanup_merged_tmp — tmp/partitions is dropped after a clean merge
# ---------------------------------------------------------------------------
#
# A successful build used to leave _merge_progress.txt, one _complete/
# sentinel per granule × beam and the emptied h3_* dirs behind. The stale
# progress file is the L2 merge-resume signal: the next update's gh3_build
# took the merge-only shortcut and silently did nothing.

def _scaffold(parquet_dir, n_sentinels=3):
    """Post-merge tmp/partitions as a clean build leaves it."""
    from gedih3.gh3builder import _COMPLETE_SENTINEL_DIRNAME
    os.makedirs(os.path.join(parquet_dir, 'h3_03=830001fffffffff'))
    complete = os.path.join(parquet_dir, _COMPLETE_SENTINEL_DIRNAME)
    os.makedirs(complete)
    for i in range(n_sentinels):
        open(os.path.join(complete, f'O{i:05d}_G01_T00001.BEAM0000.done'), 'w').close()
    with open(os.path.join(parquet_dir, '_merge_progress.txt'), 'w') as f:
        f.write(os.path.join(parquet_dir, 'h3_03=830001fffffffff', 'year=2020') + '\n')


class TestCleanupMergedTmp:
    def test_clean_merge_removes_partitions_dir_only(self, tmp_dir):
        from gedih3.gh3builder import _cleanup_merged_tmp
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir)
        # A user-supplied --tmpdir may hold unrelated files next to partitions/.
        sibling = os.path.join(tmp_dir, 'update_log.log')
        open(sibling, 'w').close()

        _cleanup_merged_tmp(parquet_dir, merge_failed=False)

        assert not os.path.exists(parquet_dir)
        assert os.path.isfile(sibling)

    def test_clean_merge_clears_l2_resume_signal(self, tmp_dir):
        from gedih3.cli.gh3_build import _detect_merge_resume_signal
        from gedih3.gh3builder import _cleanup_merged_tmp
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir)
        log = types.SimpleNamespace(previous_status='COMPLETED', granule_info=[])
        assert _detect_merge_resume_signal(log, parquet_dir) == 'merge progress file present'

        _cleanup_merged_tmp(parquet_dir, merge_failed=False)

        assert _detect_merge_resume_signal(log, parquet_dir) is None

    def test_merge_failure_keeps_everything(self, tmp_dir):
        from gedih3.gh3builder import _cleanup_merged_tmp
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir)

        _cleanup_merged_tmp(parquet_dir, merge_failed=True)

        assert os.path.isfile(os.path.join(parquet_dir, '_merge_progress.txt'))
        assert os.path.isdir(os.path.join(parquet_dir, 'h3_03=830001fffffffff'))

    def test_unfolded_merge_failed_granules_keeps_everything(self, tmp_dir):
        """A merge-only resume after a crash: preclean dropped the sentinels,
        but the granule flip-back sidecar still awaits the CLI fold."""
        from gedih3.gh3builder import _cleanup_merged_tmp, _MERGE_FAILED_GRANULES_FILENAME
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir)
        sidecar = os.path.join(parquet_dir, _MERGE_FAILED_GRANULES_FILENAME)
        with open(sidecar, 'w') as f:
            f.write(json.dumps({'orbit': 1, 'granule': 1, 'track': 1}) + '\n')

        _cleanup_merged_tmp(parquet_dir, merge_failed=False)

        assert os.path.isfile(sidecar)
        assert os.path.isfile(os.path.join(parquet_dir, '_merge_progress.txt'))

    def test_granule_failures_keep_forensics_but_drop_progress(self, tmp_dir):
        from gedih3.gh3builder import _cleanup_merged_tmp, _GRANULE_FAILURES_FILENAME
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir)
        sidecar = os.path.join(parquet_dir, _GRANULE_FAILURES_FILENAME)
        with open(sidecar, 'w') as f:
            f.write(json.dumps({'kind': 'other'}) + '\n')

        _cleanup_merged_tmp(parquet_dir, merge_failed=False)

        assert os.path.isfile(sidecar)
        assert not os.path.exists(os.path.join(parquet_dir, '_merge_progress.txt'))

    def test_missing_dir_is_noop(self, tmp_dir):
        from gedih3.gh3builder import _cleanup_merged_tmp
        _cleanup_merged_tmp(os.path.join(tmp_dir, 'partitions'), merge_failed=False)

    def test_wide_tree_fans_out_to_workers(self, tmp_dir, monkeypatch):
        from dask.distributed import Client
        import gedih3.gh3builder as gb
        monkeypatch.setattr(gb, '_REMOVE_FANOUT_MIN_ENTRIES', 2)
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        _scaffold(parquet_dir, n_sentinels=50)
        import gedih3.parallel as gp
        dispatched = []
        real_parallel_map = gp.parallel_map

        # Spy on the driver side: dask serializes the worker fn even for
        # in-process workers, so a spy on _remove_path could not report back.
        def _spy(items, fn, **kw):
            dispatched.extend(items)
            return real_parallel_map(items, fn, **kw)

        monkeypatch.setattr(gp, 'parallel_map', _spy)
        with Client(n_workers=2, threads_per_worker=1, processes=False,
                    dashboard_address=None, silence_logs='ERROR'):
            gb._cleanup_merged_tmp(parquet_dir, merge_failed=False)

        assert not os.path.exists(parquet_dir)
        assert len(dispatched) >= 50


class TestMergeAndFinalizeCleansTmp:
    def test_successful_merge_leaves_no_tmp_partitions(self, tmp_dir):
        from dask.distributed import Client
        from gedih3.gh3builder import _merge_and_finalize
        parquet_dir = os.path.join(tmp_dir, 'tmp', 'partitions')
        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)
        _write_minimal_partition(parquet_dir, '830001fffffffff', '2020')

        with Client(n_workers=2, threads_per_worker=1, processes=False,
                    dashboard_address=None, silence_logs='ERROR'):
            h3_files = _merge_and_finalize(parquet_dir, h3_dir)

        # The returned paths are derived from _merge_progress.txt, which the
        # cleanup deletes — it must run after the derivation.
        assert any('830001fffffffff' in f for f in h3_files)
        assert all(os.path.isfile(f) for f in h3_files)
        assert not os.path.exists(parquet_dir)
