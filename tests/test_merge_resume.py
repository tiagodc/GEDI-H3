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
    def _logger(self, prev_status):
        # Minimal stand-in — only previous_status is read.
        return types.SimpleNamespace(previous_status=prev_status)

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
