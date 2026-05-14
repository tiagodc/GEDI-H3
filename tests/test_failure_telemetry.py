"""
Tests for failure-telemetry primitives added to the build pipeline:

1. ``utils._iter_batches_with_path`` — wraps a pyarrow iter_batches generator
   so any mid-stream error self-identifies with ``[file=<path>]``.
2. ``gh3builder._merge_failure_sentinel_name`` /
   ``gh3builder._emit_merge_failure_sentinel`` /
   ``gh3builder._scan_merge_failure_sentinels`` — per-failed-merge sentinel
   files under ``tmp_dir/_merge_failures/``.
3. ``gh3builder._classify_load_h5_failure`` /
   ``gh3builder._append_granule_failure`` /
   ``gh3builder._read_granule_failures`` — JSONL sidecar of per-granule
   load failures, with structured classification (``missing_var`` vs. ``other``)
   and product inference.

All tests are pure-Python + tmp_path filesystem — no NASA credentials, no Dask.
"""

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gedih3.utils import _iter_batches_with_path
from gedih3 import gh3builder
from gedih3.gh3builder import (
    _MERGE_FAILURES_DIRNAME,
    _GRANULE_FAILURES_FILENAME,
    _merge_failure_sentinel_name,
    _emit_merge_failure_sentinel,
    _scan_merge_failure_sentinels,
    _classify_load_h5_failure,
    _append_granule_failure,
    _read_granule_failures,
)


# ---------------------------------------------------------------------------
# 1. _iter_batches_with_path
# ---------------------------------------------------------------------------

class TestIterBatchesWithPath:
    """Wrapper around a pyarrow ``iter_batches`` generator that re-raises
    mid-stream errors with ``[file=<path>]`` appended to the message."""

    def _make_parquet(self, path, n_rows=200):
        t = pa.table({
            'a': list(range(n_rows)),
            'b': [float(i) * 0.5 for i in range(n_rows)],
        })
        pq.write_table(t, path)

    def test_happy_path_passes_batches_through(self, tmp_path):
        p = str(tmp_path / 'sample.parquet')
        self._make_parquet(p, n_rows=300)
        pf = pq.ParquetFile(p)
        out = list(_iter_batches_with_path(pf.iter_batches(batch_size=100), p))
        assert len(out) >= 1
        total_rows = sum(b.num_rows for b in out)
        assert total_rows == 300

    def test_empty_generator_yields_nothing(self, tmp_path):
        # A drained iterator (StopIteration on first next) yields nothing.
        def empty_iter():
            if False:
                yield None
            return
        out = list(_iter_batches_with_path(empty_iter(), str(tmp_path / 'x.parquet')))
        assert out == []

    def test_exception_surfaces_with_file_path(self, tmp_path):
        bogus_path = str(tmp_path / 'corrupt.parquet')

        def raising_iter():
            yield pa.record_batch([pa.array([1, 2, 3])], names=['a'])
            raise ValueError("boom")

        gen = _iter_batches_with_path(raising_iter(), bogus_path)
        # First batch should pass through.
        first = next(gen)
        assert first.num_rows == 3
        # The next pull raises with [file=...] appended.
        with pytest.raises(ValueError) as excinfo:
            next(gen)
        assert 'boom' in str(excinfo.value)
        assert f'[file={bogus_path}]' in str(excinfo.value)

    def test_exception_type_preserved(self, tmp_path):
        # The wrapper re-raises ``type(e)(...)``. KeyError's repr-wraps its
        # message, but the type must be preserved end-to-end.
        bogus_path = str(tmp_path / 'a.parquet')

        def raising_iter():
            raise KeyError("missing")
            yield None  # pragma: no cover

        gen = _iter_batches_with_path(raising_iter(), bogus_path)
        with pytest.raises(KeyError) as excinfo:
            next(gen)
        assert bogus_path in str(excinfo.value)

    def test_exception_on_truncated_parquet_like_io_error(self, tmp_path):
        # Simulate the real-world case (truncated parquet): pyarrow raises an
        # OSError-class exception mid-stream. The wrapper must surface the
        # filename.
        bogus_path = str(tmp_path / 'truncated.parquet')

        def raising_iter():
            raise OSError("Parquet magic bytes not found in footer")
            yield None  # pragma: no cover

        gen = _iter_batches_with_path(raising_iter(), bogus_path)
        with pytest.raises(OSError) as excinfo:
            next(gen)
        assert 'Parquet magic bytes not found' in str(excinfo.value)
        assert f'[file={bogus_path}]' in str(excinfo.value)


# ---------------------------------------------------------------------------
# 2. GH3_LOG_PROGRESS env var — trivial behavioral test
# ---------------------------------------------------------------------------

class TestGH3LogProgressEnv:
    """The periodic INFO line in ``_write_partitioned_streaming`` is gated by
    ``os.environ.get('GH3_LOG_PROGRESS', '')``. We just confirm the env var
    is readable through ``os.environ.get`` with the same default contract."""

    def test_env_var_default_is_empty_string(self, monkeypatch):
        monkeypatch.delenv('GH3_LOG_PROGRESS', raising=False)
        assert os.environ.get('GH3_LOG_PROGRESS', '') == ''

    def test_env_var_set_is_readable(self, monkeypatch):
        monkeypatch.setenv('GH3_LOG_PROGRESS', '1')
        assert os.environ.get('GH3_LOG_PROGRESS', '') == '1'


# ---------------------------------------------------------------------------
# 3. Merge failure sentinels
# ---------------------------------------------------------------------------

class TestMergeFailureSentinels:
    """Per-failed-merge sentinel files under ``tmp_dir/_merge_failures/``."""

    def test_sentinel_dirname_constant(self):
        assert _MERGE_FAILURES_DIRNAME == '_merge_failures'

    def test_sentinel_name_encodes_partition_path(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(
            tmp_dir, 'h3_03=8366c1fffffffff', 'year=2019'
        )
        name = _merge_failure_sentinel_name(tmp_dir, partition_dir)
        assert name == 'h3_03=8366c1fffffffff__year=2019.fail'

    def test_sentinel_name_strips_trailing_slash(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(
            tmp_dir, 'h3_03=abc', 'year=2020'
        ) + '/'
        name = _merge_failure_sentinel_name(tmp_dir, partition_dir)
        assert name == 'h3_03=abc__year=2020.fail'

    def test_emit_then_scan_round_trip(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        err = RuntimeError("merge exploded")
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, err)

        # Sentinel file exists on disk with the expected name.
        sentinel_dir = os.path.join(tmp_dir, _MERGE_FAILURES_DIRNAME)
        assert os.path.isdir(sentinel_dir)
        expected_name = 'h3_03=abc__year=2020.fail'
        assert os.path.isfile(os.path.join(sentinel_dir, expected_name))

        # _scan picks it up and parses partition_dir + error.
        scan = _scan_merge_failure_sentinels(tmp_dir)
        assert partition_dir in scan
        assert 'RuntimeError' in scan[partition_dir]
        assert 'merge exploded' in scan[partition_dir]

    def test_scan_missing_dir_returns_empty(self, tmp_path):
        # No sentinel dir was created → empty dict, not an error.
        tmp_dir = str(tmp_path)
        assert _scan_merge_failure_sentinels(tmp_dir) == {}

    def test_scan_ignores_non_fail_files(self, tmp_path):
        tmp_dir = str(tmp_path)
        sentinel_dir = os.path.join(tmp_dir, _MERGE_FAILURES_DIRNAME)
        os.makedirs(sentinel_dir)
        # An unrelated file in the same dir.
        with open(os.path.join(sentinel_dir, 'README.txt'), 'w') as f:
            f.write('not a sentinel')
        # And one real sentinel.
        partition_dir = os.path.join(tmp_dir, 'h3_03=xx', 'year=2021')
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, ValueError("x"))

        scan = _scan_merge_failure_sentinels(tmp_dir)
        assert list(scan.keys()) == [partition_dir]

    def test_emit_multiple_partitions(self, tmp_path):
        tmp_dir = str(tmp_path)
        partitions = [
            os.path.join(tmp_dir, 'h3_03=aaa', 'year=2019'),
            os.path.join(tmp_dir, 'h3_03=bbb', 'year=2020'),
            os.path.join(tmp_dir, 'h3_03=ccc', 'year=2021'),
        ]
        for i, p in enumerate(partitions):
            _emit_merge_failure_sentinel(tmp_dir, p, IOError(f"fail-{i}"))
        scan = _scan_merge_failure_sentinels(tmp_dir)
        assert set(scan.keys()) == set(partitions)
        for i, p in enumerate(partitions):
            assert f'fail-{i}' in scan[p]


# ---------------------------------------------------------------------------
# 4. Granule failure classifier + JSONL sidecar
# ---------------------------------------------------------------------------

class TestGranuleFailures:

    def test_classify_missing_var_keyerror(self):
        msg = (
            "Unable to synchronously open object "
            "(object 'l2a_quality_flag_rel3_a10' doesn't exist)"
        )
        soc_dict = {'L2A': '/soc/GEDI02_A_xxx.h5'}
        out = _classify_load_h5_failure(KeyError(msg), soc_dict)
        assert out['kind'] == 'missing_var'
        assert out['var'] == 'l2a_quality_flag_rel3_a10'
        assert out['error_type'] == 'KeyError'
        # KeyError str() wraps the message in quotes; check substring.
        assert 'l2a_quality_flag_rel3_a10' in out['error_message']
        # Product inference: basename not in the message → None.
        assert out['product'] is None

    def test_classify_other_kind_generic(self):
        out = _classify_load_h5_failure(ValueError("something else"), {})
        assert out['kind'] == 'other'
        assert out['var'] is None
        assert out['product'] is None
        assert out['error_type'] == 'ValueError'
        assert out['error_message'] == 'something else'

    def test_classify_product_inference_via_basename(self):
        # When the exception message mentions one of the soc_dict file
        # basenames, the corresponding product code is filled in.
        h5 = '/soc/2020/123/GEDI02_A_2020123010101_O00001_01_T00001_02_005_01_V002.h5'
        msg = f"some error referring to {os.path.basename(h5)} :("
        out = _classify_load_h5_failure(RuntimeError(msg), {'L2A': h5})
        assert out['product'] == 'L2A'
        assert out['kind'] == 'other'
        assert out['error_type'] == 'RuntimeError'

    def test_classify_missing_var_with_product_inference(self):
        h5 = '/soc/2020/123/GEDI02_A_xxx.h5'
        msg = (
            f"Unable to synchronously open object "
            f"(object 'l2a_quality_flag_rel3_a10' doesn't exist) "
            f"in {os.path.basename(h5)}"
        )
        out = _classify_load_h5_failure(KeyError(msg), {'L2A': h5, 'L4A': '/other.h5'})
        assert out['kind'] == 'missing_var'
        assert out['var'] == 'l2a_quality_flag_rel3_a10'
        assert out['product'] == 'L2A'

    def test_classify_empty_soc_dict_safe(self):
        # ``soc_dict`` may be ``None`` or empty — must not raise.
        out = _classify_load_h5_failure(RuntimeError("x"), None)
        assert out['product'] is None
        out2 = _classify_load_h5_failure(RuntimeError("x"), {})
        assert out2['product'] is None

    def test_append_and_read_roundtrip(self, tmp_path):
        tmp_dir = str(tmp_path)
        records = [
            {'kind': 'missing_var', 'var': 'v1', 'product': 'L2A',
             'error_type': 'KeyError', 'error_message': 'one'},
            {'kind': 'other', 'var': None, 'product': None,
             'error_type': 'ValueError', 'error_message': 'two'},
        ]
        _append_granule_failure(tmp_dir, 'O1_G1_T1.0000', records[0])
        _append_granule_failure(tmp_dir, 'O2_G2_T2.0001', records[1])

        out = _read_granule_failures(tmp_dir)
        assert len(out) == 2
        assert out[0]['frag_name'] == 'O1_G1_T1.0000'
        assert out[0]['var'] == 'v1'
        assert out[0]['product'] == 'L2A'
        assert out[1]['frag_name'] == 'O2_G2_T2.0001'
        assert out[1]['kind'] == 'other'

    def test_read_missing_file_returns_empty(self, tmp_path):
        # No JSONL yet → empty list, not an error.
        out = _read_granule_failures(str(tmp_path))
        assert out == []

    def test_read_tolerates_torn_last_line(self, tmp_path):
        tmp_dir = str(tmp_path)
        path = os.path.join(tmp_dir, _GRANULE_FAILURES_FILENAME)
        good = {'frag_name': 'g1', 'kind': 'other', 'var': None,
                'product': None, 'error_type': 'E', 'error_message': 'm'}
        with open(path, 'w') as f:
            f.write(json.dumps(good) + '\n')
            # Torn last line — written but not newline-terminated and not
            # valid JSON (SIGKILL during writer flush).
            f.write('{"frag_name": "g2", "kind": "oth')

        out = _read_granule_failures(tmp_dir)
        # Good line survives; torn line is silently dropped.
        assert len(out) == 1
        assert out[0]['frag_name'] == 'g1'

    def test_read_skips_blank_lines(self, tmp_path):
        tmp_dir = str(tmp_path)
        path = os.path.join(tmp_dir, _GRANULE_FAILURES_FILENAME)
        rec = {'frag_name': 'g', 'kind': 'other', 'var': None,
               'product': None, 'error_type': 'E', 'error_message': 'm'}
        with open(path, 'w') as f:
            f.write('\n')
            f.write(json.dumps(rec) + '\n')
            f.write('   \n')
        out = _read_granule_failures(tmp_dir)
        assert len(out) == 1
        assert out[0]['frag_name'] == 'g'

    def test_jsonl_filename_constant(self):
        assert _GRANULE_FAILURES_FILENAME == '_granule_failures.jsonl'
        # Sanity: re-export is the same object from the module.
        assert gh3builder._GRANULE_FAILURES_FILENAME == '_granule_failures.jsonl'
