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
    _MERGE_FAILED_GRANULES_FILENAME,
    _RECOVERABLE_FRAGMENT_ERROR_MARKERS,
    _merge_failure_sentinel_name,
    _emit_merge_failure_sentinel,
    _scan_merge_failure_sentinels,
    _classify_load_h5_failure,
    _append_granule_failure,
    _read_granule_failures,
    _is_recoverable_fragment_error,
    _granules_in_partition_dir,
    _emit_merge_failed_granules,
    _read_merge_failed_granules,
    apply_merge_failures_to_logger,
    preclean_merge_failures,
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


# ---------------------------------------------------------------------------
# 5. _is_recoverable_fragment_error / _RECOVERABLE_FRAGMENT_ERROR_MARKERS
# ---------------------------------------------------------------------------

class TestRecoverableErrorMarkers:
    """``_is_recoverable_fragment_error`` returns True iff ``str(exc)``
    matches one of the curated markers for known partial-write classes."""

    def test_marker_tuple_contains_curated_set(self):
        # The implementation contract: a tuple including these three markers.
        assert isinstance(_RECOVERABLE_FRAGMENT_ERROR_MARKERS, tuple)
        assert 'Parquet file size is 0 bytes' in _RECOVERABLE_FRAGMENT_ERROR_MARKERS
        assert 'Parquet magic bytes not found in footer' in _RECOVERABLE_FRAGMENT_ERROR_MARKERS
        assert any("Couldn't deserialize thrift" in m
                   for m in _RECOVERABLE_FRAGMENT_ERROR_MARKERS)

    def test_recoverable_markers_match(self):
        # Each curated marker, even wrapped in a generic exception, must
        # trip the classifier.
        for marker in _RECOVERABLE_FRAGMENT_ERROR_MARKERS:
            assert _is_recoverable_fragment_error(OSError(marker)) is True
            # Also as a substring inside a longer message.
            assert _is_recoverable_fragment_error(
                RuntimeError(f"pyarrow blew up: {marker} [file=/x.parquet]")
            ) is True

    def test_non_recoverable_errors_return_false(self):
        # Infrastructure errors and unrelated exceptions are not flipped.
        assert _is_recoverable_fragment_error(OSError("No space left on device")) is False
        assert _is_recoverable_fragment_error(ValueError("schema mismatch")) is False
        assert _is_recoverable_fragment_error(RuntimeError("")) is False


# ---------------------------------------------------------------------------
# 6. _granules_in_partition_dir
# ---------------------------------------------------------------------------

class TestGranulesInPartitionDir:
    """Parse fragment basenames to (orbit, granule, track) tuples."""

    def _touch_parquet(self, path):
        # Minimum-cost stand-in: the function only inspects basenames, not
        # parquet body. An empty file with the right name is sufficient.
        with open(path, 'wb') as f:
            f.write(b'')

    def test_missing_dir_returns_empty_list(self, tmp_path):
        # OSError-on-scandir path: nonexistent partition dir → [].
        out = _granules_in_partition_dir(str(tmp_path / 'does_not_exist'))
        assert out == []

    def test_empty_dir_returns_empty_list(self, tmp_path):
        d = tmp_path / 'h3_03=abc' / 'year=2020'
        d.mkdir(parents=True)
        assert _granules_in_partition_dir(str(d)) == []

    def test_parses_and_dedups_sorted(self, tmp_path):
        d = tmp_path / 'h3_03=abc' / 'year=2020'
        d.mkdir(parents=True)
        # Two beams for the same granule → one (orbit, granule, track).
        self._touch_parquet(d / 'O00001_G00010_T00100.0000.parquet')
        self._touch_parquet(d / 'O00001_G00010_T00100.0001.parquet')
        # Different granule, different orbit/track.
        self._touch_parquet(d / 'O00002_G00020_T00200.0000.parquet')
        # Different orbit, same granule number — still distinct tuple.
        self._touch_parquet(d / 'O00003_G00010_T00300.0000.parquet')

        out = _granules_in_partition_dir(str(d))
        assert out == [
            (1, 10, 100),
            (2, 20, 200),
            (3, 10, 300),
        ]

    def test_ignores_non_matching_and_non_parquet_files(self, tmp_path):
        d = tmp_path / 'h3_03=xyz' / 'year=2021'
        d.mkdir(parents=True)
        # Not parquet.
        (d / 'README.txt').write_text('ignore me')
        # Parquet but not matching the fragment pattern.
        self._touch_parquet(d / 'merged.parquet')
        self._touch_parquet(d / 'O0001_NOT_A_FRAG.parquet')
        # One real fragment.
        self._touch_parquet(d / 'O00007_G00077_T00777.0002.parquet')

        out = _granules_in_partition_dir(str(d))
        assert out == [(7, 77, 777)]

    def test_ignores_subdirectories(self, tmp_path):
        d = tmp_path / 'h3_03=sub' / 'year=2022'
        d.mkdir(parents=True)
        # A nested directory with a parquet-like name must not be picked up.
        nested = d / 'O00009_G00099_T00999.0000.parquet'
        nested.mkdir()
        self._touch_parquet(d / 'O00010_G00100_T01000.0000.parquet')

        out = _granules_in_partition_dir(str(d))
        assert out == [(10, 100, 1000)]


# ---------------------------------------------------------------------------
# 7. _emit / _read merge_failed_granules + apply_merge_failures_to_logger
# ---------------------------------------------------------------------------

class _FakeLogger:
    """Minimal stand-in for ``H3BuildLogger`` — only the ``granule_info``
    list attribute is touched by ``apply_merge_failures_to_logger``."""

    def __init__(self, granule_info):
        self.granule_info = granule_info


class TestMergeFailedGranulesRoundTrip:

    def test_emit_empty_list_is_noop(self, tmp_path):
        # No granules → no file written. Empty-list short-circuit per impl.
        _emit_merge_failed_granules(
            str(tmp_path), '/some/partition', [], RuntimeError("ignored")
        )
        assert _read_merge_failed_granules(str(tmp_path)) == []
        assert not os.path.isfile(
            os.path.join(str(tmp_path), _MERGE_FAILED_GRANULES_FILENAME)
        )

    def test_emit_and_read_roundtrip(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        grans = [(1, 10, 100), (2, 20, 200)]
        _emit_merge_failed_granules(
            tmp_dir, partition_dir, grans, OSError("Parquet file size is 0 bytes")
        )

        out = _read_merge_failed_granules(tmp_dir)
        assert len(out) == 2
        assert {r['orbit'] for r in out} == {1, 2}
        for r in out:
            assert r['partition_dir'] == partition_dir
            assert 'OSError' in r['error']
            assert 'Parquet file size is 0 bytes' in r['error']
            assert {'orbit', 'granule', 'track'}.issubset(r.keys())

    def test_emit_appends_across_calls(self, tmp_path):
        tmp_dir = str(tmp_path)
        _emit_merge_failed_granules(
            tmp_dir, '/p1', [(1, 1, 1)], RuntimeError("a")
        )
        _emit_merge_failed_granules(
            tmp_dir, '/p2', [(2, 2, 2), (3, 3, 3)], RuntimeError("b")
        )
        out = _read_merge_failed_granules(tmp_dir)
        assert len(out) == 3
        assert {(r['orbit'], r['granule'], r['track']) for r in out} == \
               {(1, 1, 1), (2, 2, 2), (3, 3, 3)}

    def test_read_missing_file_returns_empty(self, tmp_path):
        assert _read_merge_failed_granules(str(tmp_path)) == []

    def test_read_tolerates_torn_last_line(self, tmp_path):
        tmp_dir = str(tmp_path)
        path = os.path.join(tmp_dir, _MERGE_FAILED_GRANULES_FILENAME)
        good = {'orbit': 1, 'granule': 10, 'track': 100,
                'partition_dir': '/p', 'error': 'E: m'}
        with open(path, 'w') as f:
            f.write(json.dumps(good) + '\n')
            f.write('{"orbit": 2, "granule": 20, "tra')  # torn

        out = _read_merge_failed_granules(tmp_dir)
        assert len(out) == 1
        assert out[0]['orbit'] == 1

    def test_apply_flips_indexed_to_merge_failed_and_truncates(self, tmp_path):
        tmp_dir = str(tmp_path)
        # Emit a record for one granule.
        _emit_merge_failed_granules(
            tmp_dir, '/p', [(1, 10, 100)],
            OSError("Parquet magic bytes not found in footer")
        )
        logger = _FakeLogger([
            {'orbit': 1, 'granule': 10, 'track': 100, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 20, 'track': 200, 'status': 'PENDING'},
        ])

        flipped = apply_merge_failures_to_logger(logger, tmp_dir)
        assert flipped == 1
        assert logger.granule_info[0]['status'] == 'MERGE_FAILED'
        # PENDING is untouched.
        assert logger.granule_info[1]['status'] == 'PENDING'
        # Sidecar truncated.
        assert not os.path.isfile(
            os.path.join(tmp_dir, _MERGE_FAILED_GRANULES_FILENAME)
        )
        # Re-applying is a no-op (file gone, nothing to flip).
        assert apply_merge_failures_to_logger(logger, tmp_dir) == 0

    def test_apply_only_flips_indexed(self, tmp_path):
        # Granules in non-INDEXED states (PENDING, FAILED, MERGE_FAILED, etc.)
        # must not be flipped, per the contract that ``_reconcile_granules_
        # from_disk`` has already moved already-recovered granules off
        # MERGE_FAILED on resume.
        tmp_dir = str(tmp_path)
        _emit_merge_failed_granules(
            tmp_dir, '/p',
            [(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)],
            RuntimeError("x"),
        )
        logger = _FakeLogger([
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 2, 'track': 2, 'status': 'PENDING'},
            {'orbit': 3, 'granule': 3, 'track': 3, 'status': 'MERGE_FAILED'},
            {'orbit': 4, 'granule': 4, 'track': 4, 'status': 'FAILED'},
        ])
        flipped = apply_merge_failures_to_logger(logger, tmp_dir)
        assert flipped == 1
        statuses = [g['status'] for g in logger.granule_info]
        assert statuses == ['MERGE_FAILED', 'PENDING', 'MERGE_FAILED', 'FAILED']

    def test_apply_with_no_records_returns_zero(self, tmp_path):
        logger = _FakeLogger([
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
        ])
        assert apply_merge_failures_to_logger(logger, str(tmp_path)) == 0
        # Untouched.
        assert logger.granule_info[0]['status'] == 'INDEXED'


# ---------------------------------------------------------------------------
# 8. preclean_merge_failures
# ---------------------------------------------------------------------------

def _write_valid_parquet(path):
    """Write a minimal but valid parquet file."""
    t = pa.table({'a': [1, 2, 3]})
    pq.write_table(t, path)


def _write_truncated_parquet(path):
    """Write a non-zero-byte file that pyarrow cannot open as parquet."""
    with open(path, 'wb') as f:
        # Some bytes but no parquet magic / footer.
        f.write(b'\x00' * 64)


class TestPrecleanMergeFailures:

    def test_no_sentinels_returns_zeros(self, tmp_path):
        out = preclean_merge_failures(str(tmp_path))
        assert out == {'partitions_cleaned': 0,
                       'parquets_removed': 0,
                       'tmps_removed': 0}

    def test_removes_zero_byte_parquet(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        os.makedirs(partition_dir)
        bad = os.path.join(partition_dir, 'O00001_G00010_T00100.0000.parquet')
        with open(bad, 'wb'):
            pass  # 0-byte
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, OSError("bad"))

        out = preclean_merge_failures(tmp_dir)
        assert out['partitions_cleaned'] == 1
        assert out['parquets_removed'] == 1
        assert out['tmps_removed'] == 0
        assert not os.path.isfile(bad)
        # Sentinel dropped.
        assert _scan_merge_failure_sentinels(tmp_dir) == {}

    def test_removes_truncated_parquet_but_keeps_valid(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        os.makedirs(partition_dir)
        good = os.path.join(partition_dir, 'O00001_G00010_T00100.0000.parquet')
        bad = os.path.join(partition_dir, 'O00002_G00020_T00200.0000.parquet')
        _write_valid_parquet(good)
        _write_truncated_parquet(bad)
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, OSError("trunc"))

        out = preclean_merge_failures(tmp_dir)
        assert out['partitions_cleaned'] == 1
        assert out['parquets_removed'] == 1
        assert os.path.isfile(good)
        assert not os.path.isfile(bad)

    def test_removes_tmp_and_merge_tmp_siblings(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        os.makedirs(partition_dir)
        tmp_a = os.path.join(partition_dir, 'merged.parquet.tmp')
        tmp_b = os.path.join(partition_dir, 'merged.parquet.merge.tmp')
        with open(tmp_a, 'wb') as f:
            f.write(b'\x00' * 16)
        with open(tmp_b, 'wb') as f:
            f.write(b'\x00' * 16)
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, OSError("x"))

        out = preclean_merge_failures(tmp_dir)
        assert out['partitions_cleaned'] == 1
        assert out['tmps_removed'] == 2
        assert not os.path.isfile(tmp_a)
        assert not os.path.isfile(tmp_b)

    def test_missing_partition_dir_drops_sentinel_no_action(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=gone', 'year=2020')
        # Partition was rm'd between runs — only the sentinel survives.
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, OSError("gone"))
        # Pre-condition: sentinel is present.
        assert partition_dir in _scan_merge_failure_sentinels(tmp_dir)

        out = preclean_merge_failures(tmp_dir)
        # Per the impl: sentinel is dropped without incrementing counters.
        assert out == {'partitions_cleaned': 0,
                       'parquets_removed': 0,
                       'tmps_removed': 0}
        # Sentinel removed so resume doesn't re-loop.
        assert _scan_merge_failure_sentinels(tmp_dir) == {}

    def test_is_idempotent(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        os.makedirs(partition_dir)
        bad = os.path.join(partition_dir, 'O00001_G00010_T00100.0000.parquet')
        with open(bad, 'wb'):
            pass
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, OSError("x"))

        first = preclean_merge_failures(tmp_dir)
        assert first['partitions_cleaned'] == 1
        # Re-running pre-clean with no fresh sentinel → all-zeros, no error.
        second = preclean_merge_failures(tmp_dir)
        assert second == {'partitions_cleaned': 0,
                          'parquets_removed': 0,
                          'tmps_removed': 0}


# ---------------------------------------------------------------------------
# 9. End-to-end coupling: sentinel + flip-back + preclean + logger fold
# ---------------------------------------------------------------------------

class TestMergeFailureEndToEnd:
    """Proves the contract between the four primitives that together drive
    the L1 resume recovery story for a recoverable merge failure."""

    def test_full_recovery_story_is_idempotent(self, tmp_path):
        tmp_dir = str(tmp_path)
        partition_dir = os.path.join(tmp_dir, 'h3_03=abc', 'year=2020')
        os.makedirs(partition_dir)

        # Two beams of the same granule, plus a second granule in the
        # partition. Mix one valid parquet (must survive) and two bad
        # ones (0-byte + truncated, both must be removed).
        good = os.path.join(partition_dir, 'O00001_G00010_T00100.0000.parquet')
        bad_zero = os.path.join(partition_dir, 'O00001_G00010_T00100.0001.parquet')
        bad_trunc = os.path.join(partition_dir, 'O00002_G00020_T00200.0000.parquet')
        _write_valid_parquet(good)
        with open(bad_zero, 'wb'):
            pass
        _write_truncated_parquet(bad_trunc)

        # Step 1: merge failed — sentinel emitted by _merge_and_finalize.
        err = OSError("Parquet magic bytes not found in footer")
        _emit_merge_failure_sentinel(tmp_dir, partition_dir, err)

        # Step 2: failure handler enumerates granules in the partition and
        # writes the flip-back records.
        grans = _granules_in_partition_dir(partition_dir)
        assert grans == [(1, 10, 100), (2, 20, 200)]
        assert _is_recoverable_fragment_error(err) is True
        _emit_merge_failed_granules(tmp_dir, partition_dir, grans, err)

        # Step 3: preclean removes bad fragments + drops the sentinel.
        pc = preclean_merge_failures(tmp_dir)
        assert pc['partitions_cleaned'] == 1
        assert pc['parquets_removed'] == 2  # zero-byte + truncated
        assert os.path.isfile(good)
        assert not os.path.isfile(bad_zero)
        assert not os.path.isfile(bad_trunc)
        assert _scan_merge_failure_sentinels(tmp_dir) == {}

        # Step 4: CLI folds flip-back records into the logger.
        logger = _FakeLogger([
            {'orbit': 1, 'granule': 10, 'track': 100, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 20, 'track': 200, 'status': 'INDEXED'},
            # A bystander granule that must remain untouched.
            {'orbit': 9, 'granule': 99, 'track': 999, 'status': 'INDEXED'},
        ])
        flipped = apply_merge_failures_to_logger(logger, tmp_dir)
        assert flipped == 2
        assert logger.granule_info[0]['status'] == 'MERGE_FAILED'
        assert logger.granule_info[1]['status'] == 'MERGE_FAILED'
        assert logger.granule_info[2]['status'] == 'INDEXED'

        # Step 5: re-running both is a no-op (idempotency).
        pc2 = preclean_merge_failures(tmp_dir)
        assert pc2 == {'partitions_cleaned': 0,
                       'parquets_removed': 0,
                       'tmps_removed': 0}
        flipped2 = apply_merge_failures_to_logger(logger, tmp_dir)
        assert flipped2 == 0
        # No status regressions.
        statuses = [g['status'] for g in logger.granule_info]
        assert statuses == ['MERGE_FAILED', 'MERGE_FAILED', 'INDEXED']
