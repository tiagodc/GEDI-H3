"""Tests for the streaming partition writer.

Covers:
* Shared helpers (frag_name, sentinel path/emit/scan, canonical schema).
* Worker function (_write_one_granule_beam) — happy path, load failure,
  empty load, atomic write resilience, hive layout.
* Reconcile sentinel-aware mode — sentinel completeness, fragments-without-
  sentinels safety, legacy migration emits sentinels.
* Feature flag dispatch (GH3_WRITE_STREAMING).

Tests use synthetic pandas DataFrames + monkey-patched load_h5_merged to
avoid HDF5 fixture cost. Coverage of the byte-equivalence vs legacy
to_parquet path is left as an integration concern (out of scope for
unit tests; should run as a hand-curated soak before flipping the flag
default).
"""
import json
import os

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from conftest import make_build_log


# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------

def _make_synthetic_df(n=20, granule_path='/soc/GEDI02_A_2020001000000_O00099_01_T00005_02_003_02_V002.h5'):
    """Return a pandas DataFrame matching what load_h5_merged would produce
    for one (granule, beam) tuple (post by_beam=True, suffix_all=True).
    Lat/lon span ~1° so multiple H3 cells get touched at res=12 (the test
    target for groupby fan-out)."""
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        'shot_number': np.arange(n, dtype=np.uint64),
        'lat_lowestmode_l2a': rng.uniform(0.0, 0.5, n),
        'lon_lowestmode_l2a': rng.uniform(-50.0, -49.5, n),
        'delta_time_l2a': rng.uniform(1e9, 1.1e9, n),
        'root_file_l2a': [granule_path] * n,
        'agbd_l4a': rng.uniform(0, 300, n),
        'rh_098_l2a': rng.uniform(0, 50, n),
    })


def _soc_dict_for_granule(orbit=99, granule=1, track=5):
    base = f"GEDI02_A_2020001000000_O{orbit:05d}_{granule:02d}_T{track:05d}_02_003_02_V002.h5"
    return {'L2A': f'/soc/{base}', 'L4A': f'/soc/{base.replace("02_A_", "04_A_")}'}


# ===========================================================================
# 1. Shared helpers
# ===========================================================================

class TestStreamingHelpers:
    def test_frag_name_format_matches_regex(self):
        from gedih3.gh3builder import _granule_beam_frag_name, _FRAGMENT_BASENAME_RE
        for beam in ['BEAM0000', 'BEAM1011']:
            name = _granule_beam_frag_name(_soc_dict_for_granule(99, 1, 5), beam)
            assert name is not None
            assert _FRAGMENT_BASENAME_RE.match(f'{name}.parquet') is not None
            assert name == f'O00099_G01_T00005.{beam}'

    def test_frag_name_returns_none_on_unparseable_path(self):
        from gedih3.gh3builder import _granule_beam_frag_name
        assert _granule_beam_frag_name({'L2A': '/not/a/gedi/file.h5'}, 'BEAM0000') is None

    def test_sentinel_path_under_complete_subdir(self, tmp_dir):
        from gedih3.gh3builder import _complete_sentinel_path
        p = _complete_sentinel_path(tmp_dir, 'O00001_G01_T00001.BEAM0000')
        assert p == os.path.join(tmp_dir, '_complete', 'O00001_G01_T00001.BEAM0000.done')

    def test_emit_and_scan_sentinels_idempotent(self, tmp_dir):
        from gedih3.gh3builder import _emit_complete_sentinel, _scan_complete_sentinels
        # No sentinel dir yet → empty scan.
        assert _scan_complete_sentinels(tmp_dir) == set()
        # Emit two, then a third re-emitting the first — must not error or duplicate.
        _emit_complete_sentinel(tmp_dir, 'O00001_G01_T00001.BEAM0000')
        _emit_complete_sentinel(tmp_dir, 'O00001_G01_T00001.BEAM0001')
        _emit_complete_sentinel(tmp_dir, 'O00001_G01_T00001.BEAM0000')  # idempotent
        assert _scan_complete_sentinels(tmp_dir) == {
            'O00001_G01_T00001.BEAM0000',
            'O00001_G01_T00001.BEAM0001',
        }

    def test_canonical_schema_drops_partition_cols(self):
        from gedih3.gh3builder import _canonical_write_schema
        meta = pd.DataFrame({
            'shot_number': pd.array([], dtype='uint64'),
            'agbd_l4a': pd.array([], dtype='float64'),
            'h3_03': pd.array([], dtype='object'),
            'year': pd.array([], dtype='int32'),
        })
        schema = _canonical_write_schema(meta, part=3)
        names = set(schema.names)
        assert 'shot_number' in names
        assert 'agbd_l4a' in names
        # Partition columns must NOT appear in the body schema.
        assert 'h3_03' not in names
        assert 'year' not in names

    def test_streaming_enabled_env_var(self, monkeypatch):
        from gedih3.gh3builder import _streaming_enabled
        for val in ('1', 'true', 'on', 'yes', 'TRUE', 'On'):
            monkeypatch.setenv('GH3_WRITE_STREAMING', val)
            assert _streaming_enabled() is True
        for val in ('0', 'false', 'off', 'no', ''):
            monkeypatch.setenv('GH3_WRITE_STREAMING', val)
            assert _streaming_enabled() is False
        monkeypatch.delenv('GH3_WRITE_STREAMING', raising=False)
        assert _streaming_enabled() is False

    def test_streaming_batch_size_default_and_override(self, monkeypatch):
        from gedih3.gh3builder import _streaming_batch_size
        monkeypatch.delenv('GH3_WRITE_STREAMING_BATCH', raising=False)
        assert _streaming_batch_size() == 2000
        monkeypatch.setenv('GH3_WRITE_STREAMING_BATCH', '500')
        assert _streaming_batch_size() == 500
        monkeypatch.setenv('GH3_WRITE_STREAMING_BATCH', 'garbage')
        assert _streaming_batch_size() == 2000  # falls back on parse failure


# ===========================================================================
# 2. Worker function _write_one_granule_beam
# ===========================================================================

class TestStreamingWorker:
    def _invoke_worker(self, tmp_dir, h3_dir, monkeypatch, df_override=None,
                       raise_on_load=False, frag_name=None, schema=None,
                       spatial_tiles=None):
        """Call the worker with a monkey-patched load_h5_merged. Returns the
        worker's stats dict."""
        import gedih3.gh3builder as gh
        soc = _soc_dict_for_granule()
        beam = 'BEAM0000'
        if frag_name is None:
            frag_name = gh._granule_beam_frag_name(soc, beam)

        def fake_load(prod_files, product_vars=None, which_beams=None,
                      shots=None, dropna=True, suffix_all=False):
            if raise_on_load:
                raise RuntimeError("simulated load failure")
            return df_override if df_override is not None else _make_synthetic_df()

        monkeypatch.setattr(gh, 'load_h5_merged', fake_load)

        os.makedirs(h3_dir, exist_ok=True)
        return gh._write_one_granule_beam(
            (soc, beam, frag_name),
            product_vars={'L2A': ['rh_098', 'shot_number'], 'L4A': ['agbd', 'shot_number']},
            res=12, part=3,
            tmp_dir=tmp_dir, h3_dir=h3_dir,
            lat_col='lat_lowestmode_l2a',
            lon_col='lon_lowestmode_l2a',
            dat_col='delta_time_l2a',
            spatial_h3_tiles=spatial_tiles,
            skip_check_enabled=False,
            schema=schema,
        )

    def test_writes_leaves_and_sentinel_on_success(self, tmp_dir, monkeypatch):
        from gedih3.gh3builder import _scan_complete_sentinels
        h3_dir = os.path.join(tmp_dir, 'database')
        partitions = os.path.join(tmp_dir, 'partitions')
        stats = self._invoke_worker(partitions, h3_dir, monkeypatch)

        assert stats['error'] is None
        assert stats['skipped'] is False
        assert stats['leaves'] > 0
        assert stats['rows'] == 20  # all synthetic rows kept

        # Sentinel was emitted only after all leaves committed.
        assert _scan_complete_sentinels(partitions) == {stats['frag_name']}

        # At least one h3_03=<cell>/year=<yyyy>/<frag>.parquet exists.
        leaves = []
        for h3_dir_entry in os.scandir(partitions):
            if not h3_dir_entry.is_dir() or not h3_dir_entry.name.startswith('h3_03='):
                continue
            for year_entry in os.scandir(h3_dir_entry.path):
                if year_entry.name.startswith('year='):
                    for fe in os.scandir(year_entry.path):
                        if fe.name.endswith('.parquet'):
                            leaves.append(fe.path)
        assert len(leaves) == stats['leaves']

    def test_no_sentinel_on_load_failure(self, tmp_dir, monkeypatch):
        from gedih3.gh3builder import _scan_complete_sentinels
        partitions = os.path.join(tmp_dir, 'partitions')
        stats = self._invoke_worker(partitions, os.path.join(tmp_dir, 'database'),
                                    monkeypatch, raise_on_load=True)

        assert stats['skipped'] is True
        assert stats['error'] is not None
        assert stats['leaves'] == 0
        # No sentinel emitted — next resume re-runs this task.
        assert _scan_complete_sentinels(partitions) == set()

    def test_no_sentinel_when_load_returns_empty(self, tmp_dir, monkeypatch):
        from gedih3.gh3builder import _scan_complete_sentinels
        partitions = os.path.join(tmp_dir, 'partitions')
        stats = self._invoke_worker(
            partitions, os.path.join(tmp_dir, 'database'), monkeypatch,
            df_override=pd.DataFrame(),
        )
        assert stats['skipped'] is True
        assert _scan_complete_sentinels(partitions) == set()

    def test_no_sentinel_when_spatial_filter_drops_all_rows(self, tmp_dir, monkeypatch):
        from gedih3.gh3builder import _scan_complete_sentinels
        partitions = os.path.join(tmp_dir, 'partitions')
        # Spatial tile that doesn't intersect any synthetic point.
        stats = self._invoke_worker(
            partitions, os.path.join(tmp_dir, 'database'), monkeypatch,
            spatial_tiles=['836021fffffffff'],  # arbitrary h3 cell that won't match
        )
        assert stats['skipped'] is True
        assert _scan_complete_sentinels(partitions) == set()

    def test_atomic_write_no_orphan_tmp_files(self, tmp_dir, monkeypatch):
        """When pq.write_table raises mid-leaf, AtomicFileWriter.__exit__
        cleans up the .tmp; no half-written .parquet remains; no sentinel
        emitted."""
        import gedih3.gh3builder as gh
        from gedih3.gh3builder import _scan_complete_sentinels

        partitions = os.path.join(tmp_dir, 'partitions')
        h3_dir = os.path.join(tmp_dir, 'database')

        # Patch pq.write_table to raise on 2nd call (1st leaf succeeds).
        real_write = gh.pq.write_table
        call_count = {'n': 0}
        def flaky_write(*args, **kwargs):
            call_count['n'] += 1
            if call_count['n'] >= 2:
                raise IOError("simulated disk full")
            return real_write(*args, **kwargs)
        monkeypatch.setattr(gh.pq, 'write_table', flaky_write)

        with pytest.raises(IOError):
            self._invoke_worker(partitions, h3_dir, monkeypatch)

        # No sentinel — worker exception bypassed the final emit step.
        assert _scan_complete_sentinels(partitions) == set()
        # No half-written .tmp leftover anywhere.
        tmp_leftovers = []
        for root, _dirs, files in os.walk(partitions):
            for f in files:
                if f.endswith('.tmp'):
                    tmp_leftovers.append(os.path.join(root, f))
        assert tmp_leftovers == [], f"orphan .tmp files: {tmp_leftovers}"


# ===========================================================================
# 3. Reconcile sentinel-aware mode
# ===========================================================================

def _logger_with_pending(h3_dir, granules):
    """Build log + H3BuildLogger with a list of (orbit, granule, track) granules
    as PENDING. Returns the logger."""
    from gedih3.logger import H3BuildLogger
    make_build_log(
        h3_dir,
        status='PARTITIONING',
        granules=[{'orbit': g[0], 'granule': g[1], 'track': g[2], 'status': 'PENDING'}
                  for g in granules],
        h3_partition_ids=[],
    )
    return H3BuildLogger(product_vars=None, dir=h3_dir)


def _emit_synthetic_fragments(tmp_partitions, h3_cell, year, orbit, granule, track, beams):
    """Write zero-row parquet fragments for the given beams under one
    h3_03=<cell>/year=<year>/ directory. Sufficient for reconcile to
    detect the (orbit, granule, track, beam) tuple via filename regex."""
    import pyarrow as pa
    leaf_dir = os.path.join(tmp_partitions, f'h3_03={h3_cell}', f'year={year}')
    os.makedirs(leaf_dir, exist_ok=True)
    table = pa.table({'shot_number': pa.array([0], type=pa.uint64())})
    for beam in beams:
        basename = f'O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}.parquet'
        pq.write_table(table, os.path.join(leaf_dir, basename))


class TestReconcileSentinelMode:
    def test_all_sentinels_present_flips_granule(self, tmp_dir):
        """The streaming-completeness contract: granule is INDEXED iff every
        expected beam has its sentinel."""
        from gedih3.gh3builder import _reconcile_granules_from_disk, _emit_complete_sentinel
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 42, 7, 13
        # Fragments AND sentinels for all 8 beams.
        _emit_synthetic_fragments(tmp_partitions, '830001fffffffff', '2020',
                                  orb, gran, trk, GEDI_BEAMS)
        for beam in GEDI_BEAMS:
            _emit_complete_sentinel(tmp_partitions,
                                    f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}')

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        assert n_flipped == 1
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'

    def test_partial_sentinels_does_not_flip(self, tmp_dir):
        """3 of 8 sentinels (e.g. streaming worker killed before completing
        the remaining 5 (granule × beam) tasks) → granule stays PENDING."""
        from gedih3.gh3builder import _reconcile_granules_from_disk, _emit_complete_sentinel
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 42, 7, 13
        _emit_synthetic_fragments(tmp_partitions, '830001fffffffff', '2020',
                                  orb, gran, trk, GEDI_BEAMS[:3])
        for beam in GEDI_BEAMS[:3]:
            _emit_complete_sentinel(tmp_partitions,
                                    f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}')

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)
        assert n_flipped == 0
        assert h3_logger.granule_info[0]['status'] == 'PENDING'

    def test_fragments_without_sentinels_in_sentinel_mode_does_not_flip(self, tmp_dir):
        """THE CRITICAL DATA-LOSS GUARD: once sentinel mode is active (i.e.
        the _complete dir exists because at least one streaming task has
        completed), a granule with all 8 beams as fragments-on-disk but NO
        sentinels is treated as partial-and-unsafe, NOT as indexed.

        This rules out the failure mode where a streaming worker writes
        leaves of N beams but is killed before any sentinel is emitted —
        the surviving fragments without sentinels must not be misread as
        a fully-committed granule."""
        from gedih3.gh3builder import _reconcile_granules_from_disk, _emit_complete_sentinel
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        # Granule A: all 8 fragments + all 8 sentinels (fully complete).
        a_orb, a_gran, a_trk = 1, 1, 1
        _emit_synthetic_fragments(tmp_partitions, '830001fffffffff', '2020',
                                  a_orb, a_gran, a_trk, GEDI_BEAMS)
        for beam in GEDI_BEAMS:
            _emit_complete_sentinel(tmp_partitions,
                                    f'O{a_orb:05d}_G{a_gran:02d}_T{a_trk:05d}.{beam}')

        # Granule B: all 8 fragments but NO sentinels (streaming worker
        # was killed mid-task — fragments survive but completion uncertain).
        b_orb, b_gran, b_trk = 2, 2, 2
        _emit_synthetic_fragments(tmp_partitions, '830002fffffffff', '2020',
                                  b_orb, b_gran, b_trk, GEDI_BEAMS)

        h3_logger = _logger_with_pending(h3_dir, [(a_orb, a_gran, a_trk),
                                                  (b_orb, b_gran, b_trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        # Only granule A should be flipped. B stays PENDING for safety.
        assert n_flipped == 1
        statuses = {(g['orbit'], g['granule'], g['track']): g['status']
                    for g in h3_logger.granule_info}
        assert statuses[(a_orb, a_gran, a_trk)] == 'INDEXED'
        assert statuses[(b_orb, b_gran, b_trk)] == 'PENDING'

    def test_legacy_migration_emits_sentinels(self, tmp_dir):
        """First resume after upgrading: tmp tree has fragments from a
        legacy ddf.to_parquet build but no _complete dir. Reconcile should
        flip complete granules per the legacy heuristic AND emit sentinels
        for them. The next reconcile call is then sentinel-aware."""
        from gedih3.gh3builder import (
            _reconcile_granules_from_disk, _scan_complete_sentinels,
            _COMPLETE_SENTINEL_DIRNAME,
        )
        from gedih3.config import GEDI_BEAMS

        h3_dir = os.path.join(tmp_dir, 'database')
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        os.makedirs(h3_dir)

        orb, gran, trk = 88, 8, 88
        _emit_synthetic_fragments(tmp_partitions, '830001fffffffff', '2020',
                                  orb, gran, trk, GEDI_BEAMS)
        # No _complete dir → legacy migration mode.
        assert not os.path.exists(os.path.join(tmp_partitions, _COMPLETE_SENTINEL_DIRNAME))

        h3_logger = _logger_with_pending(h3_dir, [(orb, gran, trk)])
        n_flipped = _reconcile_granules_from_disk(h3_dir, h3_logger, tmp_dir=tmp_partitions)

        # Granule flipped per legacy heuristic.
        assert n_flipped == 1
        assert h3_logger.granule_info[0]['status'] == 'INDEXED'

        # AND sentinels were emitted for each beam → next reconcile is
        # sentinel-aware.
        emitted = _scan_complete_sentinels(tmp_partitions)
        expected = {f'O{orb:05d}_G{gran:02d}_T{trk:05d}.{beam}' for beam in GEDI_BEAMS}
        assert emitted == expected


# ===========================================================================
# 4. Feature flag dispatch
# ===========================================================================

class TestStreamingDispatch:
    def test_streaming_enabled_returns_false_by_default(self, monkeypatch):
        from gedih3.gh3builder import _streaming_enabled
        monkeypatch.delenv('GH3_WRITE_STREAMING', raising=False)
        assert _streaming_enabled() is False
