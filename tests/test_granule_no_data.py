"""``NO_DATA`` granule status: a granule Stage 1 read in full that yielded zero rows.

Safety contract: a wrong ``NO_DATA`` is silent data loss, a missing one only a
re-read. So the status is set solely from positive evidence (every beam task
proven complete this run, the partition metadata verified to list every granule
it holds, no merge failure pending) and every doubt leaves the granule PENDING.
"""
import json
import logging
import os
import shutil
import types

import pytest

from conftest import make_build_log, make_partition_dir
from gedih3.config import BUILD_LOG_FILENAME, GEDI_BEAMS
from gedih3.logger import H3BuildLogger, GRANULE_STATUS_NO_DATA

K1, K2, K3, K4 = (1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)


@pytest.fixture
def client():
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    c = Client(cluster)
    yield c
    c.close()
    cluster.close()


def _g(key, status, **extra):
    return {'orbit': key[0], 'granule': key[1], 'track': key[2], 'status': status, **extra}


def _logger(tmp_dir, granules, **kw):
    make_build_log(tmp_dir, granules=granules, **kw)
    return H3BuildLogger(product_vars=None, dir=tmp_dir)


def _statuses(h):
    return {(g['orbit'], g['granule'], g['track']): g['status'] for g in h.granule_info}


def _db_listing(tmp_dir, keys):
    """One partition whose per-year and partition metadata both list ``keys``."""
    part_dir, _ = make_partition_dir(
        tmp_dir, granules=[{'orbit': k[0], 'granule': k[1], 'track': k[2]} for k in keys])
    meta = [f for f in os.listdir(part_dir) if f.endswith('.metadata.json')][0]
    year_dir = os.path.join(part_dir, 'year=2020')
    shutil.copy(os.path.join(part_dir, meta), os.path.join(year_dir, '830.2020.0.metadata.json'))
    return part_dir


# ---------------------------------------------------------------------------
# mark_no_data transitions
# ---------------------------------------------------------------------------

class TestMarkNoData:
    def _marked(self, tmp_dir, granules, listed, incomplete, complete, db_keys=(K1,)):
        h = _logger(tmp_dir, granules)
        _db_listing(tmp_dir, db_keys)
        h.set_post_build_info(verify_observed=True)
        return h, h.mark_no_data(listed, incomplete, complete)

    def test_pending_complete_and_unobserved_becomes_no_data(self, tmp_dir):
        h, out = self._marked(
            tmp_dir, [_g(K1, 'PENDING'), _g(K2, 'PENDING', products={'L4A': 'MISSING_SOURCE'},
                                            fill_attempts={'L4A': 1})],
            listed={K1, K2}, incomplete=set(), complete={K1, K2})
        assert out == {'marked': 1, 'reopened': 0}
        st = _statuses(h)
        assert st[K1] == 'INDEXED'                       # observed in a partition: has rows
        assert st[K2] == GRANULE_STATUS_NO_DATA
        g2 = [g for g in h.granule_info if g['orbit'] == 2][0]
        assert 'products' not in g2 and 'fill_attempts' not in g2

    def test_incomplete_granule_stays_pending(self, tmp_dir):
        h, out = self._marked(tmp_dir, [_g(K2, 'PENDING')], listed={K2}, incomplete={K2}, complete=set())
        assert out['marked'] == 0 and _statuses(h)[K2] == 'PENDING'

    def test_complete_beams_without_proof_stay_pending(self, tmp_dir):
        """Listed, never in the Stage 1 task set (filtered as incomplete download, say)."""
        h, out = self._marked(tmp_dir, [_g(K2, 'PENDING')], listed={K2}, incomplete=set(), complete=set())
        assert out['marked'] == 0 and _statuses(h)[K2] == 'PENDING'

    def test_unlisted_granule_untouched(self, tmp_dir):
        h, out = self._marked(tmp_dir, [_g(K2, 'PENDING')], listed=set(), incomplete=set(), complete={K2})
        assert out['marked'] == 0 and _statuses(h)[K2] == 'PENDING'

    def test_no_data_that_failed_this_run_is_reopened(self, tmp_dir):
        h, out = self._marked(tmp_dir, [_g(K2, GRANULE_STATUS_NO_DATA)], listed={K2},
                              incomplete={K2}, complete=set())
        assert out == {'marked': 0, 'reopened': 1} and _statuses(h)[K2] == 'PENDING'

    def test_no_data_not_listed_is_kept(self, tmp_dir):
        h, out = self._marked(tmp_dir, [_g(K2, GRANULE_STATUS_NO_DATA)], listed={K1},
                              incomplete={K2}, complete=set())
        assert _statuses(h)[K2] == GRANULE_STATUS_NO_DATA

    def test_merge_failed_is_never_touched(self, tmp_dir):
        h, out = self._marked(tmp_dir, [_g(K2, 'MERGE_FAILED')], listed={K2}, incomplete=set(),
                              complete={K2})
        assert out['marked'] == 0 and _statuses(h)[K2] == 'MERGE_FAILED'
        h, out = self._marked(tmp_dir, [_g(K2, 'MERGE_FAILED')], listed={K2}, incomplete={K2}, complete=set())
        assert _statuses(h)[K2] == 'MERGE_FAILED'

    def test_unverifiable_metadata_marks_nothing(self, tmp_dir):
        """A partition parquet without its per-year sidecar may hold granules the
        partition metadata omits: no inference from absence is allowed."""
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        part_dir, _ = make_partition_dir(tmp_dir, granules=[{'orbit': 1, 'granule': 1, 'track': 1}])
        h.set_post_build_info(verify_observed=True)
        assert h._observed_granule_keys is None
        assert h.mark_no_data({K2}, set(), {K2}) == {'marked': 0, 'reopened': 0}
        assert _statuses(h)[K2] == 'PENDING'
        h.set_post_build_info()                          # unverified scan: observed listing as before
        assert h._observed_granule_keys == {K1}

    def test_unreadable_sidecar_marks_nothing(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        part_dir = _db_listing(tmp_dir, [K1])
        with open(os.path.join(part_dir, 'bad.metadata.json'), 'w') as f:
            f.write('{not json')
        h.set_post_build_info(verify_observed=True)
        assert h._observed_granule_keys is None

    def test_no_partition_means_nothing_observed(self, tmp_dir):
        """Every granule empty on a fresh database: no partition exists, which is
        itself a complete observation."""
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        h.set_post_build_info(verify_observed=True)
        assert h._observed_granule_keys == set()
        assert h.mark_no_data({K2}, set(), {K2})['marked'] == 1

    def test_without_set_post_build_info_marks_nothing(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        assert h.mark_no_data({K2}, set(), {K2}) == {'marked': 0, 'reopened': 0}

    def test_no_data_appearing_in_metadata_becomes_indexed(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, GRANULE_STATUS_NO_DATA)])
        _db_listing(tmp_dir, [K1])
        h.set_post_build_info()
        g = h.granule_info[0]
        assert g['status'] == 'INDEXED' and g['products']


# ---------------------------------------------------------------------------
# consumers
# ---------------------------------------------------------------------------

class TestConsumers:
    def test_finished_granules_include_no_data(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA), _g(K3, 'PENDING'),
                              _g(K4, 'MERGE_FAILED')])
        assert {(g['orbit'], g['granule'], g['track']) for g in h.get_finished_granules()} == {K1, K2}
        assert all(set(g) == {'orbit', 'granule', 'track'} for g in h.get_finished_granules())

    @pytest.mark.parametrize('field', ['new_product_vars', 'new_temporal'])
    def test_finished_granules_none_when_scope_widens(self, tmp_dir, field):
        h = _logger(tmp_dir, [_g(K2, GRANULE_STATUS_NO_DATA)])
        setattr(h, field, {'L4A': ['agbd']} if field == 'new_product_vars' else ('2020-03-31', '2020-06-01'))
        assert h.get_finished_granules() is None

    def test_finished_granules_none_when_spatial_adds_partitions(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K2, GRANULE_STATUS_NO_DATA)])
        h.new_spatial = [10.0, 10.0, 11.0, 11.0]
        assert h.get_finished_granules() is None

    def test_finished_granules_kept_when_spatial_adds_no_partition(self, tmp_dir):
        """Stage 1 filters by partition cell; an expansion inside cells the database
        already holds was admitted whole before, so an empty granule stays empty."""
        import h3
        lat, lon = h3.cell_to_latlng('838041fffffffff')
        h = _logger(tmp_dir, [_g(K2, GRANULE_STATUS_NO_DATA)],
                    h3_partition_ids=sorted(h3.grid_disk('838041fffffffff', 3)))
        h.new_spatial = [lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01]
        assert h._adding_h3_parts() is False
        assert [g['orbit'] for g in h.get_finished_granules()] == [2]

    def test_up_to_date_with_no_data(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA)])
        assert h.is_up_to_date() is True
        h.granule_info.append(_g(K3, 'PENDING'))
        assert h.is_up_to_date() is False

    def test_pending_fills_and_product_gaps_exclude_no_data(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, 'INDEXED', products={'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}),
                              _g(K2, GRANULE_STATUS_NO_DATA)])
        assert h.pending_product_fills() == {K1: ['L4A']}
        assert [k for k, _ in h.get_product_gaps()] == [{'orbit': 1, 'granule': 1, 'track': 1}]

    def test_round_trip_and_no_lazy_product_upgrade(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA)])
        h.save_log('COMPLETED')
        fresh = H3BuildLogger(product_vars=None, dir=tmp_dir)
        assert _statuses(fresh)[K2] == GRANULE_STATUS_NO_DATA
        g2 = [g for g in fresh.granule_info if g['orbit'] == 2][0]
        assert 'products' not in g2                      # legacy upgrade must not invent product statuses
        assert fresh.is_up_to_date() is True

    def test_legacy_log_behaves_as_before(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K1, 'INDEXED'), _g(K2, 'PENDING')])
        assert [(g['orbit']) for g in h.get_finished_granules()] == [1]
        assert h.is_up_to_date() is False


class TestReconcile:
    def _setup(self, tmp_dir):
        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir)
        return h3_dir

    def test_all_finished_short_circuits_with_no_data(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk
        h3_dir = self._setup(tmp_dir)
        h = _logger(h3_dir, [_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA)])
        assert _reconcile_granules_from_disk(h3_dir, h, tmp_dir=None) == 0
        assert _statuses(h)[K2] == GRANULE_STATUS_NO_DATA

    def test_no_data_found_in_metadata_flips_to_indexed(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk
        h3_dir = self._setup(tmp_dir)
        make_partition_dir(h3_dir, granules=[{'orbit': 2, 'granule': 1, 'track': 2}])
        h = _logger(h3_dir, [_g(K2, GRANULE_STATUS_NO_DATA), _g(K3, 'PENDING')])
        assert _reconcile_granules_from_disk(h3_dir, h, tmp_dir=None) == 1
        assert _statuses(h) == {K2: 'INDEXED', K3: 'PENDING'}

    def test_empty_sentinels_neither_index_nor_flip(self, tmp_dir):
        """``.empty`` sentinels prove completion, not rows: an all-empty granule stays
        PENDING, a NO_DATA one stays NO_DATA, one beam with a ``.done`` indexes."""
        from gedih3.gh3builder import (
            _reconcile_granules_from_disk, _emit_complete_sentinel, _check_scope_fingerprint)
        h3_dir = self._setup(tmp_dir)
        tmp_partitions = os.path.join(tmp_dir, 'tmp', 'partitions')
        _check_scope_fingerprint(tmp_partitions, 'scope-a')  # sentinels recorded under this scope
        for beam in GEDI_BEAMS:
            _emit_complete_sentinel(tmp_partitions, f'O00002_G01_T00002.{beam}', empty=True)
            _emit_complete_sentinel(tmp_partitions, f'O00003_G01_T00003.{beam}', empty=beam != GEDI_BEAMS[0])
            _emit_complete_sentinel(tmp_partitions, f'O00004_G01_T00004.{beam}', empty=True)
        h = _logger(h3_dir, [_g(K2, GRANULE_STATUS_NO_DATA), _g(K3, 'PENDING'), _g(K4, 'PENDING')])
        _reconcile_granules_from_disk(h3_dir, h, tmp_dir=tmp_partitions, expected_scope='scope-a')
        assert _statuses(h) == {K2: GRANULE_STATUS_NO_DATA, K3: 'INDEXED', K4: 'PENDING'}

    def test_no_data_flipped_by_metadata_drops_stale_products(self, tmp_dir):
        from gedih3.gh3builder import _reconcile_granules_from_disk
        h3_dir = self._setup(tmp_dir)
        make_partition_dir(h3_dir, granules=[{'orbit': 2, 'granule': 1, 'track': 2}])
        h = _logger(h3_dir, [_g(K2, GRANULE_STATUS_NO_DATA, products={'L4A': 'MISSING_SOURCE'},
                                fill_attempts={'L4A': 2}), _g(K3, 'PENDING')])
        _reconcile_granules_from_disk(h3_dir, h, tmp_dir=None)
        g2 = [g for g in h.granule_info if g['orbit'] == 2][0]
        assert g2['status'] == 'INDEXED' and 'products' not in g2 and 'fill_attempts' not in g2


class TestDoctorAndUpdate:
    def test_log_state_flags_no_data_on_disk_only(self, tmp_dir, client):
        from gedih3.doctor.runner import run_diagnoses
        from test_doctor_diagnoses import _ctx, _make_partition, _make_build_log
        _make_partition(tmp_dir, granules=[{'orbit': 100, 'granule': 1, 'track': 50}])
        _make_build_log(tmp_dir, granules=[
            _g((100, 1, 50), GRANULE_STATUS_NO_DATA, products={'L4A': 'MISSING_SOURCE'},
               fill_attempts={'L4A': 1}),                # asserted empty yet present: drift
            _g((101, 2, 51), GRANULE_STATUS_NO_DATA),    # absent, as asserted: fine
        ])
        ctx = _ctx(tmp_dir)
        reports = run_diagnoses(ctx, ['log_state'], mode='check')
        drift = [f for f in reports[0].findings if f.get('kind') == 'granule_status_drift']
        assert [(f['orbit'], f['granule'], f['track']) for f in drift] == [(100, 1, 50)]
        run_diagnoses(ctx, ['log_state'], mode='fix')
        fresh = H3BuildLogger(product_vars=None, dir=tmp_dir)
        assert _statuses(fresh) == {(100, 1, 50): 'INDEXED', (101, 2, 51): GRANULE_STATUS_NO_DATA}
        g = [g for g in fresh.granule_info if g['orbit'] == 100][0]
        assert 'fill_attempts' not in g and 'MISSING_SOURCE' not in g['products'].values()

    def test_upstream_does_not_see_no_data_as_a_product_gap(self, tmp_dir, monkeypatch):
        from gedih3.doctor import upstream
        h = _logger(tmp_dir, [_g(K1, 'INDEXED', products={'L2A': 'INDEXED', 'L4A': 'INDEXED'}),
                              _g(K2, GRANULE_STATUS_NO_DATA)])
        h.product_vars = {'L2A': None, 'L4A': None}
        monkeypatch.setattr(upstream, 'query_available_granules',
                            lambda *a, **k: {'L2A': {K1, K2}, 'L4A': {K1, K2}})
        monkeypatch.setattr(upstream, '_local_soc_keys_for_product', lambda *a, **k: set())
        report = upstream.gather_upstream(types.SimpleNamespace(h3_logger=h, soc_dir=None, h3_dir=tmp_dir))
        assert report.classifications == {}

    @pytest.mark.parametrize('extra,expected', [([], None), ([_g(K3, 'PENDING')], '1/3')])
    def test_update_warning_excludes_no_data(self, tmp_path, monkeypatch, extra, expected):
        """gh3_update warns about non-INDEXED granules, never counting NO_DATA."""
        import argparse
        import contextlib
        import gedih3.cli.gh3_update as upd

        db = tmp_path / 'db'
        db.mkdir()
        make_build_log(str(db), granules=[_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA)] + extra)
        ds = tmp_path / 'dataset'
        ds.mkdir()
        meta = {'index_type': 'h3', 'columns': ['shot_number'], 'source_database': str(db)}
        records = []
        log = logging.getLogger('test_no_data_update')
        log.addHandler(types.SimpleNamespace(level=0, handle=lambda r: records.append(r.getMessage()) or True,
                                              filter=lambda r: True, handleError=lambda r: None))
        monkeypatch.setattr('gedih3.gh3driver.gh3_read_meta',
                            lambda field, gh3_root_dir=None: {'h3_columns': ['shot_number'],
                                                              'h3_partition_level': 3}[field])
        monkeypatch.setattr(upd, '_update_h3_partitions', lambda *a, **k: None)
        monkeypatch.setattr('gedih3.cliutils.collect_columns', lambda a, available_columns=None: ['shot_number'])

        class _FakeClient(contextlib.AbstractContextManager):
            dashboard_link = 'http://x'

            def __exit__(self, *a):
                return False

        monkeypatch.setattr('dask.distributed.Client', lambda **k: _FakeClient())
        monkeypatch.setattr('gedih3.cliutils.parse_dask_args', lambda a: {})
        args = argparse.Namespace(database=str(db), list=['shot_number'],
                                  L1B=None, L2A=None, L2B=None, L4A=None, L4C=None)
        try:
            upd._update_from_database(args, str(ds), meta, log)
        except Exception:
            pass  # only the warning matters; the rest of the update is stubbed
        warned = [m for m in records if 'non-INDEXED' in m]
        if expected is None:
            assert warned == []
        else:
            assert len(warned) == 1 and expected in warned[0]


# ---------------------------------------------------------------------------
# Stage 1 driver: the outcome callback
# ---------------------------------------------------------------------------

def _driver_fixtures():
    import test_write_streaming as tws
    return tws


class TestStage1OutcomeCallback:
    PV = {'L2A': ['shot_number', 'lat_lowestmode', 'lon_lowestmode', 'delta_time', 'rh_098'],
          'L4A': ['shot_number', 'agbd']}
    BBOX = [-51.0, -0.5, -49.5, 1.0]

    def _granule(self, tws, soc_dir, key, kind):
        orb, gran, trk = key
        paths = {p: os.path.join(soc_dir, tws._gedi_filename(code, orb, gran, trk))
                 for p, code in (('L2A', '02_A'), ('L4A', '04_A'))}
        if kind == 'rows':
            for p in paths.values():
                tws._write_synthetic_gedi_h5(p, GEDI_BEAMS, orb, gran, trk)
        elif kind == 'outside':
            for p in paths.values():
                tws._write_synthetic_gedi_h5(p, GEDI_BEAMS, orb, gran, trk, lon_range=(10.0, 10.5))
        elif kind == 'corrupt':
            for p in paths.values():
                with open(p, 'w') as f:
                    f.write('not an hdf5 file')
        elif kind == 'nosource':
            return {'L1B': paths['L2A']}                 # no product the build requests
        return paths

    def _run(self, tmp_dir, soc_files, cb, tmp_partitions=None):
        from gedih3.gedidriver import dask_h5_merged
        from gedih3.h3utils import h3_index_df
        from gedih3.gh3builder import _write_partitioned_streaming
        good = [s for s in soc_files if 'L2A' in s and os.path.getsize(s['L2A']) > 100]
        ddf = dask_h5_merged(good, self.PV, shots=None, dropna=True, by_beam=True, suffix_all=True)
        ddf = ddf.map_partitions(h3_index_df, res=12, part=3,
                                 lat_col='lat_lowestmode_l2a', lon_col='lon_lowestmode_l2a')
        h3_dir = os.path.join(tmp_dir, 'database')
        os.makedirs(h3_dir, exist_ok=True)
        return _write_partitioned_streaming(
            ddf, soc_files, self.PV, res=12, part=3,
            tmp_dir=tmp_partitions or os.path.join(tmp_dir, 'tmp', 'partitions'), h3_dir=h3_dir,
            spatial=self.BBOX, lat_col='lat_lowestmode_l2a', lon_col='lon_lowestmode_l2a',
            dat_col='delta_time_l2a', inflight_target=8, stage1_outcome_callback=cb)

    def test_callback_separates_proven_from_unproven_granules(self, tmp_dir, client):
        tws = _driver_fixtures()
        soc_dir = os.path.join(tmp_dir, 'soc')
        os.makedirs(soc_dir)
        soc_files = [self._granule(tws, soc_dir, k, kind) for k, kind in
                     (((101, 1, 201), 'rows'), ((102, 1, 202), 'outside'),
                      ((103, 1, 203), 'corrupt'), ((104, 1, 204), 'nosource'))]
        got = []
        assert self._run(tmp_dir, soc_files, lambda inc, comp: got.append((set(inc), set(comp)))) is True
        assert len(got) == 1
        incomplete, complete = got[0]
        assert incomplete == {(103, 1, 203), (104, 1, 204)}      # task error, no source
        assert complete == {(102, 1, 202)}                       # proven complete AND empty only

    def test_second_run_reads_empty_sentinels_of_skipped_tasks(self, tmp_dir, client):
        tws = _driver_fixtures()
        soc_dir = os.path.join(tmp_dir, 'soc')
        os.makedirs(soc_dir)
        soc_files = [self._granule(tws, soc_dir, k, kind) for k, kind in
                     (((101, 1, 201), 'rows'), ((102, 1, 202), 'outside'))]
        self._run(tmp_dir, soc_files, None)
        got = []
        self._run(tmp_dir, soc_files, lambda inc, comp: got.append((set(inc), set(comp))))
        # nothing left to run (early-return path): the `.done` granule is no proof, the `.empty` one is
        assert got == [(set(), {(102, 1, 202)})]

    def test_callback_not_invoked_when_the_drain_is_interrupted(self, tmp_dir, client, monkeypatch):
        import dask.distributed as dd
        tws = _driver_fixtures()
        soc_dir = os.path.join(tmp_dir, 'soc')
        os.makedirs(soc_dir)
        soc_files = [self._granule(tws, soc_dir, k, 'rows') for k in ((101, 1, 201), (102, 1, 202))]
        real = dd.as_completed

        def interrupted(futures, *a, **k):
            it = iter(real(futures, *a, **k))
            yield next(it)
            raise KeyboardInterrupt

        monkeypatch.setattr(dd, 'as_completed', interrupted)
        got = []
        with pytest.raises(KeyboardInterrupt):
            self._run(tmp_dir, soc_files, lambda inc, comp: got.append(1))
        assert got == []

    def test_a_lost_future_is_attributed_to_its_granule(self, tmp_dir, client, monkeypatch):
        import gedih3.gh3builder as gh
        tws = _driver_fixtures()
        soc_dir = os.path.join(tmp_dir, 'soc')
        os.makedirs(soc_dir)
        soc_files = [self._granule(tws, soc_dir, (101, 1, 201), 'outside'),
                     self._granule(tws, soc_dir, (102, 1, 202), 'outside')]
        real = gh._write_one_granule_beam

        def flaky(task, **kw):
            if task[2].startswith('O00102_G01_T00202.'):
                raise RuntimeError('worker blew up')
            return real(task, **kw)

        monkeypatch.setattr(gh, '_write_one_granule_beam', flaky)
        got = []
        self._run(tmp_dir, soc_files, lambda inc, comp: got.append((set(inc), set(comp))))
        assert got == [({(102, 1, 202)}, {(101, 1, 201)})]  # empty granule 101 proven; 102 lost


# ---------------------------------------------------------------------------
# end to end through the CLI
# ---------------------------------------------------------------------------

def test_empty_granule_is_recorded_no_data_and_never_re_read(tmp_dir):
    """A granule wholly outside the region is NO_DATA after one build. The next
    build is "up-to-date" (it used to stay PENDING and defeat that exit), and a
    build that adds a granule skips it: Stage 1 then reads one granule, not two
    (a re-read would show as a second granule in its task count)."""
    import subprocess
    import sys
    from test_phased_product_updates import _write_granule

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))

    def gh3_build():
        cmd = [sys.executable, '-m', 'gedih3.cli.gh3_build', '-i', soc_dir, '-o', h3_dir, '-t', tmp,
               '--gedi-version', '2', '-N', '2', '-T', '1', '-M', '2', '-P', '0', '--no-bbox-index',
               '-l2a', 'rh_098', '-l4a', 'agbd', '--region=-51,0,-50,1']
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
        return res.stdout + res.stderr

    def statuses():
        log = json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))
        return {(g['orbit'], g['granule'], g['track']): g['status'] for g in log['granules']}

    inside, outside, later = (101, 1, 201), (102, 1, 202), (103, 1, 203)
    _write_granule(soc_dir, inside, '2020100', 7.0e7)
    _write_granule(soc_dir, outside, '2020100', 7.0e7, lon0=10.0)

    out = gh3_build()
    assert 'recorded as NO_DATA' in out
    assert statuses() == {inside: 'INDEXED', outside: 'NO_DATA'}

    assert 'up-to-date' in gh3_build()

    _write_granule(soc_dir, later, '2020100', 7.0e7)
    out = gh3_build()
    assert '1 granules x 8 beams'.replace('x', '\u00d7') in out   # only the new granule is read
    assert statuses() == {inside: 'INDEXED', outside: 'NO_DATA', later: 'INDEXED'}


# ---------------------------------------------------------------------------
# partial Stage 1 scope, merge_build_logs
# ---------------------------------------------------------------------------

BOX = [10.0, 10.0, 11.0, 11.0]


class TestStage1Scope:
    """On a spatial-only (temporal-only) expansion Stage 1 reads only the added
    area (dates); a granule empty there may hold rows in the old scope."""

    def _expanding(self, tmp_dir, **new):
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        h.updating = True
        h.new_spatial = h.new_temporal = h.new_product_vars = None
        for k, v in new.items():
            setattr(h, k, v)
        h.set_post_build_info(verify_observed=True)
        return h

    @pytest.mark.parametrize('new,full', [
        ({}, True),                                                                  # plain resume / update
        ({'new_spatial': BOX}, False),                                               # spatial-only
        ({'new_temporal': ('2020-03-31', '2020-06-01')}, False),                     # temporal-only
        ({'new_spatial': BOX, 'new_temporal': ('2020-03-31', '2020-06-01')}, True),  # both: full getters
        ({'new_spatial': BOX, 'new_product_vars': {'L4A': ['agbd']}}, True),         # mixed update Phase 1
    ])
    def test_predicate_mirrors_the_getters(self, tmp_dir, new, full):
        h = self._expanding(tmp_dir, **new)
        assert h.stage1_full_scope() is full
        assert (h.get_spatial() is not h.spatial) is (not full and 'new_spatial' in new)
        assert (h.get_temporal() is not h.temporal) is (not full and 'new_temporal' in new)

    @pytest.mark.parametrize('new', [{'new_spatial': BOX}, {'new_temporal': ('2020-03-31', '2020-06-01')}])
    def test_partial_scope_leaves_empty_under_diff_pending(self, tmp_dir, new):
        h = self._expanding(tmp_dir, **new)
        assert h.mark_no_data({K2}, set(), {K2}) == {'marked': 0, 'reopened': 0}
        assert _statuses(h)[K2] == 'PENDING'

    @pytest.mark.parametrize('new', [{}, {'new_spatial': BOX, 'new_product_vars': {'L4A': ['agbd']}}])
    def test_full_scope_marks(self, tmp_dir, new):
        h = self._expanding(tmp_dir, **new)
        assert h.mark_no_data({K2}, set(), {K2})['marked'] == 1

    def test_fresh_build_marks(self, tmp_dir):
        h = _logger(tmp_dir, [_g(K2, 'PENDING')])
        h.updating = False
        assert h.stage1_full_scope()
        h.set_post_build_info(verify_observed=True)
        assert h.mark_no_data({K2}, set(), {K2})['marked'] == 1


def test_merge_build_logs_no_data_loses_to_any_other_status(tmp_dir):
    from gedih3.gh3builder import merge_build_logs
    a, b = os.path.join(tmp_dir, 'a'), os.path.join(tmp_dir, 'b')
    fc = {'type': 'FeatureCollection', 'features': []}
    make_build_log(a, granules=[_g(K1, GRANULE_STATUS_NO_DATA), _g(K2, 'INDEXED'), _g(K3, 'PENDING'),
                                _g(K4, GRANULE_STATUS_NO_DATA)], spatial=fc)
    make_build_log(b, granules=[_g(K1, 'INDEXED'), _g(K2, GRANULE_STATUS_NO_DATA), _g(K3, 'INDEXED'),
                                _g(K4, GRANULE_STATUS_NO_DATA)], spatial=fc)
    merged = merge_build_logs(os.path.join(a, BUILD_LOG_FILENAME), os.path.join(b, BUILD_LOG_FILENAME),
                              os.path.join(tmp_dir, 'out.json'))
    st = [(g['orbit'], g['status']) for g in merged['granules']]
    assert sorted(st) == [(1, 'INDEXED'), (2, 'INDEXED'), (3, 'INDEXED'), (3, 'PENDING'), (4, 'NO_DATA')]
