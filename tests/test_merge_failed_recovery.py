"""End-to-end MERGE_FAILED recovery: a corrupt fragment fails its partition's
merge, the fold flags its granules, and the next build re-extracts exactly the
lost (granule × beam) so the partition merges with every row.

The loop used to break in several places, each silently: the fold only
flipped INDEXED granules (a fresh one is still PENDING), ``set_post_build_info``
re-marked flagged granules INDEXED in the same run, and the pre-clean ran at
merge entry, after Stage 1 had already skipped the lost task by its
``_complete/`` sentinel — so the merge dropped those rows for good.
"""
import glob
import os

import h5py
import numpy as np
import pyarrow.parquet as pq
import pytest

from gedih3.config import GEDI_BEAMS

N_SHOTS = 20
PRODUCT_VARS = {'L2A': ['rh_098'], 'L4A': ['agbd']}


def _gedi_name(product, orbit, granule, track):
    return f"GEDI{product}_2020001000000_O{orbit:05d}_{granule:02d}_T{track:05d}_02_003_02_V002.h5"


def _write_granule(soc_dir, orbit, granule, track):
    """Synthetic L2A + L4A pair carrying every variable a build reads
    (L2A essentials and quality flags included), all shots in a few cells."""
    day = os.path.join(soc_dir, '2020', '001')
    os.makedirs(day, exist_ok=True)
    rng = np.random.default_rng(orbit)
    for product in ('02_A', '04_A'):
        with h5py.File(os.path.join(day, _gedi_name(product, orbit, granule, track)), 'w') as f:
            for i, beam in enumerate(GEDI_BEAMS):
                g = f.create_group(beam)
                # Unique per granule and beam: the merge dedups on shot_number.
                g['shot_number'] = np.arange(N_SHOTS, dtype=np.uint64) + np.uint64(orbit * 10**6 + i * 10**3)
                if product == '02_A':
                    g['delta_time'] = rng.uniform(6.4e7, 6.5e7, N_SHOTS)
                    g['lat_lowestmode'] = rng.uniform(0.0, 0.5, N_SHOTS)
                    g['lon_lowestmode'] = rng.uniform(-50.5, -50.0, N_SHOTS)
                    g['elev_lowestmode'] = rng.uniform(0, 100, N_SHOTS)
                    g['quality_flag'] = np.ones(N_SHOTS, dtype='u1')
                    g['degrade_flag'] = np.zeros(N_SHOTS, dtype='u1')
                    g['sensitivity'] = rng.uniform(0.9, 1.0, N_SHOTS)
                    g['rh_098'] = rng.uniform(0, 50, N_SHOTS)
                else:
                    g['agbd'] = rng.uniform(0, 300, N_SHOTS)
                    g['l4_quality_flag'] = np.ones(N_SHOTS, dtype='u1')


def _db_shots(h3_dir):
    shots = []
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')):
        shots.extend(pq.read_table(f, columns=['shot_number'])['shot_number'].to_pylist())
    return shots


@pytest.fixture
def _client():
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def test_corrupt_fragment_is_re_extracted_and_merged(tmp_dir, _client, monkeypatch):
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    parquet_dir = os.path.join(tmp, 'partitions')
    kw = dict(res=12, part=3, soc_source=soc_dir, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    a, b = (101, 1, 201), (102, 1, 202)

    # Existing database with granule A.
    _write_granule(soc_dir, *a)
    gb.build_h3db(product_vars=PRODUCT_VARS, **kw)
    log = H3BuildLogger(product_vars=PRODUCT_VARS, dir=h3_dir, res=12, part=3, version=2)
    log.set_post_build_info()
    log.save_log('COMPLETED')

    # Update with granule B; one of its fragments is truncated before the
    # merge (a worker killed mid-write on shared storage).
    _write_granule(soc_dir, *b)
    log.register_pending_granules([dict(zip(('orbit', 'granule', 'track'), b))])
    real_merge = gb._merge_and_finalize

    def _corrupt_then_merge(tmp_partitions, out_dir):
        victim = sorted(glob.glob(os.path.join(tmp_partitions, 'h3_*', 'year=*', 'O00102_*.parquet')))[0]
        with open(victim, 'wb') as f:
            f.write(b'\x00' * 64)
        monkeypatch.setattr(gb, '_merge_and_finalize', real_merge)
        return real_merge(tmp_partitions, out_dir)

    monkeypatch.setattr(gb, '_merge_and_finalize', _corrupt_then_merge)
    gb.build_h3db(product_vars=PRODUCT_VARS, skip_granules=log.get_finished_granules(), **kw)
    assert gb._scan_merge_failure_sentinels(parquet_dir)

    # What the CLI does after the merge: fold, then set_post_build_info.
    assert gb.apply_merge_failures_to_logger(log, parquet_dir) >= 1
    log.set_post_build_info()
    status = {(g['orbit'], g['granule'], g['track']): g['status'] for g in log.granule_info}
    assert status[b] == 'MERGE_FAILED' and status[a] == 'INDEXED'

    # The next build: B is not skipped, the pre-clean reopens its lost task.
    gb.build_h3db(product_vars=PRODUCT_VARS, skip_granules=log.get_finished_granules(), **kw)
    gb._release_merge_failed(log, listed={a, b})
    assert gb.apply_merge_failures_to_logger(log, parquet_dir) == 0
    log.set_post_build_info()

    shots = _db_shots(h3_dir)
    assert len(shots) == len(set(shots)) == 2 * len(GEDI_BEAMS) * N_SHOTS
    assert {g['status'] for g in log.granule_info} == {'INDEXED'}
    assert not os.path.exists(parquet_dir)
