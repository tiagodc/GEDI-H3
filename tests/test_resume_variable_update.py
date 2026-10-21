"""Resuming an update whose new variables never reached Stage 2 (#52).

The first save_log of an update stores the merged products/variables, while
Stage 2 records its own ``_pending_variable_update`` only when it starts (for a
mixed update, once Phase 1 drained). A run killed in between used to resume as
a plain build: Stage 2 never ran, and granules already indexed never got the
new columns. ``_pending_update`` now carries the new variables too.
"""
import glob
import json
import os
import subprocess
import sys

import h5py
import numpy as np
import pyarrow.parquet as pq

from conftest import make_build_log
from gedih3.config import BUILD_LOG_FILENAME, GEDI_BEAMS
from gedih3.logger import H3BuildLogger

N_SHOTS = 20


def _box(x0, y0, x1, y1):
    return json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
                       "geometry": {"type": "Polygon",
                                    "coordinates": [[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]]}}]})


A = _box(-51, 0, -50, 1)
A_PLUS_B = _box(-51, 0, -44, 1)
NEW_VARS = {'L2B': ['cover']}


class TestRestoredVariables:
    def _db(self, tmp_dir):
        make_build_log(tmp_dir, spatial=A, granules=[{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'}])

    def test_mixed_update_killed_in_phase_1_resumes_as_mixed(self, tmp_dir):
        self._db(tmp_dir)
        run = H3BuildLogger(product_vars=NEW_VARS, dir=tmp_dir, spatial=A_PLUS_B)
        run.save_log('PARTITIONING')                             # killed in Phase 1

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)

        assert resume.new_product_vars == run.new_product_vars
        assert resume.new_spatial.union_all().equals(run.new_spatial.union_all())
        assert resume.get_spatial().union_all().equals(run.get_spatial().union_all())   # Phase 1: whole region
        assert resume.get_finished_granules() is None
        assert not resume.log_data.get('_pending_variable_update')

    def test_variable_update_killed_before_stage_2_resumes_as_variable_only(self, tmp_dir):
        self._db(tmp_dir)
        run = H3BuildLogger(product_vars=NEW_VARS, dir=tmp_dir)
        run.save_log('PARTITIONING')

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)

        assert resume.new_product_vars == run.new_product_vars
        assert resume.new_spatial is None and resume.new_temporal is None
        assert not resume.is_up_to_date()

    def test_stage_2_records_its_own_marker(self, tmp_dir):
        self._db(tmp_dir)
        run = H3BuildLogger(product_vars=NEW_VARS, dir=tmp_dir, spatial=A_PLUS_B)
        run.log_data['_pending_variable_update'] = {'product_vars': dict(run.new_product_vars)}
        run.save_log('PROCESSING')                               # killed in Phase 2

        with open(run.log_file) as f:
            assert '_pending_update' not in json.load(f)
        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)
        assert resume.new_product_vars is None and resume.log_data['_pending_variable_update']

    def test_completed_clears_and_a_new_request_combines(self, tmp_dir):
        self._db(tmp_dir)
        H3BuildLogger(product_vars=NEW_VARS, dir=tmp_dir).save_log('PARTITIONING')

        resume = H3BuildLogger(product_vars={'L4A': ['agbd_se']}, dir=tmp_dir)
        assert set(resume.new_product_vars) == {'L2B', 'L4A'}
        assert {'L2B', 'L4A'} <= set(resume.product_vars)

        resume.save_log('COMPLETED')
        after = H3BuildLogger(product_vars=None, dir=tmp_dir)
        assert after.new_product_vars is None and after.is_up_to_date()


def _write_l2a(soc_dir, key, stamp, delta_time):
    orbit, granule, track = key
    day = os.path.join(soc_dir, stamp[:4], stamp[4:])
    os.makedirs(day, exist_ok=True)
    rng = np.random.default_rng(orbit)
    name = f"GEDI02_A_{stamp}000000_O{orbit:05d}_{granule:02d}_T{track:05d}_02_003_02_V002.h5"
    with h5py.File(os.path.join(day, name), 'w') as f:
        for i, beam in enumerate(GEDI_BEAMS):
            g = f.create_group(beam)
            g['shot_number'] = np.arange(N_SHOTS, dtype=np.uint64) + np.uint64(orbit * 10**6 + i * 10**3)
            g['delta_time'] = rng.uniform(delta_time, delta_time + 1e5, N_SHOTS)
            g['lat_lowestmode'] = rng.uniform(0.1, 0.9, N_SHOTS)
            g['lon_lowestmode'] = rng.uniform(-54.5, -50.5, N_SHOTS)
            g['elev_lowestmode'] = rng.uniform(0, 100, N_SHOTS)
            g['quality_flag'] = np.ones(N_SHOTS, dtype='u1')
            g['degrade_flag'] = np.zeros(N_SHOTS, dtype='u1')
            g['sensitivity'] = rng.uniform(0.9, 1.0, N_SHOTS)
            g['rh_098'] = rng.uniform(0, 50, N_SHOTS)
            g['rh_050'] = rng.uniform(0, 30, N_SHOTS)


def test_plain_resume_of_a_mixed_update_killed_in_phase_1_adds_the_variables(tmp_dir):
    """gh3_build end to end: the resume runs Phase 2, so every partition gets the column."""
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))

    def gh3_build(*extra):
        cmd = [sys.executable, '-m', 'gedih3.cli.gh3_build', '-i', soc_dir, '-o', h3_dir, '-t', tmp,
               '--gedi-version', '2', '-N', '2', '-T', '1', '-M', '2', '-P', '0', '--no-bbox-index', *extra]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
        return res.stdout + res.stderr

    _write_l2a(soc_dir, (101, 1, 201), '2020100', 7.0e7)
    gh3_build('-l2a', 'rh_098', '--region=-55,0,-50,1')
    files = glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
    assert files and all('rh_050_l2a' not in pq.read_schema(f).names for f in files)

    # A mixed update (-l2a rh_050 and a wider region) killed after its first save.
    H3BuildLogger(product_vars={'L2A': ['rh_050']}, dir=h3_dir,
                  spatial=_box(-55, 0, -45, 1)).save_log('PARTITIONING')

    gh3_build()

    files = glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
    for f in files:
        t = pq.read_table(f, columns=['rh_050_l2a'])
        assert t['rh_050_l2a'].null_count == 0, f
    log = json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))
    assert log['status'] == 'COMPLETED' and '_pending_update' not in log
    assert 'rh_050' in log['products']['L2A']['variables']
    assert 'up-to-date' in gh3_build()
