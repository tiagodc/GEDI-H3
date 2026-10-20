"""Spatial-only expansion of an existing database (#42 case 1).

A spatial-only update hands Stage 1 only the region it adds. That is all a
granule already indexed under the stored region still needs, but a granule
new to the database read that way had its rows outside the added region
dropped while its rows inside made it ``INDEXED``, so no later build read the
rest. Such granules are now left ``PENDING`` for the next build, which reads
them over the whole region.
"""
import glob
import json
import os
import subprocess
import sys

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from conftest import make_build_log
from gedih3.config import BUILD_LOG_FILENAME, GEDI_BEAMS
from gedih3.logger import H3BuildLogger

N_SHOTS = 20
STORED = '-55,0,-50,1'
WIDENED = '-55,0,-45,1'


def _box(x0, y0, x1, y1):
    return json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
                       "geometry": {"type": "Polygon",
                                    "coordinates": [[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]]}}]})


def _write_l2a(soc_dir, key, stamp, delta_time, lon=(-54.5, -45.5)):
    """A synthetic L2A granule whose shots span ``lon`` (both regions by default)."""
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
            g['lon_lowestmode'] = rng.uniform(*lon, N_SHOTS)
            g['elev_lowestmode'] = rng.uniform(0, 100, N_SHOTS)
            g['quality_flag'] = np.ones(N_SHOTS, dtype='u1')
            g['degrade_flag'] = np.zeros(N_SHOTS, dtype='u1')
            g['sensitivity'] = rng.uniform(0.9, 1.0, N_SHOTS)
            g['rh_098'] = rng.uniform(0, 50, N_SHOTS)


def _shots_of(h3_dir, orbit):
    files = glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
    shots = pa.concat_tables([pq.read_table(f, columns=['shot_number']) for f in files])['shot_number']
    return sorted(s for s in shots.to_pylist() if s // 10**6 == orbit)


def test_new_granule_in_a_widening_run_is_indexed_over_the_whole_region(tmp_dir):
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))

    def gh3_build(*extra):
        cmd = [sys.executable, '-m', 'gedih3.cli.gh3_build', '-i', soc_dir, '-o', h3_dir, '-t', tmp,
               '--gedi-version', '2', '-N', '2', '-T', '1', '-M', '2', '-P', '0', '--no-bbox-index', *extra]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
        return res.stdout + res.stderr

    def status(orbit):
        log = json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))
        return next(g['status'] for g in log['granules'] if g['orbit'] == orbit)

    every_shot = len(GEDI_BEAMS) * N_SHOTS
    _write_l2a(soc_dir, (101, 1, 201), '2020100', 7.0e7)
    gh3_build('-l2a', 'rh_098', f'--region={STORED}')
    in_stored = len(_shots_of(h3_dir, 101))
    assert 0 < in_stored < every_shot                        # its shots east of -50 are outside

    # A new granule arrives in the same run that widens the region.
    _write_l2a(soc_dir, (102, 1, 202), '2020200', 8.0e7)
    out = gh3_build(f'--region={WIDENED}')
    assert len(_shots_of(h3_dir, 101)) == every_shot         # the indexed granule got the added region
    assert status(102) == 'PENDING' and _shots_of(h3_dir, 102) == []
    assert 'left for the next build' in out

    gh3_build()                                              # the next build reads it over the whole region
    assert status(102) == 'INDEXED'
    assert len(_shots_of(h3_dir, 102)) == every_shot
    assert 'up-to-date' in gh3_build()


class TestStage1OnlyGranules:
    def _db(self, tmp_dir):
        make_build_log(tmp_dir, spatial=_box(-51, 0, -50, 1), granules=[
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'PENDING'},
            {'orbit': 3, 'granule': 1, 'track': 3, 'status': 'NO_DATA'},
        ])

    def test_spatial_only_expansion_reads_only_finished_granules(self, tmp_dir):
        self._db(tmp_dir)
        h = H3BuildLogger(product_vars=None, dir=tmp_dir, spatial=_box(-51, 0, -44, 1))
        assert h.reads_added_area_only()
        # The widening reopened the NO_DATA granule: it needs the whole region too.
        assert h.stage1_only_granules() == [{'orbit': 1, 'granule': 1, 'track': 1}]

    def test_other_runs_read_any_granule(self, tmp_dir):
        self._db(tmp_dir)
        for kw in ({}, {'temporal': ('2020-01-01', '2020-06-30')},
                   {'product_vars': {'L4C': ['wsci']}, 'spatial': _box(-51, 0, -44, 1)}):
            h = H3BuildLogger(dir=tmp_dir, **{'product_vars': None, **kw})
            assert h.stage1_only_granules() is None, kw

    def test_a_narrower_or_identical_region_is_a_plain_update(self, tmp_dir):
        self._db(tmp_dir)
        for spatial in (_box(-50.8, 0.2, -50.2, 0.8), _box(-51, 0, -50, 1)):
            h = H3BuildLogger(product_vars=None, dir=tmp_dir, spatial=spatial)
            assert h.new_spatial is None
            assert h.stage1_only_granules() is None
            assert [g['orbit'] for g in h.get_finished_granules()] == [1, 3]

    def test_new_spatial_is_the_added_area_only(self, tmp_dir):
        self._db(tmp_dir)
        h = H3BuildLogger(product_vars=None, dir=tmp_dir, spatial=_box(-45, 0, -44, 1))   # disjoint
        assert abs(h.new_spatial.union_all().area - 1.0) < 1e-9
        assert abs(h.spatial.union_all().area - 2.0) < 1e-9


def test_filter_granules_leaves_out_granules_not_admitted(monkeypatch):
    import gedih3.gh3builder as gb
    monkeypatch.setattr(gb, 'h5_is_valid', lambda f: True)
    seen = []
    monkeypatch.setattr(gb.logger, 'warning', lambda msg, *a, **k: seen.append(msg))
    files = [{'L2A': f'/soc/GEDI02_A_2020100000000_O{o:05d}_01_T{o:05d}_02_003_02_V002.h5'} for o in (1, 2)]

    kept = gb._filter_granules(files, {'L2A': ['rh_098']}, None,
                               only_granules=[{'orbit': 1, 'granule': 1, 'track': 1}])

    assert kept == files[:1]
    assert any(m.startswith('1 granule(s) not yet indexed are left for the next build') for m in seen)
