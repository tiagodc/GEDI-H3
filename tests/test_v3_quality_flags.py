"""Release 3 quality flags of L4A and L4C: the ``--quality`` filter and the build-time variable lists.

L4A and L4C V3 carry a pointing/positioning inclusion flag (``degrade_include_flag``,
1 = include) and an orbit-segment outlier flag (``elev_highestreturn_outlier_flag``,
1 = outlier) that are meant to be applied together. ``--quality`` applies both,
and every V3 build stores both, whatever variable list it was given.
"""
import copy
import glob
import json
import logging
import os
import sys

import pyarrow.parquet as pq
import pytest

from gedih3.cliutils import get_product_quality_conditions
from gedih3.config import BUILD_LOG_FILENAME, _PRODUCT_QUALITY_FLAGS
from gedih3.gedidriver import gedi_vars_static
from gedih3.gh3builder import _expand_product_vars, _withhold_flags_absent_from_db

L4_V3 = {'L4A': 'l4a_quality_flag_rel3', 'L4C': 'l4c_quality_flag_rel3'}


# ---------------------------------------------------------------------------
# --quality conditions
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('prod, qf', sorted(L4_V3.items()))
def test_v3_quality_filter_applies_inclusion_and_outlier_flags(prod, qf):
    sfx = f'_{prod.lower()}'
    cols = [qf + sfx, 'degrade_include_flag' + sfx, 'elev_highestreturn_outlier_flag' + sfx]
    assert get_product_quality_conditions([prod], version=3, available_columns=cols) == [
        (qf + sfx, '== 1'),
        ('degrade_include_flag' + sfx, '== 1'),            # 1 = include
        ('elev_highestreturn_outlier_flag' + sfx, '== 0'),  # 1 = outlier
    ]


def test_v2_l4_quality_filters_are_unchanged():
    assert _PRODUCT_QUALITY_FLAGS['L4A'][2] == [('l4_quality_flag', '== 1')]
    assert _PRODUCT_QUALITY_FLAGS['L4C'][2] == [('wsci_quality_flag', '== 1')]


def test_a_flag_the_data_lacks_is_skipped_with_a_warning(caplog):
    # A database built before gedih3 stored degrade_include_flag.
    cols = ['l4a_quality_flag_rel3_l4a', 'elev_highestreturn_outlier_flag_l4a']
    lg = logging.getLogger('gedih3.cliutils')
    lg.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger='gedih3.cliutils'):
            out = get_product_quality_conditions(['L4A'], version=3, available_columns=cols)
    finally:
        lg.removeHandler(caplog.handler)
    assert out == [('l4a_quality_flag_rel3_l4a', '== 1'), ('elev_highestreturn_outlier_flag_l4a', '== 0')]
    assert any('degrade_include_flag_l4a' in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# build-time variable lists
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('prod', sorted(L4_V3))
@pytest.mark.parametrize('spec', [['minimal'], ['default'], ['shot_number']])
def test_every_v3_build_list_stores_both_flags(prod, spec):
    pv = _expand_product_vars({prod: list(spec)}, soc_files=[], version=3)
    assert {'degrade_include_flag', 'elev_highestreturn_outlier_flag', L4_V3[prod]} <= set(pv[prod])


@pytest.mark.parametrize('prod', sorted(L4_V3))
def test_the_v3_files_hold_both_flags_at_the_beam_root(prod):
    # Names the build reads must exist in the release's files (shipped manifest).
    names = set(gedi_vars_static(prod, version=3))
    assert {'degrade_include_flag', 'elev_highestreturn_outlier_flag'} <= names


# ---------------------------------------------------------------------------
# updates of a database built before a flag was stored
# ---------------------------------------------------------------------------

def _log(h3_dir, columns):
    os.makedirs(h3_dir, exist_ok=True)
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'gedi_version': 3, 'h3_columns': columns}, f)


def test_update_withholds_flags_the_database_does_not_store(tmp_dir):
    h3_dir = os.path.join(tmp_dir, 'db')
    _log(h3_dir, ['shot_number', 'root_file_l4a', 'agbd_l4a', 'l4a_quality_flag_rel3_l4a',
                  'elev_highestreturn_outlier_flag_l4a'])
    pv = _expand_product_vars({'L4A': ['minimal'], 'L4C': ['minimal']}, soc_files=[], version=3)
    withheld = _withhold_flags_absent_from_db(pv, h3_dir, version=3)
    assert withheld == {'L4A': ['degrade_include_flag']}
    assert 'degrade_include_flag' not in pv['L4A']
    assert 'elev_highestreturn_outlier_flag' in pv['L4A']
    # A product the database does not hold yet gets every column.
    assert 'degrade_include_flag' in pv['L4C']


def test_fresh_build_and_complete_database_keep_every_flag(tmp_dir):
    pv = _expand_product_vars({'L4A': ['minimal']}, soc_files=[], version=3)
    assert _withhold_flags_absent_from_db(pv, os.path.join(tmp_dir, 'none'), version=3) == {}
    h3_dir = os.path.join(tmp_dir, 'db')
    _log(h3_dir, ['root_file_l4a', 'l4a_quality_flag_rel3_l4a', 'degrade_include_flag_l4a',
                  'elev_highestreturn_outlier_flag_l4a'])
    assert _withhold_flags_absent_from_db(pv, h3_dir, version=3) == {}
    assert 'degrade_include_flag' in pv['L4A']


@pytest.fixture
def _client():
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def test_an_update_after_the_flag_table_grew_keeps_one_column_set(tmp_dir, _client, monkeypatch):
    """Without the guard, new cells held the new flag and older files did not:
    every read naming it (``--quality``) failed on the older files."""
    import h5py
    import numpy as np
    import gedih3.gh3builder as gb
    from gedih3.config import GEDI_BEAMS
    from gedih3.gh3driver import gh3_load
    from gedih3.logger import H3BuildLogger
    sys.path.insert(0, os.path.dirname(__file__))
    from test_phased_product_updates import N_SHOTS, _write_granule

    soc, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    kw = dict(res=12, part=3, soc_source=soc, version=2, tmp_dir=tmp, h3_dir=h3_dir)

    def new_flag_in_l4a():
        for fn in glob.glob(os.path.join(soc, '*', '*', 'GEDI04_A*.h5')):
            with h5py.File(fn, 'a') as f:
                for b in GEDI_BEAMS:
                    if 'new_flag' not in f[b]:
                        f[b]['new_flag'] = np.ones(N_SHOTS, dtype='u1')

    pv = {'L2A': ['rh_098'], 'L4A': ['minimal']}
    _write_granule(soc, (101, 1, 201), '2019100', 4.0e7)
    new_flag_in_l4a()
    log = H3BuildLogger(product_vars=copy.deepcopy(pv), dir=h3_dir, res=12, part=3, version=2)
    gb.build_h3db(product_vars=log.product_vars, **kw)
    log.set_post_build_info()
    log.save_log('COMPLETED')

    # The flag table grows, then the database takes a granule in its cells and one in a new cell.
    monkeypatch.setitem(_PRODUCT_QUALITY_FLAGS['L4A'], 2, _PRODUCT_QUALITY_FLAGS['L4A'][2] + [('new_flag', '== 1')])
    _write_granule(soc, (102, 1, 202), '2019150', 4.1e7)
    _write_granule(soc, (103, 1, 203), '2019160', 4.2e7, lon0=-40.5)
    new_flag_in_l4a()
    log = H3BuildLogger(product_vars=None, dir=h3_dir, res=12, part=3, version=2)
    gb.build_h3db(product_vars=log.product_vars, skip_granules=log.get_finished_granules(), **kw)
    log.set_post_build_info()
    log.save_log('COMPLETED')

    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')))
    assert len({tuple(sorted(pq.read_schema(f).names)) for f in files}) == 1
    assert not any('new_flag_l4a' in pq.read_schema(f).names for f in files)
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME)) as f:
        assert 'new_flag_l4a' not in json.load(f)['h3_columns']
    cols = ['agbd_l4a', 'l4_quality_flag_l4a']
    assert len(gh3_load(h3_dir, columns=cols).compute()) == 3 * len(GEDI_BEAMS) * N_SHOTS


# ---------------------------------------------------------------------------
# which degrade condition a selection gets
# ---------------------------------------------------------------------------

V3_COLS = ['l2a_quality_flag_rel3_l2a', 'degrade_flag_l2a', 'l2b_quality_flag_rel3_l2b',
           'l4a_quality_flag_rel3_l4a', 'degrade_include_flag_l4a', 'elev_highestreturn_outlier_flag_l4a',
           'l4c_quality_flag_rel3_l4c', 'degrade_include_flag_l4c', 'elev_highestreturn_outlier_flag_l4c',
           'agbd_l4a', 'wsci_l4c', 'rh_098_l2a', 'cover_l2b', 'rx_clipflag_l1b', 'stale_return_flag_l1b']


def _query(cols, list_=None, **products):
    """``build_query_string`` with no database: no build log, so the column names give the release."""
    import types
    from gedih3.cliutils import build_query_string
    from gedih3.config import GEDI_PRODUCTS
    args = types.SimpleNamespace(quality=True, list=list_, database=None, time_start=None, time_end=None,
                                 beam_type=None, query=None,
                                 **{p.lower(): products.get(p.lower()) for p in GEDI_PRODUCTS})
    return build_query_string(args, available_columns=cols)


def _conds(selected, cols=V3_COLS, version=3):
    from gedih3.cliutils import quality_conditions_for_selection
    return set(quality_conditions_for_selection(selected, version, cols))


@pytest.mark.parametrize('prod', ['L4A', 'L4C'])
def test_v3_l4_only_selection_uses_the_l4_inclusion_flag_not_degrade_flag(prod):
    sfx = f'_{prod.lower()}'
    got = _conds([prod])
    assert ('degrade_flag_l2a', '== 0') not in got
    assert {('l2a_quality_flag_rel3_l2a', '== 1'), (L4_V3[prod] + sfx, '== 1'),
            ('degrade_include_flag' + sfx, '== 1'), ('elev_highestreturn_outlier_flag' + sfx, '== 0')} == got
    q = _query(V3_COLS, **{prod.lower(): ['minimal']})
    assert 'degrade_flag_l2a' not in q and f'`degrade_include_flag{sfx}` == 1' in q


@pytest.mark.parametrize('l2', ['L2A', 'L2B'])
def test_v3_l2_and_l4a_selection_applies_both_degrade_conditions(l2):
    got = _conds([l2, 'L4A'])
    assert {('degrade_flag_l2a', '== 0'), ('degrade_include_flag_l4a', '== 1'),
            ('l2a_quality_flag_rel3_l2a', '== 1')} <= got
    if l2 == 'L2B':
        assert ('l2b_quality_flag_rel3_l2b', '== 1') in got


def test_v3_l4a_without_the_inclusion_column_keeps_degrade_flag():
    cols = [c for c in V3_COLS if c != 'degrade_include_flag_l4a']
    got = _conds(['L4A'], cols=cols)
    assert ('degrade_flag_l2a', '== 0') in got
    assert not any(c.startswith('degrade_include_flag') for c, _ in got)


def test_v2_l4a_selection_is_unchanged():
    cols = ['quality_flag_l2a', 'degrade_flag_l2a', 'l4_quality_flag_l4a', 'agbd_l4a']
    assert _conds(['L4A'], cols=cols, version=2) == {
        ('quality_flag_l2a', '== 1'), ('degrade_flag_l2a', '== 0'), ('l4_quality_flag_l4a', '== 1')}
    assert '`degrade_flag_l2a` == 0' in _query(cols, l4a=['agbd'])


def test_l1b_with_l4a_counts_as_no_l2_and_no_product_keeps_full_l2a():
    assert ('degrade_flag_l2a', '== 0') not in _conds(['L1B', 'L4A'])
    assert _conds([]) == {('l2a_quality_flag_rel3_l2a', '== 1'), ('degrade_flag_l2a', '== 0')}


def test_products_inferred_from_list_suffixes_follow_the_same_rule():
    assert 'degrade_flag_l2a' not in _query(V3_COLS, list_=['agbd_l4a'])
    assert '`degrade_flag_l2a` == 0' in _query(V3_COLS, list_=['agbd_l4a', 'rh_098_l2a'])


def test_l4_only_query_keeps_a_shot_its_inclusion_flag_admits():
    import pandas as pd
    # degrade_flag 30 with inclusion 1: admitted by the L4 flag, rejected by degrade_flag == 0.
    df = pd.DataFrame({c: [1, 1] for c in V3_COLS})
    df['degrade_flag_l2a'] = [0, 30]
    df['elev_highestreturn_outlier_flag_l4a'] = 0
    df['elev_highestreturn_outlier_flag_l4c'] = 0
    assert len(df.query(_query(V3_COLS, l4a=['minimal']))) == 2
    assert len(df.query(_query(V3_COLS, l2a=['minimal'], l4a=['minimal']))) == 1
