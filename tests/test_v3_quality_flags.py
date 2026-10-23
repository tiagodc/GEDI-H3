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


def _drop_l4a_source_columns(h3_dir):
    """A database whose L4A an older gedih3 added by a variable update: no ``root_file_l4a`` / ``root_beam_l4a``."""
    from gedih3.gh3builder import _refresh_year_columns_meta, h3_merge_metadata
    drop = {'root_file_l4a', 'root_beam_l4a'}
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')):
        t = pq.ParquetFile(f).read()  # not read_table: it adds the hive partition column of the path
        pq.write_table(t.drop_columns([c for c in t.column_names if c in drop]), f)
        _refresh_year_columns_meta(f)
    for d in glob.glob(os.path.join(h3_dir, 'h3_*')):
        h3_merge_metadata(d)
    path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    with open(path) as f:
        log = json.load(f)
    log['h3_columns'] = [c for c in log['h3_columns'] if c not in drop]
    log['h3_columns_dtypes'] = {k: v for k, v in (log.get('h3_columns_dtypes') or {}).items() if k not in drop}
    with open(path, 'w') as f:
        json.dump(log, f)


@pytest.mark.parametrize('without_source_columns', [False, True])
def test_an_update_after_the_flag_table_grew_keeps_one_column_set(tmp_dir, _client, monkeypatch,
                                                                  without_source_columns):
    """Without the guard, new cells held the new flag and older files did not:
    every read naming it (``--quality``) failed on the older files. A product an
    older variable update added has no ``root_file_<p>`` and is still held."""
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
    if without_source_columns:
        _drop_l4a_source_columns(h3_dir)

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
    assert not any('new_flag_l4a' in pq.read_schema(f).names for f in files)
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME)) as f:
        assert 'new_flag_l4a' not in json.load(f)['h3_columns']
    # An update of a database without source columns also writes root_file_l4a / root_beam_l4a to
    # new cells only, whatever the flag table: that drift is not the one this test is about.
    source = {'root_file_l4a', 'root_beam_l4a'} if without_source_columns else set()
    assert len({tuple(sorted(set(pq.read_schema(f).names) - source)) for f in files}) == 1
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


# gh3_build with the L4A table grown, as after a gedih3 upgrade. Dask workers import
# this module too: the table grows in each, and only the main process runs the build.
_GROWN_TABLE_RUNNER = """
import sys
from gedih3 import config
if ('new_flag', '== 1') not in config._PRODUCT_QUALITY_FLAGS['L4A'][2]:
    config._PRODUCT_QUALITY_FLAGS['L4A'][2] = config._PRODUCT_QUALITY_FLAGS['L4A'][2] + [('new_flag', '== 1')]
if __name__ == '__main__':
    from gedih3.cli import gh3_build
    sys.exit(gh3_build.main())
"""


def test_a_replacement_after_the_flag_table_grew_keeps_one_column_set(tmp_dir):
    """Replaced rows moved into a cell-year the database did not hold must not start the new flag's column there."""
    import subprocess
    import h5py
    import numpy as np
    from gedih3.config import GEDI_BEAMS
    sys.path.insert(0, os.path.dirname(__file__))
    from test_granule_replacement import NEW, OLD, _gh3_build, _write_release

    soc, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))

    def new_flag_in_l4a():
        for fn in glob.glob(os.path.join(soc, '*', '*', 'GEDI04_A*.h5')):
            with h5py.File(fn, 'a') as f:
                for b in GEDI_BEAMS:
                    if 'new_flag' not in f[b]:
                        f[b]['new_flag'] = np.ones(f[b]['shot_number'].shape[0], dtype='u1')

    moved = (101, 1, 201)
    _write_release(soc, moved, OLD, lon0=-50.9, seed=1)
    _write_release(soc, (103, 1, 203), OLD, lon0=-49.6, seed=3)
    new_flag_in_l4a()
    _gh3_build(soc, h3_dir, tmp)
    before = {os.path.relpath(f, h3_dir) for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))}

    # The re-release moves the granule's shots north, into cells the database does not hold yet.
    names = _write_release(soc, moved, NEW, lon0=-50.3, seed=11, n=15, first=3)
    with h5py.File(os.path.join(soc, '2020', '100', names['L2A']), 'a') as f:
        for b in GEDI_BEAMS:
            f[b]['lat_lowestmode'][...] = f[b]['lat_lowestmode'][...] + 0.5
    new_flag_in_l4a()
    runner = os.path.join(tmp_dir, 'grown_table_gh3_build.py')
    with open(runner, 'w') as f:
        f.write(_GROWN_TABLE_RUNNER)
    cmd = [sys.executable, runner, '-i', soc, '-o', h3_dir, '-t', tmp, '--gedi-version', '2', '-N', '2', '-T', '1',
           '-M', '2', '-P', '0', '--no-bbox-index', '-l2a', 'rh_098', '-l4a', 'agbd', '--region=-51,0,-49,1',
           '--replace-granules', 'auto']
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    assert res.returncode == 0, (res.stdout + res.stderr)[-4000:]

    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')))
    assert {os.path.relpath(f, h3_dir) for f in files} - before      # the replacement did create files
    assert len({tuple(sorted(pq.read_schema(f).names)) for f in files}) == 1
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME)) as f:
        assert 'new_flag_l4a' not in json.load(f)['h3_columns']


# ---------------------------------------------------------------------------
# products inferred from the columns a command reads
# ---------------------------------------------------------------------------

def test_quality_products_are_inferred_from_a_list_file_wildcards_and_resolved_columns(tmp_dir):
    from gedih3.cliutils import build_query_string
    vars_file = os.path.join(tmp_dir, 'vars.txt')
    with open(vars_file, 'w') as f:
        f.write('# biomass\nagbd_l4a\n')
    for list_ in ([vars_file], ['agbd*']):
        q = _query(V3_COLS, list_=list_)
        assert '`l4a_quality_flag_rel3_l4a` == 1' in q and '`degrade_include_flag_l4a` == 1' in q, list_
    # gh3_aggregate's dict-form -a names columns that no -l lists: the caller passes the resolved columns.
    import types
    from gedih3.config import GEDI_PRODUCTS
    args = types.SimpleNamespace(quality=True, list=None, database=None, time_start=None, time_end=None,
                                 beam_type=None, query=None, **{p.lower(): None for p in GEDI_PRODUCTS})
    q = build_query_string(args, available_columns=V3_COLS, columns=['wsci_l4c', 'geometry'])
    assert '`l4c_quality_flag_rel3_l4c` == 1' in q and 'degrade_flag_l2a' not in q


def test_a_product_with_none_of_its_flag_columns_is_warned_about(caplog):
    lg = logging.getLogger('gedih3.cliutils')
    lg.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger='gedih3.cliutils'):
            out = get_product_quality_conditions(['L4C'], version=3, available_columns=['wsci_l4c'])
    finally:
        lg.removeHandler(caplog.handler)
    assert out == []
    assert any('none of its flag column' in r.getMessage() and 'l4c_quality_flag_rel3_l4c' in r.getMessage()
               for r in caplog.records)


def test_gh3_extract_passes_its_resolved_columns_to_the_quality_filter(mini_h3_database, tmp_dir, monkeypatch):
    import gedih3.cliutils as cu
    from gedih3.cli import gh3_extract
    seen = {}

    class _Stop(Exception):
        pass

    def spy(args, available_columns=None, columns=None):
        seen['columns'] = columns
        raise _Stop

    monkeypatch.setattr(cu, 'build_query_string', spy)
    monkeypatch.setattr(sys, 'argv', ['gh3_extract', '-d', mini_h3_database, '-l', 'agbd*', '-y',
                                      '-o', os.path.join(tmp_dir, 'out')])
    with pytest.raises((_Stop, SystemExit)):
        gh3_extract.main()
    assert seen['columns'] is not None and 'agbd_l4a' in seen['columns']


def test_a_variable_add_of_a_product_new_to_the_database_stores_its_quality_flags(tmp_dir, _client):
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger
    sys.path.insert(0, os.path.dirname(__file__))
    from test_phased_product_updates import _write_granule

    soc, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    kw = dict(res=12, part=3, soc_source=soc, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    _write_granule(soc, (101, 1, 201), '2019100', 4.0e7)
    _write_granule(soc, (102, 1, 202), '2019160', 4.2e7, lon0=-40.5)
    log = H3BuildLogger(product_vars={'L2A': ['rh_098']}, dir=h3_dir, res=12, part=3, version=2)
    gb.build_h3db(product_vars=log.product_vars, **kw)
    log.set_post_build_info()
    log.save_log('COMPLETED')

    # gh3_build -l4a agbd on the L2A database: a variable-only update.
    log = H3BuildLogger(product_vars={'L4A': ['agbd']}, dir=h3_dir, res=12, part=3, version=2)
    requested = copy.deepcopy(log.new_product_vars)
    gb.build_h3db(product_vars=dict(log.new_product_vars), variable_only_update=True, **kw)
    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')))
    assert len(files) > 1
    for f in files:
        assert {'agbd_l4a', 'l4_quality_flag_l4a'} <= set(pq.read_schema(f).names), f
    assert log.new_product_vars == requested          # the caller's lists are not edited
