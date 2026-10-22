"""superseded_granules: indexed granules whose source was re-released (#47)."""

import os

import pytest

import gedih3.doctor.diagnoses  # noqa: F401
from gedih3.doctor import Severity
from gedih3.doctor.runner import ALIAS_GROUPS, get_diagnoses
from test_doctor_diagnoses import _ctx, _make_build_log


@pytest.fixture(scope='module', autouse=True)
def _module_dask_client():
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def _touch(soc_dir, orbit, release, product='02_A'):
    d = os.path.join(soc_dir, '2020', '100')
    os.makedirs(d, exist_ok=True)
    name = f"GEDI{product}_2020100120000_O{orbit:05d}_01_T{orbit:05d}_{release}_V002.h5"
    open(os.path.join(d, name), 'w').close()


def _check(ctx):
    return get_diagnoses()['superseded_granules'].check(ctx)


def test_reports_newer_and_older_releases_only_for_recorded_indexed_granules(tmp_path):
    h3_dir, soc_dir = str(tmp_path / 'db'), str(tmp_path / 'soc')
    os.makedirs(h3_dir)
    rel = {'L2A': '02_003_02'}
    _make_build_log(h3_dir, granules=[
        {'orbit': 100, 'granule': 1, 'track': 100, 'status': 'INDEXED', 'releases': rel},   # newer listed
        {'orbit': 101, 'granule': 1, 'track': 101, 'status': 'NO_DATA', 'releases': rel},   # only older listed
        {'orbit': 102, 'granule': 1, 'track': 102, 'status': 'INDEXED', 'releases': rel},   # same
        {'orbit': 103, 'granule': 1, 'track': 103, 'status': 'INDEXED'},                    # unknown
        {'orbit': 104, 'granule': 1, 'track': 104, 'status': 'PENDING', 'releases': rel},   # not read yet
    ])
    for orbit, releases in ((100, ('02_003_02', '02_004_01')), (101, ('02_003_01',)), (102, ('02_003_02',)),
                            (103, ('02_004_01',)), (104, ('02_004_01',))):
        for r in releases:
            _touch(soc_dir, orbit, r)

    report = _check(_ctx(h3_dir, soc_dir=soc_dir))

    assert report.severity == Severity.WARN
    by_kind = {f['kind']: f for f in report.findings}
    assert len(report.findings) == 2
    assert by_kind['newer_release']['granule'] == {'orbit': 100, 'granule': 1, 'track': 100}
    assert by_kind['newer_release']['products'] == {'L2A': {'indexed': '02_003_02', 'listed': '02_004_01'}}
    assert by_kind['older_release']['products'] == {'L2A': {'indexed': '02_003_02', 'listed': '02_003_01'}}
    assert '1 granule(s) without a recorded release' in report.summary
    assert len(report.recommendations) == 2 and all(r.isascii() for r in report.recommendations)


def test_is_info_without_a_soc_dir_or_recorded_releases(tmp_path):
    h3_dir, soc_dir = str(tmp_path / 'db'), str(tmp_path / 'soc')
    os.makedirs(h3_dir)
    _make_build_log(h3_dir)                     # a log written before releases were recorded
    _touch(soc_dir, 100, '02_004_01')
    for ctx in (_ctx(h3_dir), _ctx(h3_dir, soc_dir=soc_dir)):
        report = _check(ctx)
        assert report.severity == Severity.INFO and not report.findings


def test_an_s3_soc_dir_says_detection_is_local_only(tmp_path):
    h3_dir = str(tmp_path / 'db')
    os.makedirs(h3_dir)
    _make_build_log(h3_dir)
    report = _check(_ctx(h3_dir, soc_dir='s3://bucket/soc'))
    assert report.severity == Severity.INFO and 'local SOC trees only' in report.summary


def test_runs_with_the_soc_group():
    assert 'superseded_granules' in ALIAS_GROUPS['soc']
    assert 'superseded_granules' not in ALIAS_GROUPS['db']


def test_files_the_build_excludes_are_not_a_newer_release(tmp_path):
    """An _SGS variant outranks the indexed file, but the build excluded it
    (gh3_build --exclude, kept in the log): it is not reported."""
    import json
    h3_dir, soc_dir = str(tmp_path / 'db'), str(tmp_path / 'soc')
    os.makedirs(h3_dir)
    log_path = _make_build_log(h3_dir, granules=[
        {'orbit': 100, 'granule': 1, 'track': 100, 'status': 'INDEXED', 'releases': {'L2A': '02_003_02'}}])
    _touch(soc_dir, 100, '02_003_02')
    d = os.path.join(soc_dir, '2020', '100')
    plain = os.listdir(d)[0]
    open(os.path.join(d, plain.replace('_V002.h5', '_V002_SGS.h5')), 'w').close()

    assert _check(_ctx(h3_dir, soc_dir=soc_dir)).findings                     # not excluded: reported
    log = json.load(open(log_path))
    log['exclude'] = ['*_SGS.h5']
    json.dump(log, open(log_path, 'w'))
    report = _check(_ctx(h3_dir, soc_dir=soc_dir))
    assert report.severity == Severity.INFO and not report.findings


def test_backfill_reads_no_file_the_build_excludes(tmp_path):
    """The backfill fix's listing applies the log's exclude patterns, so a gap is
    filled from the file the granule was built from, not from an _SGS variant."""
    from gedih3.doctor.diagnoses.backfill import _get_soc_tree
    soc_dir = str(tmp_path / 'soc')
    _touch(soc_dir, 100, '02_003_02')
    d = os.path.join(soc_dir, '2020', '100')
    plain = os.listdir(d)[0]
    open(os.path.join(d, plain.replace('_V002.h5', '_V002_SGS.h5')), 'w').close()

    assert _get_soc_tree(soc_dir, version=2)['O00100_01_T00100']['L2A'].endswith('_SGS.h5')
    tree = _get_soc_tree(soc_dir, version=2, exclude=['*_SGS.h5'])
    assert os.path.basename(tree['O00100_01_T00100']['L2A']) == plain
