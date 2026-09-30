"""Phased product updates: index L2A before later products are published, backfill them after.

L2B / L4A / L4C are derived from L2A and published later. A database that
already carries those products must still take new dates as soon as their L2A
exists (``allow_missing_products``): the rows store the missing products'
columns as null and the build log records the product as ``MISSING_SOURCE``.
Once the product files appear, ``_build_fill_products`` writes their values
into exactly those null cells — each product file read once, only the files
holding those granules rewritten — which ``gh3_build`` runs automatically and
``gh3_doctor --fix backfill`` on demand.
"""
import glob
import json
import os
import types

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gedih3.config import GEDI_BEAMS, BUILD_LOG_FILENAME

N_SHOTS = 20
PRODUCT_VARS = {'L2A': ['rh_098'], 'L4A': ['agbd']}


# ---------------------------------------------------------------------------
# synthetic granules
# ---------------------------------------------------------------------------

def _gedi_name(product, stamp, orbit, granule, track):
    return f"GEDI{product}_{stamp}000000_O{orbit:05d}_{granule:02d}_T{track:05d}_02_003_02_V002.h5"


def _write_granule(soc_dir, key, stamp, delta_time, products=('02_A', '04_A'), shot_offset=0, lon0=-50.5):
    """Synthetic granule files with every variable a build reads. ``stamp`` is
    the ``YYYYDDD`` of the file name; ``delta_time`` puts its shots in that year."""
    orbit, granule, track = key
    day = os.path.join(soc_dir, stamp[:4], stamp[4:])
    os.makedirs(day, exist_ok=True)
    rng = np.random.default_rng(orbit)
    for product in products:
        with h5py.File(os.path.join(day, _gedi_name(product, stamp, *key)), 'w') as f:
            for i, beam in enumerate(GEDI_BEAMS):
                g = f.create_group(beam)
                g['shot_number'] = np.arange(N_SHOTS, dtype=np.uint64) + np.uint64(orbit * 10**6 + i * 10**3 + shot_offset)
                if product == '02_A':
                    g['delta_time'] = rng.uniform(delta_time, delta_time + 1e5, N_SHOTS)
                    g['lat_lowestmode'] = rng.uniform(0.0, 0.5, N_SHOTS)
                    g['lon_lowestmode'] = rng.uniform(lon0, lon0 + 0.5, N_SHOTS)
                    g['elev_lowestmode'] = rng.uniform(0, 100, N_SHOTS)
                    g['quality_flag'] = np.ones(N_SHOTS, dtype='u1')
                    g['degrade_flag'] = np.zeros(N_SHOTS, dtype='u1')
                    g['sensitivity'] = rng.uniform(0.9, 1.0, N_SHOTS)
                    g['rh_098'] = rng.uniform(0, 50, N_SHOTS)
                    g['rh_050'] = rng.uniform(0, 30, N_SHOTS)
                else:
                    g['agbd'] = rng.uniform(1, 300, N_SHOTS)
                    g['agbd_se'] = rng.uniform(1, 30, N_SHOTS)
                    g['l4_quality_flag'] = np.ones(N_SHOTS, dtype='u1')


def _db_table(h3_dir, year=None):
    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', f"year={year or '*'}", '*.parquet')))
    return pa.concat_tables([pq.read_table(f) for f in files], promote_options='default')


@pytest.fixture
def _client():
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


# ---------------------------------------------------------------------------
# end to end: phase 1 (L2A only), phase 2 (backfill)
# ---------------------------------------------------------------------------

def test_l2a_first_then_backfill(tmp_dir, _client):
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    kw = dict(res=12, part=3, soc_source=soc_dir, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    a, b = (101, 1, 201), (102, 1, 202)

    # Existing database: granule A (2019) with every product.
    _write_granule(soc_dir, a, '2019100', 4.0e7)
    gb.build_h3db(product_vars=PRODUCT_VARS, **kw)
    log = H3BuildLogger(product_vars=PRODUCT_VARS, dir=h3_dir, res=12, part=3, version=2)
    log.set_post_build_info()
    log.save_log('COMPLETED')
    a_files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=2019', '*.parquet')))
    a_mtimes = {f: os.path.getmtime(f) for f in a_files}

    # Granule B (2020): only L2A is published so far.
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    log.register_pending_granules([dict(zip(('orbit', 'granule', 'track'), b))])

    # Default behaviour is unchanged: a granule needs every product.
    assert gb.build_h3db(product_vars=PRODUCT_VARS, skip_granules=log.get_finished_granules(), **kw) is None

    seen = {}
    gb.build_h3db(product_vars=PRODUCT_VARS, skip_granules=log.get_finished_granules(),
                  allow_missing_products=True, granule_products_callback=seen.update, **kw)
    assert seen == {b: ['L4A']}
    log.set_product_statuses({b: {'L4A': 'MISSING_SOURCE', 'L2A': 'PENDING'}})
    log.set_post_build_info()
    log.save_log('COMPLETED')

    t = _db_table(h3_dir, 2020)
    assert t.num_rows == len(GEDI_BEAMS) * N_SHOTS
    assert t['agbd_l4a'].null_count == t.num_rows          # stored, typed, null
    assert set(_db_table(h3_dir).schema.names) >= {'agbd_l4a', 'l4_quality_flag_l4a', 'root_file_l4a'}
    assert log.pending_product_fills() == {b: ['L4A']}     # survived set_post_build_info

    # L4A arrives: the backfill fills B's null cells only.
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',))
    out = gb._build_fill_products(h3_dir, log.pending_product_fills(), soc_source=soc_dir, version=2,
                                  tmp_dir=tmp)
    assert out['filled'] == {b: ['L4A']} and not out['failed'] and not out['unavailable']

    t = _db_table(h3_dir, 2020)
    assert t['agbd_l4a'].null_count == 0
    assert t['l4_quality_flag_l4a'].null_count == 0
    assert set(t['root_file_l4a'].to_pylist()) == {_gedi_name('04_A', '2020200', *b)}
    # Files that hold no target granule are never opened for writing.
    assert {f: os.path.getmtime(f) for f in a_files} == a_mtimes
    # Scaffolding cleaned after a clean run.
    assert not os.path.exists(os.path.join(tmp, '_product_fill', '_var_frags'))

    log.set_product_statuses({k: {p: 'INDEXED' for p in ps} for k, ps in out['filled'].items()})
    log.set_post_build_info()
    assert log.pending_product_fills() == {}

    # The phased database equals a from-scratch build of the same granules.
    ref_dir = os.path.join(tmp_dir, 'ref_db')
    gb.build_h3db(product_vars=PRODUCT_VARS, **{**kw, 'h3_dir': ref_dir, 'tmp_dir': os.path.join(tmp_dir, 'ref_tmp')})
    got, ref = _db_table(h3_dir), _db_table(ref_dir)
    assert sorted(got.schema.names) == sorted(ref.schema.names)
    order = lambda t: t.select(sorted(ref.schema.names)).sort_by('shot_number')  # noqa: E731
    assert order(got).equals(order(ref))


def test_fill_reports_unavailable_until_the_product_exists(tmp_dir, _client):
    import gedih3.gh3builder as gb
    out = gb._build_fill_products(os.path.join(tmp_dir, 'db'), {(1, 1, 1): ['L4A']},
                                  soc_source=[], version=2)
    assert out['unavailable'] == {(1, 1, 1): ['L4A']} and not out['filled']


# ---------------------------------------------------------------------------
# schema completion and leaf conform
# ---------------------------------------------------------------------------

def test_complete_schema_appends_db_columns_in_db_types(tmp_dir):
    from gedih3.gh3builder import _complete_schema_from_db
    with open(os.path.join(tmp_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'h3_columns_dtypes': {'rh_098_l2a': 'double', 'agbd_l4a': 'float',
                                         'l4_quality_flag_l4a': 'uint8', 'empty_l4a': 'null'}}, f)
    src = pa.schema([('rh_098_l2a', pa.float64())], metadata={b'geo': b'{}'})

    out, added = _complete_schema_from_db(src, tmp_dir)

    # A null-typed database column is kept too: every fragment must carry every column.
    assert added == ['agbd_l4a', 'empty_l4a', 'l4_quality_flag_l4a']
    assert out.field('empty_l4a').type == pa.null()
    assert out.field('agbd_l4a').type == pa.float32() and out.field('agbd_l4a').nullable
    assert out.field('l4_quality_flag_l4a').type == pa.uint8()
    assert out.metadata == src.metadata


def test_complete_schema_resolves_non_alias_types_from_a_footer(tmp_dir):
    from gedih3.gh3builder import _complete_schema_from_db
    ydir = os.path.join(tmp_dir, 'h3_03=830001fffffffff', 'year=2020')
    os.makedirs(ydir)
    pq.write_table(pa.table({'t_l4a': pa.array([0], pa.timestamp('ms', tz='UTC'))}),
                   os.path.join(ydir, '830001fffffffff.2020.0.parquet'))
    with open(os.path.join(tmp_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'h3_partition_level': 3, 'h3_partition_ids': ['830001fffffffff'],
                   'h3_columns_dtypes': {'t_l4a': 'timestamp[ms, tz=UTC]', 'x_l4a': 'odd<type>'}}, f)

    from gedih3.exceptions import GediValidationError
    with pytest.raises(GediValidationError, match='x_l4a'):
        _complete_schema_from_db(pa.schema([('a', pa.int8())]), tmp_dir)
    with open(os.path.join(tmp_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'h3_partition_level': 3, 'h3_partition_ids': ['830001fffffffff'],
                   'h3_columns_dtypes': {'t_l4a': 'timestamp[ms, tz=UTC]'}}, f)
    out, added = _complete_schema_from_db(pa.schema([('a', pa.int8())]), tmp_dir)
    assert added == ['t_l4a'] and out.field('t_l4a').type == pa.timestamp('ms', tz='UTC')


def test_conform_fills_missing_columns_and_refuses_extras():
    from gedih3.exceptions import GediValidationError
    from gedih3.gh3builder import _conform_table_to_schema
    schema = pa.schema([('a', pa.int32()), ('b', pa.float32()), ('c', pa.uint8())], metadata={b'k': b'v'})
    out = _conform_table_to_schema(pa.table({'c': pa.array([1, 2], pa.int64()), 'a': [5, 6]}), schema)
    assert out.schema == schema and out.schema.metadata == {b'k': b'v'}
    assert out['b'].null_count == 2 and out['a'].to_pylist() == [5, 6]
    with pytest.raises(GediValidationError, match='zzz'):
        _conform_table_to_schema(pa.table({'a': [1], 'zzz': [1]}), schema)


# ---------------------------------------------------------------------------
# parquet_fill_columns (Arrow)
# ---------------------------------------------------------------------------

def test_fill_matches_adjacent_large_shot_numbers(tmp_dir):
    """GEDI shot numbers (~1e17) exceed float64's exact integers: a uint64
    base against an int64 patch must still match key for key."""
    from gedih3.utils import parquet_fill_columns
    shots = np.uint64(177780000200000000) + np.arange(6, dtype=np.uint64)
    base = os.path.join(tmp_dir, 'base.parquet')
    pq.write_table(pa.table({'shot_number': shots,
                             'agbd_l4a': pa.array([1.0, None, None, 4.0, None, float('nan')], pa.float32()),
                             'flag_l4a': pa.array([1, None, None, 1, None, None], pa.uint8())},
                            metadata={b'geo': b'{"x": 1}'}), base, row_group_size=4)
    patch = os.path.join(tmp_dir, 'patch.parquet')
    pq.write_table(pa.table({'shot_number': pa.array(shots.astype(np.int64)[::-1]),
                             'agbd_l4a': pa.array([60.0, 50.0, 40.0, 30.0, 20.0, 10.0]),
                             'flag_l4a': pa.array([1, 1, 1, 1, 1, 1], pa.int64())}), patch)

    parquet_fill_columns(base, [patch])

    t = pq.read_table(base)
    assert t['agbd_l4a'].to_pylist() == [1.0, 20.0, 30.0, 4.0, 50.0, 60.0]  # existing values kept
    assert t.schema.field('agbd_l4a').type == pa.float32()
    assert t['flag_l4a'].to_pylist() == [1, 1, 1, 1, 1, 1] and t.schema.field('flag_l4a').type == pa.uint8()
    assert pq.ParquetFile(base).metadata.num_row_groups == 2
    assert t.schema.metadata[b'geo'] == b'{"x": 1}'


def test_fill_that_cannot_cast_leaves_the_base_untouched(tmp_dir):
    from gedih3.utils import parquet_fill_columns
    base = os.path.join(tmp_dir, 'base.parquet')
    pq.write_table(pa.table({'shot_number': pa.array([1, 2], pa.uint64()),
                             'flag_l4a': pa.array([None, None], pa.uint8())}), base)
    patch = os.path.join(tmp_dir, 'patch.parquet')
    pq.write_table(pa.table({'shot_number': [1, 2], 'flag_l4a': [300, 1]}), patch)
    before = open(base, 'rb').read()

    with pytest.raises(pa.ArrowInvalid):
        parquet_fill_columns(base, [patch])

    assert open(base, 'rb').read() == before
    assert os.listdir(tmp_dir) == sorted(['base.parquet', 'patch.parquet']) or \
        set(os.listdir(tmp_dir)) == {'base.parquet', 'patch.parquet'}


# ---------------------------------------------------------------------------
# build log and detection
# ---------------------------------------------------------------------------

def _logger_with(tmp_dir, granules):
    from gedih3.logger import H3BuildLogger
    with open(os.path.join(tmp_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'gedi_version': 2, 'h3_resolution_level': 12, 'h3_partition_level': 3,
                   'products': {'L2A': {'variables': ['rh_098']}, 'L4A': {'variables': ['agbd']}},
                   'granules': granules}, f)
    return H3BuildLogger(product_vars=None, dir=tmp_dir)


def test_pending_fills_and_bulk_status_updates(tmp_dir):
    log = _logger_with(tmp_dir, [
        {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED',
         'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}},
        {'orbit': 2, 'granule': 1, 'track': 1, 'status': 'PENDING',
         'products': {'L2A': 'PENDING', 'L4A': 'MISSING_SOURCE'}},   # not in the database yet
    ])
    assert log.pending_product_fills() == {(1, 1, 1): ['L4A']}
    assert log.set_product_statuses({(1, 1, 1): {'L4A': 'INDEXED'}, (9, 9, 9): {'L4A': 'INDEXED'}}) == 1
    assert log.pending_product_fills() == {}
    from gedih3.exceptions import GediValidationError
    with pytest.raises(GediValidationError):
        log.set_product_statuses({(1, 1, 1): {'L4A': 'BOGUS'}})


def test_detects_awaited_product_file(tmp_dir):
    from gedih3.cli.gh3_build import _has_new_local_granules
    soc = os.path.join(tmp_dir, 'soc')
    log = types.SimpleNamespace(
        gedi_version=2,
        granule_info=[{'orbit': 102, 'granule': 1, 'track': 202}],
        pending_product_fills=lambda: {(102, 1, 202): ['L4A']},
    )
    os.makedirs(os.path.join(soc, '2020', '200'))
    open(os.path.join(soc, '2020', '200', _gedi_name('02_A', '2020200', 102, 1, 202)), 'w').close()
    assert _has_new_local_granules(soc, log) is False       # tracked, nothing awaited on disk
    open(os.path.join(soc, '2020', '200', _gedi_name('04_A', '2020200', 102, 1, 202)), 'w').close()
    assert _has_new_local_granules(soc, log) is True


def test_doctor_reports_awaited_products_from_the_log(tmp_dir):
    """The log names the gap; the row scan's partial-NaN findings for the same
    granule × product are folded in, not listed twice."""
    from gedih3.doctor.diagnoses.backfill import _finalize_backfill_check
    log = _logger_with(tmp_dir, [{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED',
                                  'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}}])
    ctx = types.SimpleNamespace(h3_logger=log, args=None)
    scan = {'/db/h3_03=x': [
        {'kind': 'partial_nan', 'partition_dir': '/db/h3_03=x', 'parquet_file': 'f', 'product': 'L4A',
         'granule': {'orbit': 1, 'granule': 1, 'track': 1}, 'null_rows': 5},
        {'kind': 'partial_nan', 'partition_dir': '/db/h3_03=x', 'parquet_file': 'f', 'product': 'L4A',
         'granule': {'orbit': 7, 'granule': 1, 'track': 1}, 'null_rows': 1},
    ]}
    report = _finalize_backfill_check(ctx, scan)
    kinds = [(f['kind'], f['granule']['orbit']) for f in report.findings]
    assert kinds == [('missing_source', 1), ('partial_nan', 7)]
    assert 'awaiting products' in report.summary


# ---------------------------------------------------------------------------
# resume state, CLI integration, doctor guard
# ---------------------------------------------------------------------------

def test_stale_fill_state_of_another_target_set_is_discarded(tmp_dir, _client):
    """A per-granule sentinel left by an earlier fill (say L4A, interrupted)
    must not stand in for a later fill of another product for the same granule."""
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    kw = dict(res=12, part=3, soc_source=soc_dir, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, (101, 1, 201), '2020100', 7.0e7)
    gb.build_h3db(product_vars=PRODUCT_VARS, **kw)
    log = H3BuildLogger(product_vars=PRODUCT_VARS, dir=h3_dir, res=12, part=3, version=2)
    log.set_post_build_info()
    log.save_log('COMPLETED')
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)

    fill_tmp = os.path.join(tmp, '_product_fill')
    gb._emit_var_fan_sentinel(fill_tmp, gb._granule_key_str(b))          # left by another run
    with open(os.path.join(fill_tmp, '_fill_targets.json'), 'w') as f:
        f.write('[["O00102_01_T00202", ["L4C"]]]')

    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',))
    out = gb._build_fill_products(h3_dir, {b: ['L4A']}, soc_source=soc_dir, version=2, tmp_dir=tmp)

    assert out['filled'] == {b: ['L4A']}
    assert _db_table(h3_dir, 2020).filter(pa.compute.field('root_file_l2a').isin(
        [_gedi_name('02_A', '2020200', *b)]))['agbd_l4a'].null_count == 0


def test_phased_updates_through_the_cli(tmp_dir):
    """gh3_build end to end: a phased run records MISSING_SOURCE and remembers
    the mode; a plain later run notices the product file and fills it."""
    import subprocess
    import sys

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))

    def gh3_build(*extra):
        cmd = [sys.executable, '-m', 'gedih3.cli.gh3_build', '-i', soc_dir, '-o', h3_dir, '-t', tmp,
               '--gedi-version', '2', '-N', '2', '-T', '1', '-M', '2', '-P', '0', '--no-bbox-index', *extra]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
        return res.stdout + res.stderr

    def log():
        return json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))

    b = (102, 1, 202)
    _write_granule(soc_dir, (101, 1, 201), '2020100', 7.0e7)
    gh3_build('-l2a', 'rh_098', '-l4a', 'agbd')

    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    gh3_build('--allow-missing-products')
    g = next(g for g in log()['granules'] if (g['orbit'], g['granule'], g['track']) == b)
    assert g['status'] == 'INDEXED' and g['products']['L4A'] == 'MISSING_SOURCE'
    assert log()['allow_missing_products'] is True
    assert _db_table(h3_dir)['agbd_l4a'].null_count == len(GEDI_BEAMS) * N_SHOTS

    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',))
    out = gh3_build()
    assert 'Product backfill: filled 1 granule(s)' in out
    g = next(g for g in log()['granules'] if (g['orbit'], g['granule'], g['track']) == b)
    assert g['products']['L4A'] == 'INDEXED'
    assert _db_table(h3_dir)['agbd_l4a'].null_count == 0

    assert 'up-to-date' in gh3_build()
    # Turning the mode off on an up-to-date database is remembered too.
    assert 'up-to-date' in gh3_build('--no-allow-missing-products')
    assert 'allow_missing_products' not in log()


def test_doctor_fix_refuses_while_a_build_is_in_flight(tmp_dir):
    """Refusal writes nothing, the build log included (applied=False)."""
    from gedih3.doctor.diagnoses.backfill import backfill_fix
    from gedih3.doctor.report import Report, Severity
    log = _logger_with(tmp_dir, [{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED',
                                  'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}}])
    log.log_data['status'] = 'MERGING'
    ctx = types.SimpleNamespace(h3_logger=log, h3_dir=tmp_dir, tmp_dir=None, soc_dir=None,
                                args=types.SimpleNamespace(s3=False))
    report = Report(name='backfill', severity=Severity.WARN, findings=[
        {'kind': 'missing_source', 'granule': {'orbit': 1, 'granule': 1, 'track': 1}, 'products': ['L4A']}])

    out = backfill_fix(ctx, report)

    assert out.applied is False and out.severity == Severity.ERROR and 'MERGING' in out.summary


def _phased_db(tmp_dir):
    """A database with complete granule A (2020), a logger, and the build kwargs."""
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    kw = dict(res=12, part=3, soc_source=soc_dir, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    _write_granule(soc_dir, (101, 1, 201), '2020100', 7.0e7)
    gb.build_h3db(product_vars=PRODUCT_VARS, **kw)
    log = H3BuildLogger(product_vars=PRODUCT_VARS, dir=h3_dir, res=12, part=3, version=2)
    log.set_post_build_info()
    log.save_log('COMPLETED')
    return soc_dir, h3_dir, tmp, kw


def _l4a_nulls(h3_dir, key):
    t = _db_table(h3_dir, 2020)
    return t.filter(pa.compute.field('root_file_l2a').isin([_gedi_name('02_A', '2020200', *key)]))['agbd_l4a'].null_count


def test_consecutive_phased_fills_into_the_same_file(tmp_dir, _client):
    """Workers outlive one update: a base file rewritten by a later phase-1 run
    must not be routed against its cached shot list (every row must fill)."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    for key in ((102, 1, 202), (103, 1, 203)):
        _write_granule(soc_dir, key, '2020200', 8.0e7, products=('02_A',))
        gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)
        _write_granule(soc_dir, key, '2020200', 8.0e7, products=('04_A',))
        out = gb._build_fill_products(h3_dir, {key: ['L4A']}, soc_source=soc_dir, version=2, tmp_dir=tmp)
        assert out['filled'] == {key: ['L4A']}
        assert _l4a_nulls(h3_dir, key) == 0


def test_retry_of_the_same_targets_fills_what_failed(tmp_dir, _client):
    """A failed read leaves the granule pending; the retry (same targets) must
    merge it even into files the first attempt already filled for others."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b, c = (102, 1, 202), (103, 1, 203)
    for key in (b, c):
        _write_granule(soc_dir, key, '2020200', 8.0e7, products=('02_A',))
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)
    for key in (b, c):
        _write_granule(soc_dir, key, '2020200', 8.0e7, products=('04_A',))
    bad = glob.glob(os.path.join(soc_dir, '2020', '200', _gedi_name('04_A', '2020200', *c)))[0]
    good = open(bad, 'rb').read()
    open(bad, 'wb').write(b'\0' * 64)
    targets = {b: ['L4A'], c: ['L4A']}

    first = gb._build_fill_products(h3_dir, targets, soc_source=soc_dir, version=2, tmp_dir=tmp)
    assert first['filled'] == {b: ['L4A']} and set(first['failed']) == {c}

    open(bad, 'wb').write(good)
    second = gb._build_fill_products(h3_dir, targets, soc_source=soc_dir, version=2, tmp_dir=tmp)
    assert set(second['filled']) == {b, c} and not second['failed']
    assert _l4a_nulls(h3_dir, b) == 0 and _l4a_nulls(h3_dir, c) == 0


def test_admitting_build_refuses_a_schema_without_a_requested_product(tmp_dir, _client):
    """No granule of the batch has L4A and the database has no L4A columns:
    writing now would narrow the schema and later merges would drop L4A."""
    import gedih3.gh3builder as gb
    from gedih3.exceptions import GediValidationError
    soc_dir = os.path.join(tmp_dir, 'soc')
    _write_granule(soc_dir, (102, 1, 202), '2020200', 8.0e7, products=('02_A',))
    with pytest.raises(GediValidationError, match='L4A'):
        gb.build_h3db(product_vars=PRODUCT_VARS, res=12, part=3, soc_source=soc_dir, version=2,
                      tmp_dir=os.path.join(tmp_dir, 'tmp'), h3_dir=os.path.join(tmp_dir, 'db'),
                      allow_missing_products=True)


def test_fill_mode_never_appends_columns(tmp_dir):
    from gedih3.utils import parquet_fill_columns
    base = os.path.join(tmp_dir, 'base.parquet')
    pq.write_table(pa.table({'shot_number': pa.array([1, 2], pa.uint64()),
                             'agbd_l4a': pa.array([None, 2.0], pa.float32())}), base)
    patch = os.path.join(tmp_dir, 'patch.parquet')
    pq.write_table(pa.table({'shot_number': [1, 2], 'agbd_l4a': [1.0, 9.0], 'stray_l4a': [1, 2]}), patch)
    parquet_fill_columns(base, [patch], append_new=False)
    t = pq.read_table(base)
    assert t.schema.names == ['shot_number', 'agbd_l4a'] and t['agbd_l4a'].to_pylist() == [1.0, 2.0]


def test_a_fill_that_matches_no_rows_is_never_reported_filled(tmp_dir, _client):
    """A product file whose shots match none of the granule's rows wrote
    nothing: a retry must not see a leftover sentinel and call it filled."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',), shot_offset=500)

    for _ in range(2):
        out = gb._build_fill_products(h3_dir, {b: ['L4A']}, soc_source=soc_dir, version=2, tmp_dir=tmp)
        assert out['failed'] == {b: ['L4A']} and not out['filled']
    assert _l4a_nulls(h3_dir, b) == len(GEDI_BEAMS) * N_SHOTS


def test_fill_results_are_recorded_and_given_up_on(tmp_dir):
    log = _logger_with(tmp_dir, [
        {'orbit': o, 'granule': 1, 'track': 1, 'status': 'INDEXED',
         'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}} for o in (1, 2, 3)
    ])
    a, b, c = (1, 1, 1), (2, 1, 1), (3, 1, 1)
    for attempt in (1, 2):
        assert log.record_fill_results({'failed': {a: ['L4A']}, 'unlocated': {}}) == {}
    assert log.pending_product_fills() == {a: ['L4A'], b: ['L4A'], c: ['L4A']}
    given_up = log.record_fill_results({'failed': {a: ['L4A']}, 'unlocated': {b: ['L4A']}, 'filled': {c: ['L4A']}})
    assert given_up == {a: ['L4A'], b: ['L4A']}
    assert log.pending_product_fills() == {}                               # no longer triggers every build
    assert log.pending_product_fills(statuses=('FAILED',)) == {a: ['L4A'], b: ['L4A']}
    # A manual retry (the doctor) that fails does not count; one that fills clears the record.
    log.record_fill_results({'failed': {b: ['L4A']}}, count_failures=False)
    log.record_fill_results({'filled': {a: ['L4A']}})
    g = {x['orbit']: x for x in log.granule_info}
    assert g[1]['products']['L4A'] == 'INDEXED' and 'fill_attempts' not in g[1]
    assert g[2]['products']['L4A'] == 'FAILED'


def test_products_published_before_l2a_stay_required(tmp_dir):
    from gedih3.gh3builder import _PRODUCTS_BEFORE_L2B, _filter_granules
    pv = {'L1B': ['rx_energy'], 'L2A': ['rh_098'], 'L4A': ['agbd']}
    no_l1b = {'L2A': os.path.join(tmp_dir, _gedi_name('02_A', '2020200', 102, 1, 202))}
    assert _filter_granules([no_l1b], pv, None, required_products=_PRODUCTS_BEFORE_L2B & set(pv)) == []


def test_a_merge_failed_granule_is_not_backfilled(tmp_dir):
    """Its rows are being re-extracted; the fill waits until it is INDEXED again."""
    log = _logger_with(tmp_dir, [{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'MERGE_FAILED',
                                  'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}}])
    assert log.pending_product_fills() == {}


def test_missing_products_are_reported_before_stage1_writes(tmp_dir, _client, monkeypatch):
    """A crash mid-Stage 1 must not lose the record: the callback fires first."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))

    def _crash(*a, **k):
        raise RuntimeError('killed mid-write')
    monkeypatch.setattr(gb, '_write_partitioned_streaming', _crash)
    seen = {}
    with pytest.raises(RuntimeError, match='killed'):
        gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True,
                      granule_products_callback=seen.update, **kw)
    assert seen[b] == ['L4A']


def test_variable_update_reaches_granules_missing_another_product(tmp_dir, _client):
    """A granule still waiting for L4A gets new L2A variables like any other."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)

    gb._build_add_variables(h3_dir, {'L2A': ['rh_050']}, soc_source=soc_dir, version=2,
                            tmp_dir=os.path.join(tmp_dir, 'var_tmp'))

    t = _db_table(h3_dir, 2020)
    rows_b = t.filter(pa.compute.field('root_file_l2a').isin([_gedi_name('02_A', '2020200', *b)]))
    assert rows_b.num_rows == len(GEDI_BEAMS) * N_SHOTS and rows_b['rh_050_l2a'].null_count == 0


def test_doctor_fix_backfills_awaited_products(tmp_dir, _client):
    """gh3_doctor --fix backfill: the log names the gap, the build's engine fills it."""
    import gedih3.gh3builder as gb
    import gedih3.doctor.diagnoses  # noqa: F401
    from gedih3.doctor import DoctorContext, run_diagnoses
    from gedih3.doctor.inspect import discover_partition_dirs
    from gedih3.logger import H3BuildLogger
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    log = H3BuildLogger(product_vars=None, dir=h3_dir)
    log.register_pending_granules([dict(zip(('orbit', 'granule', 'track'), b))])
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True,
                  granule_products_callback=lambda d: log.set_product_statuses(
                      {k: {p: 'MISSING_SOURCE' for p in m} for k, m in d.items() if m}), **kw)
    log.set_post_build_info()
    log.save_log('COMPLETED')
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',))

    ctx = DoctorContext(h3_dir=h3_dir, soc_dir=soc_dir, tmp_dir=tmp,
                        h3_logger=H3BuildLogger(product_vars=None, dir=h3_dir),
                        partition_dirs=discover_partition_dirs(h3_dir),
                        args=types.SimpleNamespace(orphan_age_hours=0.0, s3=False, online=False))
    check = run_diagnoses(ctx, ['backfill'], mode='check')[0]
    assert [f['kind'] for f in check.findings] == ['missing_source']
    fixed = run_diagnoses(ctx, ['backfill'], mode='fix')[0]

    assert fixed.applied and any(f.get('action') == 'filled' for f in fixed.findings)
    assert _l4a_nulls(h3_dir, b) == 0
    assert ctx.h3_logger.pending_product_fills() == {}


def test_later_patches_fill_a_column_an_earlier_patch_appended(tmp_dir):
    """First value wins across patches, appended columns included."""
    from gedih3.utils import parquet_fill_columns
    base = os.path.join(tmp_dir, 'base.parquet')
    pq.write_table(pa.table({'shot_number': pa.array([1, 2, 3], pa.uint64())}), base)
    p1 = pa.table({'shot_number': pa.array([1], pa.uint64()), 'x': [10.0]})
    p2 = pa.table({'shot_number': pa.array([1, 2], pa.uint64()), 'x': [99.0, 20.0]})
    parquet_fill_columns(base, [p1, p2])
    assert pq.read_table(base)['x'].to_pylist() == [10.0, 20.0, None]


def test_a_backfill_give_up_survives_the_build_finalize(tmp_dir, _client):
    """gh3_build records fill results, then runs set_post_build_info: FAILED
    must not turn back into INDEXED just because the columns exist."""
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    log = H3BuildLogger(product_vars=None, dir=h3_dir)
    log.register_pending_granules([dict(zip(('orbit', 'granule', 'track'), b))])
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)
    log.set_product_statuses({b: {'L4A': 'MISSING_SOURCE'}})
    log.set_post_build_info()

    assert log.record_fill_results({'unlocated': {b: ['L4A']}}) == {b: ['L4A']}
    log.set_post_build_info()

    assert log.pending_product_fills(statuses=('FAILED',)) == {b: ['L4A']}


def test_products_still_unpublished_are_reported_alongside_a_fill(tmp_dir, _client):
    """Awaiting L4A and L2B with only L4A on disk: L4A fills, L2B still waits."""
    import gedih3.gh3builder as gb
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',))
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',))

    out = gb._build_fill_products(h3_dir, {b: ['L4A', 'L2B']}, soc_source=soc_dir, version=2, tmp_dir=tmp)

    assert out['filled'] == {b: ['L4A']} and out['unavailable'] == {b: ['L2B']} and not out['failed']


def test_a_product_that_routes_no_rows_fails_alone(tmp_dir, monkeypatch):
    """One granule, two products: the one whose shots match the base fills;
    the one that matches nothing is failed, and the granule gets no sentinel."""
    import pandas as pd
    import gedih3.gh3builder as gb
    base_shots = np.arange(5, dtype=np.uint64)
    frames = {'a.h5': pd.DataFrame({'shot_number': base_shots, 'agbd': 1.0}),
              'b.h5': pd.DataFrame({'shot_number': base_shots + np.uint64(100), 'pai': 1.0})}
    monkeypatch.setattr(gb, 'load_h5', lambda path, **k: frames[path].set_index('shot_number'))
    monkeypatch.setattr(gb, '_cached_base_shots', lambda year_pf: base_shots)
    tmp = os.path.join(tmp_dir, 'fill')
    res = gb._var_fan_granule(('O00001_01_T00001', {'L4A': 'a.h5', 'L2B': 'b.h5'}, [os.path.join(tmp_dir, 'x.parquet')]),
                              new_product_vars={'L4A': ['agbd'], 'L2B': ['pai']}, tmp_dir=tmp, split_products=True)
    assert res['product_fragments'] == {'L4A': 1, 'L2B': 0}
    assert not os.path.exists(gb._var_fan_sentinel_path(tmp, 'O00001_01_T00001'))

    # _build_fill_products turns that into per-product buckets.
    key = (102, 1, 202)
    ks = gb._granule_key_str(key)
    entry = {p: os.path.join(tmp_dir, _gedi_name(c, '2020200', *key)) for p, c in (('L4A', '04_A'), ('L2B', '02_B'))}
    h3_dir = os.path.join(tmp_dir, 'db')
    os.makedirs(h3_dir)
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'h3_partition_level': 3, 'h3_partition_ids': ['830000fffffffff']}, f)
    monkeypatch.setattr(gb, 'get_dask_client', lambda: object())
    monkeypatch.setattr('gedih3.parallel.parallel_map', lambda items, fn, **k: [(i, [ks]) for i in items])
    monkeypatch.setattr(gb, '_product_fill_vars', lambda *a, **k: {})
    monkeypatch.setattr(gb, '_fan_merge_products', lambda *a, **k: {
        'updated_files': ['f'], 'failed_granules': set(), 'failed_products': {ks: {'L2B'}}, 'fragments': 1})
    out = gb._build_fill_products(h3_dir, {key: ['L4A', 'L2B']}, soc_source=[entry], tmp_dir=tmp_dir)
    assert out['filled'] == {key: ['L4A']} and out['failed'] == {key: ['L2B']}


def test_new_cells_of_a_phased_update_keep_the_database_column_order(tmp_dir, _client):
    """An L2A-only sample orders columns differently from a complete one. Files it
    creates must still use the database's order: gh3_load rejects a partition
    whose column order differs from its metadata."""
    import gedih3.gh3builder as gb
    from gedih3 import gh3_load
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    old = {tuple(pq.read_schema(f).names) for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))}
    _write_granule(soc_dir, (102, 1, 202), '2020200', 8.0e7, products=('02_A',), lon0=-40.0)   # new cells
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)

    files = glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
    assert len(files) > 1 and {tuple(pq.read_schema(f).names) for f in files} == old
    df = gh3_load(h3_dir).compute()
    assert len(df) == 2 * len(GEDI_BEAMS) * N_SHOTS


def test_complete_schema_follows_the_database_column_order(tmp_dir):
    from gedih3.gh3builder import _complete_schema_from_db
    ydir = os.path.join(tmp_dir, 'h3_03=830001fffffffff', 'year=2020')
    os.makedirs(ydir)
    pq.write_table(pa.table({'shot_number': pa.array([1], pa.uint64()), 'agbd_l4a': [1.0], 'rh_098_l2a': [1.0],
                             'b_l2a': [1.0]}), os.path.join(ydir, '830001fffffffff.2020.0.parquet'))
    with open(os.path.join(tmp_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({'h3_partition_level': 3, 'h3_partition_ids': ['830001fffffffff'],
                   'h3_columns_dtypes': {'shot_number': 'uint64', 'agbd_l4a': 'double', 'rh_098_l2a': 'double',
                                         'b_l2a': 'double'}}, f)
    src = pa.schema([('b_l2a', pa.float64()), ('shot_number', pa.uint64()), ('new_l2a', pa.float32()),
                     ('rh_098_l2a', pa.float64())], metadata={b'geo': b'{}'})
    out, added = _complete_schema_from_db(src, tmp_dir)
    assert added == ['agbd_l4a']
    assert out.names == ['shot_number', 'agbd_l4a', 'rh_098_l2a', 'b_l2a', 'new_l2a'] and out.metadata == src.metadata


def test_a_variable_update_while_a_product_is_pending_reaches_every_file(tmp_dir, _client):
    """New cells hold only granules still waiting for L4A, so no fragment carries
    a new L4A variable there. They must still get the column (null), in the
    same type as everywhere else: readers reject files with other columns, and
    the backfill, which never adds columns, must be able to fill it later."""
    import gedih3.gh3builder as gb
    from gedih3 import gh3_load
    soc_dir, h3_dir, tmp, kw = _phased_db(tmp_dir)
    b = (102, 1, 202)
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('02_A',), lon0=-40.0)   # new cells, no L4A yet
    gb.build_h3db(product_vars=PRODUCT_VARS, allow_missing_products=True, **kw)

    added = {'L4A': ['agbd', 'agbd_se']}
    gb._build_add_variables(h3_dir, added, soc_source=soc_dir, version=2, tmp_dir=os.path.join(tmp_dir, 'var_tmp'))
    from gedih3.logger import H3BuildLogger
    log = H3BuildLogger(product_vars=None, dir=h3_dir)
    log.set_post_build_info()                     # as gh3_build does after the update
    log.save_log('COMPLETED')

    files = glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
    schemas = {tuple((f.name, str(f.type)) for f in pq.read_schema(p)) for p in files}
    assert len(schemas) == 1 and ('agbd_se_l4a', 'double') in next(iter(schemas))
    assert len(gh3_load(h3_dir).compute()) == 2 * len(GEDI_BEAMS) * N_SHOTS
    assert _db_table(h3_dir)['agbd_se_l4a'].null_count == len(GEDI_BEAMS) * N_SHOTS   # B's rows only

    # L4A arrives: the backfill fills the new column too (the log records it).
    log_path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    log = json.load(open(log_path))
    log['products']['L4A']['variables'] = added['L4A']
    json.dump(log, open(log_path, 'w'))
    _write_granule(soc_dir, b, '2020200', 8.0e7, products=('04_A',), lon0=-40.0)
    out = gb._build_fill_products(h3_dir, {b: ['L4A']}, soc_source=soc_dir, version=2, tmp_dir=tmp)
    assert out['filled'] == {b: ['L4A']}
    assert _db_table(h3_dir)['agbd_se_l4a'].null_count == 0
