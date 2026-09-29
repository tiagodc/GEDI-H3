"""dtype_drift diagnosis: partition columns stored in a type other than the source's.

Reproduces the production case that motivated it: a V3 database built by
gedih3 0.12.7 stored ``land_cover_data/worldcover_class_l4c`` as ``int32``
(the L4C V002 type) while the V003 source files, and so ``gh3_build`` today,
write ``uint8``. The reference types come from the source HDF5 through the
build's own schema derivation; those tests stub the derivation so no HDF5 is
needed, and ``_sample_granule`` is tested on its own.
"""

import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import gedih3.doctor.diagnoses  # noqa: F401
from gedih3.config import BUILD_LOG_FILENAME, PARTITION_META_FILENAME
from gedih3.doctor import DoctorContext, Severity, run_diagnoses
from gedih3.doctor.inspect import discover_partition_dirs

COL = 'land_cover_data/worldcover_class_l4c'
CELL = '830e41fffffffff'


@pytest.fixture(scope='module', autouse=True)
def _module_dask_client():
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def _make_db(h3_dir, values=(10, 20, 95), stored='int32', years=(2020, 2021)):
    """Geoparquet partition files storing ``COL`` as ``stored``, plus metas and log."""
    import geopandas as gpd
    from shapely.geometry import Point
    from gedih3.gh3builder import h3_write_metadata, h3_merge_metadata

    part_dir = os.path.join(h3_dir, f'h3_03={CELL}')
    paths = []
    for year in years:
        n = len(values)
        gdf = gpd.GeoDataFrame({
            'shot_number': np.arange(1, n + 1, dtype=np.uint64) + year * 1000,
            'root_file_l2a': [f'GEDI02_A_{year}001000000_O00100_01_T00050_02_003_02_V003.h5'] * n,
            'datetime': pd.to_datetime([f'{year}-06-01'] * n),
            COL: np.asarray(values, dtype=stored),
            'agbd_l4a': np.linspace(0, 1, n, dtype=np.float32),
            'geometry': [Point(0.1 * i, 0.1 * i) for i in range(n)],
        }, crs='EPSG:4326')
        path = os.path.join(part_dir, f'year={year}', f'{CELL}.{year}.0.parquet')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        gdf.to_parquet(path, row_group_size=2)
        h3_write_metadata(path)
        paths.append(path)
    h3_merge_metadata(part_dir)
    with open(os.path.join(h3_dir, BUILD_LOG_FILENAME), 'w') as f:
        json.dump({
            'gedi_version': 3, 'h3_resolution_level': 12, 'h3_partition_level': 3,
            'status': 'COMPLETED', 'date_range': ['2020-06-01', '2021-06-01'],
            'products': {'L4C': {'variables': ['land_cover_data/worldcover_class'], 'status': 'COMPLETED'}},
            'granules': [{'orbit': 100, 'granule': 1, 'track': 50, 'status': 'INDEXED'}],
            'h3_columns_dtypes': {COL: stored, 'agbd_l4a': 'float'},
        }, f)
    return paths


def _ctx(h3_dir, soc_dir=None):
    from gedih3.logger import H3BuildLogger
    return DoctorContext(
        h3_dir=h3_dir, soc_dir=soc_dir, tmp_dir=os.path.join(h3_dir, '.tmp'),
        h3_logger=H3BuildLogger(product_vars=None, dir=h3_dir),
        partition_dirs=discover_partition_dirs(h3_dir),
        args=type('A', (), {'orphan_age_hours': 0.0, 's3': False, 'online': False})(),
    )


@pytest.fixture
def source_uint8(monkeypatch):
    """Stub the HDF5-derived reference: the source stores COL as uint8."""
    from gedih3.doctor.diagnoses import dtype_drift
    reference = {COL: 'uint8', 'agbd_l4a': 'float', 'shot_number': 'uint64'}
    monkeypatch.setattr(dtype_drift, '_reference_types', lambda ctx: (reference, None))
    return reference


def test_check_reports_drift_per_file(tmp_dir, source_uint8):
    paths = _make_db(tmp_dir)
    report = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]

    assert report.severity == Severity.WARN
    drift = [f for f in report.findings if f['kind'] == 'dtype_drift']
    assert sorted(f['path'] for f in drift) == sorted(paths)
    assert all(f['columns'] == {COL: ['int32', 'uint8']} for f in drift)
    assert '--fix dtype_drift' in report.recommendations[0]


def test_check_survives_a_database_sized_reference(tmp_dir, monkeypatch):
    """A real reference has ~1.5k columns. Broadcast as a dict kwarg, dask
    turned it into a graph dependency (``<TaskState 'reference' processing>``)
    and every partition came back as an exception; it must travel inside
    the scan callable instead."""
    from gedih3.doctor.diagnoses import dtype_drift
    reference = {f'col_{i:04d}': 'float' for i in range(1500)}
    reference[COL] = 'uint8'
    monkeypatch.setattr(dtype_drift, '_reference_types', lambda ctx: (reference, None))
    paths = _make_db(tmp_dir)

    report = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]

    assert not [f for f in report.findings if f['kind'] == 'unreadable']
    assert len([f for f in report.findings if f['kind'] == 'dtype_drift']) == len(paths)


def test_fix_retypes_files_and_every_dtype_cache(tmp_dir, source_uint8):
    paths = _make_db(tmp_dir)
    before = {p: pq.ParquetFile(p).read() for p in paths}
    check = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]

    from gedih3.doctor.diagnoses.dtype_drift import dtype_drift_fix
    fixed = dtype_drift_fix(_ctx(tmp_dir), check)

    assert fixed.severity == Severity.INFO, fixed.summary
    for p in paths:
        pf = pq.ParquetFile(p)
        assert pf.schema_arrow.field(COL).type == pa.uint8()
        assert pf.metadata.num_row_groups == 2          # layout preserved
        t = pf.read()
        assert t[COL].to_pylist() == before[p][COL].to_pylist()
        assert t.drop_columns([COL]).equals(before[p].drop_columns([COL]))
        geo = json.loads(pf.schema_arrow.metadata[b'geo'])
        assert geo == json.loads(before[p].schema.metadata[b'geo'])
        assert t.to_pandas()[COL].dtype == np.uint8
        year_meta = json.load(open(p.replace('.parquet', PARTITION_META_FILENAME)))
        assert year_meta['column_dtypes'][COL] == 'uint8'
    cell_meta = json.load(open(os.path.join(tmp_dir, f'h3_03={CELL}', f'{CELL}{PARTITION_META_FILENAME}')))
    assert cell_meta['column_dtypes'][COL] == 'uint8'
    log = json.load(open(os.path.join(tmp_dir, BUILD_LOG_FILENAME)))
    assert log['h3_columns_dtypes'][COL] == 'uint8'
    assert log['status'] == 'COMPLETED'

    recheck = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]
    assert recheck.severity == Severity.INFO
    assert not [f for f in recheck.findings if f['kind'] == 'dtype_drift']


def test_fix_never_truncates(tmp_dir, source_uint8):
    """300 does not fit uint8: the file stays int32 and the log keeps int32."""
    paths = _make_db(tmp_dir, values=(10, 300, 95))
    before = {p: open(p, 'rb').read() for p in paths}
    check = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]

    from gedih3.doctor.diagnoses.dtype_drift import dtype_drift_fix
    fixed = dtype_drift_fix(_ctx(tmp_dir), check)

    assert fixed.severity == Severity.WARN
    assert sum('fix_error' in f for f in fixed.findings) == len(paths)
    assert all(open(p, 'rb').read() == before[p] for p in paths)
    assert not any(n.endswith('.tmp') for p in paths for n in os.listdir(os.path.dirname(p)))
    log = json.load(open(os.path.join(tmp_dir, BUILD_LOG_FILENAME)))
    assert log['h3_columns_dtypes'][COL] == 'int32'


def test_fix_refuses_while_build_is_live(tmp_dir, source_uint8, monkeypatch):
    from gedih3.doctor.diagnoses import tmp_partitions_health
    paths = _make_db(tmp_dir)
    check = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]
    monkeypatch.setattr(tmp_partitions_health, '_build_is_active', lambda h3_dir, tmp_dir: (True, {'pid': 42}))

    from gedih3.doctor.diagnoses.dtype_drift import dtype_drift_fix
    fixed = dtype_drift_fix(_ctx(tmp_dir), check)

    assert fixed.severity == Severity.ERROR and 'refused' in fixed.summary
    assert all(pq.read_schema(p).field(COL).type == pa.int32() for p in paths)


def test_fix_refuses_while_build_log_is_in_flight(tmp_dir, source_uint8):
    """Nothing writes gh3_build.log by default, so _build_is_active alone
    misses a live build; the build log's own status does not."""
    paths = _make_db(tmp_dir)
    check = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]
    log_path = os.path.join(tmp_dir, BUILD_LOG_FILENAME)
    log = json.load(open(log_path))
    log['status'] = 'MERGING'
    json.dump(log, open(log_path, 'w'))

    from gedih3.doctor.diagnoses.dtype_drift import dtype_drift_fix
    fixed = dtype_drift_fix(_ctx(tmp_dir), check)

    assert fixed.severity == Severity.ERROR and 'MERGING' in fixed.summary
    assert all(pq.read_schema(p).field(COL).type == pa.int32() for p in paths)


def test_check_ignores_string_offset_width(tmp_dir, monkeypatch):
    """string vs large_string follows the pandas major version (both
    supported): never drift, or a full-database rewrite would follow."""
    from gedih3.doctor.diagnoses import dtype_drift
    paths = _make_db(tmp_dir)
    stored = str(pq.read_schema(paths[0]).field('root_file_l2a').type)
    other = 'string' if stored == 'large_string' else 'large_string'
    monkeypatch.setattr(dtype_drift, '_reference_types',
                        lambda ctx: ({COL: 'int32', 'root_file_l2a': other}, None))

    report = run_diagnoses(_ctx(tmp_dir), ['dtype_drift'], mode='check')[0]

    assert report.severity == Severity.INFO
    assert not [f for f in report.findings if f['kind'] == 'dtype_drift']


def test_no_soc_source_skips_without_error(tmp_dir):
    _make_db(tmp_dir)
    report = run_diagnoses(_ctx(tmp_dir, soc_dir=os.path.join(tmp_dir, 'missing')), ['dtype_drift'],
                           mode='check')[0]
    assert report.severity == Severity.INFO
    assert report.findings[0]['kind'] == 'no_reference'


# --- sample granule ----------------------------------------------------------

def _touch(soc_dir, doy_dir, names):
    d = os.path.join(soc_dir, *doy_dir.split('/'))
    os.makedirs(d, exist_ok=True)
    for n in names:
        open(os.path.join(d, n), 'w').close()


def test_sample_granule_picks_release_files_near_db_end(tmp_dir):
    from gedih3.doctor.diagnoses.dtype_drift import _sample_granule
    stem = '2025190005055_O37224_01_T09655_02_004_02'
    _touch(tmp_dir, '2025/190', [
        f'GEDI02_A_{stem}_V002.h5', f'GEDI04_C_{stem}_V002.h5',
        f'GEDI02_A_{stem}_V003_SGS.h5', f'GEDI04_C_{stem}_V003_SGS.h5',
        f'GEDI02_A_{stem}_V003.h5', f'GEDI04_C_{stem}_V003.h5',
    ])
    # Newer days hold only another release: the search must start at the
    # database's own end date, not at the newest directory.
    _touch(tmp_dir, '2026/100', ['GEDI02_A_2026100005055_O40000_01_T09655_02_004_02_V002.h5'])

    soc = _sample_granule(tmp_dir, 3, ['L2A', 'L4C'], latest='2025-07-10')

    assert soc is not None
    assert sorted(soc) == ['L2A', 'L4C']
    assert all(p.endswith(f'{stem}_V003.h5') for p in soc.values())


def test_sample_granule_requires_every_product(tmp_dir):
    from gedih3.doctor.diagnoses.dtype_drift import _sample_granule
    _touch(tmp_dir, '2025/190', ['GEDI02_A_2025190005055_O37224_01_T09655_02_004_02_V003.h5'])
    assert _sample_granule(tmp_dir, 3, ['L2A', 'L4C'], latest='2025-07-09') is None


# --- parquet_cast_columns ------------------------------------------------------

def test_parquet_cast_columns_is_noop_when_types_match(tmp_dir):
    from gedih3.utils import parquet_cast_columns
    path = os.path.join(tmp_dir, 'a.parquet')
    pq.write_table(pa.table({'a': pa.array([1, 2], pa.uint8())}), path)
    mtime = os.path.getmtime(path)
    assert parquet_cast_columns(path, {'a': 'uint8', 'missing': 'int32'}) == 'ok'
    assert os.path.getmtime(path) == mtime


def test_parquet_cast_columns_keeps_nullable_extension_dtype(tmp_dir):
    """An Int32 column with nulls must read back as UInt8 with <NA>, not
    float64 with NaN, after the retype."""
    from gedih3.utils import parquet_cast_columns
    path = os.path.join(tmp_dir, 'n.parquet')
    df = pd.DataFrame({'c': pd.array([1, None, 3], dtype='Int32'), 'd': np.array([1, 2, 3], dtype='int32')})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

    assert parquet_cast_columns(path, {'c': 'uint8', 'd': 'uint8'}) == 'rewritten'

    out = pd.read_parquet(path)
    assert str(out['c'].dtype) == 'UInt8'
    assert out['c'].isna().tolist() == [False, True, False]
    assert out['d'].dtype == np.uint8
