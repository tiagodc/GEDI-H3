"""End-to-end tests for individual diagnoses on synthetic gedih3 databases.

Each test injects exactly one defect and verifies (a) the relevant diagnosis
detects it, (b) the fix resolves it, (c) other diagnoses do not flag false
positives on adjacent state.
"""

import json
import os
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Ensure all diagnoses are registered.
import gedih3.doctor.diagnoses  # noqa: F401
from gedih3.doctor import DoctorContext, Severity, run_diagnoses
from gedih3.doctor.inspect import discover_partition_dirs
from gedih3.config import PARTITION_META_FILENAME


# --- fixtures ---------------------------------------------------------------

def _write_parquet(path, df, row_group_size=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=row_group_size)


def _make_partition(h3_dir, h3_part='8c2a', year=2020, n_shots=20,
                    columns=('rh_98_l2a', 'agbd_l4a'), nan_fraction=0.0,
                    granules=None):
    """Build one h3_03=<part>/<year>=year/<part>.<year>.parquet plus partition meta."""
    part_dir = os.path.join(h3_dir, f'h3_03={h3_part}')
    year_dir = os.path.join(part_dir, f'year={year}')
    pq_file = os.path.join(year_dir, f'{h3_part}.{year}.parquet')

    np.random.seed(hash((h3_part, year)) & 0xFFFFFFFF)
    if granules is None:
        granules = [{'orbit': 100, 'granule': 1, 'track': 50}]
    # Synthesize root_file_l2a per shot, cycling across granules
    root_files = []
    for i in range(n_shots):
        g = granules[i % len(granules)]
        root_files.append(f"GEDI02_A_2020001000000_O{g['orbit']:05d}_{g['granule']:02d}_T{g['track']:05d}_02_003_02_V002.h5")

    df = pd.DataFrame({
        'shot_number': np.arange(1, n_shots + 1, dtype=np.int64),
        'root_file_l2a': root_files,
        'datetime': pd.to_datetime(['2020-06-01'] * n_shots),
    })
    for c in columns:
        df[c] = np.random.uniform(0, 100, n_shots)
        if nan_fraction > 0:
            n_nan = int(n_shots * nan_fraction)
            df.loc[df.index[:n_nan], c] = np.nan

    _write_parquet(pq_file, df)

    base_meta = {
        'last_modified': '2020-06-01',
        'l2a_version': 2,
        'h3_partition': h3_part,
        'shot_count': n_shots,
        'shot_range': [int(df['shot_number'].min()), int(df['shot_number'].max())],
        'date_range': ['2020-06-01', '2020-06-01'],
        'granules': granules,
        'columns': ['shot_number', 'root_file_l2a', 'datetime'] + list(columns),
    }

    # Per-year meta sits next to the parquet (h3_write_metadata convention).
    year_meta_file = pq_file.replace('.parquet', PARTITION_META_FILENAME)
    with open(year_meta_file, 'w') as f:
        json.dump({**base_meta, 'year': year}, f)

    # Partition-level merged meta (h3_merge_metadata convention).
    part_meta_file = os.path.join(part_dir, f'{h3_part}{PARTITION_META_FILENAME}')
    with open(part_meta_file, 'w') as f:
        json.dump({**base_meta, 'years': [year]}, f)

    return part_dir, pq_file


def _make_build_log(h3_dir, products=None, granules=None):
    if products is None:
        products = {'L2A': ['rh_98'], 'L4A': ['agbd']}
    if granules is None:
        granules = [{'orbit': 100, 'granule': 1, 'track': 50, 'status': 'INDEXED'}]
    log = {
        'gedi_version': 2,
        'h3_resolution_level': 12,
        'h3_partition_level': 3,
        'status': 'COMPLETED',
        'spatial_filter': None,
        'temporal_filter': None,
        'products': {p: {'variables': v, 'status': 'COMPLETED'} for p, v in products.items()},
        'granules': granules,
        'h3_partition_ids': ['8c2a'],
    }
    log_path = os.path.join(h3_dir, 'gedih3_build_log.json')
    with open(log_path, 'w') as f:
        json.dump(log, f)
    return log_path


def _ctx(h3_dir, soc_dir=None, args=None):
    from gedih3.logger import H3BuildLogger
    try:
        h3_logger = H3BuildLogger(product_vars=None, dir=h3_dir)
    except Exception:
        h3_logger = None
    return DoctorContext(
        h3_dir=h3_dir, soc_dir=soc_dir,
        tmp_dir=os.path.join(h3_dir, '.tmp'),
        h3_logger=h3_logger,
        partition_dirs=discover_partition_dirs(h3_dir),
        args=type('A', (), {'orphan_age_hours': 0.0, 's3': False, 'online': False})(),
    )


# --- metadata ---------------------------------------------------------------

def _delete_partition_level_meta(part_dir):
    """Remove only the merged partition-level meta (keep per-year meta)."""
    h3_part = os.path.basename(part_dir.rstrip('/').rstrip(os.sep)).split('=', 1)[1]
    target = os.path.join(part_dir, f'{h3_part}{PARTITION_META_FILENAME}')
    if os.path.exists(target):
        os.remove(target)


def test_metadata_detects_missing_partition_json(tmp_dir):
    part_dir, _ = _make_partition(tmp_dir)
    _delete_partition_level_meta(part_dir)

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['metadata'], mode='check')
    assert any(f['kind'] == 'missing_partition_meta' for f in reports[0].findings)


def test_metadata_fix_regenerates_partition_json(tmp_dir):
    part_dir, _ = _make_partition(tmp_dir)
    _delete_partition_level_meta(part_dir)

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['metadata'], mode='fix')
    h3_part = os.path.basename(part_dir.rstrip('/').rstrip(os.sep)).split('=', 1)[1]
    expected = os.path.join(part_dir, f'{h3_part}{PARTITION_META_FILENAME}')
    assert os.path.exists(expected)
    assert reports[0].applied


# --- orphans ----------------------------------------------------------------

def test_orphans_detects_and_removes_temp_files(tmp_dir):
    _make_partition(tmp_dir)
    leftover = os.path.join(tmp_dir, 'h3_03=8c2a', 'whatever.fill.tmp')
    open(leftover, 'w').write('garbage')
    # set mtime in the past so it passes the age threshold
    past = time.time() - 3600
    os.utime(leftover, (past, past))

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['orphans'], mode='check')
    assert any(f['path'] == leftover for f in reports[0].findings)

    run_diagnoses(ctx, ['orphans'], mode='fix')
    assert not os.path.exists(leftover)


# --- log_state --------------------------------------------------------------

def test_log_state_clears_pending_flag(tmp_dir):
    _make_partition(tmp_dir)
    log_path = _make_build_log(tmp_dir)
    data = json.load(open(log_path))
    data['_pending_variable_update'] = {'something': True}
    json.dump(data, open(log_path, 'w'))

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['log_state'], mode='check')
    assert any(f['kind'] == 'stuck_pending_flag' for f in reports[0].findings)

    run_diagnoses(ctx, ['log_state'], mode='fix')
    # In-memory state cleared
    assert '_pending_variable_update' not in ctx.h3_logger.log_data


def test_log_state_detects_and_fixes_granule_status_drift(tmp_dir):
    """After a merge-only resume, granule_info can hold stale PENDING entries
    even though the data is in finalized partition metadata. log_state must
    detect this drift and (in fix mode) flip those entries to INDEXED."""
    # Partition with two granules in its metadata.
    granules_in_partition = [
        {'orbit': 100, 'granule': 1, 'track': 50},
        {'orbit': 101, 'granule': 2, 'track': 51},
    ]
    _make_partition(tmp_dir, granules=granules_in_partition)

    # Build log: granule (100,1,50) is INDEXED but (101,2,51) is stuck PENDING
    # — the simulated post-merge-resume state.
    log_granules = [
        {'orbit': 100, 'granule': 1, 'track': 50, 'status': 'INDEXED'},
        {'orbit': 101, 'granule': 2, 'track': 51, 'status': 'PENDING'},
    ]
    _make_build_log(tmp_dir, granules=log_granules)

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['log_state'], mode='check')
    drift = [f for f in reports[0].findings if f['kind'] == 'granule_status_drift']
    assert len(drift) == 1
    assert (drift[0]['orbit'], drift[0]['granule'], drift[0]['track']) == (101, 2, 51)

    run_diagnoses(ctx, ['log_state'], mode='fix')

    # Re-read the log fresh from disk to confirm persistence.
    from gedih3.logger import H3BuildLogger
    fresh = H3BuildLogger(product_vars=None, dir=tmp_dir)
    statuses = {(g['orbit'], g['granule'], g['track']): g['status']
                for g in fresh.granule_info}
    assert statuses[(100, 1, 50)] == 'INDEXED'
    assert statuses[(101, 2, 51)] == 'INDEXED'  # was PENDING, now flipped


# --- parquet_health ---------------------------------------------------------

def test_parquet_health_detects_and_fixes_duplicates(tmp_dir):
    _, pq_file = _make_partition(tmp_dir, n_shots=10)
    df = pd.read_parquet(pq_file)
    dup_df = pd.concat([df, df.iloc[[0, 1]]], ignore_index=True)
    _write_parquet(pq_file, dup_df)

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['parquet_health'], mode='check')
    assert any(f['kind'] == 'duplicate_shots' for f in reports[0].findings)

    reports = run_diagnoses(ctx, ['parquet_health'], mode='fix')
    out = pd.read_parquet(pq_file)
    assert len(out) == 10
    assert out['shot_number'].duplicated().sum() == 0


def test_parquet_health_detects_corrupt(tmp_dir):
    _, pq_file = _make_partition(tmp_dir)
    # Truncate to corrupt
    with open(pq_file, 'r+b') as f:
        f.truncate(50)
    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['parquet_health'], mode='check')
    assert any(f['kind'] == 'corrupt' for f in reports[0].findings)
    assert reports[0].severity == Severity.ERROR


def test_parquet_health_detects_schema_drift(tmp_dir):
    # Make 4 partitions, 3 with same schema and 1 with extra columns missing
    for i, h in enumerate(['c1', 'c2', 'c3', 'c4']):
        cols = ('rh_98_l2a', 'agbd_l4a') if h != 'c4' else ('rh_98_l2a',)
        _make_partition(tmp_dir, h3_part=h, columns=cols)

    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['parquet_health'], mode='check')
    drift = [f for f in reports[0].findings if f['kind'] == 'schema_drift']
    assert len(drift) == 1
    assert any('agbd_l4a' in c for c in drift[0]['missing_columns'])


# --- backfill ---------------------------------------------------------------

def test_backfill_detects_partial_nan(tmp_dir):
    _make_partition(tmp_dir, nan_fraction=0.3)   # 30% of rows NaN
    _make_build_log(tmp_dir)
    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['backfill'], mode='check')
    assert any(f['kind'] == 'partial_nan' for f in reports[0].findings)


def test_backfill_detects_missing_column(tmp_dir):
    _make_partition(tmp_dir, columns=('rh_98_l2a',))   # no L4A column at all
    _make_build_log(tmp_dir)
    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['backfill'], mode='check')
    missing = [f for f in reports[0].findings if f['kind'] == 'missing_column']
    assert len(missing) == 1
    assert missing[0]['product'] == 'L4A'


def test_backfill_clean_when_all_indexed(tmp_dir):
    _make_partition(tmp_dir, nan_fraction=0.0, columns=('rh_98_l2a', 'agbd_l4a'))
    _make_build_log(tmp_dir)
    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['backfill'], mode='check')
    # No NaN, all columns present → no findings
    assert not reports[0].has_findings


def test_backfill_read_patch_unlinks_temp_on_exception(tmp_dir, monkeypatch):
    """Regression test: ``_read_patch_for_partition`` opens a
    tempfile + ParquetWriter. If the loop body raises (e.g.
    ``writer.write_table`` fails mid-write), the temp file at
    ``out_path`` MUST be unlinked before the exception propagates.
    Otherwise the caller (``_heal_partition``) sees a None ``patch``
    and its finally cleanup skips the unlink — leaking temp parquets
    once per failed partition fix on a continental backfill run."""
    import glob as _glob
    import tempfile
    import pandas as pd
    import pyarrow.parquet as pq
    from gedih3.doctor.diagnoses import backfill as bf

    # Mock load_h5 to return distinct shot_numbers per granule so the
    # ``seen_shots`` dedup gate doesn't short-circuit the second
    # iteration before the writer is exercised.
    def fake_load_h5(soc_file, columns=None, include_source=False, dropna=False):
        # Use the file path's hash to pick a non-overlapping shot range
        base = abs(hash(str(soc_file))) % 10_000
        return pd.DataFrame({
            'shot_number': [base + 1, base + 2, base + 3],
            'agbd': [1.0, 2.0, 3.0],
        })
    monkeypatch.setattr('gedih3.gedidriver.load_h5', fake_load_h5)

    # Patch the ParquetWriter inside backfill's import context so that
    # the SECOND ``write_table`` call (i.e. mid-write, after the temp
    # parquet has been opened) raises. This drives the function down
    # the exception path while ``out_path`` already exists on disk.
    real_writer = pq.ParquetWriter
    state = {'count': 0}

    class _FaultyWriter(real_writer):
        def write_table(self, table, **kwargs):
            state['count'] += 1
            if state['count'] >= 2:
                raise RuntimeError('simulated writer failure mid-write')
            return super().write_table(table, **kwargs)

    # ``pq`` is imported inside ``_read_patch_for_partition``; patch
    # the symbol on ``pyarrow.parquet`` itself so the runtime import
    # resolves to our faulty subclass.
    monkeypatch.setattr(pq, 'ParquetWriter', _FaultyWriter)

    tmpdir = tempfile.gettempdir()
    before = set(_glob.glob(os.path.join(tmpdir, 'tmp*.parquet')))
    try:
        # Trigger the bug: two granules; second granule's write raises.
        soc_tree = {
            'O00001_01_T00001': {'L4A': '/fake/file1.h5'},
            'O00002_01_T00002': {'L4A': '/fake/file2.h5'},
        }
        affected = {'L4A': [(1, 1, 1), (2, 1, 2)]}
        vars_per_product = {'L4A': ['agbd']}
        with pytest.raises(Exception):
            bf._read_patch_for_partition(affected, soc_tree, vars_per_product)
    finally:
        after = set(_glob.glob(os.path.join(tmpdir, 'tmp*.parquet')))
        leaked = after - before
        # Clean up anything we left behind, regardless of pass/fail
        for f in leaked:
            try:
                os.unlink(f)
            except OSError:
                pass
        assert not leaked, (
            f"Temp parquet leaked on exception: {leaked!r}. The "
            "fix in backfill.py must unlink ``out_path`` from an "
            "explicit except: branch — the post-finally ``n_written "
            "== 0`` cleanup is unreachable when the body raises."
        )


# --- geoparquet_bbox -------------------------------------------------------

def _make_geoparquet_partition(h3_dir, h3_part='8c2a', year=2020,
                               n_shots=10, drop_bbox=False):
    """Write a partition parquet via geopandas (real GeoParquet metadata).
    If drop_bbox is True, rewrite the file with the 'bbox' field stripped
    from columns.geometry, simulating a pre-v0.8.14 merge.
    """
    import json
    import geopandas as gpd
    from shapely.geometry import Point

    part_dir = os.path.join(h3_dir, f'h3_03={h3_part}')
    year_dir = os.path.join(part_dir, f'year={year}')
    os.makedirs(year_dir, exist_ok=True)
    pq_file = os.path.join(year_dir, f'{h3_part}.{year}.parquet')

    np.random.seed(hash((h3_part, year)) & 0xFFFFFFFF)
    lons = np.random.uniform(-1, 1, n_shots)
    lats = np.random.uniform(-1, 1, n_shots)
    gdf = gpd.GeoDataFrame({
        'shot_number': np.arange(1, n_shots + 1, dtype=np.uint64),
        'agbd_l4a': np.random.uniform(0, 100, n_shots),
        'datetime': pd.to_datetime(['2020-06-01'] * n_shots),
        'geometry': [Point(x, y) for x, y in zip(lons, lats)],
    }, crs='EPSG:4326')
    gdf.to_parquet(pq_file)

    if drop_bbox:
        # Strip bbox from geo metadata. Use ParquetFile.read() (not pq.read_table)
        # to avoid pyarrow inferring h3_03=/year= as partition columns from the path.
        pf = pq.ParquetFile(pq_file)
        tbl = pf.read()
        schema = tbl.schema
        meta = dict(schema.metadata or {})
        geo = json.loads(meta[b'geo'])
        primary = geo.get('primary_column', 'geometry')
        if 'bbox' in geo['columns'][primary]:
            del geo['columns'][primary]['bbox']
        meta[b'geo'] = json.dumps(geo).encode('utf-8')
        new_schema = schema.with_metadata(meta)
        tbl = tbl.replace_schema_metadata(meta)
        tmp = pq_file + '.tmp'
        pq.write_table(tbl, tmp, compression='zstd')
        os.replace(tmp, pq_file)

    return part_dir, pq_file


def test_geoparquet_bbox_passes_when_bbox_present(tmp_dir):
    _make_geoparquet_partition(tmp_dir, drop_bbox=False)
    _make_build_log(tmp_dir)
    ctx = _ctx(tmp_dir)
    reports = run_diagnoses(ctx, ['geoparquet_bbox'], mode='check')
    assert reports[0].severity == Severity.INFO
    assert not reports[0].has_findings


def test_geoparquet_bbox_detects_and_backfills_missing_bbox(tmp_dir):
    _make_geoparquet_partition(tmp_dir, drop_bbox=True)
    _make_build_log(tmp_dir)

    # Detect
    ctx = _ctx(tmp_dir)
    check_reports = run_diagnoses(ctx, ['geoparquet_bbox'], mode='check')
    assert check_reports[0].severity == Severity.WARN
    kinds = {f['kind'] for f in check_reports[0].findings}
    assert 'missing_bbox' in kinds

    # Fix
    ctx = _ctx(tmp_dir)
    fix_reports = run_diagnoses(ctx, ['geoparquet_bbox'], mode='fix')
    assert fix_reports[0].severity == Severity.INFO
    actions = [f.get('action') for f in fix_reports[0].findings]
    assert 'bbox_backfilled' in actions

    # Re-check: clean
    ctx = _ctx(tmp_dir)
    recheck = run_diagnoses(ctx, ['geoparquet_bbox'], mode='check')
    assert not recheck[0].has_findings

    # Verify the bbox is sensible (within the synthetic data extent [-1, 1])
    from gedih3.utils import _bbox_from_geo_metadata
    pq_file = os.path.join(tmp_dir, 'h3_03=8c2a', 'year=2020', '8c2a.2020.parquet')
    bbox = _bbox_from_geo_metadata(pq_file)
    assert bbox is not None
    assert -1.0 <= bbox[0] <= bbox[2] <= 1.0
    assert -1.0 <= bbox[1] <= bbox[3] <= 1.0


# --- soc_health -------------------------------------------------------------

def test_soc_health_enumerates_files_from_partial_product_orbits(tmp_dir):
    """Regression test for the pivot+dropna() bug — soc_health used to
    enumerate via ``soc_file_tree(..., to_list=True)`` which silently
    drops orbit/tracks missing one product. The fix uses a manifest-
    aware glob so EVERY GEDI*.h5 under the SOC tree is enumerated for
    HDF5 validity checking, regardless of which product subset is
    present per orbit/track. (Continuation of v0.8.x lesson #4: don't
    let pivot-shape constraints lose data.)"""
    from gedih3.doctor.diagnoses.soc_health import _enumerate_soc_files

    soc_dir = os.path.join(tmp_dir, 'soc')
    os.makedirs(soc_dir, exist_ok=True)
    files = [
        # complete trio: O01956 has L2A + L2B + L4A
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI04_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        # partial: O01957 has only L2A + L2B (missing L4A) — this is
        # the exact case soc_file_tree's pivot+dropna() would drop.
        'GEDI02_A_2019108002012_O01957_03_T03910_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01957_03_T03910_02_003_01_V003.h5',
    ]
    for n in files:
        with open(os.path.join(soc_dir, n), 'wb') as f:
            f.write(b'not a real h5')

    enumerated = sorted(os.path.basename(p)
                        for p in _enumerate_soc_files(soc_dir))
    assert enumerated == sorted(files), (
        "soc_health must enumerate EVERY GEDI*.h5 file regardless of "
        "per-orbit-track product completeness — the pivot+dropna() in "
        "soc_file_tree silently dropped partial-download granules."
    )


def test_soc_health_enumeration_prefers_manifest(tmp_dir):
    """When ``_soc_manifest.txt`` is present, the soc_health
    enumerator must read from it (the O(1)-on-the-metadata-server
    path) rather than recursive-globbing."""
    from gedih3.doctor.diagnoses.soc_health import _enumerate_soc_files
    from gedih3.gedidriver import write_soc_manifest

    soc_dir = os.path.join(tmp_dir, 'soc_manifest')
    os.makedirs(soc_dir, exist_ok=True)
    listed_files = [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]
    for n in listed_files:
        with open(os.path.join(soc_dir, n), 'wb') as f:
            f.write(b'x')
    n_written = write_soc_manifest(soc_dir)
    assert n_written == 2

    # Drop a file NOT in the manifest. If the enumerator reads the
    # manifest it must NOT appear; if it falls back to recursive glob
    # it would.
    extra = 'GEDI02_A_2025001000000_O99999_99_T99999_99_999_99_V003.h5'
    with open(os.path.join(soc_dir, extra), 'wb') as f:
        f.write(b'x')

    enumerated = sorted(os.path.basename(p)
                        for p in _enumerate_soc_files(soc_dir))
    assert extra not in enumerated, (
        "Manifest must be preferred over the live recursive glob"
    )
    assert sorted(listed_files) == enumerated
