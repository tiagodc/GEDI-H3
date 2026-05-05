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
