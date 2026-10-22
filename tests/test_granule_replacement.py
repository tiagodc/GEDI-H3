"""Re-indexing re-released granules: the merge primitive (#47 part 4).

``h3_merge_files(replace_granules=)`` / ``parquet_merge_files(replace_granules=)``
drop a destination's rows of the replaced granules unless they already come
from the new files, then merge the fragments' rows in their place.
"""
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gedih3.config import PARTITION_META_FILENAME

CELL = '830001fffffffff'
OLD, NEW = '02_003_02', '02_004_01'


def _name(product, orbit, release):
    return f"GEDI0{product[1]}_{product[2]}_2020001000000_O{orbit:05d}_01_T{orbit + 1000:05d}_{release}_V002.h5"


def _key(orbit):
    return (orbit, 1, orbit + 1000)


def _shots(orbit, idx):
    """Shot numbers that encode ``orbit`` and granule 1 (beam 0)."""
    return [orbit * 10**13 + 1 * 10**8 + i for i in idx]


def _rows(orbit, idx, release, value, l4a_release=None):
    n = len(idx)
    return pd.DataFrame({
        'shot_number': np.asarray(_shots(orbit, idx), dtype=np.uint64),
        'root_file_l2a': [_name('L2A', orbit, release)] * n,
        'root_file_l4a': [_name('L4A', orbit, l4a_release or release)] * n,
        'datetime': pd.to_datetime(['2020-01-01'] * n),
        'agbd_l4a': np.full(n, value, dtype=np.float64),
    })


def _write(path, *frames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.concat(frames, ignore_index=True), preserve_index=False), path)


@pytest.fixture
def layout(tmp_dir):
    in_dir = os.path.join(tmp_dir, 'tmp', f'h3_03={CELL}', 'year=2020') + '/'
    out_dir = os.path.join(tmp_dir, 'db')
    dest = os.path.join(out_dir, f'h3_03={CELL}', 'year=2020', f'{CELL}.2020.0.parquet')
    return in_dir, out_dir, dest


def _new_files(orbit):
    return {'L2A': _name('L2A', orbit, NEW), 'L4A': _name('L4A', orbit, NEW)}


def _table(dest):
    return pq.read_table(dest).to_pandas().sort_values('shot_number').reset_index(drop=True)


def test_replaced_granule_rows_are_the_new_files_and_others_are_untouched(layout):
    from gedih3.gh3builder import h3_merge_files, h3_read_metadata
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 6), OLD, 1.0), _rows(200, range(0, 4), OLD, 2.0))
    # The re-release keeps shots 0-2 (new values), drops 3-5, adds 6-7.
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), _rows(100, [0, 1, 2, 6, 7], NEW, 9.0))

    h3_merge_files(in_dir, out_dir, rm_src=True, replace_granules={_key(100): _new_files(100)})

    t = _table(dest)
    replaced, other = t[t.shot_number // 10**13 == 100], t[t.shot_number // 10**13 == 200]
    assert sorted(replaced.shot_number) == _shots(100, [0, 1, 2, 6, 7])
    assert set(replaced.agbd_l4a) == {9.0} and set(replaced.root_file_l2a) == {_name('L2A', 100, NEW)}
    assert len(other) == 4 and set(other.agbd_l4a) == {2.0}           # another granule: untouched
    assert t.shot_number.is_unique
    assert not os.path.exists(in_dir)
    meta = h3_read_metadata(dest)
    assert meta['shot_count'] == 9


def test_rerunning_a_replacement_gives_the_same_file(layout):
    """Rows a replacement wrote name the new files and are kept: a re-run,
    with the fragments (kill before cleanup) or without, changes nothing."""
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 6), OLD, 1.0), _rows(200, range(0, 4), OLD, 2.0))
    frag = os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet')
    _write(frag, _rows(100, [0, 1, 6], NEW, 9.0))
    replace = {_key(100): _new_files(100)}

    h3_merge_files(in_dir, out_dir, rm_src=False, replace_granules=replace)
    first = _table(dest)
    h3_merge_files(in_dir, out_dir, rm_src=True, replace_granules=replace)      # fragments still there
    pd.testing.assert_frame_equal(_table(dest), first)
    h3_merge_files(in_dir, out_dir, rm_src=True, replace_granules=replace)      # fragments gone
    pd.testing.assert_frame_equal(_table(dest), first)


def test_a_re_release_of_one_product_replaces_the_granule_rows(layout):
    """Only L4A was re-released: the rows still name the old L4A file, so
    they are dropped although their L2A file is unchanged."""
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0))
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), _rows(100, [0, 1], OLD, 9.0, l4a_release=NEW))

    h3_merge_files(in_dir, out_dir, rm_src=True, replace_granules={
        _key(100): {'L2A': _name('L2A', 100, OLD), 'L4A': _name('L4A', 100, NEW)}})

    t = _table(dest)
    assert sorted(t.shot_number) == _shots(100, [0, 1]) and set(t.agbd_l4a) == {9.0}


def test_shots_that_left_the_cell_year_are_dropped_without_fragments(layout):
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0), _rows(200, range(0, 2), OLD, 2.0))

    assert h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)}) == dest

    assert sorted(_table(dest).shot_number) == _shots(200, [0, 1])


def test_a_file_left_empty_is_removed_with_its_metadata_and_cell_summary(layout):
    from gedih3.gh3builder import h3_merge_files, h3_merge_metadata, h3_write_metadata
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0))
    h3_write_metadata(dest)
    cell_dir = os.path.dirname(os.path.dirname(dest))
    assert h3_merge_metadata(cell_dir)
    assert os.path.exists(dest.replace('.parquet', PARTITION_META_FILENAME))

    assert h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)}) is None

    assert not os.path.exists(os.path.dirname(dest))                   # file, metadata and year dir
    assert h3_merge_metadata(cell_dir) is None
    assert not os.path.exists(cell_dir)                                # no stale summary, no empty cell


def test_the_already_merged_skip_never_applies_while_replacing(layout):
    """The destination's metadata lists the granule (its old rows): no proof
    that the fragments are in, so the merge must run."""
    from gedih3.gh3builder import h3_merge_files, h3_write_metadata
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0))
    h3_write_metadata(dest)
    frag = os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet')
    _write(frag, _rows(100, [0], NEW, 9.0))
    os.utime(frag, (1, 1))                                             # older than the destination

    h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)})

    t = _table(dest)
    assert list(t.agbd_l4a) == [9.0]


def test_without_replace_granules_existing_rows_still_win(layout):
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0))
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), _rows(100, [0, 5], NEW, 9.0))

    h3_merge_files(in_dir, out_dir)

    t = _table(dest)
    assert list(t.agbd_l4a) == [1.0, 1.0, 1.0, 9.0]                    # shot 0 kept, shot 5 added


def test_merge_tasks_read_the_targets_from_the_journal(layout, monkeypatch):
    """A replacement's merges take the target map from its journal, once per
    worker process: no task carries it, whatever the number of targets."""
    import cloudpickle
    from dask.distributed import Client, LocalCluster
    import gedih3.gh3builder as gb
    in_dir, out_dir, dest = layout
    parquet_dir = os.path.dirname(os.path.dirname(in_dir.rstrip('/')))
    rtmp = os.path.join(os.path.dirname(parquet_dir), 'replace_tmp')
    _write(dest, _rows(100, range(0, 6), OLD, 1.0), _rows(200, range(0, 4), OLD, 2.0))
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), _rows(100, [0, 1, 6], NEW, 9.0))
    targets = {_key(o): _new_files(o) for o in range(1000, 6000)}
    targets[_key(100)] = _new_files(100)
    os.makedirs(os.path.dirname(gb.replacement_journal(rtmp)))
    with open(gb.replacement_journal(rtmp), 'w') as f:
        json.dump(sorted([gb._granule_key_str(k), sorted(v.items())] for k, v in targets.items()), f)
    assert gb.read_replacement_journal(rtmp) == targets

    payloads = []
    with LocalCluster(n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None,
                      silence_logs='ERROR') as cluster, Client(cluster) as client:
        real_map = client.map

        def recording_map(fn, *args, **kwargs):
            payloads.append(len(cloudpickle.dumps(fn)))
            return real_map(fn, *args, **kwargs)
        monkeypatch.setattr(client, 'map', recording_map)
        gb._merge_and_finalize(parquet_dir, out_dir, replacement_tmp=rtmp)

    assert payloads and max(payloads) < 4096 < len(cloudpickle.dumps(targets))
    t = _table(dest)
    replaced, other = t[t.shot_number // 10**13 == 100], t[t.shot_number // 10**13 == 200]
    assert sorted(replaced.shot_number) == _shots(100, [0, 1, 6]) and set(replaced.agbd_l4a) == {9.0}
    assert len(other) == 4 and set(other.agbd_l4a) == {2.0}


def test_a_merge_never_uses_another_replacements_map(tmp_dir, monkeypatch):
    """Two journals with the same path, size and mtime (coarse or cached file
    stamps): each task still merges with the map the driver read, and a journal
    that changed or is gone fails the merge instead of merging another map."""
    import gedih3.gh3builder as gb
    from gedih3.exceptions import GediMergeError
    monkeypatch.setattr(gb, 'h3_merge_files', lambda in_dir, out_dir, replace_granules=None, **kw: replace_granules)
    rtmp, path = str(tmp_dir), gb.replacement_journal(str(tmp_dir))
    os.makedirs(os.path.dirname(path))

    def journal(targets, mtime_ns=None):
        entries = sorted([gb._granule_key_str(k), sorted(v.items())] for k, v in targets.items())
        with open(path, 'w') as f:
            json.dump(entries, f)
        if mtime_ns is not None:
            os.utime(path, ns=(mtime_ns, mtime_ns))
        assert gb._targets_digest(entries) == gb._replacement_digest(rtmp)   # what the driver passes
        return gb._replacement_digest(rtmp)

    first, second = {_key(100): _new_files(100)}, {_key(200): _new_files(200)}
    d1 = journal(first)
    stamp = (os.stat(path).st_size, os.stat(path).st_mtime_ns)
    assert gb._h3_merge_replacing('in/', 'out', rtmp, d1) == first
    d2 = journal(second, mtime_ns=stamp[1])
    assert (os.stat(path).st_size, os.stat(path).st_mtime_ns) == stamp and d2 != d1
    assert gb._h3_merge_replacing('in/', 'out', rtmp, d2) == second
    with pytest.raises(GediMergeError, match='changed'):
        gb._h3_merge_replacing('in/', 'out', rtmp, d1)
    os.remove(path)
    with pytest.raises(GediMergeError, match='changed or is gone'):
        gb._h3_merge_replacing('in/', 'out', rtmp, d1)
    with pytest.raises(GediMergeError, match='no journal'):
        gb._replacement_digest(rtmp)
    assert gb.read_replacement_journal(rtmp) is None


def test_a_granule_of_another_track_with_the_same_orbit_is_kept(tmp_dir):
    """Orbit and granule come from the shot number; the track from the file."""
    from gedih3.utils import parquet_merge_files
    src = os.path.join(tmp_dir, 'a.parquet')
    other = _rows(100, range(0, 2), OLD, 2.0)
    other['root_file_l2a'] = other['root_file_l2a'].str.replace('_T01100_', '_T07777_')
    _write(src, _rows(100, range(2, 4), OLD, 1.0), other)
    out = os.path.join(tmp_dir, 'b.parquet')

    stats = parquet_merge_files(out, [src], replace_granules={_key(100): _new_files(100)}, replace_in=src)

    assert stats['shot_count'] == 2 and set(pq.read_table(out)['agbd_l4a'].to_pylist()) == {2.0}


def test_replacing_needs_the_source_columns(tmp_dir):
    from gedih3.exceptions import GediMergeError
    from gedih3.utils import parquet_merge_files
    src = os.path.join(tmp_dir, 'a.parquet')
    _write(src, _rows(100, range(0, 2), OLD, 1.0).drop(columns='root_file_l2a'))
    with pytest.raises(GediMergeError, match='root_file_l2a'):
        parquet_merge_files(os.path.join(tmp_dir, 'b.parquet'), [src], replace_granules={_key(100): {}})


def test_stale_rows_mask_reads_dictionary_columns(tmp_dir):
    from gedih3.utils import _stale_granule_rows
    df = _rows(100, range(0, 3), OLD, 1.0)
    table = pa.Table.from_pandas(df, preserve_index=False)
    batch = table.to_batches()[0]
    i = table.schema.get_field_index('root_file_l2a')
    dict_batch = table.set_column(i, 'root_file_l2a', table['root_file_l2a'].dictionary_encode()).to_batches()[0]
    for b in (batch, dict_batch):
        assert _stale_granule_rows(b, {_key(100): _new_files(100)}).tolist() == [True] * 3
        assert _stale_granule_rows(b, {_key(101): _new_files(101)}).tolist() == [False] * 3


def test_new_rows_that_do_not_name_the_given_files_are_refused(layout):
    """Idempotence holds only if the new rows name the files in the map: a
    mismatch (a path, a wrong release, a null source) would make a re-run
    delete them, so the merge refuses before touching the destination."""
    from gedih3.exceptions import GediMergeError
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 3), OLD, 1.0))
    before = pq.read_table(dest)
    frag = os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet')
    new = _rows(100, [0, 1], NEW, 9.0)
    new.loc[1, 'root_file_l4a'] = None
    _write(frag, new)

    with pytest.raises(GediMergeError, match='do not come from the files'):
        h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)})
    assert pq.read_table(dest).equals(before) and os.path.exists(frag)
    with pytest.raises(GediMergeError, match='basenames'):
        h3_merge_files(in_dir, out_dir, replace_granules={
            _key(100): {p: '/soc/2020/001/' + f for p, f in _new_files(100).items()}})
    assert pq.read_table(dest).equals(before)


def test_rows_without_a_gedi_source_name_are_kept(tmp_dir):
    from gedih3.utils import parquet_merge_files
    src = os.path.join(tmp_dir, 'a.parquet')
    odd = _rows(100, range(0, 2), OLD, 2.0)
    odd['root_file_l2a'] = [None, 'custom.h5']
    _write(src, _rows(100, range(2, 4), OLD, 1.0), odd)
    out = os.path.join(tmp_dir, 'b.parquet')
    stats = parquet_merge_files(out, [src], replace_granules={_key(100): _new_files(100)}, replace_in=src)
    assert stats['shot_count'] == 2 and set(pq.read_table(out)['agbd_l4a'].to_pylist()) == {2.0}


def test_a_destination_without_a_product_source_column_checks_the_others(layout):
    """No root_file_l4a column (a product added by a variable update): its
    file cannot be checked, so L2A alone decides."""
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 2), OLD, 1.0).drop(columns='root_file_l4a'))
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'),
           _rows(100, [0], NEW, 9.0).drop(columns='root_file_l4a'))
    h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)})
    assert list(_table(dest).agbd_l4a) == [9.0]


def test_a_zero_row_file_left_by_a_kill_is_removed_on_resume(layout):
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 0), OLD, 1.0))                    # metadata gone, file emptied
    assert h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)}) is None
    assert not os.path.exists(dest)


def test_replace_granules_refuses_replace_true(layout):
    from gedih3.exceptions import GediValidationError
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    _write(dest, _rows(100, range(0, 2), OLD, 1.0))
    with pytest.raises(GediValidationError):
        h3_merge_files(in_dir, out_dir, replace=True, replace_granules={_key(100): _new_files(100)})


def test_a_first_merge_checks_the_new_rows_too(layout):
    """No destination yet (the granule's shots moved into this cell-year):
    nothing to drop, but a row a re-run would delete is still refused."""
    from gedih3.exceptions import GediMergeError
    from gedih3.gh3builder import h3_merge_files
    in_dir, out_dir, dest = layout
    new = _rows(100, [0, 1], NEW, 9.0)
    new.loc[1, 'root_file_l4a'] = None
    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), new)
    with pytest.raises(GediMergeError, match='do not come from the files'):
        h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)})
    assert not os.path.exists(dest)

    _write(os.path.join(in_dir, 'O00100_G01_T01100.BEAM0000.parquet'), _rows(100, [0, 1], NEW, 9.0))
    h3_merge_files(in_dir, out_dir, rm_src=False, replace_granules={_key(100): _new_files(100)})
    first = _table(dest)
    h3_merge_files(in_dir, out_dir, replace_granules={_key(100): _new_files(100)})
    pd.testing.assert_frame_equal(_table(dest), first)
    assert len(first) == 2


# ---------------------------------------------------------------------------
# end to end: gh3_build --replace-granules (#47 part 5)
# ---------------------------------------------------------------------------

def _write_release(soc_dir, key, release, lon0, seed, n=20, first=0, products=('02_A', '04_A')):
    """L2A + L4A files of one granule release, with real GEDI shot numbers
    (orbit, beam and granule encoded), so the replacement can match rows."""
    import h5py
    from gedih3.config import GEDI_BEAMS
    orbit, granule, track = key
    day = os.path.join(soc_dir, '2020', '100')
    os.makedirs(day, exist_ok=True)
    rng = np.random.default_rng(seed)
    names = {}
    for product in products:
        name = f"GEDI{product}_2020100000000_O{orbit:05d}_{granule:02d}_T{track:05d}_{release}_V002.h5"
        names['L' + product.replace('0', '', 1).replace('_', '')] = name
        with h5py.File(os.path.join(day, name), 'w') as f:
            for i, beam in enumerate(GEDI_BEAMS):
                g = f.create_group(beam)
                g['shot_number'] = (np.uint64(orbit) * np.uint64(10**13) + np.uint64(i) * np.uint64(10**11)
                                    + np.uint64(granule) * np.uint64(10**8)
                                    + np.arange(first, first + n, dtype=np.uint64))
                if product == '02_A':
                    g['delta_time'] = rng.uniform(7.0e7, 7.0e7 + 1e5, n)
                    g['lat_lowestmode'] = rng.uniform(0.0, 0.5, n)
                    g['lon_lowestmode'] = rng.uniform(lon0, lon0 + 0.5, n)
                    g['elev_lowestmode'] = rng.uniform(0, 100, n)
                    g['quality_flag'] = np.ones(n, dtype='u1')
                    g['degrade_flag'] = np.zeros(n, dtype='u1')
                    g['sensitivity'] = rng.uniform(0.9, 1.0, n)
                    g['rh_098'] = rng.uniform(0, 50, n)
                    g['rh_050'] = rng.uniform(0, 30, n)
                else:
                    g['agbd'] = rng.uniform(1, 300, n)
                    g['agbd_se'] = rng.uniform(1, 30, n)
                    g['l4_quality_flag'] = np.ones(n, dtype='u1')
    return names


def _db_rows(h3_dir):
    import glob
    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')))
    return pd.concat([pq.read_table(f, columns=['shot_number', 'root_file_l2a', 'root_file_l4a', 'agbd_l4a'])
                      .to_pandas() for f in files], ignore_index=True)


def _gh3_build(soc_dir, h3_dir, tmp, *extra, ok=True, products=('-l2a', 'rh_098', '-l4a', 'agbd')):
    import subprocess
    import sys
    cmd = [sys.executable, '-m', 'gedih3.cli.gh3_build', '-i', soc_dir, '-o', h3_dir, '-t', tmp,
           '--gedi-version', '2', '-N', '2', '-T', '1', '-M', '2', '-P', '0', '--no-bbox-index',
           *products, '--region=-51,0,-49,1', *extra]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    out = res.stdout + res.stderr
    assert (res.returncode == 0) is ok, out[-4000:]
    return out


def test_replace_granules_end_to_end(tmp_dir):
    """A re-release that moves and revalues a granule, and one that leaves the
    region: after --replace-granules auto, every row of both comes from the
    new files (or is gone), the other granule is untouched, the log records
    the new releases, and the database reads as up to date."""
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    moved, leaves, kept = (101, 1, 201), (102, 1, 202), (103, 1, 203)
    _write_release(soc_dir, moved, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, leaves, OLD, lon0=-50.2, seed=2)
    _write_release(soc_dir, kept, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    before = _db_rows(h3_dir)
    kept_before = before[before.shot_number // 10**13 == 103].sort_values('shot_number').reset_index(drop=True)

    # NASA re-releases two of them: one moved and revalued (fewer shots), one now outside the region.
    new_moved = _write_release(soc_dir, moved, NEW, lon0=-49.9, seed=11, n=15, first=3)
    _write_release(soc_dir, leaves, NEW, lon0=10.0, seed=12)
    assert '2 granule(s) were indexed from an older release' in _gh3_build(soc_dir, h3_dir, tmp)

    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    assert '2 granule(s) re-indexed from their new release (1 with no row left in the region' in out

    after = _db_rows(h3_dir)
    assert after.shot_number.is_unique
    rows_moved = after[after.shot_number // 10**13 == 101]
    assert set(rows_moved.root_file_l2a) == {new_moved['L2A']} and set(rows_moved.root_file_l4a) == {new_moved['L4A']}
    assert len(rows_moved) == 8 * 15
    assert (after.shot_number // 10**13 == 102).sum() == 0
    kept_after = after[after.shot_number // 10**13 == 103].sort_values('shot_number').reset_index(drop=True)
    pd.testing.assert_frame_equal(kept_after, kept_before)

    log = json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))
    granules = {g['orbit']: g for g in log['granules']}
    assert granules[101]['status'] == 'INDEXED' and granules[101]['releases'] == {'L2A': NEW, 'L4A': NEW}
    assert granules[102]['status'] == 'NO_DATA' and granules[102]['releases'] == {'L2A': NEW, 'L4A': NEW}
    assert granules[103]['releases'] == {'L2A': OLD, 'L4A': OLD}
    assert log['status'] == 'COMPLETED' and log['update_history'][-1]['action'] == 'granule_replacement'
    assert not os.path.exists(os.path.join(tmp, '_replace'))
    # Every partition the log lists exists, and every metadata file lists only live granules.
    for cell in log['h3_partition_ids']:
        assert os.path.isdir(os.path.join(h3_dir, f"h3_03={cell}"))

    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert 'up-to-date' in out and 'older release' not in out


def test_replace_granules_refuses_what_it_cannot_do_safely(tmp_dir):
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key = (101, 1, 201)
    indexed = _write_release(soc_dir, key, NEW, lon0=-50.9, seed=1)
    _gh3_build(soc_dir, h3_dir, tmp)
    for name in indexed.values():                                    # only an older release is left
        os.unlink(os.path.join(soc_dir, '2020', '100', name))
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=2)
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'O00101_01_T00201,O00999_01_T00001')
    assert 'O00101_01_T00201: only an older release than the indexed one is listed' in out
    assert 'O00999_01_T00001: not indexed in this database' in out
    assert 'No granule to re-index' in out
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto', '--region=-52,0,-49,1', ok=False)
    assert 'in a run of its own' in out
    assert 'No granule to re-index' in _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', '')
    out = _gh3_build(soc_dir, os.path.join(tmp_dir, 'none'), tmp, '--replace-granules', 'auto', ok=False)
    assert 'No database at' in out and 'Traceback' not in out


def test_the_build_log_keeps_exclude_for_every_later_run(tmp_dir):
    """An _SGS variant outranks the indexed file. Once a run passes --exclude
    '*_SGS.h5' (here a replacement that finds nothing to do), later runs without
    the flag (replacement, variable add, up-to-date check) still leave it out,
    and new patterns add to the log's."""
    import h5py
    import shutil
    from gedih3.config import BUILD_LOG_FILENAME, GEDI_BEAMS
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    names = _write_release(soc_dir, (101, 1, 201), OLD, lon0=-50.9, seed=1)
    _gh3_build(soc_dir, h3_dir, tmp, '--exclude', '*_BETA.h5')

    sgs = os.path.join(soc_dir, '2020', '100', names['L2A'].replace('_V002.h5', '_V002_SGS.h5'))
    shutil.copy(os.path.join(soc_dir, '2020', '100', names['L2A']), sgs)
    with h5py.File(sgs, 'r+') as f:
        for beam in GEDI_BEAMS:
            f[beam]['rh_098'][...] = 999.0
            f[beam]['rh_050'][...] = 999.0

    def log():
        return json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME))).get('exclude')

    def rows():
        import glob
        return pd.concat([pq.read_table(f).to_pandas() for f in
                          glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))], ignore_index=True)

    logs = [log()]
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto', '--exclude', '*_SGS.h5')
    assert 'No granule to re-index' in out
    logs.append(log())
    assert 'No granule to re-index' in _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    _gh3_build(soc_dir, h3_dir, tmp, '-l2a', 'rh_098', 'rh_050', '--exclude', '*_TEST.h5', '--exclude', '*_TEST.h5')
    after = rows()
    assert set(after.root_file_l2a) == {names['L2A']}
    assert not (after.rh_098_l2a == 999).any() and not (after.rh_050_l2a == 999).any()
    assert after.rh_050_l2a.notna().all()
    logs.append(log())
    out = _gh3_build(soc_dir, h3_dir, tmp, '-l2a', 'rh_098', 'rh_050', '--exclude', '*_LAST.h5')
    assert 'up-to-date' in out and 'older release' not in out
    logs.append(log())
    assert logs == [['*_BETA.h5'], ['*_BETA.h5', '*_SGS.h5'], ['*_BETA.h5', '*_SGS.h5', '*_TEST.h5'],
                    ['*_BETA.h5', '*_SGS.h5', '*_TEST.h5', '*_LAST.h5']]


def _merge_failing_one_cell(ofile, *args, **kwargs):
    """parquet_merge_files, failing for one cell that holds only old rows."""
    from gedih3.utils import parquet_merge_files
    if '83806afffffffff' in ofile:
        raise OSError("simulated failure of one cell-year")
    return parquet_merge_files(ofile, *args, **kwargs)


def test_an_interrupted_replacement_is_finished_by_the_next_gh3_build(tmp_dir, monkeypatch):
    """A merge fails part-way (as a kill would leave it): the journal stays,
    a plain gh3_build resumes the replacement, and the result is exact."""
    from dask.distributed import Client, LocalCluster
    import gedih3.gh3builder as gb
    from gedih3.config import BUILD_LOG_FILENAME
    from gedih3.exceptions import GediError
    from gedih3.gedidriver import soc_file_tree
    from gedih3.logger import H3BuildLogger

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    moved, kept = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, moved, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, kept, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    kept_before = (_db_rows(h3_dir).query('shot_number // 10**13 == 103')
                   .sort_values('shot_number').reset_index(drop=True))
    new = _write_release(soc_dir, moved, NEW, lon0=-49.9, seed=11, n=15, first=3)

    log = H3BuildLogger(product_vars=None, dir=h3_dir)
    log.log_data['_pending_replacement'] = tmp                       # as the CLI marks it
    log.save_log('PROCESSING')
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                           dashboard_address=None, silence_logs='ERROR')
    client = Client(cluster)
    try:
        entry = next(e for e in soc_file_tree(soc_dir, to_list=True, require_all=False)
                     if 'O00101' in e['L2A'])
        monkeypatch.setattr(gb, 'parquet_merge_files', _merge_failing_one_cell)
        with pytest.raises(GediError, match='merge'):
            gb._replace_granules_build(h3_dir, {moved: entry}, log.get_product_vars(), 12, 3,
                                       spatial=log.get_spatial(), version=2, tmp_dir=tmp)
    finally:
        client.close()
        cluster.close()
    monkeypatch.undo()
    assert os.path.exists(gb.replacement_journal(tmp))

    # A still newer release lands meanwhile; the resume runs with another -t.
    _write_release(soc_dir, moved, '02_005_01', lon0=-49.0, seed=21)
    out = _gh3_build(soc_dir, h3_dir, os.path.join(tmp_dir, 'other_tmp'))  # no flag: the log points at the journal
    assert 'Resuming the re-indexing of 1 granule(s)' in out
    after = _db_rows(h3_dir)
    assert after.shot_number.is_unique
    rows = after[after.shot_number // 10**13 == 101]
    assert set(rows.root_file_l2a) == {new['L2A']} and len(rows) == 8 * 15
    pd.testing.assert_frame_equal(
        after.query('shot_number // 10**13 == 103').sort_values('shot_number').reset_index(drop=True), kept_before)
    g = {x['orbit']: x for x in json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))['granules']}
    assert g[101]['releases'] == {'L2A': NEW, 'L4A': NEW}            # the journal's release, not the latest
    assert not os.path.exists(gb.replacement_journal(tmp))
    assert '_pending_replacement' not in json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))


def test_a_different_replacement_is_refused_while_one_is_unfinished(tmp_dir):
    import gedih3.gh3builder as gb
    from gedih3.exceptions import GediValidationError
    journal = gb.replacement_journal(str(tmp_dir))
    os.makedirs(os.path.dirname(journal))
    with open(journal, 'w') as f:
        json.dump([['O00101_01_T00201', [['L2A', 'a.h5']]]], f)
    assert gb.read_replacement_journal(str(tmp_dir)) == {(101, 1, 201): {'L2A': 'a.h5'}}
    with pytest.raises(GediValidationError, match='unfinished'):
        from dask.distributed import Client, LocalCluster
        with LocalCluster(n_workers=1, processes=False, dashboard_address=None) as c, Client(c):
            gb._replace_granules_build(str(tmp_dir), {(102, 1, 202): {'L2A': '/x/b.h5'}}, {'L2A': ['rh_098']},
                                       12, 3, tmp_dir=str(tmp_dir))


def test_parse_replace_granules(tmp_dir):
    from gedih3.cli.gh3_build import _parse_replace_granules
    from gedih3.exceptions import GediValidationError
    assert _parse_replace_granules(' AUTO ') is None
    assert _parse_replace_granules('') == set()
    assert _parse_replace_granules('O00101_01_T00201, O00102_02_T00202\nGEDI04_A_2020100000000_O00103_01_T00203'
                                   '_02_003_02_V002.h5') == {(101, 1, 201), (102, 2, 202), (103, 1, 203)}
    keys = os.path.join(tmp_dir, 'keys.txt')
    with open(keys, 'w') as f:
        f.write('O00101_01_T00201\n\nO00104_03_T00204\n')
    assert _parse_replace_granules(keys) == {(101, 1, 201), (104, 3, 204)}
    with pytest.raises(GediValidationError, match='file not found'):
        _parse_replace_granules(os.path.join(tmp_dir, 'missing.txt'))
    with pytest.raises(GediValidationError, match='not a granule key'):
        _parse_replace_granules('O00101_01_T00201,garbage')
    gone = os.path.join(tmp_dir, 'old', 'GEDI02_A_2020100000000_O00105_01_T00205_02_003_02_V002.h5')
    assert _parse_replace_granules(gone) == {(105, 1, 205)}                # a GEDI file path, even a removed one
    binary = os.path.join(tmp_dir, 'keys.bin')
    with open(binary, 'wb') as f:
        f.write(bytes([0xff, 0xfe, 0x00, 0x81]))
    with pytest.raises(GediValidationError, match='not a text file'):
        _parse_replace_granules(binary)


def test_year_files_without_metadata_are_rewritten_too(tmp_dir):
    """A lost sidecar, or a 0-row file a killed replacement emptied between
    its two unlinks: neither can be ruled out from metadata, so both are
    rewritten, and the merge decides from the rows' source columns."""
    import glob
    from dask.distributed import Client, LocalCluster
    import gedih3.gh3builder as gb
    from gedih3.config import PARTITION_META_FILENAME
    from gedih3.gedidriver import soc_file_tree
    from gedih3.logger import H3BuildLogger
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    moved = (101, 1, 201)
    _write_release(soc_dir, moved, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, (103, 1, 203), OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    new = _write_release(soc_dir, moved, NEW, lon0=-49.9, seed=11)
    files = sorted(glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')))
    only_moved = [f for f in files if set(pq.read_table(f, columns=['root_file_l2a'])['root_file_l2a'].to_pylist())
                  == {f"GEDI02_A_2020100000000_O00101_01_T00201_{OLD}_V002.h5"}]
    assert len(only_moved) >= 2
    lost_sidecar, emptied = only_moved[:2]
    os.unlink(lost_sidecar.replace('.parquet', PARTITION_META_FILENAME))
    pq.write_table(pq.read_table(emptied).slice(0, 0), emptied)        # a kill between the two unlinks
    os.unlink(emptied.replace('.parquet', PARTITION_META_FILENAME))

    log = H3BuildLogger(product_vars=None, dir=h3_dir)
    with LocalCluster(n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None,
                      silence_logs='ERROR') as cluster, Client(cluster):
        entry = next(e for e in soc_file_tree(soc_dir, to_list=True, require_all=False) if 'O00101' in e['L2A'])
        gb._replace_granules_build(h3_dir, {moved: entry}, log.get_product_vars(), 12, 3,
                                   spatial=log.get_spatial(), version=2, tmp_dir=tmp)
    assert not os.path.exists(emptied)
    after = _db_rows(h3_dir)
    assert after.shot_number.is_unique
    assert set(after[after.shot_number // 10**13 == 101].root_file_l2a) == {new['L2A']}


def test_a_product_added_by_a_variable_update_is_replaced_like_any_other(tmp_dir):
    """L4A added to an L2A database by a variable update gets its source
    columns and recorded release, as a build writes them (#68). A re-release
    of L4A alone is then found by auto and its rows take the new values; the
    L2A columns, from an unchanged file, stay."""
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    old = _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp, products=('-l2a', 'rh_098'))
    _gh3_build(soc_dir, h3_dir, tmp)                                  # adds L4A: a variable update

    def rows():
        import glob
        return pd.concat([pq.read_table(f).to_pandas() for f in
                          glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))], ignore_index=True)

    def log():
        return json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))
    added = rows()
    mine = added[added.shot_number // 10**13 == 101]
    assert set(mine.root_file_l4a) == {old['L4A']} and mine.root_beam_l4a.notna().all()
    assert {'root_file_l4a', 'root_beam_l4a'} <= set(log()['h3_columns'])
    assert {g['orbit']: g['releases'] for g in log()['granules']} == {101: {'L2A': OLD, 'L4A': OLD},
                                                                       103: {'L2A': OLD, 'L4A': OLD}}

    new = _write_release(soc_dir, key, NEW, lon0=-50.9, seed=21, products=('04_A',))   # L4A re-released alone
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    assert '1 granule(s) re-indexed from their new release' in out
    after = rows()
    mine_after = after[after.shot_number // 10**13 == 101].set_index('shot_number').sort_index()
    mine_before = mine.set_index('shot_number').sort_index()
    assert set(mine_after.root_file_l4a) == {new['L4A']} and set(mine_after.root_file_l2a) == {old['L2A']}
    assert not np.allclose(mine_after.agbd_l4a, mine_before.agbd_l4a)
    pd.testing.assert_series_equal(mine_after.rh_098_l2a, mine_before.rh_098_l2a)
    theirs = after[after.shot_number // 10**13 == 103].set_index('shot_number').sort_index()
    pd.testing.assert_frame_equal(theirs, added[added.shot_number // 10**13 == 103].set_index('shot_number')
                                  .sort_index()[theirs.columns])
    assert {g['orbit']: g['releases'] for g in log()['granules']}[101] == {'L2A': OLD, 'L4A': NEW}


def test_a_product_without_a_source_column_is_never_replaced_blind(tmp_dir):
    """A database whose L4A an older gedih3 added without root_file_l4a: a
    replacement where only L4A changed is refused before anything is written,
    one where L2A changed too goes ahead, and a later variable add of L4A does
    not give it a source column its older values would not match (#68)."""
    from gedih3.config import BUILD_LOG_FILENAME
    from gedih3.exceptions import GediValidationError
    from gedih3.gh3builder import check_replacement_sources
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key = (101, 1, 201)
    old = _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _gh3_build(soc_dir, h3_dir, tmp, products=('-l2a', 'rh_098'))
    _gh3_build(soc_dir, h3_dir, tmp)
    # What an older gedih3 left: no L4A source columns, no L4A release recorded.
    import glob
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')):
        t = pq.ParquetFile(f).read()                # no hive columns from the path
        pq.write_table(t.drop_columns(['root_file_l4a', 'root_beam_l4a']), f)
    path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    log = json.load(open(path))
    log['h3_columns'] = [c for c in log['h3_columns'] if c not in ('root_file_l4a', 'root_beam_l4a')]
    for g in log['granules']:
        g['releases'].pop('L4A')
    json.dump(log, open(path, 'w'))

    new = _write_release(soc_dir, key, NEW, lon0=-50.9, seed=21, products=('04_A',))
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'O00101_01_T00201', ok=False)
    assert 'Cannot replace granules: 1 granule(s) (O00101_01_T00201)' in out and 'root_file_l4a' in out
    after = json.load(open(path))
    assert after['status'] == log['status'] and '_pending_replacement' not in after
    assert not os.path.exists(os.path.join(tmp, '_replace'))

    with pytest.raises(GediValidationError, match='Cannot replace'):
        check_replacement_sources(h3_dir, {key: {'L2A': old['L2A'], 'L4A': new['L4A']}})
    check_replacement_sources(h3_dir, {key: {'L2A': old['L2A'].replace(OLD, NEW), 'L4A': new['L4A']}})

    # A run an older gedih3 journaled and had started merging is finished, not stranded; the
    # log keeps the release the L4A rows still hold.
    import gedih3.gh3builder as gb
    os.makedirs(os.path.join(tmp, '_replace'), exist_ok=True)
    with open(gb.replacement_journal(tmp), 'w') as f:
        json.dump([[gb._granule_key_str(key), sorted({'L2A': old['L2A'], 'L4A': new['L4A']}.items())]], f)
    open(os.path.join(tmp, '_replace', gb._REPLACE_MERGING), 'w').close()
    out = _gh3_build(soc_dir, h3_dir, tmp, products=())     # no L4A release recorded: none is invented
    assert 'finishing the run started earlier' in out and '1 granule(s) re-indexed' in out
    assert json.load(open(path))['granules'][0]['releases'] == {'L2A': OLD}
    assert not os.path.exists(gb.replacement_journal(tmp))

    _gh3_build(soc_dir, h3_dir, tmp, products=('-l2a', 'rh_098', '-l4a', 'agbd', 'agbd_se'))
    cols = set(json.load(open(path))['h3_columns'])
    assert 'agbd_se_l4a' in cols and 'root_file_l4a' not in cols


def test_files_without_the_source_column_are_found_even_when_the_log_lists_it(tmp_dir):
    """The log's h3_columns is the union of every file's: after an expansion,
    newer cells list root_file_l4a while older ones lack it. The replacement
    reads the footers of the files holding each target: auto leaves out one
    whose only re-released product has no source column there, a named one is
    refused, and the log and the files stay as they were (#68)."""
    import glob
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp, products=('-l2a', 'rh_098'))
    _gh3_build(soc_dir, h3_dir, tmp)
    stripped = {}
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')):
        t = pq.ParquetFile(f).read()                # no hive columns from the path
        if (t['shot_number'].to_numpy() // 10**13 == 101).any() and not (t['shot_number'].to_numpy() // 10**13 == 103).any():
            pq.write_table(t.drop_columns(['root_file_l4a', 'root_beam_l4a']), f)
            stripped[f] = os.stat(f).st_mtime_ns
    assert stripped
    path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    before = json.load(open(path))
    assert 'root_file_l4a' in before['h3_columns']

    _write_release(soc_dir, key, NEW, lon0=-50.9, seed=21, products=('04_A',))
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')        # auto leaves it out
    assert 'leaving out 1 granule(s) (O00101_01_T00201) cannot be replaced' in out and 'No granule to re-index' in out
    assert {f: os.stat(f).st_mtime_ns for f in stripped} == stripped
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'O00101_01_T00201', ok=False)   # named: refused
    assert 'Cannot replace granules: 1 granule(s) (O00101_01_T00201)' in out and 'This run changed nothing' in out
    after = json.load(open(path))
    assert after['status'] == before['status'] and '_pending_replacement' not in after
    assert {g['orbit']: g['releases'] for g in after['granules']}[101] == {'L2A': OLD, 'L4A': OLD}
    assert not os.path.exists(os.path.join(tmp, '_replace', '_replace_targets.json'))
    assert {f: os.stat(f).st_mtime_ns for f in stripped} == stripped


def test_auto_leaves_out_what_it_cannot_check_and_replaces_the_rest(tmp_dir):
    """One granule whose only re-released product has no source column in its
    files, one with every product re-released: auto leaves the first out
    (its files untouched, its releases as they were) and replaces the second."""
    import glob
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp, products=('-l2a', 'rh_098'))
    _gh3_build(soc_dir, h3_dir, tmp)
    stripped = {}
    for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet')):
        t = pq.ParquetFile(f).read()                # no hive columns from the path
        orbits = t['shot_number'].to_numpy() // 10**13
        if (orbits == 101).any() and not (orbits == 103).any():
            pq.write_table(t.drop_columns(['root_file_l4a', 'root_beam_l4a']), f)
            stripped[f] = os.stat(f).st_mtime_ns
    assert stripped
    _write_release(soc_dir, key, NEW, lon0=-50.9, seed=21, products=('04_A',))
    new_other = _write_release(soc_dir, other, NEW, lon0=-49.6, seed=23)

    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    assert 'leaving out 1 granule(s) (O00101_01_T00201)' in out and '1 granule(s) re-indexed' in out
    assert {f: os.stat(f).st_mtime_ns for f in stripped} == stripped
    rows = pd.concat([pq.ParquetFile(f).read(columns=['shot_number', 'root_file_l2a']).to_pandas()
                      for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))], ignore_index=True)
    assert rows.shot_number.is_unique
    assert set(rows[rows.shot_number // 10**13 == 103].root_file_l2a) == {new_other['L2A']}
    rel = {g['orbit']: g['releases'] for g in json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))['granules']}
    assert rel == {101: {'L2A': OLD, 'L4A': OLD}, 103: {'L2A': NEW, 'L4A': NEW}}
    assert not os.path.exists(os.path.join(tmp, '_replace'))


def _forget_releases(h3_dir):
    """The build log of a database built before gedih3 recorded releases (0.20.10)."""
    from gedih3.config import BUILD_LOG_FILENAME
    path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    log = json.load(open(path))
    for g in log['granules']:
        g.pop('releases', None)
    json.dump(log, open(path, 'w'))
    return path


def test_releases_of_a_database_built_before_they_were_recorded_come_from_its_rows(tmp_dir):
    """A log with no recorded release: the first run that checks for
    re-releases reads them from the rows' root_file columns, once, so an
    up-to-date run warns and auto re-indexes the re-released granule (#67)."""
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    path = _forget_releases(h3_dir)
    new = _write_release(soc_dir, key, NEW, lon0=-50.9, seed=11, products=('04_A',))

    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert 'Reading the source releases' in out and '1 granule(s) were indexed from an older release' in out
    log = json.load(open(path))
    assert {g['orbit']: g['releases'] for g in log['granules']} == {101: {'L2A': OLD, 'L4A': OLD},
                                                                    103: {'L2A': OLD, 'L4A': OLD}}
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    assert 'Reading the source releases' not in out                          # once per database
    assert '1 granule(s) re-indexed from their new release' in out
    rows = _db_rows(h3_dir)
    assert set(rows[rows.shot_number // 10**13 == 101].root_file_l4a) == {new['L4A']}
    assert {g['orbit']: g['releases'] for g in json.load(open(path))['granules']}[101] == {'L2A': OLD, 'L4A': NEW}


def test_rows_from_two_releases_leave_the_granule_unknown(tmp_dir):
    """A granule whose rows name two releases of a product (a replacement left
    half done) is reported, never guessed; the doctor reads the rows' releases
    in memory and lists the re-released granule all the same."""
    import glob
    import types
    from dask.distributed import Client, LocalCluster
    from gedih3.doctor.diagnoses.superseded_granules import superseded_granules_check
    from gedih3.gh3builder import releases_from_rows, _year_file_source_releases
    from gedih3.logger import H3BuildLogger
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    _forget_releases(h3_dir)
    f = sorted(f for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
               if (pq.read_table(f, columns=['shot_number'])['shot_number'].to_numpy() // 10**13 == 103).any())[0]
    t = pq.read_table(f).to_pandas().reset_index(drop=True)
    mine = t.shot_number // 10**13 == 103
    t.loc[mine & (np.arange(len(t)) % 2 == 0), 'root_file_l2a'] = t.loc[mine, 'root_file_l2a'].iloc[0].replace(OLD, NEW)
    pq.write_table(pa.Table.from_pandas(t, preserve_index=False), f)
    _write_release(soc_dir, key, NEW, lon0=-50.9, seed=11, products=('04_A',))

    with LocalCluster(n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None,
                      silence_logs='ERROR') as cluster, Client(cluster):
        releases, mixed, unread, failed = releases_from_rows(h3_dir, ['L2A', 'L4A'], [key, other], version=2)
        assert releases[key] == {'L2A': OLD, 'L4A': OLD} and releases[other] == {'L4A': OLD}
        assert mixed == {other: {'L2A': [OLD, NEW]}} and unread == set() and failed == []
        assert releases_from_rows(h3_dir, ['L2A', 'L4A'], [key])[0] == {key: {'L2A': OLD, 'L4A': OLD}}
        import gedih3.gh3builder as gb
        saved, gb._ROW_RELEASE_SHIPPED_TARGETS = gb._ROW_RELEASE_SHIPPED_TARGETS, 0
        try:                                       # a large target set: every granule read, filtered here
            assert releases_from_rows(h3_dir, ['L2A', 'L4A'], [key, other], version=2)[:2] == (releases, mixed)
            assert releases_from_rows(h3_dir, ['L2A', 'L4A'], [key])[:2] == ({key: releases[key]}, {})
        finally:
            gb._ROW_RELEASE_SHIPPED_TARGETS = saved
        assert _year_file_source_releases(f, products=['L2A'], targets={'O00103_01_T00203'}, version=3) == {}
        log = H3BuildLogger(product_vars=None, dir=h3_dir)
        report = superseded_granules_check(types.SimpleNamespace(h3_logger=log, soc_dir=soc_dir, h3_dir=h3_dir))
        assert all('releases' not in g for g in log.granule_info)                 # the shared logger is untouched
    kinds = {(f['kind'], f['granule']['orbit']) for f in report.findings}
    assert kinds == {('newer_release', 101), ('mixed_releases', 103)}
    assert 'read from the rows' in report.summary

    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert '1 granule(s) have rows from more than one release of a product (e.g. O00103_01_T00203 L2A' in out


def test_each_indexed_granule_without_a_release_is_read_from_its_rows_once(tmp_dir):
    """row_release_targets is log-only: INDEXED granules with no releases entry,
    on a database with a source column. Adopting marks every granule read, even
    when its rows named nothing, keeps a product not INDEXED out, and a granule
    indexed later without a release (registered by an older gedih3) is a
    target again."""
    from gedih3.logger import H3BuildLogger
    from conftest import make_build_log
    make_build_log(tmp_dir, granules=[
        {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED',
         'products': {'L2A': 'INDEXED', 'L4A': 'MISSING_SOURCE'}},
        {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'INDEXED'},
        {'orbit': 3, 'granule': 1, 'track': 3, 'status': 'PENDING'},
        {'orbit': 4, 'granule': 1, 'track': 4, 'status': 'INDEXED', 'releases': {'L2A': OLD}}])
    path = os.path.join(tmp_dir, 'gedih3_build_log.json')
    log = json.load(open(path))
    log['products'] = {'L2A': {'variables': ['rh_098']}, 'L4A': {'variables': ['agbd']}}
    log['h3_columns'] = ['shot_number', 'root_file_l2a', 'root_file_l4a']
    json.dump(log, open(path, 'w'))
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    keys = h.row_release_targets()
    assert keys == {(1, 1, 1), (2, 1, 2)}
    assert h.adopt_row_releases({(1, 1, 1): {'L2A': NEW, 'L4A': OLD}, (4, 1, 4): {'L2A': NEW}}, keys) == 1
    by = {g['orbit']: g.get('releases') for g in h.granule_info}
    assert by == {1: {'L2A': NEW}, 2: {}, 3: None, 4: {'L2A': OLD}}
    assert h.row_release_targets() == set()
    h.granule_info[2]['status'] = 'INDEXED'                          # built later, registered without tags
    assert h.row_release_targets() == {(3, 1, 3)}
    h.log_data['h3_columns'] = ['shot_number']
    assert H3BuildLogger.row_release_targets(h) == set()


def _files_of(h3_dir, orbit):
    import glob
    return sorted(f for f in glob.glob(os.path.join(h3_dir, 'h3_*', 'year=*', '*.parquet'))
                  if (pq.read_table(f, columns=['shot_number'])['shot_number'].to_numpy() // 10**13 == orbit).any())


def _mix_rows(f, orbit, col='root_file_l2a'):
    """Half of a granule's rows in ``f`` name the NEW release of a product, as
    a replacement left half done would; the file keeps its schema metadata."""
    t = pq.ParquetFile(f).read()
    mine = t['shot_number'].to_numpy() // 10**13 == orbit
    values = t[col].to_pylist()
    new = values[int(np.argmax(mine))].replace(OLD, NEW)
    values = [new if m and i % 2 == 0 else v for i, (v, m) in enumerate(zip(values, mine))]
    i = t.schema.get_field_index(col)
    pq.write_table(t.set_column(i, t.schema.field(i), pa.array(values, type=t.schema.field(i).type)), f)


def _doctor_superseded(h3_dir, soc_dir):
    import types
    from dask.distributed import Client, LocalCluster
    from gedih3.doctor.diagnoses.superseded_granules import superseded_granules_check
    from gedih3.logger import H3BuildLogger
    with LocalCluster(n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None,
                      silence_logs='ERROR') as cluster, Client(cluster):
        log = H3BuildLogger(product_vars=None, dir=h3_dir)
        return superseded_granules_check(types.SimpleNamespace(h3_logger=log, soc_dir=soc_dir, h3_dir=h3_dir))


def test_a_granule_whose_rows_name_two_releases_stays_reported_until_re_indexed(tmp_dir):
    """The releases a mixed product's rows name are kept in the log after the
    first read: the doctor keeps listing the granule, and once the newer file
    is listed, auto re-indexes it whole from there (#72 review)."""
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    _forget_releases(h3_dir)
    _mix_rows(_files_of(h3_dir, 103)[0], 103)

    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert 'have rows from more than one release of a product' in out
    g103 = {g['orbit']: g for g in json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))['granules']}[103]
    assert g103['releases'] == {'L4A': OLD} and g103['mixed_releases'] == {'L2A': [OLD, NEW]}
    report = _doctor_superseded(h3_dir, soc_dir)
    assert [(f['kind'], f['granule']['orbit']) for f in report.findings] == [('mixed_releases', 103)]

    new = _write_release(soc_dir, other, NEW, lon0=-49.6, seed=3, products=('02_A',))
    out = _gh3_build(soc_dir, h3_dir, tmp, '--replace-granules', 'auto')
    assert '1 granule(s) re-indexed from their new release' in out
    g103 = {g['orbit']: g for g in json.load(open(os.path.join(h3_dir, BUILD_LOG_FILENAME)))['granules']}[103]
    assert g103['releases'] == {'L2A': NEW, 'L4A': OLD} and 'mixed_releases' not in g103
    rows = pd.concat([pq.ParquetFile(f).read(columns=['shot_number', 'root_file_l2a']).to_pandas()
                      for f in _files_of(h3_dir, 103)], ignore_index=True)
    assert set(rows[rows.shot_number // 10**13 == 103].root_file_l2a) == {new['L2A']}
    assert _doctor_superseded(h3_dir, soc_dir).findings == []


def test_granules_an_unreadable_file_may_hold_are_read_again_on_the_next_run(tmp_dir):
    """A granule is marked read only when every file its metadata names was
    read: rows of a file that fails are neither guessed from the others nor
    forgotten (#72 review)."""
    from gedih3.config import BUILD_LOG_FILENAME
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    key, other = (101, 1, 201), (103, 1, 203)
    _write_release(soc_dir, key, OLD, lon0=-50.9, seed=1)
    _write_release(soc_dir, other, OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    path = _forget_releases(h3_dir)
    files = _files_of(h3_dir, 103)
    _mix_rows(files[0], 103)                     # the readable part names one release, the lost one another
    backups = {f: open(f, 'rb').read() for f in files[1:]}
    assert backups
    for f in backups:
        open(f, 'wb').write(b'garbage' * 10)

    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert 'could not be read' in out and 'read again on the next run' in out
    by = {g['orbit']: g for g in json.load(open(path))['granules']}
    assert 'releases' not in by[103] and 'mixed_releases' not in by[103]
    for f, b in backups.items():
        open(f, 'wb').write(b)
    out = _gh3_build(soc_dir, h3_dir, tmp)
    assert 'Reading the source releases' in out
    by = {g['orbit']: g for g in json.load(open(path))['granules']}
    assert by[103]['releases'] == {'L4A': OLD} and by[103]['mixed_releases'] == {'L2A': [OLD, NEW]}


def test_the_doctor_reports_mixed_rows_when_no_release_is_known(tmp_dir):
    """A database whose only granule's rows name two releases of every product:
    nothing to compare, the mixed rows are reported all the same."""
    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    _write_release(soc_dir, (103, 1, 203), OLD, lon0=-49.6, seed=3)
    _gh3_build(soc_dir, h3_dir, tmp)
    _forget_releases(h3_dir)
    for f in _files_of(h3_dir, 103):
        _mix_rows(f, 103, 'root_file_l2a')
        _mix_rows(f, 103, 'root_file_l4a')
    report = _doctor_superseded(h3_dir, soc_dir)
    assert [f['kind'] for f in report.findings] == ['mixed_releases'], report.summary
