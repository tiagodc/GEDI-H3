"""Re-indexing re-released granules: the merge primitive (#47 part 4).

``h3_merge_files(replace_granules=)`` / ``parquet_merge_files(replace_granules=)``
drop a destination's rows of the replaced granules unless they already come
from the new files, then merge the fragments' rows in their place.
"""
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
    batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
    dict_batch = batch.set_column(batch.schema.get_field_index('root_file_l2a'), 'root_file_l2a',
                                  batch['root_file_l2a'].dictionary_encode())
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
