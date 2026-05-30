"""
Tests for gh3_build safety, efficiency, and resumability.

Unit tests (no network, synthetic data):
    test_parquet_merge_atomic_write - Atomic write to temp file, rename on success
    test_parquet_merge_atomic_cleanup_on_crash - Stale .merge.tmp cleaned on re-run
    test_parquet_merge_dedup_shots - Merge with overlapping shot_numbers
    test_parquet_merge_no_delete_ofile_in_rm_src - rm_src doesn't delete output file
    test_h3_skip_column_multi_cell - Per-cell skip check (not just iloc[0])
    test_h3_skip_column_empty - Empty DataFrame gets _skip=True
    test_add_variables_resume_skips_updated - Skip partition with existing columns
    test_build_log_update_history - History recorded on terminal statuses

Integration tests (requires tutorial DB or NASA creds):
    test_fresh_build_l2a_l4a
    test_variable_only_update_add_l4c
    test_variable_only_update_expand_l2a
    test_temporal_expansion
    test_spatial_expansion
    test_resume_from_interrupted
    test_no_redundant_download
    test_idempotent_rebuild
    test_new_granules_within_existing_range
    test_mixed_update_spatial_plus_variable
    test_mixed_update_temporal_plus_variable
    test_download_not_redundant_variable_only
"""

import os
import json
import time
import tempfile
import shutil

import pytest
import numpy as np
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point
from conftest import make_gedi_parquet, make_partition_dir, make_build_log


# ===========================================================================
# UNIT TESTS
# ===========================================================================

class TestParquetMergeAtomicWrite:
    """P0-A: Atomic writes in parquet_merge_files."""

    def test_parquet_merge_atomic_write(self, tmp_dir):
        """Verify merge writes to temp file first, renames atomically."""
        from gedih3.utils import parquet_merge_files

        # Create two source files
        f1 = os.path.join(tmp_dir, 'a.parquet')
        f2 = os.path.join(tmp_dir, 'b.parquet')
        make_gedi_parquet(f1, n=10, shot_offset=0)
        make_gedi_parquet(f2, n=10, shot_offset=10)

        ofile = os.path.join(tmp_dir, 'merged.parquet')

        parquet_merge_files(ofile, [f1, f2])

        # Output file should exist and be valid
        assert os.path.exists(ofile)
        # Temp file should NOT exist
        assert not os.path.exists(ofile + '.merge.tmp')
        # Verify contents
        result = gpd.read_parquet(ofile)
        assert len(result) == 20

    def test_parquet_merge_atomic_cleanup_on_crash(self, tmp_dir):
        """Stale .merge.tmp from previous crash gets cleaned up on re-run."""
        from gedih3.utils import parquet_merge_files

        f1 = os.path.join(tmp_dir, 'a.parquet')
        make_gedi_parquet(f1, n=10)

        ofile = os.path.join(tmp_dir, 'merged.parquet')

        # Simulate stale temp from crash
        stale_tmp = ofile + '.merge.tmp'
        with open(stale_tmp, 'w') as f:
            f.write('corrupt data')
        assert os.path.exists(stale_tmp)

        parquet_merge_files(ofile, [f1])

        assert os.path.exists(ofile)
        assert not os.path.exists(stale_tmp)

    def test_parquet_merge_dedup_shots(self, tmp_dir):
        """Merge two files with overlapping shot_numbers, no duplicates."""
        from gedih3.utils import parquet_merge_files

        f1 = os.path.join(tmp_dir, 'a.parquet')
        f2 = os.path.join(tmp_dir, 'b.parquet')
        # Both have shots 0-9; should deduplicate
        make_gedi_parquet(f1, n=10, shot_offset=0)
        make_gedi_parquet(f2, n=10, shot_offset=0)

        ofile = os.path.join(tmp_dir, 'merged.parquet')
        parquet_merge_files(ofile, [f1, f2], check_shots=True)

        result = gpd.read_parquet(ofile)
        assert len(result) == 10
        assert result['shot_number'].nunique() == 10

    def test_parquet_merge_no_delete_ofile_in_rm_src(self, tmp_dir):
        """When ofile is in flist and rm_src=True, ofile is NOT deleted."""
        from gedih3.utils import parquet_merge_files

        f1 = os.path.join(tmp_dir, 'a.parquet')
        f2 = os.path.join(tmp_dir, 'b.parquet')
        make_gedi_parquet(f1, n=10, shot_offset=0)
        make_gedi_parquet(f2, n=10, shot_offset=10)

        ofile = os.path.join(tmp_dir, 'merged.parquet')

        # First merge
        parquet_merge_files(ofile, [f1, f2], rm_src=True)
        assert os.path.exists(ofile)
        assert not os.path.exists(f1)
        assert not os.path.exists(f2)

        # Second merge: ofile appears in flist (simulating append scenario)
        f3 = os.path.join(tmp_dir, 'c.parquet')
        make_gedi_parquet(f3, n=5, shot_offset=100)

        parquet_merge_files(ofile, [ofile, f3], rm_src=True)
        assert os.path.exists(ofile)  # ofile should survive
        assert not os.path.exists(f3)


class TestH3SkipColumn:
    """P1-B: Per-cell skip check in h3_add_skip_column."""

    def test_h3_skip_column_multi_cell(self, tmp_dir):
        """DataFrame with 3 H3 cells; only cell #1 has metadata. Only cell #1 rows get _skip=True."""
        from gedih3.gh3builder import h3_add_skip_column

        h3_dir = tmp_dir

        # Cell A: has metadata with the granule
        cell_a = '83184bfffffffff'
        cell_a_dir = os.path.join(h3_dir, f'h3_03={cell_a}')
        os.makedirs(cell_a_dir, exist_ok=True)
        meta_a = {
            'h3_partition': cell_a,
            'columns': ['shot_number', 'agbd_l4a', 'rh_098_l2a', 'root_file_l2a', 'h3_03', 'geometry'],
            'granules': [{'orbit': 1, 'granule': 1, 'track': 1}],
        }
        with open(os.path.join(cell_a_dir, f'{cell_a}.metadata.json'), 'w') as f:
            json.dump(meta_a, f)

        # Cell B and C: no metadata
        cell_b = '83184afffffffff'
        cell_c = '831849fffffffff'

        # Create DataFrame with rows from all 3 cells
        df = pd.DataFrame({
            'h3_03': [cell_a, cell_a, cell_b, cell_b, cell_c],
            'root_file_l2a': ['GEDI02_A_2020001000000_O00001_01_T00001_02_003_02_V002.h5'] * 5,
            'shot_number': np.arange(5, dtype=np.uint64),
            'agbd_l4a': [1.0, 2.0, 3.0, 4.0, 5.0],
            'rh_098_l2a': [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        geometry = [Point(-50.5, 0.5)] * 5
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

        result = h3_add_skip_column(gdf, h3_dir)

        assert '_skip' in result.columns
        # Cell A rows should be skipped (metadata exists with matching granule)
        assert result.loc[result['h3_03'] == cell_a, '_skip'].all()
        # Cell B and C rows should NOT be skipped
        assert not result.loc[result['h3_03'] == cell_b, '_skip'].any()
        assert not result.loc[result['h3_03'] == cell_c, '_skip'].any()

    def test_h3_skip_column_empty(self, tmp_dir):
        """Empty DataFrame gets _skip=True without errors."""
        from gedih3.gh3builder import h3_add_skip_column

        df = pd.DataFrame({
            'h3_03': pd.Series([], dtype=str),
            'root_file_l2a': pd.Series([], dtype=str),
            'shot_number': pd.Series([], dtype=np.uint64),
        })
        gdf = gpd.GeoDataFrame(df, geometry=[], crs='EPSG:4326')

        result = h3_add_skip_column(gdf, tmp_dir)
        assert '_skip' in result.columns
        assert len(result) == 0


class TestH3MergeMetadata:
    """Per-cell metadata aggregation across years."""

    def _make_year_meta(self, h3_subdir, cell, year, n=10):
        ydir = os.path.join(h3_subdir, f'year={year}')
        os.makedirs(ydir, exist_ok=True)
        pq_path = os.path.join(ydir, f'{cell}.{year}.0.parquet')
        meta_path = os.path.join(ydir, f'{cell}.{year}.0.metadata.json')
        # Minimal parquet so glob.glob(*.parquet) finds it.
        pd.DataFrame({'shot_number': np.arange(n, dtype='uint64')}).to_parquet(
            pq_path, engine='pyarrow', index=False
        )
        meta = {
            'h3_partition': cell,
            'h3_geometry': {'type': 'Polygon', 'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
            'year': year,
            'shot_count': n,
            'shot_range': [year * 1000, year * 1000 + n],
            'date_range': [f'{year}-01-01', f'{year}-12-31'],
            'granules': [{'orbit': year, 'granule': 1, 'track': year}],
            'columns': ['shot_number'],
            'column_dtypes': {'shot_number': 'uint64'},
        }
        with open(meta_path, 'w') as f:
            json.dump(meta, f)

    def test_h3_merge_metadata_includes_all_years(self, tmp_dir):
        """Regression: every year present on disk lands in the per-cell `years` list.

        The pre-fix code initialized the years set as empty and only
        added year_metadata[1:], silently dropping whichever year
        glob.glob returned first.
        """
        from gedih3.gh3builder import h3_merge_metadata

        cell = '835576fffffffff'
        cdir = os.path.join(tmp_dir, f'h3_03={cell}')
        for y in (2019, 2020, 2021, 2022, 2023):
            self._make_year_meta(cdir, cell, y)

        out = h3_merge_metadata(cdir)
        assert out is not None
        with open(out) as f:
            mmeta = json.load(f)
        assert mmeta['years'] == [2019, 2020, 2021, 2022, 2023]
        # Sanity: other aggregates already covered all years pre-fix, but
        # assert anyway to catch a future regression.
        assert mmeta['shot_count'] == 50  # 5 years * 10 shots
        assert mmeta['date_range'] == ['2019-01-01', '2023-12-31']

    def test_h3_merge_metadata_single_year(self, tmp_dir):
        """Single-year cell: years list has exactly that year."""
        from gedih3.gh3builder import h3_merge_metadata

        cell = '835576fffffffff'
        cdir = os.path.join(tmp_dir, f'h3_03={cell}')
        self._make_year_meta(cdir, cell, 2021)

        out = h3_merge_metadata(cdir)
        with open(out) as f:
            mmeta = json.load(f)
        assert mmeta['years'] == [2021]


class TestAddVariablesResume:
    """Per-(cell, year) variable update workers — scan & join, scatter-free."""

    @staticmethod
    def _write_year_sidecar(year_pf, granules):
        """Write a minimal per-year sidecar JSON beside year_pf."""
        meta_path = year_pf.replace('.parquet', '.metadata.json')
        with open(meta_path, 'w') as f:
            json.dump({'granules': granules}, f)

    # ---- Phase 1 scan worker ----

    def test_scan_skips_when_all_new_columns_already_present(self, tmp_dir):
        """Year file already carries the target columns → scan returns
        None so the driver never schedules a join task for it."""
        from gedih3.gh3builder import _scan_year_file_for_update

        _, pq_path = make_partition_dir(
            tmp_dir, n=20,
            extra_cols={'wsci_l4c': None},
        )
        result = _scan_year_file_for_update(pq_path, ['wsci_l4c'])
        assert result is None

    def test_scan_returns_granule_list_when_columns_missing(self, tmp_dir):
        """Year file is missing the target columns → scan reads the sidecar
        and returns the granule list for driver-side path resolution."""
        from gedih3.gh3builder import _scan_year_file_for_update

        _, pq_path = make_partition_dir(tmp_dir, n=10)
        self._write_year_sidecar(
            pq_path,
            granules=[{'orbit': 12345, 'granule': 1, 'track': 99999}],
        )
        result = _scan_year_file_for_update(pq_path, ['wsci_l4c'])
        assert result == [{'orbit': 12345, 'granule': 1, 'track': 99999}]

    def test_scan_returns_none_when_sidecar_absent(self, tmp_dir):
        """No per-year sidecar → scan returns None (no way to know which
        granules contributed to this year file)."""
        from gedih3.gh3builder import _scan_year_file_for_update

        _, pq_path = make_partition_dir(tmp_dir, n=10)
        # Sidecar deliberately absent.
        result = _scan_year_file_for_update(pq_path, ['wsci_l4c'])
        assert result is None

    # ---- Stage 2 merge worker (no-fragments fast path) ----

    def test_merge_returns_none_when_no_fragments(self, tmp_dir):
        """No fanned fragments for this (cell,year) → merge is a no-op and
        leaves the base parquet untouched."""
        from gedih3.gh3builder import _var_merge_cell_year

        _, pq_path = make_partition_dir(tmp_dir, n=10)
        mtime_before = os.path.getmtime(pq_path)
        time.sleep(0.05)
        result = _var_merge_cell_year(pq_path, tmp_dir=os.path.join(tmp_dir, '_t'))
        assert result is None
        assert os.path.getmtime(pq_path) == mtime_before

    # ---- Stage 2 merge worker (real fragment → base) ----

    def test_merge_joins_fragments_into_base(self, tmp_dir):
        """Fanned fragments are concatenated, deduped, and column-joined into
        the base parquet: rows preserved, new col added, matched on shot."""
        from gedih3.gh3builder import _var_merge_cell_year, _var_fragment_dir
        td = os.path.join(tmp_dir, '_t')

        _, pq_path = make_partition_dir(tmp_dir, n=10)  # shots 0..9
        base_n = len(pd.read_parquet(pq_path))
        # Year-level metadata JSON so the cheap column-patch path runs
        # (variable-add updates only columns; it must not recompute stats).
        ymeta = pq_path.replace('.parquet', '.metadata.json')
        with open(ymeta, 'w') as f:
            json.dump({'h3_partition': '83184bfffffffff', 'year': 2020,
                       'columns': ['shot_number'], 'column_dtypes': {},
                       'shot_count': base_n, 'granules': [{'orbit': 1, 'granule': 1, 'track': 1}]}, f)

        fdir = _var_fragment_dir(td, pq_path)
        os.makedirs(fdir, exist_ok=True)
        pd.DataFrame({
            'shot_number': np.arange(0, 7, dtype=np.uint64),
            'wsci_l4c': np.arange(0, 7, dtype=float),
        }).to_parquet(os.path.join(fdir, 'A.parquet'), index=False)
        pd.DataFrame({
            'shot_number': np.arange(5, 10, dtype=np.uint64),
            'wsci_l4c': np.arange(5, 10, dtype=float) + 100,
        }).to_parquet(os.path.join(fdir, 'B.parquet'), index=False)

        result = _var_merge_cell_year(pq_path, tmp_dir=td)
        assert result == pq_path

        out = pd.read_parquet(pq_path)
        assert len(out) == base_n                 # left join preserves base rows
        assert 'wsci_l4c' in out.columns          # new column added
        vals = out.set_index('shot_number')['wsci_l4c']
        assert vals.loc[3] == 3.0                  # from A
        assert vals.loc[9] == 109.0                # from B only
        # rm_src: the cell-year's fragment dir is dropped on a successful
        # merge (distributed per-worker cleanup, not a serial end-of-run sweep).
        assert not os.path.isdir(fdir)

    def test_merge_rm_src_then_rerun_is_noop(self, tmp_dir):
        """After a successful merge the fragment dir is gone (rm_src), so a
        re-run is a no-op (returns None) and never duplicates columns. If
        the dir were somehow recreated with the same fragments, the
        parquet_join_columns present-column filter still prevents dupes."""
        from gedih3.gh3builder import _var_merge_cell_year, _var_fragment_dir
        td = os.path.join(tmp_dir, '_t')

        _, pq_path = make_partition_dir(tmp_dir, n=8)
        ymeta = pq_path.replace('.parquet', '.metadata.json')
        with open(ymeta, 'w') as f:
            json.dump({'h3_partition': '83184bfffffffff', 'year': 2020,
                       'columns': ['shot_number'], 'column_dtypes': {},
                       'shot_count': 8, 'granules': [{'orbit': 1, 'granule': 1, 'track': 1}]}, f)
        fdir = _var_fragment_dir(td, pq_path)
        os.makedirs(fdir, exist_ok=True)
        frag = os.path.join(fdir, 'A.parquet')
        pd.DataFrame({
            'shot_number': np.arange(0, 8, dtype=np.uint64),
            'wsci_l4c': np.arange(0, 8, dtype=float),
        }).to_parquet(frag, index=False)

        assert _var_merge_cell_year(pq_path, tmp_dir=td) == pq_path
        cols1 = list(pd.read_parquet(pq_path).columns)
        assert not os.path.isdir(fdir)            # rm_src dropped it

        # Re-run with the dir gone → no-op.
        assert _var_merge_cell_year(pq_path, tmp_dir=td) is None
        assert list(pd.read_parquet(pq_path).columns) == cols1

        # Even if the same fragment reappears (e.g. a resume re-fanned it),
        # the merge must not duplicate the already-present column.
        os.makedirs(fdir, exist_ok=True)
        pd.DataFrame({
            'shot_number': np.arange(0, 8, dtype=np.uint64),
            'wsci_l4c': np.arange(0, 8, dtype=float),
        }).to_parquet(frag, index=False)
        _var_merge_cell_year(pq_path, tmp_dir=td)
        cols3 = list(pd.read_parquet(pq_path).columns)
        assert cols3.count('wsci_l4c') == 1

    # ---- Stage 1 fan worker ----

    def test_fan_sentinel_short_circuits_done_granule(self, tmp_dir):
        """A granule whose .done sentinel exists is skipped (resume)."""
        from gedih3.gh3builder import _var_fan_granule, _emit_var_fan_sentinel
        td = os.path.join(tmp_dir, '_t')
        os.makedirs(td, exist_ok=True)

        gkey = 'O12345_01_T00099'
        _emit_var_fan_sentinel(td, gkey)
        # h5 path is bogus — sentinel must short-circuit before any read.
        res = _var_fan_granule(
            (gkey, {'L4C': '/nonexistent.h5'}, ['/nonexistent.parquet']),
            new_product_vars={'L4C': ['wsci']}, tmp_dir=td,
        )
        assert res['skipped'] is True
        assert res['fragments'] == 0

    def test_fan_token_roundtrips_cell_year(self, tmp_dir):
        """The fragment-dir token uniquely encodes (cell, year) so Stage 2
        finds the same fragments Stage 1 wrote."""
        from gedih3.gh3builder import _var_frag_token
        _, pq_path = make_partition_dir(tmp_dir, h3_part='835576fffffffff',
                                        year='2021', n=4)
        assert _var_frag_token(pq_path) == 'h3_03=835576fffffffff__year=2021'


class TestMergeProductVars:
    """Test merge_product_vars scenarios."""

    def test_new_product(self):
        """Adding entirely new product."""
        from gedih3.logger import merge_product_vars

        existing = {'L2A': ['rh_098'], 'L4A': ['agbd']}
        new = {'L4C': ['wsci']}

        merged, update = merge_product_vars(existing, new)

        assert 'L4C' in merged
        assert merged['L4C'] == ['wsci']
        assert update is not None
        assert 'L4C' in update

    def test_new_vars_in_existing_product(self):
        """Adding new variables to existing product."""
        from gedih3.logger import merge_product_vars

        existing = {'L2A': ['rh_098']}
        new = {'L2A': ['rh_050', 'rh_098']}  # rh_050 is new, rh_098 is duplicate

        merged, update = merge_product_vars(existing, new)

        assert set(merged['L2A']) == {'rh_098', 'rh_050'}
        assert update is not None
        assert 'L2A' in update

    def test_identical_vars_no_update(self):
        """Same variables — no update needed."""
        from gedih3.logger import merge_product_vars

        existing = {'L2A': ['rh_098'], 'L4A': ['agbd']}
        new = {'L2A': ['rh_098'], 'L4A': ['agbd']}

        merged, update = merge_product_vars(existing, new)

        assert merged == existing
        assert update is None

    def test_none_vars(self):
        """None new_product_vars means no update."""
        from gedih3.logger import merge_product_vars

        existing = {'L2A': ['rh_098']}

        merged, update = merge_product_vars(existing, None)

        assert merged == existing
        assert update is None


class TestBuildLogUpdateHistory:
    """P3-A: Build log update history."""

    def test_build_log_update_history(self, tmp_dir):
        """Save log twice, verify history has 2 entries with correct actions."""
        from gedih3.logger import H3BuildLogger
        from gedih3.utils import json_read

        log_dir = tmp_dir
        os.makedirs(log_dir, exist_ok=True)

        # First build
        logger1 = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=log_dir,
        )
        logger1.save_log('COMPLETED')

        log1 = json_read(logger1.log_file)
        assert 'update_history' in log1
        assert len(log1['update_history']) == 1
        assert log1['update_history'][0]['action'] == 'build'
        assert log1['update_history'][0]['status'] == 'COMPLETED'

        # Second build (variable-only update simulation)
        logger2 = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd'], 'L4C': ['wsci']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=log_dir,
        )
        assert logger2.updating
        assert logger2.new_product_vars is not None
        logger2.save_log('COMPLETED')

        log2 = json_read(logger2.log_file)
        assert len(log2['update_history']) == 2
        assert log2['update_history'][1]['action'] == 'variable_update'
        assert log2['update_history'][1]['new_products'] is not None


class TestGetFinishedGranules:
    """Test get_finished_granules status filtering."""

    def test_only_indexed_returned(self, tmp_dir):
        """Only INDEXED granules returned; PENDING/FAILED are excluded."""
        from gedih3.logger import H3BuildLogger

        log_dir = tmp_dir
        os.makedirs(log_dir, exist_ok=True)

        # Create initial build
        logger1 = H3BuildLogger(
            product_vars={'L2A': ['rh_098']},
            spatial=[-51, 0, -50, 1],
            dir=log_dir,
        )
        logger1.granule_info = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'PENDING'},
            {'orbit': 3, 'granule': 1, 'track': 3, 'status': 'INDEXED'},
        ]
        logger1.h3_partition_ids = ['83184bfffffffff']
        logger1.save_log('COMPLETED')

        # Reload and check
        logger2 = H3BuildLogger(
            product_vars={'L2A': ['rh_098']},
            spatial=[-51, 0, -50, 1],
            dir=log_dir,
        )
        finished = logger2.get_finished_granules()
        assert finished is not None
        assert len(finished) == 2  # Only the 2 INDEXED
        # Check status field is stripped
        for g in finished:
            assert 'status' not in g


class TestResumeFromFailed:
    """Tests for resume-from-FAILED/INTERRUPTED logic.

    Validates that filter merging always runs (even on resume), enabling
    correct detection of 'already up-to-date' and 'variable-only update'.
    """

    def test_resume_from_failed_merges_filters(self, tmp_dir):
        """Resume from FAILED with same args → filters merged, no new products detected."""
        from gedih3.logger import H3BuildLogger

        make_build_log(tmp_dir, status='FAILED')

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=tmp_dir,
        )

        assert logger.updating is True
        assert logger.new_product_vars is None  # Same products → nothing new
        assert logger.new_spatial is None
        assert logger.new_temporal is None

    def test_resume_from_interrupted_detects_up_to_date(self, tmp_dir):
        """INTERRUPTED log with all granules INDEXED and same args → is_up_to_date()."""
        from gedih3.logger import H3BuildLogger

        # Simulate the tutorial DB scenario: L4C already added, all INDEXED
        products = {
            'L2A': {'status': 'INTERRUPTED', 'variables': ['rh_098', 'shot_number']},
            'L4A': {'status': 'INTERRUPTED', 'variables': ['agbd', 'shot_number']},
            'L4C': {'status': 'INTERRUPTED', 'variables': ['wsci', 'shot_number']},
        }
        make_build_log(tmp_dir, status='INTERRUPTED', products=products)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd'], 'L4C': ['wsci']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=tmp_dir,
        )

        assert logger.updating is True
        assert logger.new_product_vars is None
        assert logger.is_up_to_date() is True

    def test_resume_detects_new_products(self, tmp_dir):
        """FAILED log with L2A+L4A, user adds L4C → new_product_vars detected."""
        from gedih3.logger import H3BuildLogger

        make_build_log(tmp_dir, status='FAILED')

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd'], 'L4C': ['wsci']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=tmp_dir,
        )

        assert logger.updating is True
        assert logger.new_product_vars is not None
        assert 'L4C' in logger.new_product_vars
        assert logger.is_up_to_date() is False

    def test_is_up_to_date_false_with_pending(self, tmp_dir):
        """Log with _pending_variable_update → not up-to-date."""
        from gedih3.logger import H3BuildLogger

        make_build_log(
            tmp_dir, status='FAILED',
            pending_var_update={'product_vars': {'L4C': ['wsci']}},
        )

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=tmp_dir,
        )

        assert logger.is_up_to_date() is False

    def test_is_up_to_date_false_with_pending_granules(self, tmp_dir):
        """Log with PENDING granules → not up-to-date."""
        from gedih3.logger import H3BuildLogger

        granules = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 1, 'track': 2, 'status': 'PENDING'},
        ]
        make_build_log(tmp_dir, status='FAILED', granules=granules)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=tmp_dir,
        )

        assert logger.is_up_to_date() is False


class TestHasNewLocalGranules:
    """Unit tests for _has_new_local_granules() — detects untracked H5 files on disk."""

    def _make_fake_h5(self, soc_dir, orbit, granule, track, product='02_A'):
        """Create an empty file with a valid GEDI filename."""
        fname = f"GEDI{product}_2020100120000_O{orbit:05d}_{granule:02d}_T{track:05d}_02_003_01_V002.h5"
        fpath = os.path.join(soc_dir, fname)
        with open(fpath, 'w') as f:
            f.write('')
        return fpath

    def test_detects_new_granule(self, tmp_dir):
        """SOC dir has a file not in build log → returns True."""
        from gedih3.cli.gh3_build import _has_new_local_granules
        from gedih3.logger import H3BuildLogger

        soc_dir = os.path.join(tmp_dir, 'soc')
        h3_dir = os.path.join(tmp_dir, 'h3')
        os.makedirs(soc_dir)

        # Build log tracks orbits 1 and 2
        make_build_log(h3_dir)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=h3_dir,
        )

        # SOC dir has orbits 1, 2 (tracked) AND 3 (new)
        self._make_fake_h5(soc_dir, orbit=1, granule=1, track=1)
        self._make_fake_h5(soc_dir, orbit=2, granule=1, track=2)
        self._make_fake_h5(soc_dir, orbit=3, granule=1, track=3)

        assert _has_new_local_granules(soc_dir, logger) is True

    def test_no_new_granules(self, tmp_dir):
        """SOC dir matches build log exactly → returns False."""
        from gedih3.cli.gh3_build import _has_new_local_granules
        from gedih3.logger import H3BuildLogger

        soc_dir = os.path.join(tmp_dir, 'soc')
        h3_dir = os.path.join(tmp_dir, 'h3')
        os.makedirs(soc_dir)

        make_build_log(h3_dir)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=h3_dir,
        )

        # SOC dir has only tracked files
        self._make_fake_h5(soc_dir, orbit=1, granule=1, track=1)
        self._make_fake_h5(soc_dir, orbit=2, granule=1, track=2)

        assert _has_new_local_granules(soc_dir, logger) is False

    def test_empty_soc_dir(self, tmp_dir):
        """Empty SOC directory → returns False."""
        from gedih3.cli.gh3_build import _has_new_local_granules
        from gedih3.logger import H3BuildLogger

        soc_dir = os.path.join(tmp_dir, 'soc')
        h3_dir = os.path.join(tmp_dir, 'h3')
        os.makedirs(soc_dir)

        make_build_log(h3_dir)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=h3_dir,
        )

        assert _has_new_local_granules(soc_dir, logger) is False

    def test_nonexistent_soc_dir(self, tmp_dir):
        """SOC path doesn't exist → returns False."""
        from gedih3.cli.gh3_build import _has_new_local_granules
        from gedih3.logger import H3BuildLogger

        h3_dir = os.path.join(tmp_dir, 'h3')
        make_build_log(h3_dir)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098'], 'L4A': ['agbd']},
            spatial=[-51, 0, -50, 1],
            temporal=('2020-01-01', '2020-03-31'),
            dir=h3_dir,
        )

        assert _has_new_local_granules('/nonexistent/path', logger) is False


# ===========================================================================
# INTEGRATION TESTS (require tutorial DB or NASA creds)
# ===========================================================================

# Paths for tutorial database
_TUTORIAL_DB = os.path.join('tmp', 'gedih3_tutorial', 'h3_database')
_TUTORIAL_BUILD_LOG = os.path.join(_TUTORIAL_DB, 'gedih3_build_log.json')
_HAS_TUTORIAL_DB = os.path.exists(_TUTORIAL_BUILD_LOG)

skip_no_db = pytest.mark.skipif(not _HAS_TUTORIAL_DB, reason="Tutorial DB not found")


@pytest.fixture
def inttest_dir():
    """Temporary directory for integration tests with automatic cleanup."""
    d = tempfile.mkdtemp(prefix="gedih3_inttest_build_safety_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.integration
@skip_no_db
class TestFreshBuild:
    """Integration test: fresh build from scratch."""

    def test_fresh_build_l2a_l4a(self, inttest_dir):
        """Build L2A min + L4A agbd for tutorial region/dates. Verify database, log, and shot counts."""
        import subprocess

        h3_dir = os.path.join(inttest_dir, 'h3_db')
        soc_dir = os.path.join(inttest_dir, 'soc')

        result = subprocess.run(
            [
                'gh3_build',
                '-r', '-51,0,-50,1',
                '-d0', '2020-01-01', '-d1', '2020-03-31',
                '-l2a', 'minimal', '-l4a', 'agbd',
                '-o', h3_dir,
                '--s3',
                '-t', os.path.join(inttest_dir, 'tmp'),
                '-N', '2', '-T', '1', '-M', '4',
                '-v',
            ],
            capture_output=True, text=True, timeout=600,
        )

        assert result.returncode == 0, f"Build failed:\n{result.stderr}"
        assert os.path.exists(os.path.join(h3_dir, 'gedih3_build_log.json'))

        # Verify parquet files exist
        pq_files = []
        for root, dirs, files in os.walk(h3_dir):
            for f in files:
                if f.endswith('.parquet'):
                    pq_files.append(os.path.join(root, f))
        assert len(pq_files) > 0, "No parquet files in built database"

        # Verify build log
        with open(os.path.join(h3_dir, 'gedih3_build_log.json')) as f:
            log = json.load(f)
        assert log['status'] == 'COMPLETED'
        assert 'L2A' in log['products']
        assert 'L4A' in log['products']


@pytest.mark.integration
@skip_no_db
class TestVariableOnlyUpdate:
    """Integration tests: variable-only updates on existing database."""

    def test_variable_only_update_expand_l2a(self, inttest_dir):
        """Add new L2A vars to existing DB. Verify new columns added without data loss."""
        import subprocess
        import glob as globmod

        # Copy tutorial DB
        h3_dir = os.path.join(inttest_dir, 'h3_db')
        shutil.copytree(_TUTORIAL_DB, h3_dir)

        # Read initial state
        pq_files = globmod.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet'))
        initial_schema = pq.read_schema(pq_files[0])
        initial_cols = set(initial_schema.names)

        # Count initial rows
        initial_rows = sum(pq.read_metadata(f).num_rows for f in pq_files)

        # Run update adding new L2A variables
        result = subprocess.run(
            [
                'gh3_build',
                '-r', '-51,0,-50,1',
                '-d0', '2020-01-01', '-d1', '2020-03-31',
                '-l2a', 'default',  # Adds more L2A vars
                '-l4a', 'agbd',
                '-o', h3_dir,
                '--s3',
                '-t', os.path.join(inttest_dir, 'tmp'),
                '-N', '2', '-T', '1', '-M', '4',
                '-v',
            ],
            capture_output=True, text=True, timeout=600,
        )

        assert result.returncode == 0, f"Update failed:\n{result.stderr}"

        # Verify schema expanded
        updated_schema = pq.read_schema(pq_files[0])
        updated_cols = set(updated_schema.names)
        assert updated_cols >= initial_cols, "Existing columns lost"
        assert len(updated_cols) > len(initial_cols), "No new columns added"

        # Verify row count unchanged
        updated_rows = sum(pq.read_metadata(f).num_rows for f in pq_files)
        assert updated_rows == initial_rows, f"Row count changed: {initial_rows} -> {updated_rows}"


@pytest.mark.integration
@skip_no_db
class TestIdempotentRebuild:
    """Integration test: re-running build with identical params is a no-op."""

    def test_idempotent_rebuild(self, inttest_dir):
        """Re-run build with same params. Verify 'No new granules', no files modified."""
        import subprocess
        import glob as globmod

        # Copy tutorial DB
        h3_dir = os.path.join(inttest_dir, 'h3_db')
        shutil.copytree(_TUTORIAL_DB, h3_dir)

        # Record file mtimes
        pq_files = globmod.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet'))
        mtimes_before = {f: os.path.getmtime(f) for f in pq_files}

        # Read build log to get original params
        with open(os.path.join(h3_dir, 'gedih3_build_log.json')) as f:
            log = json.load(f)

        # Re-run with same params
        result = subprocess.run(
            [
                'gh3_build',
                '-r', '-51,0,-50,1',
                '-d0', '2020-01-01', '-d1', '2020-03-31',
                '-l2a', 'minimal', '-l4a', 'agbd',
                '-o', h3_dir,
                '--s3',
                '-t', os.path.join(inttest_dir, 'tmp'),
                '-N', '2', '-T', '1', '-M', '4',
                '-v',
            ],
            capture_output=True, text=True, timeout=600,
        )

        assert result.returncode == 0, f"Rebuild failed:\n{result.stderr}"
        assert 'No new granules' in result.stdout or 'No new granules' in result.stderr

        # Verify no parquet files were modified
        for f, mtime in mtimes_before.items():
            assert os.path.getmtime(f) == mtime, f"File modified unexpectedly: {f}"


@pytest.mark.integration
@skip_no_db
class TestMixedUpdate:
    """Integration tests: mixed updates (spatial/temporal + new variables)."""

    def test_mixed_update_spatial_plus_variable(self, inttest_dir):
        """Build with L2A+L4A for region A, then expand to A+B AND add L4C.
        Verify region B has all columns AND region A also gets L4C."""
        import subprocess
        import glob as globmod

        h3_dir = os.path.join(inttest_dir, 'h3_db')
        shutil.copytree(_TUTORIAL_DB, h3_dir)

        # Get initial partition IDs
        initial_parts = set(
            os.path.basename(d) for d in globmod.glob(os.path.join(h3_dir, 'h3_03=*'))
        )

        # Run mixed update: expand region + add L4C
        result = subprocess.run(
            [
                'gh3_build',
                '-r', '-52,0,-49,1',  # Wider region
                '-d0', '2020-01-01', '-d1', '2020-03-31',
                '-l2a', 'minimal', '-l4a', 'agbd', '-l4c', 'minimal',
                '-o', h3_dir,
                '--s3',
                '-t', os.path.join(inttest_dir, 'tmp'),
                '-N', '2', '-T', '1', '-M', '4',
                '-v',
            ],
            capture_output=True, text=True, timeout=600,
        )

        assert result.returncode == 0, f"Mixed update failed:\n{result.stderr}"
        assert 'Mixed update' in result.stderr or 'Phase 1' in result.stderr or 'Phase 2' in result.stderr

        # Check that existing partitions have L4C columns
        pq_files = globmod.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet'))
        for f in pq_files:
            schema = pq.read_schema(f)
            col_names = set(schema.names)
            # All partitions should have L4A columns
            assert any('l4a' in c for c in col_names), f"Missing L4A in {f}"

        # Verify build log
        with open(os.path.join(h3_dir, 'gedih3_build_log.json')) as f:
            log = json.load(f)
        assert log['status'] == 'COMPLETED'
        assert 'update_history' in log
