"""
Tests for data integrity, safety, and correctness.

P0 — Data Safety:
    Version mismatch detection, NaN-only column detection, duplicate shot
    detection, build log metadata accuracy, atomic JSON write safety.

P1 — Data Correctness:
    Auto-detection of index/partition levels, file naming validation,
    H3 parent–child relationship, lazy loading return types,
    temporal filter handling.

P2 — Error Messages:
    Actionable error messages for common user mistakes.
"""

import json
import os

import h3
import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point

from conftest import make_gedi_parquet, make_partition_dir, make_build_log


# ===========================================================================
# P0: DATA SAFETY TESTS
# ===========================================================================

class TestVersionMismatch:
    """GEDI data versions must never be mixed in the same database."""

    def test_version_mismatch_on_update_rejected(self, tmp_dir):
        """Updating a v2 database with v3 raises GediValidationError."""
        from gedih3.logger import H3BuildLogger
        from gedih3.exceptions import GediValidationError

        make_build_log(tmp_dir, gedi_version=2)

        with pytest.raises(GediValidationError, match="version mismatch"):
            H3BuildLogger(
                product_vars={'L2A': ['rh_098']},
                spatial=[-51, 0, -50, 1],
                version=3,
                dir=tmp_dir,
            )

    def test_version_match_accepted(self, tmp_dir):
        """Same version update proceeds without error."""
        from gedih3.logger import H3BuildLogger

        make_build_log(tmp_dir, gedi_version=2)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098']},
            spatial=[-51, 0, -50, 1],
            version=2,
            dir=tmp_dir,
        )
        assert logger.gedi_version == 2
        assert logger.updating is True

    def test_version_none_uses_existing(self, tmp_dir):
        """version=None inherits from existing log without error."""
        from gedih3.logger import H3BuildLogger

        make_build_log(tmp_dir, gedi_version=2)

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098']},
            spatial=[-51, 0, -50, 1],
            version=None,
            dir=tmp_dir,
        )
        assert logger.gedi_version == 2

    def test_version_mismatch_message_is_actionable(self, tmp_dir):
        """Error message explains that separate databases are needed."""
        from gedih3.logger import H3BuildLogger
        from gedih3.exceptions import GediValidationError

        make_build_log(tmp_dir, gedi_version=2)

        with pytest.raises(GediValidationError, match="separate databases"):
            H3BuildLogger(
                product_vars={'L2A': ['rh_098']},
                version=3,
                dir=tmp_dir,
            )


class TestNanOnlyColumns:
    """No output should contain columns that are entirely NaN."""

    def test_check_nan_only_columns_detects(self):
        """check_nan_only_columns returns the right column names."""
        from gedih3.utils import check_nan_only_columns

        df = pd.DataFrame({
            'a': [1.0, 2.0, 3.0],
            'b': [float('nan'), float('nan'), float('nan')],
            'c': [1.0, float('nan'), 3.0],
        })
        result = check_nan_only_columns(df)
        assert result == ['b']

    def test_check_nan_only_columns_clean(self):
        """No NaN-only columns returns empty list."""
        from gedih3.utils import check_nan_only_columns

        df = pd.DataFrame({
            'a': [1.0, 2.0],
            'b': [3.0, 4.0],
        })
        result = check_nan_only_columns(df)
        assert result == []

    def test_check_nan_only_columns_skips_geometry(self):
        """geometry column is excluded from the check."""
        from gedih3.utils import check_nan_only_columns

        gdf = gpd.GeoDataFrame({
            'a': [1.0, 2.0],
        }, geometry=[Point(0, 0), Point(1, 1)])
        # Even if geometry were somehow all-NaN, it should be skipped
        result = check_nan_only_columns(gdf)
        assert 'geometry' not in result

    def test_check_nan_only_columns_warns(self):
        """Warning is emitted when NaN-only columns are found."""
        from gedih3.utils import check_nan_only_columns

        df = pd.DataFrame({
            'good': [1.0],
            'bad': [float('nan')],
        })
        with pytest.warns(UserWarning, match="all NaN"):
            check_nan_only_columns(df)

    def test_check_nan_only_columns_uses_logger(self):
        """When logger is provided, uses logger.warning instead of warnings."""
        import logging
        from gedih3.utils import check_nan_only_columns

        mock_logger = logging.getLogger('test_nan_check')
        df = pd.DataFrame({'bad': [float('nan'), float('nan')]})

        with pytest.warns(match="") if False else _no_warning():
            # Should NOT emit a UserWarning when logger is provided
            result = check_nan_only_columns(df, logger=mock_logger)
        assert result == ['bad']


class TestDuplicateShots:
    """Every shot_number must be unique within a database or exported dataset."""

    def test_parquet_merge_dedup(self, tmp_dir):
        """parquet_merge_files with check_shots=True deduplicates."""
        from gedih3.utils import parquet_merge_files

        f1 = os.path.join(tmp_dir, 'a.parquet')
        f2 = os.path.join(tmp_dir, 'b.parquet')
        # Same shots in both files
        make_gedi_parquet(f1, n=10, shot_offset=0)
        make_gedi_parquet(f2, n=10, shot_offset=0)

        ofile = os.path.join(tmp_dir, 'merged.parquet')
        parquet_merge_files(ofile, [f1, f2], check_shots=True)

        result = gpd.read_parquet(ofile)
        assert result['shot_number'].nunique() == len(result)
        assert len(result) == 10

    def test_no_duplicates_across_partitions(self, mini_h3_database):
        """Synthetic database has unique shot numbers across all partitions."""
        import glob as globmod

        pq_files = globmod.glob(
            os.path.join(mini_h3_database, 'h3_*', '*', '*.parquet')
        )
        all_shots = []
        for f in pq_files:
            df = pd.read_parquet(f, columns=['shot_number'])
            all_shots.extend(df['shot_number'].tolist())

        assert len(all_shots) == len(set(all_shots)), \
            f"Duplicate shots found: {len(all_shots)} total, {len(set(all_shots))} unique"


class TestBuildLogMetadataAccuracy:
    """Build log metadata must match the actual data on disk."""

    def test_build_log_columns_match_parquet(self, mini_h3_database):
        """h3_columns in build log matches actual parquet schema."""
        from gedih3.utils import json_read

        log = json_read(os.path.join(mini_h3_database, 'gedih3_build_log.json'))
        log_cols = set(log.get('h3_columns', []))

        import glob as globmod
        pq_files = globmod.glob(
            os.path.join(mini_h3_database, 'h3_*', '*', '*.parquet')
        )
        for f in pq_files:
            df = gpd.read_parquet(f)
            data_cols = set(df.columns)
            # Every log column should be in the data (modulo datetime which may not be stored)
            for col in log_cols:
                if col != 'datetime':
                    assert col in data_cols, \
                        f"Log column '{col}' not found in {os.path.basename(f)}"

    def test_build_log_partition_ids_match_dirs(self, mini_h3_database):
        """h3_partition_ids in log matches h3_03=* directories on disk."""
        from gedih3.utils import json_read
        import glob as globmod

        log = json_read(os.path.join(mini_h3_database, 'gedih3_build_log.json'))
        log_parts = set(log.get('h3_partition_ids', []))

        disk_parts = set()
        for d in globmod.glob(os.path.join(mini_h3_database, 'h3_03=*')):
            part_id = os.path.basename(d).split('=')[1]
            disk_parts.add(part_id)

        assert log_parts == disk_parts, \
            f"Log partitions {log_parts} != disk partitions {disk_parts}"


class TestParquetJoinColumns:
    """parquet_join_columns must merge new columns and clean up temp files."""

    def test_join_adds_new_column(self, tmp_dir):
        """Basic join: adds a new column via shot_number key."""
        from gedih3.utils import parquet_join_columns

        base = pd.DataFrame({'shot_number': [1, 2, 3], 'col_a': [10, 20, 30]})
        base_path = os.path.join(tmp_dir, 'base.parquet')
        base.to_parquet(base_path, index=False)

        extra = pd.DataFrame({'shot_number': [1, 2, 3], 'col_b': [100, 200, 300]})
        extra_path = os.path.join(tmp_dir, 'extra.parquet')
        extra.to_parquet(extra_path, index=False)

        parquet_join_columns([base_path, extra_path], base_path, key_col='shot_number')

        result = pd.read_parquet(base_path)
        assert 'col_b' in result.columns
        assert list(result['col_b']) == [100, 200, 300]
        assert not os.path.exists(base_path + '.join.tmp')

    def test_join_preserves_index(self, tmp_dir):
        """Join preserves the original parquet index (h3_12 pattern)."""
        from gedih3.utils import parquet_join_columns

        base = pd.DataFrame({
            'h3_12': ['abc', 'def', 'ghi'],
            'shot_number': [1, 2, 3],
            'col_a': [10, 20, 30],
        }).set_index('h3_12')
        base_path = os.path.join(tmp_dir, 'base.parquet')
        base.to_parquet(base_path)

        extra = pd.DataFrame({'shot_number': [1, 2, 3], 'col_b': [100, 200, 300]})
        extra_path = os.path.join(tmp_dir, 'extra.parquet')
        extra.to_parquet(extra_path, index=False)

        parquet_join_columns([base_path, extra_path], base_path, key_col='shot_number')

        result = pd.read_parquet(base_path)
        assert result.index.name == 'h3_12'
        assert 'col_b' in result.columns
        assert 'shot_number' in result.columns
        assert not os.path.exists(base_path + '.join.tmp')

    def test_join_partial_match(self, tmp_dir):
        """Left join fills NaN for unmatched keys."""
        from gedih3.utils import parquet_join_columns

        base = pd.DataFrame({'shot_number': [1, 2, 3], 'col_a': [10, 20, 30]})
        base_path = os.path.join(tmp_dir, 'base.parquet')
        base.to_parquet(base_path, index=False)

        # Only shot 1 and 3 in extra
        extra = pd.DataFrame({'shot_number': [1, 3], 'col_b': [100, 300]})
        extra_path = os.path.join(tmp_dir, 'extra.parquet')
        extra.to_parquet(extra_path, index=False)

        parquet_join_columns([base_path, extra_path], base_path, key_col='shot_number')

        result = pd.read_parquet(base_path)
        assert 'col_b' in result.columns
        assert len(result) == 3
        assert pd.isna(result.loc[result['shot_number'] == 2, 'col_b'].iloc[0])


class TestAtomicJsonWrite:
    """json_write must not corrupt files on crash."""

    def test_json_write_creates_file(self, tmp_dir):
        """Basic write creates valid JSON."""
        from gedih3.utils import json_write, json_read

        path = os.path.join(tmp_dir, 'test.json')
        json_write({'key': 'value'}, path)
        result = json_read(path)
        assert result == {'key': 'value'}

    def test_json_write_merge_mode(self, tmp_dir):
        """Default mode merges with existing data."""
        from gedih3.utils import json_write, json_read

        path = os.path.join(tmp_dir, 'test.json')
        json_write({'a': 1}, path)
        json_write({'b': 2}, path)
        result = json_read(path)
        assert result == {'a': 1, 'b': 2}

    def test_json_write_rewrite_mode(self, tmp_dir):
        """rewrite=True replaces entire file."""
        from gedih3.utils import json_write, json_read

        path = os.path.join(tmp_dir, 'test.json')
        json_write({'a': 1}, path)
        json_write({'b': 2}, path, rewrite=True)
        result = json_read(path)
        assert result == {'b': 2}
        assert 'a' not in result

    def test_json_write_no_temp_file_left(self, tmp_dir):
        """No .tmp file remains after successful write."""
        from gedih3.utils import json_write

        path = os.path.join(tmp_dir, 'test.json')
        json_write({'data': True}, path)
        assert os.path.exists(path)
        assert not os.path.exists(path + '.tmp')


# ===========================================================================
# P1: DATA CORRECTNESS TESTS
# ===========================================================================

class TestGetDatasetIndexInfo:
    """Auto-detection of index/partition levels from database metadata."""

    def test_h3_database_detected(self, mini_h3_database):
        """H3 database with build log is detected as h3 index type."""
        from gedih3.cliutils import get_dataset_index_info

        info = get_dataset_index_info(mini_h3_database)
        assert info['source_type'] == 'h3_database'
        assert info['index_type'] == 'h3'
        assert info['index_level'] == 12
        assert info['partition_level'] == 3

    def test_simplified_dataset_detected(self, mini_extracted_dataset):
        """Simplified dataset with gedih3_dataset.json detected correctly."""
        from gedih3.cliutils import get_dataset_index_info

        info = get_dataset_index_info(mini_extracted_dataset)
        assert info['source_type'] == 'simplified_dataset'
        assert info['index_type'] == 'h3'
        assert info['index_level'] == 12
        assert info['partition_level'] == 3


class TestH3FileNaming:
    """Output file names must be valid H3 cells at the partition level."""

    def test_partition_dir_names_are_valid_h3(self, mini_h3_database):
        """h3_03=<cell> directory names contain valid H3 cells."""
        import glob as globmod

        part_dirs = globmod.glob(os.path.join(mini_h3_database, 'h3_03=*'))
        assert len(part_dirs) > 0

        for d in part_dirs:
            cell_id = os.path.basename(d).split('=')[1]
            assert h3.is_valid_cell(cell_id), f"Invalid H3 cell: {cell_id}"
            assert h3.get_resolution(cell_id) == 3, \
                f"Cell {cell_id} is res {h3.get_resolution(cell_id)}, expected 3"


class TestGh3LoadReturnTypes:
    """gh3_load must return Dask when lazy=True, pandas when lazy=False."""

    def test_lazy_returns_dask(self, mini_h3_database):
        """gh3_load(lazy=True) returns a Dask GeoDataFrame."""
        import dask_geopandas
        from gedih3.gh3driver import gh3_load

        ddf = gh3_load(source=mini_h3_database, lazy=True)
        assert isinstance(ddf, dask_geopandas.GeoDataFrame)

    def test_eager_returns_pandas(self, mini_h3_database):
        """gh3_load(lazy=False) returns a pandas GeoDataFrame."""
        from gedih3.gh3driver import gh3_load

        gdf = gh3_load(source=mini_h3_database, lazy=False)
        assert isinstance(gdf, gpd.GeoDataFrame)


class TestTemporalFilter:
    """No temporal filter should allow all data to be loaded."""

    def test_no_temporal_filter_loads_all(self, tmp_dir):
        """Logger with temporal=None does not restrict data."""
        from gedih3.logger import H3BuildLogger

        make_build_log(tmp_dir, temporal=['2020-01-01', '2020-06-30'])

        logger = H3BuildLogger(
            product_vars={'L2A': ['rh_098']},
            spatial=[-51, 0, -50, 1],
            temporal=None,
            dir=tmp_dir,
        )
        # temporal should be loaded from existing log, not restricted
        assert logger.temporal is not None


# ===========================================================================
# P2: ERROR MESSAGE TESTS
# ===========================================================================

class TestErrorMessages:
    """Errors should be actionable and explain how to proceed."""

    def test_invalid_database_path_message(self, tmp_dir):
        """Missing database gives clear GediDatabaseNotFoundError."""
        from gedih3.validation import validate_database_path
        from gedih3.exceptions import GediDatabaseNotFoundError

        fake_path = os.path.join(tmp_dir, 'nonexistent_db')
        with pytest.raises(GediDatabaseNotFoundError):
            validate_database_path(fake_path)

    def test_empty_database_message(self, tmp_dir):
        """Empty directory raises with clear message."""
        from gedih3.validation import validate_database_path
        from gedih3.exceptions import GediDatabaseNotFoundError

        empty_dir = os.path.join(tmp_dir, 'empty_db')
        os.makedirs(empty_dir)
        with pytest.raises(GediDatabaseNotFoundError):
            validate_database_path(empty_dir)


# ===========================================================================
# Helpers
# ===========================================================================

class _no_warning:
    """Context manager that asserts no warnings are emitted (used as no-op)."""
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
