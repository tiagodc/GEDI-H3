"""
Tests for merge_build_logs function using synthetic build log data.

Verifies merging of two build logs with proper metadata aggregation.
"""

import json
import os
import tempfile
import shutil

import pytest

from gedih3.gh3builder import merge_build_logs


# =============================================================================
# Fixtures
# =============================================================================

def _make_build_log(
    gedi_version=2,
    h3_resolution=12,
    h3_partition=3,
    status='COMPLETED',
    granules=None,
    h3_columns=None,
    h3_partition_ids=None,
    date_range=None,
    products=None,
):
    """Create a synthetic build log dict."""
    return {
        'metadata': {'package_version': '0.10.10'},
        'gedi_version': gedi_version,
        'h3_resolution_level': h3_resolution,
        'h3_partition_level': h3_partition,
        'status': status,
        'previous_status': None,
        'source_mode': 'download',
        'last_modified': '2026-01-01T00:00:00',
        'build_duration_seconds': 100.0,
        'spatial_filter': {
            'type': 'FeatureCollection',
            'features': [{
                'type': 'Feature',
                'properties': {},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[-51, 0], [-51, 1], [-50, 1], [-50, 0], [-51, 0]]]
                },
                'bbox': [-51, 0, -50, 1]
            }],
            'bbox': [-51, 0, -50, 1]
        },
        'temporal_filter': ['2020-01-01', '2020-06-30'],
        'products': products or {
            'L2A': {
                'status': 'COMPLETED',
                'last_modified': '2026-01-01T00:00:00',
                'variables': ['rh', 'quality_flag', 'shot_number'],
            },
            'L4A': {
                'status': 'COMPLETED',
                'last_modified': '2026-01-01T00:00:00',
                'variables': ['agbd', 'shot_number'],
            },
        },
        'granules': granules or [
            {'orbit': 1000, 'granule': 1, 'track': 100, 'status': 'INDEXED'},
            {'orbit': 1001, 'granule': 1, 'track': 101, 'status': 'INDEXED'},
        ],
        'h3_columns': h3_columns or [
            'agbd_l4a', 'rh_098_l2a', 'quality_flag_l2a', 'datetime', 'geometry',
        ],
        'h3_partition_ids': h3_partition_ids or [
            '838041fffffffff', '83804cfffffffff',
        ],
        'date_range': date_range or ['2020-01-11', '2020-03-15'],
    }



# =============================================================================
# Tests
# =============================================================================

class TestMergeBuildLogs:

    def test_basic_merge(self, tmp_dir):
        """Two logs with different granules and partitions merge correctly."""
        log1 = _make_build_log(
            granules=[
                {'orbit': 1000, 'granule': 1, 'track': 100, 'status': 'INDEXED'},
            ],
            h3_partition_ids=['838041fffffffff'],
            date_range=['2020-01-11', '2020-02-15'],
        )
        log2 = _make_build_log(
            granules=[
                {'orbit': 2000, 'granule': 1, 'track': 200, 'status': 'INDEXED'},
            ],
            h3_partition_ids=['83804cfffffffff'],
            date_range=['2020-02-10', '2020-03-15'],
        )

        path1 = os.path.join(tmp_dir, 'log1.json')
        path2 = os.path.join(tmp_dir, 'log2.json')
        out_path = os.path.join(tmp_dir, 'merged.json')

        with open(path1, 'w') as f:
            json.dump(log1, f)
        with open(path2, 'w') as f:
            json.dump(log2, f)

        merged = merge_build_logs(path1, path2, out_path)

        # Output file created
        assert os.path.exists(out_path)

        # Structure checks
        assert isinstance(merged, dict)
        assert merged['status'] == 'COMPLETED'
        assert merged['gedi_version'] == 2
        assert merged['h3_resolution_level'] == 12
        assert merged['h3_partition_level'] == 3

        # Granules merged (union)
        granules = merged.get('granules', [])
        orbits = {g['orbit'] for g in granules}
        assert 1000 in orbits
        assert 2000 in orbits

        # Partitions merged (union)
        parts = merged.get('h3_partition_ids', [])
        assert '838041fffffffff' in parts
        assert '83804cfffffffff' in parts

        # Date range expanded
        dates = merged.get('date_range', [])
        assert dates[0] <= '2020-01-11'
        assert dates[1] >= '2020-03-15'

    def test_columns_merged(self, tmp_dir):
        """Columns from both logs are combined."""
        log1 = _make_build_log(h3_columns=['agbd_l4a', 'datetime'])
        log2 = _make_build_log(h3_columns=['rh_098_l2a', 'datetime', 'new_col'])

        path1 = os.path.join(tmp_dir, 'log1.json')
        path2 = os.path.join(tmp_dir, 'log2.json')
        out_path = os.path.join(tmp_dir, 'merged.json')

        with open(path1, 'w') as f:
            json.dump(log1, f)
        with open(path2, 'w') as f:
            json.dump(log2, f)

        merged = merge_build_logs(path1, path2, out_path)
        cols = merged.get('h3_columns', [])
        assert 'agbd_l4a' in cols
        assert 'rh_098_l2a' in cols
        assert 'new_col' in cols
        assert 'datetime' in cols

    def test_incompatible_versions_raises(self, tmp_dir):
        """Merging logs with different gedi_version should raise."""
        log1 = _make_build_log(gedi_version=2)
        log2 = _make_build_log(gedi_version=3)

        path1 = os.path.join(tmp_dir, 'log1.json')
        path2 = os.path.join(tmp_dir, 'log2.json')
        out_path = os.path.join(tmp_dir, 'merged.json')

        with open(path1, 'w') as f:
            json.dump(log1, f)
        with open(path2, 'w') as f:
            json.dump(log2, f)

        with pytest.raises((ValueError, Exception)):
            merge_build_logs(path1, path2, out_path)

    def test_incompatible_resolutions_raises(self, tmp_dir):
        """Merging logs with different H3 resolution should raise."""
        log1 = _make_build_log(h3_resolution=12)
        log2 = _make_build_log(h3_resolution=9)

        path1 = os.path.join(tmp_dir, 'log1.json')
        path2 = os.path.join(tmp_dir, 'log2.json')
        out_path = os.path.join(tmp_dir, 'merged.json')

        with open(path1, 'w') as f:
            json.dump(log1, f)
        with open(path2, 'w') as f:
            json.dump(log2, f)

        with pytest.raises((ValueError, Exception)):
            merge_build_logs(path1, path2, out_path)

    def test_missing_file_raises(self, tmp_dir):
        """Missing input file should raise."""
        log1 = _make_build_log()
        path1 = os.path.join(tmp_dir, 'log1.json')
        with open(path1, 'w') as f:
            json.dump(log1, f)

        with pytest.raises((FileNotFoundError, Exception)):
            merge_build_logs(path1, '/nonexistent/log.json', os.path.join(tmp_dir, 'out.json'))

    def test_output_is_valid_json(self, tmp_dir):
        """Output file is valid JSON."""
        log1 = _make_build_log()
        log2 = _make_build_log(
            granules=[{'orbit': 3000, 'granule': 1, 'track': 300, 'status': 'INDEXED'}],
            h3_partition_ids=['83806afffffffff'],
        )

        path1 = os.path.join(tmp_dir, 'log1.json')
        path2 = os.path.join(tmp_dir, 'log2.json')
        out_path = os.path.join(tmp_dir, 'merged.json')

        with open(path1, 'w') as f:
            json.dump(log1, f)
        with open(path2, 'w') as f:
            json.dump(log2, f)

        merge_build_logs(path1, path2, out_path)

        with open(out_path, 'r') as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert data['status'] == 'COMPLETED'
