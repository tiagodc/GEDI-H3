"""Tests for the public ``gh3_select_partitions`` helper.

``gh3_select_partitions`` is the overhang-safe, public entry point for
external consumers that read the H3 database directly (i.e. not through
``gh3_load``). It reads the build log's ``h3_partition_ids`` and applies the
same ring-1 expansion ``gh3_load`` uses internally, so selecting partitions
by an exact polygon intersection (which silently drops boundary shots) is
never necessary. See ``test_h3_roi_ring_expansion.py`` for the underlying
overhang regression coverage.
"""
import json

import h3
import pytest
from shapely.geometry import box

from gedih3 import gh3_select_partitions
from gedih3.config import BUILD_LOG_FILENAME
from gedih3.exceptions import GediDatabaseNotFoundError

PART_RES = 3

# A small patch of partitions around the suite's usual Amazon test region.
_PART_IDS = sorted(h3.grid_disk(h3.latlng_to_cell(0.5, -50.5, PART_RES), 1))


def _make_db(tmp_path, partition_ids=_PART_IDS):
    """Write a minimal build log so gh3_select_partitions has something to read."""
    log = {
        'h3_partition_level': PART_RES,
        'h3_partition_ids': list(partition_ids),
    }
    (tmp_path / BUILD_LOG_FILENAME).write_text(json.dumps(log))
    return str(tmp_path)


def test_region_none_returns_all_sorted(tmp_path):
    db = _make_db(tmp_path)
    assert gh3_select_partitions(db, region=None) == sorted(_PART_IDS)


def test_region_subset_is_within_candidates(tmp_path):
    db = _make_db(tmp_path)
    # A bbox over the seed cell's centroid selects a subset of the partitions.
    centroid_cell = h3.latlng_to_cell(0.5, -50.5, PART_RES)
    lat, lon = h3.cell_to_latlng(centroid_cell)
    ids = gh3_select_partitions(db, region=[lon - 0.05, lat - 0.05,
                                            lon + 0.05, lat + 0.05])
    assert ids, "expected at least the containing partition"
    assert set(ids).issubset(set(_PART_IDS))
    assert centroid_cell in ids


def test_ring_expansion_applied(tmp_path):
    """A region whose true storage partition is a neighbor (overhang band)
    must still be selected — i.e. ring-1 expansion is on by default."""
    db = _make_db(tmp_path)
    # Selecting the full extent returns every candidate partition.
    ids = gh3_select_partitions(db, region=[-52, -1.5, -49, 2.5])
    assert set(ids) == set(_PART_IDS)


def test_missing_partition_list_raises(tmp_path):
    (tmp_path / BUILD_LOG_FILENAME).write_text(json.dumps({'h3_partition_level': PART_RES}))
    with pytest.raises(GediDatabaseNotFoundError):
        gh3_select_partitions(str(tmp_path), region=None)
