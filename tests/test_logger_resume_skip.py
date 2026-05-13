"""Tests for H3BuildLogger resume-skip behavior across partial build state.

Regression coverage for the case where the very first partition-write phase
was killed before persisting ``h3_partition_ids`` to the build log. On the
next resume:

  * ``_reconcile_granules_from_disk`` correctly flips matching granules to
    ``status='INDEXED'`` in ``granule_info``.
  * The skip filter then queries ``H3BuildLogger.get_finished_granules()`` to
    build ``stage1_skip``.

Previously the guard ``_adding_h3_parts`` returned True whenever
``h3_partition_ids`` was absent, which short-circuited ``get_finished_granules``
to ``None`` and caused the resume to re-process every already-INDEXED granule
(8.8% wasted compute on the v3 rebuild). Fix: ``_adding_h3_parts`` checks
``new_spatial`` first — if no expansion is requested, no partitions are being
added, regardless of whether the attribute was persisted.
"""
import json
import os

from gedih3.logger import H3BuildLogger


def _write_log_no_partition_ids(log_dir: str, granules: list[dict]) -> str:
    """Write a build log that intentionally omits the ``h3_partition_ids``
    key — the file shape produced when stage 1 was killed before persisting
    its partition set."""
    os.makedirs(log_dir, exist_ok=True)
    log = {
        'metadata': {'package_version': 'test'},
        'gedi_version': 2,
        'status': 'PROCESSING',
        'h3_resolution_level': 12,
        'h3_partition_level': 3,
        'spatial_filter': '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-50,0],[-50,1],[-51,1],[-51,0],[-50,0]]]}}]}',
        'temporal_filter': ['2020-01-01', '2020-03-31'],
        'products': {
            'L2A': {'status': 'PROCESSING', 'variables': ['rh_098', 'shot_number']},
        },
        'granules': granules,
        # 'h3_partition_ids' INTENTIONALLY OMITTED — the bug condition.
        'h3_columns': ['rh_098_l2a', 'geometry', 'datetime'],
        'date_range': ['2020-01-11', '2020-03-15'],
    }
    path = os.path.join(log_dir, 'gedih3_build_log.json')
    with open(path, 'w') as f:
        json.dump(log, f)
    return path


class TestAddingH3PartsGuard:
    """Direct unit tests for the _adding_h3_parts guard logic — covers all
    four (h3_partition_ids present|absent) × (new_spatial None|set) cases."""

    def _logger(self, tmp_dir, has_partition_ids: bool):
        granules = [{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'}]
        if has_partition_ids:
            from conftest import make_build_log
            make_build_log(tmp_dir, status='PROCESSING', granules=granules,
                           h3_partition_ids=['838041fffffffff'])
        else:
            _write_log_no_partition_ids(tmp_dir, granules)
        return H3BuildLogger(product_vars=None, dir=tmp_dir)

    def test_no_partition_ids_no_expansion_does_not_add(self, tmp_dir):
        """Resume after a killed first write: no h3_partition_ids on disk,
        no new spatial filter requested. Must not claim we are adding."""
        h3_logger = self._logger(tmp_dir, has_partition_ids=False)
        assert not hasattr(h3_logger, 'h3_partition_ids')
        h3_logger.new_spatial = None
        assert h3_logger._adding_h3_parts() is False

    def test_no_partition_ids_with_expansion_does_add(self, tmp_dir):
        """Spatial expansion requested but no recorded partition set — be
        conservative and treat all new cells as added."""
        h3_logger = self._logger(tmp_dir, has_partition_ids=False)
        h3_logger.new_spatial = (
            '{"type":"FeatureCollection","features":[{"type":"Feature",'
            '"properties":{},"geometry":{"type":"Polygon","coordinates":'
            '[[[-40,0],[-40,1],[-41,1],[-41,0],[-40,0]]]}}]}'
        )
        assert h3_logger._adding_h3_parts() is True

    def test_with_partition_ids_no_expansion_does_not_add(self, tmp_dir):
        h3_logger = self._logger(tmp_dir, has_partition_ids=True)
        assert hasattr(h3_logger, 'h3_partition_ids')
        h3_logger.new_spatial = None
        assert h3_logger._adding_h3_parts() is False


class TestGetFinishedGranulesAfterKilledFirstWrite:
    """End-to-end: the resume sequence (reconcile → get_finished_granules)
    must yield the INDEXED granules even when the build log lacks
    h3_partition_ids (the killed-first-write fingerprint)."""

    def test_returns_indexed_granules_when_partition_ids_missing(self, tmp_dir):
        granules = [
            {'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'},
            {'orbit': 2, 'granule': 2, 'track': 2, 'status': 'INDEXED'},
            {'orbit': 3, 'granule': 3, 'track': 3, 'status': 'PENDING'},
        ]
        _write_log_no_partition_ids(tmp_dir, granules)
        h3_logger = H3BuildLogger(product_vars=None, dir=tmp_dir)

        # Simulate a same-plan resume: no new spatial/temporal/products.
        h3_logger.new_spatial = None
        h3_logger.new_temporal = None
        h3_logger.new_product_vars = None

        skip = h3_logger.get_finished_granules()
        assert skip is not None, (
            "skip list collapsed to None — _adding_h3_parts is still gating "
            "the lookup on the missing h3_partition_ids attribute"
        )
        skip_ids = {(g['orbit'], g['granule'], g['track']) for g in skip}
        assert skip_ids == {(1, 1, 1), (2, 2, 2)}, (
            f"expected only INDEXED granules in skip list, got {skip_ids}"
        )

    def test_returns_none_on_spatial_expansion_without_partition_ids(self, tmp_dir):
        """Expansion requested but no recorded partition set — the guard
        correctly returns None (don't skip anything; re-examine all)."""
        granules = [{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'}]
        _write_log_no_partition_ids(tmp_dir, granules)
        h3_logger = H3BuildLogger(product_vars=None, dir=tmp_dir)

        h3_logger.new_spatial = (
            '{"type":"FeatureCollection","features":[{"type":"Feature",'
            '"properties":{},"geometry":{"type":"Polygon","coordinates":'
            '[[[-40,0],[-40,1],[-41,1],[-41,0],[-40,0]]]}}]}'
        )
        h3_logger.new_temporal = None
        h3_logger.new_product_vars = None

        assert h3_logger.get_finished_granules() is None
