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

import pytest

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


def _box(x0, y0, x1, y1):
    return json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
                       "geometry": {"type": "Polygon",
                                    "coordinates": [[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]]}}]})


class TestInterruptedExpansionResume:
    """#42 case 2: the first save_log of an expansion stores the union region,
    so a plain resume after a kill used to see no expansion at all: Stage 1
    read the whole scope while skipping every INDEXED granule, whose rows in
    the added area were then never extracted."""
    A = _box(-51, 0, -50, 1)
    B = _box(-51, 0, -44, 1)        # A widened eastwards: adds partitions

    def _db(self, tmp_dir):
        from conftest import make_build_log
        from gedih3.h3utils import intersect_h3_geometries
        from gedih3.utils import parse_spatial
        make_build_log(tmp_dir, granules=[{'orbit': 1, 'granule': 1, 'track': 1, 'status': 'INDEXED'}],
                       spatial=self.A,
                       h3_partition_ids=sorted(set(intersect_h3_geometries(parse_spatial(self.A), res=3))))

    def _interrupted(self, tmp_dir, status='PARTITIONING', **request):
        self._db(tmp_dir)
        run = H3BuildLogger(product_vars=None, dir=tmp_dir, **request)
        assert run.get_finished_granules() is None and not run.stage1_full_scope()
        run.save_log(status)      # the run dies after this save
        return run

    def test_plain_resume_keeps_the_added_area(self, tmp_dir):
        run = self._interrupted(tmp_dir, spatial=self.B)

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)

        added = run.get_spatial().union_all()
        assert added.area == pytest.approx(6.0)                # the added area, not the union
        assert resume.get_spatial().union_all().equals(added)
        assert resume.get_finished_granules() is None          # INDEXED granules are read in B
        assert not resume.stage1_full_scope()                  # so no NO_DATA proof either
        assert not resume.is_up_to_date()

    def test_marker_survives_failed_and_interrupted_saves(self, tmp_dir):
        for status in ('FAILED', 'INTERRUPTED', 'MERGING'):
            run = self._interrupted(os.path.join(tmp_dir, status), status=status, spatial=self.B)
            resume = H3BuildLogger(product_vars=None, dir=run._PARENT_DIR)
            assert resume.get_finished_granules() is None, status

    def test_completed_save_clears_the_marker(self, tmp_dir):
        self._interrupted(tmp_dir, spatial=self.B)
        H3BuildLogger(product_vars=None, dir=tmp_dir).save_log('COMPLETED')

        after = H3BuildLogger(product_vars=None, dir=tmp_dir)

        assert after.new_spatial is None
        assert after.get_finished_granules() == [{'orbit': 1, 'granule': 1, 'track': 1}]
        with open(after.log_file) as f:
            assert '_pending_expansion' not in json.load(f)

    def test_a_new_widening_combines_with_the_pending_one(self, tmp_dir):
        from shapely.geometry import box
        self._interrupted(tmp_dir, spatial=self.B)

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir, spatial=_box(-51, 0, -39, 1))

        added = resume.get_spatial().union_all()
        assert added.contains(box(-49.9, 0.1, -44.1, 0.9))     # the interrupted run's area
        assert added.contains(box(-43.9, 0.1, -39.1, 0.9))     # and this run's
        assert not added.contains(box(-50.9, 0.1, -50.1, 0.9))  # never the stored region

    def test_temporal_expansion_is_restored_too(self, tmp_dir):
        run = self._interrupted(tmp_dir, temporal=['2020-01-01', '2020-06-30'])

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)

        assert resume.new_temporal == run.new_temporal
        assert resume.get_temporal() == run.get_temporal()
        assert resume.get_finished_granules() is None


    def test_resumed_mixed_update_phase_2_gets_the_whole_region(self, tmp_dir):
        """Phase 2 of a mixed update starts once Phase 1 drained; a resume of it
        must not narrow Stage 2 (in S3 mode, its download) to the added area."""
        self._db(tmp_dir)
        run = H3BuildLogger(product_vars={'L2B': ['cover']}, dir=tmp_dir, spatial=self.B)
        whole = run.get_spatial().union_all()
        run.log_data['_pending_variable_update'] = {'product_vars': dict(run.new_product_vars)}
        run.save_log('PROCESSING')       # killed in Phase 2

        resume = H3BuildLogger(product_vars=None, dir=tmp_dir)

        assert resume.new_spatial is None
        assert resume.get_spatial().union_all().equals(whole)
