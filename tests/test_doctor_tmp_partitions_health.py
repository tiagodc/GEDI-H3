"""Tests for the ``tmp_partitions_health`` doctor diagnosis.

The check is audit-only and acts strictly on signals the build persisted
under ``tmp_dir`` — no healthy partitions are ever iterated.

Covered:
  * clean tmp_dir → INFO + no findings
  * merge_failure sentinels → grouped findings
  * granule_failures.jsonl with mixed ``kind`` → grouped summary findings
  * progress > manifest drift → drift finding
  * --fix invokes preclean_merge_failures and reports counts
  * build-active detection refuses to fix and returns ERROR severity
"""

import json
import os

import pytest

# Ensure registration runs.
import gedih3.doctor.diagnoses  # noqa: F401
from gedih3.doctor.report import DoctorContext, Severity
from gedih3.doctor.diagnoses.tmp_partitions_health import (
    tmp_partitions_health_check,
    tmp_partitions_health_fix,
)
from gedih3 import gh3builder
from gedih3.gh3builder import (
    _emit_merge_failure_sentinel,
    _append_granule_failure,
    _MERGE_FAILURES_DIRNAME,
    _GRANULE_FAILURES_FILENAME,
)
from gedih3.config import MANIFEST_FILENAME


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_ctx(h3_dir, tmp_dir):
    return DoctorContext(
        h3_dir=str(h3_dir),
        soc_dir=None,
        tmp_dir=str(tmp_dir),
        h3_logger=None,
        partition_dirs=[],
        args=type('A', (), {})(),
    )


def _write_manifest(h3_dir, n_lines):
    """Write a fake _manifest.txt with the requested number of entries."""
    path = os.path.join(str(h3_dir), MANIFEST_FILENAME)
    with open(path, 'w') as f:
        for i in range(n_lines):
            f.write(f'h3_03=aaa{i:04d}/year=2020/file_{i}.parquet\n')


def _write_merge_progress(tmp_dir, n_lines):
    path = os.path.join(str(tmp_dir), '_merge_progress.txt')
    with open(path, 'w') as f:
        for i in range(n_lines):
            f.write(f'partition_{i}\n')


@pytest.fixture
def clean_dirs(tmp_path):
    """An h3_dir + tmp_dir that exist on disk but contain no failure signals."""
    h3_dir = tmp_path / 'h3db'
    tmp_dir = tmp_path / 'tmp'
    h3_dir.mkdir()
    tmp_dir.mkdir()
    return h3_dir, tmp_dir


# ---------------------------------------------------------------------------
# 1. Clean tmp_dir → INFO + no findings
# ---------------------------------------------------------------------------

class TestCleanTmpDir:
    def test_no_signals_returns_info(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)
        assert report.severity == Severity.INFO
        assert report.findings == []
        assert report.name == 'tmp_partitions_health'

    def test_missing_tmp_dir_returns_info(self, tmp_path):
        h3_dir = tmp_path / 'h3db'
        h3_dir.mkdir()
        ctx = DoctorContext(
            h3_dir=str(h3_dir),
            tmp_dir=None,
            partition_dirs=[],
            args=type('A', (), {})(),
        )
        report = tmp_partitions_health_check(ctx)
        assert report.severity == Severity.INFO
        assert report.findings == []
        assert 'no tmp_dir' in report.summary


# ---------------------------------------------------------------------------
# 2. Merge failure sentinels → merge_failure findings
# ---------------------------------------------------------------------------

class TestMergeFailureFindings:
    def test_single_merge_failure_sentinel_detected(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        part = str(tmp_dir / 'h3_03=aaa' / 'year=2020')
        _emit_merge_failure_sentinel(str(tmp_dir), part, RuntimeError('boom'))

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        assert report.severity == Severity.WARN
        merge_findings = [f for f in report.findings if f['kind'] == 'merge_failure']
        assert len(merge_findings) == 1
        assert merge_findings[0]['partition_dir'] == part
        assert 'RuntimeError' in merge_findings[0]['error']
        assert 'boom' in merge_findings[0]['error']

    def test_multiple_sentinels_one_finding_each(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        parts = [
            str(tmp_dir / 'h3_03=aaa' / 'year=2019'),
            str(tmp_dir / 'h3_03=bbb' / 'year=2020'),
            str(tmp_dir / 'h3_03=ccc' / 'year=2021'),
        ]
        for i, p in enumerate(parts):
            _emit_merge_failure_sentinel(str(tmp_dir), p, IOError(f"err-{i}"))

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        merge_findings = [f for f in report.findings if f['kind'] == 'merge_failure']
        assert len(merge_findings) == 3
        found_dirs = {f['partition_dir'] for f in merge_findings}
        assert found_dirs == set(parts)


# ---------------------------------------------------------------------------
# 3. Granule failures JSONL → grouped summary findings
# ---------------------------------------------------------------------------

class TestGranuleFailureGrouping:
    def test_mixed_kinds_group_with_counts_and_examples(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        tmp_str = str(tmp_dir)

        # 4 missing_var records, 2 other records.
        for i in range(4):
            _append_granule_failure(tmp_str, f'O{i}_G{i}_T{i}.0000', {
                'kind': 'missing_var',
                'var': f'v{i}',
                'product': 'L2A',
                'error_type': 'KeyError',
                'error_message': f'missing v{i}',
            })
        for i in range(2):
            _append_granule_failure(tmp_str, f'X{i}.0000', {
                'kind': 'other',
                'var': None,
                'product': None,
                'error_type': 'ValueError',
                'error_message': f'oops-{i}',
            })

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        gran_findings = [f for f in report.findings if f['kind'] == 'granule_failures']
        assert len(gran_findings) == 2
        by_kind = {f['failure_kind']: f for f in gran_findings}
        assert by_kind['missing_var']['count'] == 4
        assert by_kind['other']['count'] == 2
        # Examples are capped at 5 (we wrote 4 missing_var → all 4 should be there).
        assert len(by_kind['missing_var']['examples']) == 4
        assert len(by_kind['other']['examples']) == 2
        assert report.severity == Severity.WARN

    def test_more_than_five_examples_capped(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        tmp_str = str(tmp_dir)
        for i in range(10):
            _append_granule_failure(tmp_str, f'O{i}.0000', {
                'kind': 'missing_var',
                'var': f'v{i}',
                'product': 'L2A',
                'error_type': 'KeyError',
                'error_message': 'm',
            })
        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)
        gran = [f for f in report.findings if f['kind'] == 'granule_failures'][0]
        assert gran['count'] == 10
        assert len(gran['examples']) == 5


# ---------------------------------------------------------------------------
# 4. Progress vs. manifest drift
# ---------------------------------------------------------------------------

class TestProgressManifestDrift:
    def test_progress_much_larger_emits_drift(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        _write_manifest(h3_dir, n_lines=10)
        _write_merge_progress(tmp_dir, n_lines=100)

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        drift = [f for f in report.findings if f['kind'] == 'progress_manifest_drift']
        assert len(drift) == 1
        assert drift[0]['progress_count'] == 100
        assert drift[0]['manifest_count'] == 10
        assert report.severity == Severity.WARN

    def test_within_tolerance_no_drift(self, clean_dirs):
        h3_dir, tmp_dir = clean_dirs
        _write_manifest(h3_dir, n_lines=100)
        _write_merge_progress(tmp_dir, n_lines=102)  # delta 2 < tolerance 5

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        drift = [f for f in report.findings if f['kind'] == 'progress_manifest_drift']
        assert drift == []

    def test_manifest_larger_no_drift(self, clean_dirs):
        # Manifest > progress is the healthy steady state (manifest reflects
        # all merged partitions; progress only the ones recorded this run).
        h3_dir, tmp_dir = clean_dirs
        _write_manifest(h3_dir, n_lines=1000)
        _write_merge_progress(tmp_dir, n_lines=5)
        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)
        drift = [f for f in report.findings if f['kind'] == 'progress_manifest_drift']
        assert drift == []


# ---------------------------------------------------------------------------
# 5. --fix runs preclean_merge_failures
# ---------------------------------------------------------------------------

class TestFixRunsPreclean:
    def test_fix_invokes_preclean_and_reports_counts(self, clean_dirs, monkeypatch):
        h3_dir, tmp_dir = clean_dirs
        part = str(tmp_dir / 'h3_03=aaa' / 'year=2020')
        _emit_merge_failure_sentinel(str(tmp_dir), part, RuntimeError('x'))

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)
        assert report.severity == Severity.WARN

        # Stub preclean_merge_failures so we don't depend on real parquet
        # cleanup semantics — the contract under test is that the fix
        # invokes it and folds the returned counts into the finding.
        calls = []

        def fake_preclean(td):
            calls.append(td)
            return {'partitions_cleaned': 1, 'parquets_removed': 3, 'tmps_removed': 2}

        monkeypatch.setattr(gh3builder, 'preclean_merge_failures', fake_preclean)
        # Also patch the build-active probe to a guaranteed-False return so
        # the test doesn't accidentally depend on the host's process list.
        monkeypatch.setattr(
            'gedih3.doctor.diagnoses.tmp_partitions_health._build_is_active',
            lambda h, t: (False, {}),
        )

        fixed = tmp_partitions_health_fix(ctx, report)
        assert calls == [str(tmp_dir)]
        assert fixed.applied is True
        merge = [f for f in fixed.findings if f['kind'] == 'merge_failure']
        assert len(merge) == 1
        assert merge[0]['action'] == 'preclean_merge_failures'
        assert merge[0]['preclean_stats']['parquets_removed'] == 3
        assert merge[0]['preclean_stats']['tmps_removed'] == 2

    def test_fix_marks_granule_failures_as_reported_only(self, clean_dirs, monkeypatch):
        h3_dir, tmp_dir = clean_dirs
        _append_granule_failure(str(tmp_dir), 'O1.0000', {
            'kind': 'missing_var',
            'var': 'v1',
            'product': 'L2A',
            'error_type': 'KeyError',
            'error_message': 'm',
        })
        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        monkeypatch.setattr(
            'gedih3.doctor.diagnoses.tmp_partitions_health._build_is_active',
            lambda h, t: (False, {}),
        )
        fixed = tmp_partitions_health_fix(ctx, report)
        gran = [f for f in fixed.findings if f['kind'] == 'granule_failures'][0]
        assert gran['action'] == 'reported_only'
        assert 'gh3_update' in gran['recommendation']


# ---------------------------------------------------------------------------
# 6. Build-active detection refuses to fix
# ---------------------------------------------------------------------------

class TestBuildActiveGuard:
    def test_active_build_refuses_fix(self, clean_dirs, monkeypatch):
        h3_dir, tmp_dir = clean_dirs
        part = str(tmp_dir / 'h3_03=aaa' / 'year=2020')
        _emit_merge_failure_sentinel(str(tmp_dir), part, RuntimeError('x'))

        ctx = _make_ctx(h3_dir, tmp_dir)
        report = tmp_partitions_health_check(ctx)

        # Synthetic active-build signal: pretend the probe found a live PID.
        monkeypatch.setattr(
            'gedih3.doctor.diagnoses.tmp_partitions_health._build_is_active',
            lambda h, t: (True, {'pid': 12345, 'log': '/tmp/gh3_build.log'}),
        )

        # Trip-wire: preclean must NOT be invoked while build is active.
        called = []

        def boom(td):
            called.append(td)
            raise AssertionError('preclean should not run when build is active')

        monkeypatch.setattr(gh3builder, 'preclean_merge_failures', boom)

        fixed = tmp_partitions_health_fix(ctx, report)
        assert called == []
        assert fixed.severity == Severity.ERROR
        assert any(f['kind'] == 'build_active' for f in fixed.findings)
        ba = [f for f in fixed.findings if f['kind'] == 'build_active'][0]
        assert ba['pid'] == 12345


# ---------------------------------------------------------------------------
# 7. Registration sanity
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_registered_in_doctor_registry(self):
        from gedih3.doctor.runner import get_diagnoses
        diags = get_diagnoses()
        assert 'tmp_partitions_health' in diags
        d = diags['tmp_partitions_health']
        assert d.scope == 'global'
        assert d.fix is not None
