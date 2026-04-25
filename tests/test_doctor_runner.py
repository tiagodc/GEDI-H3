"""Tests for the doctor runner: registry, dispatch, alias resolution, error capture."""

import pytest

from gedih3.doctor import DoctorContext, Report, Severity, run_diagnoses, get_diagnoses
from gedih3.doctor.runner import register, resolve_names, _REGISTRY


@pytest.fixture(autouse=True)
def isolate_registry():
    """Each test runs against a clean registry to avoid cross-pollution."""
    saved = dict(_REGISTRY)
    _REGISTRY.clear()
    yield
    _REGISTRY.clear()
    _REGISTRY.update(saved)


def _ctx(tmp_dir):
    return DoctorContext(h3_dir=tmp_dir)


def test_register_and_get(tmp_dir):
    @register('alpha', 'A check', scope='global')
    def alpha_check(ctx):
        return Report(name='alpha', summary='ok')

    assert 'alpha' in get_diagnoses()
    assert get_diagnoses()['alpha'].description == 'A check'


def test_resolve_names_unknown_raises(tmp_dir):
    @register('a', 'desc', scope='global')
    def _check(ctx):
        return Report(name='a')

    with pytest.raises(ValueError):
        resolve_names(['nonexistent_diagnosis'])


def test_check_failure_captured_as_error_report(tmp_dir):
    @register('boom', 'fails', scope='global')
    def _check(ctx):
        raise RuntimeError("oops")

    reports = run_diagnoses(_ctx(tmp_dir), ['boom'], mode='check')
    assert len(reports) == 1
    assert reports[0].severity == Severity.ERROR
    assert 'oops' in reports[0].summary


def test_fix_only_runs_when_findings_exist(tmp_dir):
    fix_called = []

    def _fix(ctx, report):
        fix_called.append(True)
        report.applied = True
        return report

    @register('clean_check', 'no findings', scope='global', fix=_fix)
    def _check(ctx):
        return Report(name='clean_check', summary='clean')

    run_diagnoses(_ctx(tmp_dir), ['clean_check'], mode='fix')
    assert fix_called == [], "fix must not run when check produced zero findings"


def test_fix_runs_on_findings(tmp_dir):
    @register('dirty', 'has findings', scope='global',
              fix=lambda ctx, r: Report(name='dirty', applied=True, summary='fixed'))
    def _check(ctx):
        return Report(name='dirty', findings=[{'kind': 'thing'}], summary='found 1')

    reports = run_diagnoses(_ctx(tmp_dir), ['dirty'], mode='fix')
    assert reports[0].applied is True


def test_fix_failure_marks_error(tmp_dir):
    def _bad_fix(ctx, report):
        raise RuntimeError("fix failed")

    @register('half', 'check ok, fix bad', scope='global', fix=_bad_fix)
    def _check(ctx):
        return Report(name='half', findings=[{'kind': 'x'}])

    reports = run_diagnoses(_ctx(tmp_dir), ['half'], mode='fix')
    assert reports[0].severity == Severity.ERROR
    assert any('fix_error' in f for f in reports[0].findings)


def test_alias_all_runs_every_registered(tmp_dir):
    @register('a', 'A', scope='global')
    def _a(ctx): return Report(name='a')

    @register('b', 'B', scope='global')
    def _b(ctx): return Report(name='b')

    reports = run_diagnoses(_ctx(tmp_dir), ['all'], mode='check')
    assert {r.name for r in reports} == {'a', 'b'}


def test_resolve_names_dedupes(tmp_dir):
    @register('a', 'A', scope='global')
    def _a(ctx): return Report(name='a')

    @register('b', 'B', scope='global')
    def _b(ctx): return Report(name='b')

    out = resolve_names(['a', 'all', 'b'])
    assert out == ['a', 'b']
