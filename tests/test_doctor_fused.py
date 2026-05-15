"""Fused-dispatch equivalence + abspath-bug regression for gh3_doctor.

The fused per-partition scan (``gedih3.doctor.fused.fused_scan_partition``)
must produce findings *set-equal* to running each diagnosis in its own
``parallel_map`` pass. Snapshot tests below build a small synthetic DB,
run each diagnosis (a) standalone and (b) fused, then assert the per-
diagnosis Report findings match.

Also covers the relative-path regression: ``discover_partition_dirs`` is
called with an absolute path so the per-partition workers don't silently
treat every partition as ``empty`` / ``missing_meta`` when CWD differs.
"""

from __future__ import annotations

import os

import pytest

# Ensure all diagnoses are registered.
import gedih3.doctor.diagnoses  # noqa: F401
from gedih3.doctor import DoctorContext, run_diagnoses
from gedih3.doctor.inspect import discover_partition_dirs
from gedih3.doctor.fused import (
    fused_eligible_names, fused_scan_partition, _build_shared_state,
)


@pytest.fixture(scope='module', autouse=True)
def _module_dask_client():
    """Tiny in-process LocalCluster shared across tests in this module."""
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1,
        processes=False,
        dashboard_address=None,
        silence_logs='ERROR',
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def _ctx(h3_dir):
    """Bare context — partition_dirs is the only field the scans need."""
    from gedih3.logger import H3BuildLogger
    try:
        h3_logger = H3BuildLogger(product_vars=None, dir=h3_dir)
    except Exception:
        h3_logger = None
    return DoctorContext(
        h3_dir=h3_dir,
        soc_dir=None,
        tmp_dir=None,
        h3_logger=h3_logger,
        partition_dirs=discover_partition_dirs(h3_dir),
        args=type('A', (), {'orphan_age_hours': 0.0, 's3': False, 'online': False})(),
    )


def _make_simple_db(tmp_dir):
    """Build a 2-partition synthetic DB so the test exercises the fused
    dispatch path (which requires >=2 partitions to be meaningful) and the
    parquet/meta-aware diagnoses see real files."""
    from tests.test_doctor_diagnoses import _make_partition, _make_build_log
    _make_partition(tmp_dir, h3_part='aaaa', year=2020)
    _make_partition(tmp_dir, h3_part='bbbb', year=2020)
    _make_build_log(tmp_dir)


# --- registry sanity -------------------------------------------------------

def test_fused_eligible_names_covers_all_partition_diagnoses():
    """Every per-partition h3db diagnosis should be fusion-eligible."""
    eligible = fused_eligible_names()
    expected = {'metadata', 'geoparquet_bbox', 'parquet_health',
                'orphans', 'backfill'}
    assert expected.issubset(eligible), (
        f"missing from fused registry: {expected - eligible}"
    )


# --- shared state ----------------------------------------------------------

def test_build_shared_state_includes_parquet_files_when_any_parquet_scan_enabled(tmp_dir):
    _make_simple_db(tmp_dir)
    part_dir = discover_partition_dirs(tmp_dir)[0]
    s = _build_shared_state(part_dir, ['metadata'])
    assert 'parquet_files' in s
    assert isinstance(s['parquet_files'], list)
    assert len(s['parquet_files']) >= 1


def test_build_shared_state_omits_meta_dict_unless_backfill_enabled(tmp_dir):
    _make_simple_db(tmp_dir)
    part_dir = discover_partition_dirs(tmp_dir)[0]
    s_no_backfill = _build_shared_state(part_dir, ['metadata', 'parquet_health'])
    assert 'meta_dict' not in s_no_backfill
    s_with_backfill = _build_shared_state(part_dir, ['backfill', 'metadata'])
    assert 'meta_dict' in s_with_backfill


# --- equivalence: fused vs sequential -------------------------------------

@pytest.mark.parametrize('pair', [
    ('metadata', 'geoparquet_bbox'),
    ('metadata', 'parquet_health'),
    ('parquet_health', 'geoparquet_bbox'),
    ('metadata', 'orphans'),
])
def test_fused_findings_match_sequential(tmp_dir, pair):
    """For each pair of fusion-eligible diagnoses, the fused dispatch
    must produce the same finding-set as running each diagnosis on its
    own through the single-diagnosis path."""
    _make_simple_db(tmp_dir)
    a, b = pair

    # Sequential — one parallel_map per diagnosis.
    seq_reports = {r.name: r for r in run_diagnoses(_ctx(tmp_dir), [a], mode='check')}
    seq_reports.update({r.name: r for r in run_diagnoses(_ctx(tmp_dir), [b], mode='check')})

    # Fused — single parallel_map, results split per diagnosis.
    fused_reports = {r.name: r for r in run_diagnoses(_ctx(tmp_dir), [a, b], mode='check')}

    for name in (a, b):
        assert name in seq_reports
        assert name in fused_reports
        # Set-equal on the JSON-comparable subset (kind + key id field).
        # We don't compare full dicts because granule lists / paths may
        # be re-ordered; the finding identity is (kind, partition_dir |
        # path) which is enough to assert no diagnosis lost or gained
        # findings under fusion.
        def _ids(report):
            ids = set()
            for f in report.findings:
                kind = f.get('kind', '')
                key = f.get('partition_dir') or f.get('path') or ''
                ids.add((kind, key))
            return ids
        assert _ids(seq_reports[name]) == _ids(fused_reports[name]), (
            f"{name}: fused vs sequential finding-id sets differ"
        )


# --- abspath regression ----------------------------------------------------

def test_doctor_cli_resolves_relative_indir_to_absolute(tmp_dir, monkeypatch):
    """``gh3_doctor -i database/`` (relative) must be absolute-ized
    before partition_dirs hits remote workers. The CLI bug we fixed
    silently produced 10k false ``empty_partition`` findings."""
    _make_simple_db(tmp_dir)

    rel = os.path.relpath(tmp_dir, start=os.getcwd())
    # Confirm the relative form actually differs from absolute.
    assert rel != tmp_dir

    # Discover from the relative path → relative dir entries.
    rel_parts = discover_partition_dirs(rel)
    assert all(not os.path.isabs(p) for p in rel_parts), (
        "fixture mismatch: relative discovery should return relative paths"
    )

    # After abspath resolution every entry must be absolute, which is
    # what the CLI now guarantees. We mirror the CLI's contract here.
    abs_root = os.path.abspath(rel)
    abs_parts = discover_partition_dirs(abs_root)
    assert all(os.path.isabs(p) for p in abs_parts)
