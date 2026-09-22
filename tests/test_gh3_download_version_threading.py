"""
Regression test for ``gh3_download``'s CLI wiring of ``--gedi-version`` on
resume.

``SOCDownloadLogger`` resolves ``product_vars`` against the *persisted* log
version when ``--gedi-version`` is omitted on resume (``logger.py``), and
updates ``self.gedi_version`` to that persisted value via
``_load_filters_from_log``. But ``gh3_download.py``'s ``main()`` used to pass
the raw, still-``None`` ``args.version`` into ``download_soc`` /
``s3_etl_subset`` instead of the resolved ``soc_logger.gedi_version`` --
mirroring the exact append-without-purge hazard fixed in
``_expand_product_vars`` (gh3builder.py), just reached through a different
CLI path. ``download_soc``'s quality-flag injection loop
(``_get_versioned(flag_map, version)``) would then resolve against the
fallback version 2 and inject a stale v2 flag name (e.g. ``l2b_quality_flag``)
alongside the already-correctly-baked v3 name (``l2b_quality_flag_rel3``),
even though the real archive being resumed against is v3.

``gh3_build.py`` already threads ``h3_logger.gedi_version`` (not
``args.version``) into every downstream call for exactly this reason; this
fixes ``gh3_download.py`` to match.
"""

import json
import os
import sys

import pytest

from gedih3.config import BUILD_LOG_FILENAME  # noqa: F401  (keep config import path warm)


@pytest.fixture
def soc_dir_with_v3_log(tmp_path):
    soc_dir = str(tmp_path / "soc")
    os.makedirs(soc_dir, exist_ok=True)
    log = {
        "metadata": {"package_version": "0.0.0"},
        "gedi_version": 3,
        "status": "COMPLETED",
        "last_modified": "2026-08-19T00:00:00Z",
        "spatial_filter": None,
        "temporal_filter": None,
        "s3_access": False,
        "products": {
            "L2B": {"status": "COMPLETED", "last_modified": "2026-08-19T00:00:00Z",
                     "variables": ["shot_number", "cover", "pai", "l2b_quality_flag_rel3"]},
        },
        "granules": [],
    }
    with open(os.path.join(soc_dir, "gedih3_download_log.json"), "w") as f:
        json.dump(log, f)
    return soc_dir


class _DummyClient:
    dashboard_link = "http://dummy"

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _run_gh3_download_main(monkeypatch, argv, capture):
    from gedih3.cli import gh3_download

    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr("dask.distributed.Client", _DummyClient)
    monkeypatch.setattr("gedih3.logger.SOCDownloadLogger.set_post_download_info", lambda self: None)
    monkeypatch.setattr("gedih3.logger.SOCDownloadLogger.save_log", lambda self, status: None)

    def fake_s3_etl_subset(**kwargs):
        capture["s3_etl_subset"] = kwargs
        return "unused"

    def fake_download_soc(**kwargs):
        capture["download_soc"] = kwargs
        return []

    monkeypatch.setattr("gedih3.gh3builder.s3_etl_subset", fake_s3_etl_subset)
    monkeypatch.setattr("gedih3.gh3builder.download_soc", fake_download_soc)

    gh3_download.main()


class TestGh3DownloadResumeVersionThreading:

    def test_daac_resume_without_gedi_version_uses_persisted_version(self, monkeypatch, soc_dir_with_v3_log):
        capture = {}
        argv = ["gh3_download", "-l2b", "minimal", "-o", soc_dir_with_v3_log]
        _run_gh3_download_main(monkeypatch, argv, capture)

        assert "download_soc" in capture
        assert capture["download_soc"]["version"] == 3, (
            "download_soc must receive the persisted gedi_version (3) resolved by "
            "SOCDownloadLogger on resume, not the raw --gedi-version CLI arg (None)."
        )

    def test_s3_etl_resume_without_gedi_version_uses_persisted_version(self, monkeypatch, soc_dir_with_v3_log):
        capture = {}
        argv = ["gh3_download", "-l2b", "minimal", "-o", soc_dir_with_v3_log, "-s3"]
        _run_gh3_download_main(monkeypatch, argv, capture)

        assert "s3_etl_subset" in capture
        assert capture["s3_etl_subset"]["version"] == 3, (
            "s3_etl_subset must receive the persisted gedi_version (3) resolved by "
            "SOCDownloadLogger on resume, not the raw --gedi-version CLI arg (None)."
        )

    def test_explicit_gedi_version_still_wins_on_fresh_download(self, monkeypatch, tmp_path):
        soc_dir = str(tmp_path / "fresh_soc")
        os.makedirs(soc_dir, exist_ok=True)
        capture = {}
        argv = ["gh3_download", "-l2b", "minimal", "-o", soc_dir, "--gedi-version", "3"]
        _run_gh3_download_main(monkeypatch, argv, capture)

        assert capture["download_soc"]["version"] == 3
