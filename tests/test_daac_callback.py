"""Tests for gedi_download callback payload and granule_names filter.

Covers three recent fixes in daac.py:
  1. on_granule_complete ginfo now includes a 'path' key.
  2. gedi_download accepts a granule_names filter forwarded to CMR
     (with .h5 stripped).
  3. GEDIAccessor.search_data DOI fallback warning names both dropped
     filters (short_name and version).

No network required — earthaccess and download_granule are patched.
"""

import logging
import os
from unittest.mock import MagicMock

import pytest


FAKE_FNAME = "GEDI02_A_2020123120000_O12345_01_T00001_02_003_01_V002.h5"
# Derived from FAKE_FNAME: year=2020, doy=123


def _make_granule(fname=FAKE_FNAME):
    g = MagicMock()
    g.data_links.return_value = [f"s3://test-bucket/{fname}"]
    return g


@pytest.fixture
def patch_daac(monkeypatch):
    """Patch GEDIAccessor, get_dask_client, gedi_vars_expand, pqdm, download_granule.

    Yields (daac_module, accessor_mock, calls) where `calls` is a dict that
    pqdm's side_effect will populate with the download_func partial used.
    """
    from gedih3 import daac

    # Force pqdm path, not dask
    monkeypatch.setattr(daac, "get_dask_client", lambda: None)

    # Skip product-variable expansion — the test passes explicit lists
    monkeypatch.setattr(daac, "gedi_vars_expand", lambda pv, version=None: pv)

    # Fake accessor — authenticate() never runs, search_data returns granules
    accessor = MagicMock()
    monkeypatch.setattr(daac, "GEDIAccessor", MagicMock(return_value=accessor))

    state = {"download_calls": [], "pqdm_results": None}

    def fake_pqdm(items, download_func, n_jobs):
        # Invoke the partial — this exercises download_granule's signature path
        return [download_func(i) for i in items]

    monkeypatch.setattr(daac, "pqdm", fake_pqdm)

    yield daac, accessor, state


def test_callback_path_on_pending_and_downloaded(patch_daac, tmp_path, monkeypatch):
    """PENDING gets the target path; DOWNLOADED gets the real downloaded path."""
    daac, accessor, _ = patch_daac
    odir = str(tmp_path / "soc")
    expected_target = os.path.join(odir, "2020", "123", FAKE_FNAME)

    accessor.search_data.return_value = [_make_granule()]

    # download_granule returns the real path on success
    monkeypatch.setattr(daac, "download_granule",
                        lambda granule, **kw: expected_target)

    events = []

    def cb(ginfo, status):
        events.append((status, dict(ginfo)))

    daac.gedi_download(
        product_vars={"L2A": ["shot_number"]},
        odir=odir,
        on_granule_complete=cb,
    )

    # Exactly one PENDING and one DOWNLOADED event
    statuses = [s for s, _ in events]
    assert statuses == ["PENDING", "DOWNLOADED"], statuses

    pending = events[0][1]
    downloaded = events[1][1]

    # All four keys present on both
    for g in (pending, downloaded):
        assert set(g.keys()) >= {"orbit", "granule", "track", "path"}
        assert g["orbit"] == 12345
        assert g["granule"] == 1
        assert g["track"] == 1

    assert pending["path"] == expected_target
    assert downloaded["path"] == expected_target


def test_callback_path_on_failed(patch_daac, tmp_path, monkeypatch):
    """FAILED status still fires the callback and reports path=None."""
    daac, accessor, _ = patch_daac
    odir = str(tmp_path / "soc")

    accessor.search_data.return_value = [_make_granule()]
    monkeypatch.setattr(daac, "download_granule", lambda granule, **kw: None)

    events = []
    daac.gedi_download(
        product_vars={"L2A": ["shot_number"]},
        odir=odir,
        on_granule_complete=lambda g, s: events.append((s, dict(g))),
    )

    statuses = [s for s, _ in events]
    assert statuses == ["PENDING", "FAILED"]
    failed = events[1][1]
    assert failed["path"] is None
    # PENDING still carries the target path even though the download failed
    assert events[0][1]["path"].endswith(FAKE_FNAME)


def test_granule_names_forwarded_with_h5_suffix_stripped(patch_daac, tmp_path, monkeypatch):
    """granule_names reaches search_data as granule_name=[stems] (no .h5)."""
    daac, accessor, _ = patch_daac
    odir = str(tmp_path / "soc")

    accessor.search_data.return_value = []  # no downloads, just checking forwarding
    monkeypatch.setattr(daac, "download_granule", lambda granule, **kw: None)

    names = [
        FAKE_FNAME,                              # with .h5
        FAKE_FNAME.replace(".h5", ""),           # already a stem
        "GEDI04_A_2021001000000_O00001_01_T00001_02_003_02_V002.h5",
    ]

    daac.gedi_download(
        product_vars={"L2A": ["shot_number"]},
        odir=odir,
        granule_names=names,
    )

    accessor.search_data.assert_called_once()
    _, kwargs = accessor.search_data.call_args
    assert "granule_name" in kwargs
    forwarded = kwargs["granule_name"]
    assert all(not n.endswith(".h5") for n in forwarded), forwarded
    assert forwarded[0] == FAKE_FNAME[:-3]
    assert forwarded[1] == FAKE_FNAME[:-3]  # already a stem, unchanged
    assert len(forwarded) == 3


def test_granule_names_none_does_not_forward(patch_daac, tmp_path, monkeypatch):
    """Default (None) must NOT add granule_name to the search call."""
    daac, accessor, _ = patch_daac
    odir = str(tmp_path / "soc")
    accessor.search_data.return_value = []
    monkeypatch.setattr(daac, "download_granule", lambda granule, **kw: None)

    daac.gedi_download(
        product_vars={"L2A": ["shot_number"]},
        odir=odir,
    )

    _, kwargs = accessor.search_data.call_args
    assert "granule_name" not in kwargs


def test_doi_fallback_warning_mentions_dropped_version(monkeypatch):
    """DOI fallback log must name 'version' as a dropped filter.

    gedih3.daac's logger has propagate=False, so caplog (root-based) can't
    observe it. Attach a list handler directly to the module logger.
    """
    from gedih3 import daac

    call_count = {"n": 0}

    def fake_search(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [object(), object()]

    monkeypatch.setattr(daac.earthaccess, "search_data", fake_search)

    captured = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            captured.append(record)

    handler = ListHandler(level=logging.WARNING)
    daac.logger.addHandler(handler)
    try:
        acc = daac.GEDIAccessor.__new__(daac.GEDIAccessor)
        acc.product_files = {}
        acc.search_data("L2A", version=2)
    finally:
        daac.logger.removeHandler(handler)

    fallback_logs = [r for r in captured if "retrying with DOI" in r.getMessage()]
    assert fallback_logs, f"expected a DOI-fallback warning, got {[r.getMessage() for r in captured]}"
    msg = fallback_logs[0].getMessage()
    assert "version" in msg, msg
    assert "short_name" in msg, msg
    assert call_count["n"] == 2  # confirm fallback actually fired
