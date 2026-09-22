"""GEDI release defaults: v3 for every product, one release per database.

Pins the contract introduced with L4A/L4C V3 support:

* ``GEDI_DEFAULT_VERSION`` is the only ``version=None`` fallback and every
  product registers the same default release.
* ORNL DAAC identifiers resolve per version (L4A V3 → 2508, L4C V3 → 2520).
* ``search_data`` always pins the CMR ``version`` filter for LP DAAC
  products (002 and 003 share a short_name in CMR).
* ``resolve_soc_version`` reads the release already on disk a priori.
* Loggers keep an existing tree's / database's release on resume.
"""
import json
import os

import pytest

from gedih3.config import (
    GEDI_DEFAULT_VERSION, GEDI_PRODUCTS, _get_versioned, _resolve_identifier,
    get_default_vars_file, _GEDI_MIN_VARS, _PRODUCT_QUALITY_FLAGS,
)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

def test_every_product_defaults_to_v3():
    assert GEDI_DEFAULT_VERSION == 3
    for prod, spec in GEDI_PRODUCTS.items():
        assert spec['version'] == 3, prod


@pytest.mark.parametrize('prod', list(GEDI_PRODUCTS))
def test_v3_manifest_and_flags_exist_for_every_product(prod):
    assert get_default_vars_file(prod, version=3).is_file()
    assert get_default_vars_file(prod).name.endswith('_003.txt')
    assert 3 in _GEDI_MIN_VARS[prod]
    assert 3 in _PRODUCT_QUALITY_FLAGS[prod]


def test_get_versioned_none_is_package_default():
    assert _get_versioned({2: 'two', 3: 'three'}, None) == 'three'
    assert _get_versioned({2: 'two'}, None) == 'two'  # nearest lower


@pytest.mark.parametrize('prod, version, short_name, doi', [
    ('L4A', 3,   'GEDI_L4A_AGB_Density_V3_2508',   '10.3334/ORNLDAAC/2508'),
    ('L4A', 2,   'GEDI_L4A_AGB_Density_V2_1_2056', '10.3334/ORNLDAAC/2056'),
    ('L4A', 2.1, 'GEDI_L4A_AGB_Density_V2_1_2056', '10.3334/ORNLDAAC/2056'),
    ('L4C', 3,   'GEDI_L4C_WSCI_V3_2520',          '10.3334/ORNLDAAC/2520'),
    ('L4C', 2,   'GEDI_L4C_WSCI_2338',             '10.3334/ORNLDAAC/2338'),
])
def test_ornl_identifiers_resolve_per_version(prod, version, short_name, doi):
    spec = GEDI_PRODUCTS[prod]
    assert _resolve_identifier(spec['short_name'], version, product=prod, field='short_name') == short_name
    assert _resolve_identifier(spec['doi'], version, product=prod, field='doi') == doi


def test_lpdaac_dois_track_the_default_release():
    for prod in ('L1B', 'L2A', 'L2B'):
        assert GEDI_PRODUCTS[prod]['doi'].endswith('.003'), prod


# ---------------------------------------------------------------------------
# variable expansion
# ---------------------------------------------------------------------------

def test_gedi_vars_expand_default_is_v3_names():
    from gedih3.gedidriver import gedi_vars_expand
    out = gedi_vars_expand({'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['default']})
    assert 'l2a_quality_flag_rel3' in out['L2A']
    assert 'l4a_quality_flag_rel3' in out['L4A']
    assert 'l4c_quality_flag_rel3' in out['L4C']
    assert 'wsci_quality_flag' not in out['L4C']


def test_gedi_vars_expand_explicit_v2_keeps_v2_names():
    from gedih3.gedidriver import gedi_vars_expand
    out = gedi_vars_expand({'L4A': ['minimal'], 'L4C': ['minimal']}, version=2)
    assert 'l4_quality_flag' in out['L4A']
    assert 'wsci_quality_flag' in out['L4C']


# ---------------------------------------------------------------------------
# CMR search params
# ---------------------------------------------------------------------------

def _accessor_with_fake_search(monkeypatch):
    from gedih3 import daac

    seen = {}

    def fake_search(params, label):
        seen['params'] = dict(params)
        return [object()]

    monkeypatch.setattr(daac, '_search_verified', fake_search)
    acc = daac.GEDIAccessor.__new__(daac.GEDIAccessor)
    acc.product_files = {}
    return acc, seen


def test_search_data_pins_lpdaac_version_by_default(monkeypatch):
    acc, seen = _accessor_with_fake_search(monkeypatch)
    acc.search_data('L2A')
    assert seen['params']['short_name'] == 'GEDI02_A'
    assert seen['params']['version'] == '003'


def test_search_data_explicit_lpdaac_version(monkeypatch):
    acc, seen = _accessor_with_fake_search(monkeypatch)
    acc.search_data('L2B', version=2)
    assert seen['params'] == {'short_name': 'GEDI02_B', 'version': '002'}


@pytest.mark.parametrize('prod, version, short_name', [
    ('L4A', None, 'GEDI_L4A_AGB_Density_V3_2508'),
    ('L4C', None, 'GEDI_L4C_WSCI_V3_2520'),
    ('L4A', 2,    'GEDI_L4A_AGB_Density_V2_1_2056'),
    ('L4C', 2,    'GEDI_L4C_WSCI_2338'),
])
def test_search_data_ornl_short_name_is_version_pinned(monkeypatch, prod, version, short_name):
    acc, seen = _accessor_with_fake_search(monkeypatch)
    acc.search_data(prod, version=version)
    assert seen['params']['short_name'] == short_name
    assert 'version' not in seen['params']


# ---------------------------------------------------------------------------
# resolve_soc_version
# ---------------------------------------------------------------------------

def _touch(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb'):
        pass


def test_resolve_soc_version_prefers_download_log(tmp_path):
    from gedih3.logger import resolve_soc_version, SOCDownloadLogger
    _touch(str(tmp_path / '2020' / '100' / 'GEDI02_A_2020100123456_O07000_02_T01234_02_003_02_V002.h5'))
    with open(tmp_path / SOCDownloadLogger._LOG_FILE_NAME, 'w') as fh:
        json.dump({'gedi_version': 3}, fh)
    assert resolve_soc_version(str(tmp_path)) == 3


def test_resolve_soc_version_from_filename_without_log(tmp_path):
    from gedih3.logger import resolve_soc_version
    _touch(str(tmp_path / '2020' / '100' / 'GEDI02_A_2020100123456_O07000_02_T01234_02_003_02_V002.h5'))
    _touch(str(tmp_path / '2020' / '100' / 'notes.txt'))
    assert resolve_soc_version(str(tmp_path)) == 2


def test_resolve_soc_version_empty_or_missing_is_none(tmp_path):
    from gedih3.logger import resolve_soc_version
    assert resolve_soc_version(str(tmp_path)) is None
    assert resolve_soc_version(str(tmp_path / 'nope')) is None
    assert resolve_soc_version(None) is None
    (tmp_path / '2021').mkdir()
    assert resolve_soc_version(str(tmp_path)) is None


# ---------------------------------------------------------------------------
# loggers keep the persisted release on resume
# ---------------------------------------------------------------------------

def test_download_logger_adopts_persisted_version_for_expansion(tmp_path):
    from gedih3.logger import SOCDownloadLogger
    with open(tmp_path / SOCDownloadLogger._LOG_FILE_NAME, 'w') as fh:
        json.dump({'gedi_version': 2, 'status': 'COMPLETED',
                   'products': {'L2A': {'variables': ['shot_number']}}}, fh)
    lg = SOCDownloadLogger({'L4A': ['minimal']}, dir=str(tmp_path))
    assert lg.gedi_version == 2
    assert 'l4_quality_flag' in lg.new_product_vars['L4A']


def test_download_logger_warns_on_version_mismatch(tmp_path, caplog):
    import logging
    from gedih3.logger import SOCDownloadLogger
    with open(tmp_path / SOCDownloadLogger._LOG_FILE_NAME, 'w') as fh:
        json.dump({'gedi_version': 2, 'status': 'COMPLETED',
                   'products': {'L2A': {'variables': ['shot_number']}}}, fh)
    # The package root logger has its own handler with propagate=False, so
    # wire caplog's handler onto the module logger directly.
    lg = logging.getLogger('gedih3.logger')
    lg.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger='gedih3.logger'):
            SOCDownloadLogger({'L2A': ['minimal']}, dir=str(tmp_path), version=3)
    finally:
        lg.removeHandler(caplog.handler)
    assert any('version mismatch' in r.getMessage().lower() for r in caplog.records)


def test_fresh_download_logger_default_version_is_none_until_files_land(tmp_path):
    # The CLI resolves the version (resolve_soc_version → package default)
    # before download; the logger itself never invents one.
    from gedih3.logger import SOCDownloadLogger
    lg = SOCDownloadLogger({'L2A': ['minimal']}, dir=str(tmp_path))
    assert lg.gedi_version is None
    assert 'l2a_quality_flag_rel3' in lg.product_vars['L2A']


def test_build_logger_resume_keeps_v2_and_rejects_v3(tmp_path):
    from gedih3.logger import H3BuildLogger
    from gedih3.exceptions import GediValidationError
    with open(tmp_path / H3BuildLogger._LOG_FILE_NAME, 'w') as fh:
        json.dump({'gedi_version': 2, 'status': 'COMPLETED', 'h3_resolution_level': 12,
                   'h3_partition_level': 3,
                   'products': {'L2A': {'variables': ['shot_number']}}}, fh)
    lg = H3BuildLogger({'L4C': ['minimal']}, dir=str(tmp_path))
    assert lg.gedi_version == 2
    assert 'wsci_quality_flag' in lg.new_product_vars['L4C']
    with pytest.raises(GediValidationError, match='version mismatch'):
        H3BuildLogger({'L4C': ['minimal']}, dir=str(tmp_path), version=3)


# ---------------------------------------------------------------------------
# download-time completion of explicit lists (shared by DAAC + S3 ETL paths)
# ---------------------------------------------------------------------------

def test_download_essentials_union_into_explicit_l2a_list():
    from gedih3.gh3builder import _ensure_download_essentials
    pv = _ensure_download_essentials({'L2A': ['elev_highestreturn'], 'L4A': ['agbd']}, version=3)
    for v in ('lat_lowestmode', 'lon_lowestmode', 'elev_lowestmode', 'l2a_quality_flag_rel3',
              'degrade_flag', 'sensitivity', 'shot_number', 'elev_highestreturn'):
        assert v in pv['L2A'], v
    assert pv['L4A'][:1] == ['agbd']
    assert {'shot_number', 'l4a_quality_flag_rel3', 'elev_highestreturn_outlier_flag'} <= set(pv['L4A'])


def test_download_essentials_adds_l2a_when_absent_and_respects_ensure_l2a_false():
    from gedih3.gh3builder import _ensure_download_essentials
    pv = _ensure_download_essentials({'L4C': ['wsci']}, version=2)
    assert 'quality_flag' in pv['L2A'] and 'shot_number' in pv['L2A']
    assert 'wsci_quality_flag' in pv['L4C']
    pv = _ensure_download_essentials({'L4C': ['wsci']}, version=2, ensure_l2a=False)
    assert 'L2A' not in pv
    assert sorted(pv['L4C']) == sorted(['wsci', 'shot_number', 'wsci_quality_flag'])


def test_download_essentials_leaves_dump_all_alone():
    from gedih3.gh3builder import _ensure_download_essentials
    pv = _ensure_download_essentials({'L2A': None, 'L4A': None}, version=3)
    assert pv == {'L2A': None, 'L4A': None}


def test_download_essentials_purges_other_release_names():
    # A product_vars persisted by an older run (or resolved under another
    # release) carries v2 essential/flag names. Downloading v3 must drop
    # them — S3 ETL would otherwise request a variable the v3 granule lacks —
    # while keeping shared names (degrade_flag) and explicit requests.
    from gedih3.gh3builder import _ensure_download_essentials
    pv = _ensure_download_essentials(
        {'L2A': ['quality_flag', 'degrade_flag', 'rh'], 'L4A': ['agbd', 'l4_quality_flag']},
        version=3,
    )
    assert 'quality_flag' not in pv['L2A']
    assert {'l2a_quality_flag_rel3', 'degrade_flag', 'rh'} <= set(pv['L2A'])
    assert 'l4_quality_flag' not in pv['L4A']
    assert {'agbd', 'l4a_quality_flag_rel3', 'elev_highestreturn_outlier_flag'} <= set(pv['L4A'])


def test_minimal_expansion_never_aliases_preset_table():
    # gedi_vars_expand must hand out a copy of the `minimal` preset: the
    # essentials/flag completion mutates lists in place, and an alias would
    # rewrite _GEDI_MIN_VARS for the rest of the process.
    from gedih3.config import _GEDI_MIN_VARS
    from gedih3.gedidriver import gedi_vars_expand
    from gedih3.gh3builder import _ensure_download_essentials
    before = list(_GEDI_MIN_VARS['L4A'][2])
    pv = gedi_vars_expand({'L4A': ['minimal']}, version=2)
    assert pv['L4A'] is not _GEDI_MIN_VARS['L4A'][2]
    _ensure_download_essentials(pv, version=3)
    assert _GEDI_MIN_VARS['L4A'][2] == before
