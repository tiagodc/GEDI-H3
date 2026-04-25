"""Backwards-compat tests for H3BuildLogger after the per-product status extension.

The contract:
  - Loading a legacy log (no per-granule ``products`` field) must succeed and
    behave identically to the pre-change loader from the perspective of the
    existing API (``get_finished_granules``, ``is_up_to_date``, ``to_dict``).
  - Lazy upgrade only mutates in-memory state; the on-disk file is unchanged
    until ``save_log()`` runs.
  - Writing then reading back the log preserves all data, including the new
    ``products`` field.
  - The new helpers (``get_granule_product_status``, ``get_product_gaps``,
    ``mark_granule_product``) work on both legacy-then-upgraded and fresh logs.
"""

import json
import os

import pytest

from gedih3.logger import (
    H3BuildLogger,
    PRODUCT_STATUS_INDEXED,
    PRODUCT_STATUS_PARTIAL_NAN,
    PRODUCT_STATUS_PENDING,
)


def _legacy_log_dict():
    return {
        'gedi_version': 2,
        'h3_resolution_level': 12,
        'h3_partition_level': 3,
        'status': 'COMPLETED',
        'spatial_filter': None,
        'temporal_filter': None,
        'products': {
            'L2A': {'variables': ['rh_98'], 'status': 'COMPLETED'},
            'L4A': {'variables': ['agbd'], 'status': 'COMPLETED'},
        },
        'granules': [
            {'orbit': 100, 'granule': 1, 'track': 50, 'status': 'INDEXED'},
            {'orbit': 200, 'granule': 1, 'track': 50, 'status': 'PENDING'},
        ],
        'h3_partition_ids': ['8c2a'],
    }


def _write_log(tmp_path, payload):
    path = os.path.join(tmp_path, 'gedih3_build_log.json')
    with open(path, 'w') as f:
        json.dump(payload, f)
    return path


def test_legacy_log_loads_unchanged_on_disk(tmp_dir):
    path = _write_log(tmp_dir, _legacy_log_dict())
    before = open(path).read()

    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert h.granule_info[0]['products'] == {'L2A': 'INDEXED', 'L4A': 'INDEXED'}
    assert h.granule_info[1]['products'] == {'L2A': 'PENDING', 'L4A': 'PENDING'}

    after = open(path).read()
    assert before == after, "loading should not mutate the on-disk file"


def test_legacy_log_save_roundtrip_preserves_data(tmp_dir):
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    h.save_log('COMPLETED')

    h2 = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert {(g['orbit'], g['granule'], g['track']) for g in h2.granule_info} == \
        {(100, 1, 50), (200, 1, 50)}
    # After save, the products field is on-disk; load returns it as-is (no upgrade).
    assert h2.granule_info[0]['products'] == {'L2A': 'INDEXED', 'L4A': 'INDEXED'}


def test_unknown_keys_are_ignored_by_existing_readers(tmp_dir):
    """An old reader using .get('products', None) must still work."""
    payload = _legacy_log_dict()
    payload['granules'][0]['products'] = {'L2A': 'INDEXED', 'L4A': 'PARTIAL_NAN'}
    payload['granules'][0]['some_future_key'] = {'unknown': True}
    path = _write_log(tmp_dir, payload)

    # Simulate old reader pattern.
    data = json.load(open(path))
    granule = data['granules'][0]
    # Old code used g['status'], not g.get('products'); confirm that still works.
    assert granule['status'] == 'INDEXED'

    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert h.granule_info[0]['products'] == {'L2A': 'INDEXED', 'L4A': 'PARTIAL_NAN'}


def test_get_finished_granules_matches_legacy_semantics(tmp_dir):
    """The skip-list returned by get_finished_granules must be unchanged."""
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    skip = h.get_finished_granules()
    # Strip status; only INDEXED should appear.
    assert skip == [{'orbit': 100, 'granule': 1, 'track': 50}]


def test_is_up_to_date_unaffected_by_per_product_field(tmp_dir):
    """Adding the products field to granule entries must not flip is_up_to_date."""
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    # All-INDEXED + no new spatial/temporal/products should be up-to-date.
    # However, the pending granule (200,1,50) has status=PENDING, so this is
    # not up-to-date. That matches legacy behavior — ensure we haven't drifted.
    assert h.is_up_to_date() is False


def test_is_up_to_date_true_when_all_indexed(tmp_dir):
    payload = _legacy_log_dict()
    for g in payload['granules']:
        g['status'] = 'INDEXED'
    _write_log(tmp_dir, payload)
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert h.is_up_to_date() is True


def test_get_product_gaps_finds_missing_products(tmp_dir):
    payload = _legacy_log_dict()
    payload['granules'][0]['products'] = {'L2A': 'INDEXED', 'L4A': 'PARTIAL_NAN'}
    payload['granules'][1]['products'] = {'L2A': 'INDEXED', 'L4A': 'INDEXED'}
    payload['granules'][1]['status'] = 'INDEXED'
    _write_log(tmp_dir, payload)

    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    gaps = h.get_product_gaps()
    assert len(gaps) == 1
    gran, missing = gaps[0]
    assert gran == {'orbit': 100, 'granule': 1, 'track': 50}
    assert missing == ['L4A']


def test_mark_granule_product_updates_in_memory(tmp_dir):
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert h.mark_granule_product({'orbit': 100, 'granule': 1, 'track': 50}, 'L4A', 'INDEXED')
    assert h.get_granule_product_status((100, 1, 50), 'L4A') == 'INDEXED'


def test_mark_granule_product_rejects_invalid_status(tmp_dir):
    from gedih3.exceptions import GediValidationError
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    with pytest.raises(GediValidationError):
        h.mark_granule_product({'orbit': 100, 'granule': 1, 'track': 50}, 'L4A', 'BOGUS_STATUS')


def test_mark_granule_product_returns_false_for_unknown_granule(tmp_dir):
    _write_log(tmp_dir, _legacy_log_dict())
    h = H3BuildLogger(product_vars=None, dir=tmp_dir)
    assert h.mark_granule_product({'orbit': 999, 'granule': 9, 'track': 99}, 'L4A', 'INDEXED') is False
