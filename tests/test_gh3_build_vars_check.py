"""
Tests for ``explicit_vars_missing_in_sample`` — the pre-flight check that
catches typos in user-supplied variable lists before ``gh3_build`` starts.

Uses synthetic minimal-GEDI HDF5 fixtures (one BEAM group with a handful of
named datasets) so the check can be exercised without real NASA data.
"""

import os

import h5py
import numpy as np
import pytest

from gedih3.cli.gh3_build import explicit_vars_missing_in_sample


L2A_VARS = ('shot_number', 'rh_098', 'rh_050', 'lat_lowestmode', 'lon_lowestmode')
L4A_VARS = ('shot_number', 'agbd', 'agbd_se', 'lat_lowestmode', 'lon_lowestmode')


def _write_h5(path, var_names, beam='BEAM0000'):
    with h5py.File(path, 'w') as f:
        grp = f.create_group(beam)
        for v in var_names:
            grp.create_dataset(v, data=np.zeros(4, dtype=np.float64))


@pytest.fixture
def sample_files(tmp_path):
    l2a = str(tmp_path / 'l2a_sample.h5')
    l4a = str(tmp_path / 'l4a_sample.h5')
    _write_h5(l2a, L2A_VARS)
    _write_h5(l4a, L4A_VARS)
    return {'L2A': l2a, 'L4A': l4a}


class TestExplicitVarsCheck:

    def test_all_present_returns_empty(self, sample_files):
        product_vars = {'L2A': ['rh_098', 'rh_050'], 'L4A': ['agbd']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {}

    def test_single_typo_surfaced(self, sample_files):
        product_vars = {'L2A': ['rh_098', 'rh_TYPO']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {'L2A': ['rh_TYPO']}

    def test_multiple_typos_per_product(self, sample_files):
        product_vars = {'L2A': ['rh_098', 'bogus_one', 'bogus_two']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert set(result.keys()) == {'L2A'}
        assert set(result['L2A']) == {'bogus_one', 'bogus_two'}

    def test_typos_across_multiple_products(self, sample_files):
        product_vars = {
            'L2A': ['rh_098', 'l2a_phantom'],
            'L4A': ['agbd', 'l4a_phantom'],
        }
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {'L2A': ['l2a_phantom'], 'L4A': ['l4a_phantom']}

    def test_wildcard_matches_passes(self, sample_files):
        product_vars = {'L2A': ['rh_*']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {}

    def test_wildcard_matches_nothing_surfaced(self, sample_files):
        product_vars = {'L2A': ['nonexistent_*']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert 'L2A' in result
        assert len(result['L2A']) == 1
        assert 'nonexistent_*' in result['L2A'][0]

    def test_default_products_skipped(self, sample_files):
        # Even with a bogus name, products marked as `default` are skipped
        # here — those go through the static-manifest check (Stage 1).
        product_vars = {'L2A': ['this_would_be_a_typo']}
        result = explicit_vars_missing_in_sample(
            product_vars, {'L2A'}, sample_files,
        )
        assert result == {}

    def test_none_vars_skipped(self, sample_files):
        # vars=None encodes `*` / `all` — every variable in the HDF5.
        product_vars = {'L2A': None, 'L4A': ['agbd']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {}

    def test_empty_sample_short_circuits(self):
        product_vars = {'L2A': ['rh_098', 'bogus']}
        result = explicit_vars_missing_in_sample(product_vars, set(), {})
        assert result == {}

    def test_empty_product_vars_short_circuits(self, sample_files):
        result = explicit_vars_missing_in_sample({}, set(), sample_files)
        assert result == {}
        result = explicit_vars_missing_in_sample(None, set(), sample_files)
        assert result == {}

    def test_product_missing_from_sample_is_soft_skipped(self, sample_files):
        # User requested a product that isn't in the sample dict at all —
        # downstream gate handles product-presence; this helper should not
        # falsely report all requested vars as missing.
        product_vars = {'L2B': ['some_var']}
        result = explicit_vars_missing_in_sample(product_vars, set(), sample_files)
        assert result == {}

    def test_unreadable_h5_is_soft_skipped(self, tmp_path):
        # A corrupt sample file shouldn't block an otherwise-valid request.
        bad = tmp_path / 'corrupt.h5'
        bad.write_bytes(b'not an hdf5 file')
        product_vars = {'L2A': ['anything']}
        result = explicit_vars_missing_in_sample(
            product_vars, set(), {'L2A': str(bad)},
        )
        assert result == {}
