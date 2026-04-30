"""Tests for SOC file discovery: exclude patterns + per-file tolerance."""

import os
import tempfile

import pytest

from gedih3.gedidriver import soc_file_tree, check_soc_file_vars


# Filenames that follow the canonical GEDI release naming convention.
_RELEASE_NAMES = [
    'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
]
# Internal SGS variants — same orbit/track as a separate release pair so the
# pivot in soc_file_tree distinguishes them by the orb_track key.
_SGS_NAMES = [
    'GEDI02_A_2019108002012_O01957_03_T03910_02_003_01_V003_SGS.h5',
    'GEDI02_B_2019108002012_O01957_03_T03910_02_003_01_V003_SGS.h5',
]
# Internal-but-usable algorithm variants the user wants kept.
_ALGS_NAMES = [
    'GEDI02_A_2019108002012_O01958_03_T03911_02_003_01_V003_7algs.h5',
    'GEDI02_B_2019108002012_O01958_03_T03911_02_003_01_V003_7algs.h5',
]


@pytest.fixture
def soc_tree_with_variants(tmp_path):
    """A SOC dir containing one release granule, one SGS granule, one 7algs granule."""
    for n in _RELEASE_NAMES + _SGS_NAMES + _ALGS_NAMES:
        (tmp_path / n).touch()
    return str(tmp_path)


def test_soc_file_tree_default_includes_all_variants(soc_tree_with_variants):
    tree = soc_file_tree(soc_tree_with_variants, to_list=True)
    assert len(tree) == 3, "Without exclude, all three granule pairs should be discovered"


def test_soc_file_tree_exclude_drops_sgs_keeps_algs(soc_tree_with_variants):
    """SGS-only exclusion must not affect 7algs files — the user's key invariant."""
    tree = soc_file_tree(soc_tree_with_variants, to_list=True, exclude=['*_SGS.h5'])
    assert len(tree) == 2
    kept = sorted(os.path.basename(v) for d in tree for v in d.values())
    assert any('_7algs.h5' in n for n in kept), "7algs files must survive *_SGS.h5 exclusion"
    assert not any('_SGS.h5' in n for n in kept), "SGS files must be excluded"


def test_soc_file_tree_exclude_pattern_with_no_matches_is_noop(soc_tree_with_variants):
    tree = soc_file_tree(soc_tree_with_variants, to_list=True,
                         exclude=['*_SGS.h5', '*_BETA.h5'])
    assert len(tree) == 2, "BETA pattern matches nothing; result identical to *_SGS.h5 alone"


def test_soc_file_tree_exclude_multiple_patterns(soc_tree_with_variants):
    tree = soc_file_tree(soc_tree_with_variants, to_list=True,
                         exclude=['*_SGS.h5', '*_7algs.h5'])
    assert len(tree) == 1, "Both internal variants excluded; only release pair remains"
    kept = sorted(os.path.basename(v) for d in tree for v in d.values())
    assert all('_V003.h5' in n and '_SGS' not in n and '_7algs' not in n for n in kept)


def test_check_soc_file_vars_tolerates_unreadable_file():
    """A single broken HDF5 must not abort the validation bag.

    Core contract: an unreadable file yields an empty product map and
    does NOT raise — that's what keeps the validation bag alive when
    one granule is truncated. (A WARNING is also logged; that side
    effect isn't asserted here because the project's logger routes
    around pytest's capture machinery.)
    """
    with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as f:
        # HDF5 magic header followed by garbage — passes the extension test
        # but fails to open as a real HDF5 file.
        f.write(b'\x89HDF\r\n\x1a\n' + b'\x00' * 100)
        fpath = f.name

    try:
        out = check_soc_file_vars({'L2A': fpath}, {'L2A': set(), 'L2B': set()})
        assert out == {}, "Truncated file must yield an empty product map, not raise"
    finally:
        os.unlink(fpath)
