"""Tests for SOC file discovery: exclude patterns + per-file tolerance + manifest validation."""

import os
import tempfile

import pytest

from gedih3.gedidriver import soc_file_tree, check_soc_file_vars, validate_soc_files
from gedih3.config import get_default_vars_file


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


# ── validate_soc_files (manifest-based) ────────────────────────────────────

@pytest.fixture
def soc_tree_two_release_files(tmp_path):
    """A SOC dir with one empty L2A and one empty L2B release file (V003)."""
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]:
        (tmp_path / n).touch()
    return str(tmp_path)


def test_validate_soc_files_uses_static_manifest(soc_tree_two_release_files):
    """Happy path: variables present in the manifest pass; report.can_skip is True."""
    report = validate_soc_files(
        {'L2A': ['rh'], 'L2B': ['cover']},
        soc_tree_two_release_files, version=3,
    )
    assert report['can_skip'] is True
    assert report['missing_products'] == []
    assert report['missing_variables'] == {}

    # available_products must reflect the manifest line counts exactly
    for prod in ('L2A', 'L2B'):
        with open(get_default_vars_file(prod, version=3)) as f:
            expected = {ln.strip() for ln in f if ln.strip()}
        assert set(report['available_products'][prod]) == expected


def test_validate_soc_files_detects_typoed_variable(soc_tree_two_release_files):
    """A typoed variable name lands in missing_variables and trips can_skip."""
    report = validate_soc_files(
        {'L2A': ['rh098_typo']},
        soc_tree_two_release_files, version=3,
    )
    assert report['can_skip'] is False
    assert report['missing_variables'] == {'L2A': ['rh098_typo']}
    assert 'Missing variables in L2A' in report.get('error_msg', '')


def test_validate_soc_files_completes_quickly(soc_tree_two_release_files):
    """Sanity: manifest lookup must finish in well under a second.

    (The point of this rewrite is that this used to take ~30 min when
    backed by a per-file HDF5 scan.)
    """
    import time
    t0 = time.time()
    validate_soc_files(
        {'L2A': ['rh'], 'L2B': ['cover']},
        soc_tree_two_release_files, version=3,
    )
    elapsed = time.time() - t0
    assert elapsed < 1.0, f"Manifest validation should be near-instant, took {elapsed:.2f}s"


def test_validate_soc_files_empty_directory_returns_error_tuple(tmp_path):
    """No files in the SOC dir → (False, {error: ...}) early return; no crash."""
    result = validate_soc_files({'L2A': ['rh']}, str(tmp_path), version=3)
    assert isinstance(result, tuple)
    assert result[0] is False
    assert 'No SOC files found' in result[1]['error']
