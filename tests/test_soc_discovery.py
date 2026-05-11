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


# ── gedi_vars_static + SOC manifest sentinel ──────────────────────────────


def test_gedi_vars_static_matches_default_vars_file_minus_comments():
    """The static helper should expose every uncommented variable line in
    the shipped manifest — and only those (the ``#`` comment lines are
    filtered because the result is consumed as a literal variable-name
    list, not a membership-check set)."""
    from gedih3.gedidriver import gedi_vars_static
    for prod in ('L2A', 'L2B', 'L4A'):
        vars_list = gedi_vars_static(prod, version=3)
        assert vars_list is not None and len(vars_list) > 0
        with open(get_default_vars_file(prod, version=3)) as f:
            expected = [ln.strip() for ln in f
                        if ln.strip() and not ln.startswith('#')]
        assert vars_list == expected


def test_gedi_vars_static_returns_none_for_unknown_version():
    """No static manifest for an out-of-band version → helper signals the
    caller (which should fall back to gedi_vars_from_h5)."""
    from gedih3.gedidriver import gedi_vars_static
    assert gedi_vars_static('L2A', version=99) is None


def test_soc_file_tree_prefers_manifest_when_present(tmp_path):
    """When ``_soc_manifest.txt`` is present at the SOC root,
    soc_file_tree must read from it instead of recursive-globbing.

    Crucially the marker must be a *paired* (L2A + L2B) granule so
    that the live recursive-glob path would survive the
    ``pivot_table().dropna()`` step inside ``soc_file_tree`` —
    otherwise an L2A-only marker is silently dropped on either
    code path, and the test passes vacuously without proving that
    the manifest is actually preferred.
    """
    from gedih3.gedidriver import soc_file_tree, write_soc_manifest
    from gedih3.config import SOC_MANIFEST_FILENAME
    # Two paired release files (granule with both L2A and L2B)
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]:
        (tmp_path / n).touch()
    n_written = write_soc_manifest(str(tmp_path))
    assert n_written == 2
    assert (tmp_path / SOC_MANIFEST_FILENAME).exists()

    # Drop a paired marker that the live recursive glob would discover
    # AND that survives the pivot+dropna() inside soc_file_tree (both
    # L2A and L2B for the same orbit/track). The manifest does NOT list
    # it, so if soc_file_tree reads the manifest it must not appear.
    for n in [
        'GEDI02_A_2025001000000_O99999_99_T99999_99_999_99_V003.h5',
        'GEDI02_B_2025001000000_O99999_99_T99999_99_999_99_V003.h5',
    ]:
        (tmp_path / n).touch()
    tree = soc_file_tree(str(tmp_path), to_list=True)
    seen = sorted(os.path.basename(v) for d in tree for v in d.values())
    assert all('O99999' not in n for n in seen), \
        "soc_file_tree should read the manifest, not the live SOC tree"
    # Sanity: the manifest-listed pair IS visible.
    assert any('O01956' in n for n in seen), \
        "manifest-listed granule must still be discovered"


def test_soc_file_tree_falls_back_to_glob_without_manifest(tmp_path):
    """Without a manifest, soc_file_tree must still find files via glob
    (preserves backward compatibility for SOC trees that predate the
    manifest sentinel or live on read-only filesystems)."""
    from gedih3.gedidriver import soc_file_tree
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]:
        (tmp_path / n).touch()
    tree = soc_file_tree(str(tmp_path), to_list=True)
    assert len(tree) == 1, "Glob fallback must still discover the granule pair"


def test_write_soc_manifest_returns_zero_for_empty_dir(tmp_path):
    """No GEDI files → manifest is not written and count is 0."""
    from gedih3.gedidriver import write_soc_manifest
    from gedih3.config import SOC_MANIFEST_FILENAME
    n = write_soc_manifest(str(tmp_path))
    assert n == 0
    assert not (tmp_path / SOC_MANIFEST_FILENAME).exists()


def test_write_soc_manifest_writes_atomically(tmp_path):
    """The SOC manifest write must be atomic (.tmp + os.replace) so a
    SIGKILL between truncate and final write never leaves a partial
    or empty manifest at the final path. Now that
    ``write_soc_manifest`` delegates to the shared
    ``utils.generate_manifest`` (which uses AtomicFileWriter), the
    contract is shared with the H3 manifest path."""
    from gedih3.gedidriver import write_soc_manifest
    from gedih3.config import SOC_MANIFEST_FILENAME
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]:
        (tmp_path / n).touch()
    n = write_soc_manifest(str(tmp_path))
    assert n == 2
    manifest = tmp_path / SOC_MANIFEST_FILENAME
    assert manifest.exists()
    # No leftover .tmp sibling
    assert not (tmp_path / (SOC_MANIFEST_FILENAME + '.tmp')).exists()
    # Manifest content has both granule files
    lines = manifest.read_text().strip().split('\n')
    assert len(lines) == 2


def test_read_manifest_supports_filename_kwarg(tmp_path):
    """The shared ``utils._read_manifest`` accepts a custom filename so
    the SOC manifest path uses the same primitive as the H3 manifest.
    Verifies the cache key is also keyed on (root, filename) so
    different manifests at the same root don't collide."""
    from gedih3 import utils as u
    from gedih3.config import MANIFEST_FILENAME, SOC_MANIFEST_FILENAME

    h3_manifest = tmp_path / MANIFEST_FILENAME
    soc_manifest = tmp_path / SOC_MANIFEST_FILENAME
    h3_manifest.write_text("h3_03=8c2a/2020/data.parquet\n")
    soc_manifest.write_text("2019/108/GEDI02_A_test.h5\n")

    # Clear any cached entries for this tmp path before testing
    u._manifest_cache.clear()

    h3_lines = u._read_manifest(str(tmp_path))  # default = MANIFEST_FILENAME
    soc_lines = u._read_manifest(str(tmp_path),
                                 manifest_filename=SOC_MANIFEST_FILENAME)
    assert h3_lines == ['h3_03=8c2a/2020/data.parquet']
    assert soc_lines == ['2019/108/GEDI02_A_test.h5']
    # Both cached separately
    assert (str(tmp_path), MANIFEST_FILENAME) in u._manifest_cache
    assert (str(tmp_path), SOC_MANIFEST_FILENAME) in u._manifest_cache
