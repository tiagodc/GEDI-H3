"""Tests for SOC file discovery: exclude patterns + per-file tolerance + manifest validation."""

import os
import tempfile

import pytest

from gedih3.gedidriver import soc_file_tree, check_soc_file_vars, validate_soc_files
from gedih3.config import get_default_vars_file


# ``soc_file_tree``'s no-manifest fallback now uses ``walk_soc_parallel``
# (per the R2 + always-parallel refactor), which requires a registered
# dask Client. Module-scope autouse fixture so every test in this file
# has one without needing to declare it individually. The cluster is
# tiny + threaded to stay cheap.
@pytest.fixture(scope='module', autouse=True)
def _module_dask_client():
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1,
        processes=False, dashboard_address=None, silence_logs='ERROR',
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


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

    # available_products must reflect the uncommented manifest line counts
    # exactly: ``validate_soc_files`` strips ``#``-prefixed lines (the
    # commented entries are documentation, not a membership set — keeping
    # them would silently fail set-comparison against literal user requests).
    for prod in ('L2A', 'L2B'):
        with open(get_default_vars_file(prod, version=3)) as f:
            expected = {ln.strip() for ln in f
                        if ln.strip() and not ln.startswith('#')}
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


def test_soc_file_tree_ignores_stale_manifest(tmp_path):
    """``soc_file_tree`` must NOT trust ``_soc_manifest.txt`` — external
    population paths (manual rsync, NASA delivery) bypass the
    producer-driven refresh and a stale manifest would silently narrow
    every downstream scan. The read path was removed; the canonical
    discovery method is now an always-on parallel walk.

    Setup: write a manifest that lists only one paired granule, then
    drop a second paired granule on disk without refreshing the
    manifest. The newly-dropped granule MUST appear in the result —
    proving the walk wins over the (stale) manifest.
    """
    from gedih3.gedidriver import soc_file_tree, write_soc_manifest
    from gedih3.config import SOC_MANIFEST_FILENAME
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V003.h5',
    ]:
        (tmp_path / n).touch()
    n_written = write_soc_manifest(str(tmp_path))
    assert n_written == 2
    assert (tmp_path / SOC_MANIFEST_FILENAME).exists()

    # Drop a second paired granule WITHOUT refreshing the manifest
    # (mimics an out-of-band rsync). The stale manifest does not list
    # it; the always-on walk must still surface it.
    for n in [
        'GEDI02_A_2025001000000_O99999_99_T99999_99_999_99_V003.h5',
        'GEDI02_B_2025001000000_O99999_99_T99999_99_999_99_V003.h5',
    ]:
        (tmp_path / n).touch()
    tree = soc_file_tree(str(tmp_path), to_list=True)
    seen = sorted(os.path.basename(v) for d in tree for v in d.values())
    assert any('O99999' in n for n in seen), \
        "soc_file_tree must walk the live SOC tree, not the stale manifest"
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


# ── Regime-aware validator gating (H3BuildLogger.default_products) ────────
#
# These tests exist to ensure the regime gate in gh3_build.py never
# regresses to the v0.9.0 behavior of validating a literal expanded
# variable list from the build log against the currently-shipped static
# manifest. The bug: a database built when the manifest was broader,
# resumed after the manifest was slimmed, reported every drifted variable
# as "missing" even though the actual HDF5 files contained it.
#
# Contract under test:
#   - default_products captures *only* products requested via the literal
#     "default"/"def" keyword, before gedi_vars_expand replaces the
#     keyword with the expanded list.
#   - The keyword does not leak into the persisted log (the log always
#     records the post-expansion literal list).
#   - validate_soc_files strips ``#``-prefixed manifest lines, so a
#     commented-out entry is treated as absent — preventing the original
#     bug class from re-emerging if a future caller forgets the regime gate.


def test_default_products_captured_for_default_keyword(tmp_path):
    """H3BuildLogger captures products whose value was the literal
    ``default``/``def`` keyword. Captured *before* gedi_vars_expand
    runs — otherwise the keyword is already replaced by an expanded list
    and the bit is lost."""
    from gedih3.logger import H3BuildLogger
    logger = H3BuildLogger(
        product_vars={'L2A': ['default'], 'L4A': ['def']},
        dir=str(tmp_path), version=3,
    )
    assert logger.default_products == {'L2A', 'L4A'}
    # After init, product_vars must be the expanded literal list — the
    # keyword is gone.
    for prod in ('L2A', 'L4A'):
        assert 'default' not in logger.product_vars[prod]
        assert 'def' not in logger.product_vars[prod]
        assert len(logger.product_vars[prod]) > 1


def test_default_products_empty_for_explicit_list(tmp_path):
    """Explicit variable lists do not set default_products — the gate
    must distinguish ``-l2a default`` from ``-l2a rh agbd ...`` so that
    Regime D (explicit-list resume) bypasses manifest consultation."""
    from gedih3.logger import H3BuildLogger
    logger = H3BuildLogger(
        product_vars={'L2A': ['rh', 'sensitivity']},
        dir=str(tmp_path), version=3,
    )
    assert logger.default_products == set()


def test_default_products_empty_for_no_product_vars(tmp_path):
    """A bare resume (no CLI product args) leaves default_products empty
    and the validator gate evaluates to false — Regimes B and C with
    granules-only update bypass manifest consultation."""
    from gedih3.logger import H3BuildLogger
    logger = H3BuildLogger(product_vars=None, dir=str(tmp_path), version=3)
    assert logger.default_products == set()


def test_default_products_not_persisted_to_log(tmp_path):
    """default_products is a runtime-only flag — never written to the
    build log. The persisted log stores the resolved literal list so
    Regime B resumes work without re-consulting the manifest."""
    import json
    from gedih3.logger import H3BuildLogger
    h3_logger = H3BuildLogger(
        product_vars={'L2A': ['default']},
        dir=str(tmp_path), version=3,
    )
    h3_logger.save_log('PROCESSING')
    log_path = tmp_path / 'gedih3_build_log.json'
    on_disk = json.loads(log_path.read_text())
    assert 'default_products' not in on_disk
    assert 'default' not in str(on_disk.get('products', {}))


def test_manifest_check_scope_regime_a_fresh_build_with_default(tmp_path):
    """Regime A: fresh build with ``default`` for some product → scope
    includes that product."""
    from gedih3.logger import H3BuildLogger
    from gedih3.cli.gh3_build import manifest_check_scope
    h3_logger = H3BuildLogger(
        product_vars={'L2A': ['default'], 'L4A': ['rh']},
        dir=str(tmp_path), version=3,
    )
    scope = manifest_check_scope(h3_logger, h3_logger.product_vars)
    assert set(scope) == {'L2A'}, "Only the default-requested product is in scope"


def test_manifest_check_scope_regime_b_granules_only_resume(tmp_path):
    """Regime B: resume with no schema change → empty scope (log is
    authoritative; manifest must NOT be consulted)."""
    from gedih3.logger import H3BuildLogger
    from gedih3.cli.gh3_build import manifest_check_scope
    # Seed an existing build log
    seed = H3BuildLogger(
        product_vars={'L2A': ['default']},
        dir=str(tmp_path), version=3,
    )
    seed.save_log('COMPLETED')
    # Resume with no CLI product args (granules-only update)
    h3_logger = H3BuildLogger(product_vars=None, dir=str(tmp_path), version=3)
    assert h3_logger.updating
    assert h3_logger.new_product_vars is None
    scope = manifest_check_scope(h3_logger, h3_logger.product_vars)
    assert scope == {}, "Granules-only resume must skip manifest validation"


def test_manifest_check_scope_regime_d_explicit_list_resume(tmp_path):
    """Regime D: resume with an explicit non-default var list added →
    empty scope (the user typed literal names; manifest is not the
    contract)."""
    from gedih3.logger import H3BuildLogger
    from gedih3.cli.gh3_build import manifest_check_scope
    seed = H3BuildLogger(
        product_vars={'L2A': ['rh']},
        dir=str(tmp_path), version=3,
    )
    seed.save_log('COMPLETED')
    # Resume with explicit-list expansion
    h3_logger = H3BuildLogger(
        product_vars={'L2A': ['rh', 'sensitivity']},
        dir=str(tmp_path), version=3,
    )
    assert h3_logger.updating
    assert h3_logger.new_product_vars is not None  # there IS a delta
    assert h3_logger.default_products == set()  # but not via default
    scope = manifest_check_scope(h3_logger, h3_logger.product_vars)
    assert scope == {}, "Explicit-list resume must skip manifest validation"


def test_manifest_check_scope_regime_c_default_reexpansion(tmp_path):
    """Regime C: resume where the user re-requests ``default`` for a
    product → scope is exactly that product. This is the only resume
    case where the manifest is the contract."""
    from gedih3.logger import H3BuildLogger
    from gedih3.cli.gh3_build import manifest_check_scope
    # Seed a DB whose L2A var list is a strict subset of `default`
    seed = H3BuildLogger(
        product_vars={'L2A': ['rh']},
        dir=str(tmp_path), version=3,
    )
    seed.save_log('COMPLETED')
    # User re-requests `default` for L2A
    h3_logger = H3BuildLogger(
        product_vars={'L2A': ['default']},
        dir=str(tmp_path), version=3,
    )
    assert h3_logger.default_products == {'L2A'}
    assert h3_logger.new_product_vars is not None
    assert 'L2A' in h3_logger.new_product_vars
    scope = manifest_check_scope(h3_logger, h3_logger.product_vars)
    assert set(scope) == {'L2A'}


def test_manifest_check_scope_resume_with_drifted_log_var(tmp_path):
    """The original bug: log records a var that the current static
    manifest does not list. On a granules-only resume the gate must
    refuse to call the validator at all — the log is authoritative.
    """
    from gedih3.logger import H3BuildLogger
    from gedih3.cli.gh3_build import manifest_check_scope
    # Seed a DB whose log contains a variable that does NOT exist in the
    # current shipped v3 manifest (simulates manifest drift between
    # package versions).
    seed = H3BuildLogger(
        product_vars={'L2A': ['rh', 'this_var_is_not_in_manifest_anymore']},
        dir=str(tmp_path), version=3,
    )
    seed.save_log('COMPLETED')
    # User resumes with no product args (typical "build new granules" run).
    h3_logger = H3BuildLogger(product_vars=None, dir=str(tmp_path), version=3)
    assert h3_logger.updating
    assert 'this_var_is_not_in_manifest_anymore' in h3_logger.product_vars['L2A']
    scope = manifest_check_scope(h3_logger, h3_logger.product_vars)
    assert scope == {}, (
        "Resume with drifted log var must bypass the static-manifest "
        "validator; otherwise it would falsely flag the var as missing."
    )


@pytest.fixture
def soc_tree_two_v2_release_files(tmp_path):
    """V002-named release fixture for tests that need to read the v2
    static manifest (which retains commented ``_a10`` algorithm
    variants — the exact case that triggered the original bug)."""
    for n in [
        'GEDI02_A_2019108002012_O01956_03_T03909_02_003_01_V002.h5',
        'GEDI02_B_2019108002012_O01956_03_T03909_02_003_01_V002.h5',
    ]:
        (tmp_path / n).touch()
    return str(tmp_path)


def test_validate_soc_files_strips_commented_manifest_lines(soc_tree_two_v2_release_files):
    """Regression for the bug that motivated the regime-gating refactor.

    A variable that is *commented out* (``#``-prefixed) in the shipped
    manifest must be treated as absent — not as present-with-comment-
    marker — so that a literal user request that happens to match the
    commented text never silently set-mismatches against the available
    set. The manifest had previously kept ``# foo`` in the available
    set; a request for ``foo`` would then be flagged as missing because
    ``'foo' != '# foo'``.
    """
    # Find a commented variable for L2A v2. The v2 manifest commentifies
    # `_a10` algorithm variants — the exact entries that triggered the
    # original false-negative on the user's resume case.
    with open(get_default_vars_file('L2A', version=2)) as f:
        commented = [ln.strip().lstrip('# ').strip() for ln in f
                     if ln.strip().startswith('#')]
    if not commented:
        pytest.skip("L2A v2 manifest has no commented entries to test against")
    commented_var = commented[0]
    report = validate_soc_files(
        {'L2A': [commented_var]},
        soc_tree_two_v2_release_files, version=2,
    )
    # The variable is commented in the manifest → treated as absent →
    # flagged in missing_variables. (This is the *correct* behavior for
    # the validator in isolation; the regime gate in gh3_build.py
    # decides whether to call the validator at all.)
    assert report['can_skip'] is False
    assert commented_var in report['missing_variables'].get('L2A', [])


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
