"""
Tests for ``_expand_product_vars``'s version-specific essentials/quality-flag
injection — specifically, that it never leaves a stale, other-version-only
variable name behind when a preset keyword (``minimal``/``default``) was
resolved under a provisional version before the real archive version was
known.

Regression scenario: ``H3BuildLogger.__init__`` resolves a preset keyword
using whatever version it has at construction time (falling back to 2 for a
fresh build with no explicit ``--gedi-version``). By the time
``_expand_product_vars`` runs with the real, auto-detected version, the
keyword is already gone -- replaced with concrete names -- so a plain
union/append of the correct-version essential/quality-flag name only adds it
alongside the stale one rather than replacing it. Since the real HDF5 reader
raises on any missing column with no partial-skip, that stale name fails
every beam of every orbit for that product, silently shrinking the database
relative to a build that resolved under the correct version from the start.
"""

from gedih3.logger import H3BuildLogger
from gedih3.gh3builder import _expand_product_vars


class TestExpandProductVarsPurgesStaleVersionNames:

    def test_minimal_l2b_drops_stale_v2_flag_keeps_v3(self):
        h3_logger = H3BuildLogger({'L2B': ['minimal']}, dir='/tmp/_test_expand_minimal_l2b')
        # Fallback resolution (no explicit --gedi-version) baked in the v2 name.
        assert 'l2b_quality_flag' in h3_logger.product_vars['L2B']

        expanded = _expand_product_vars(dict(h3_logger.product_vars), soc_files=[], version=3)
        assert 'l2b_quality_flag' not in expanded['L2B']
        assert 'l2b_quality_flag_rel3' in expanded['L2B']

    def test_default_l2b_drops_stale_v2_flag_keeps_v3(self):
        h3_logger = H3BuildLogger({'L2B': ['default']}, dir='/tmp/_test_expand_default_l2b')
        assert 'l2b_quality_flag' in h3_logger.product_vars['L2B']

        expanded = _expand_product_vars(dict(h3_logger.product_vars), soc_files=[], version=3)
        assert 'l2b_quality_flag' not in expanded['L2B']
        assert 'l2b_quality_flag_rel3' in expanded['L2B']

    def test_l2a_essentials_drop_stale_v2_name_keep_shared_flag(self):
        h3_logger = H3BuildLogger({'L2A': ['minimal']}, dir='/tmp/_test_expand_l2a_essentials')
        assert 'quality_flag' in h3_logger.product_vars['L2A']

        expanded = _expand_product_vars(dict(h3_logger.product_vars), soc_files=[], version=3)
        assert 'quality_flag' not in expanded['L2A']
        assert 'l2a_quality_flag_rel3' in expanded['L2A']
        # degrade_flag is identical across v2/v3 -- must survive the purge.
        assert 'degrade_flag' in expanded['L2A']

    def test_l4a_quality_flag_purge_keeps_version_specific_extra_flag(self):
        # v3 adds elev_highestreturn_outlier_flag with no v2 equivalent --
        # a pure addition, not a rename, and must not be affected by the purge.
        h3_logger = H3BuildLogger({'L4A': ['minimal']}, dir='/tmp/_test_expand_l4a')
        assert 'l4_quality_flag' in h3_logger.product_vars['L4A']

        expanded = _expand_product_vars(dict(h3_logger.product_vars), soc_files=[], version=3)
        assert 'l4_quality_flag' not in expanded['L4A']
        assert 'l4a_quality_flag_rel3' in expanded['L4A']
        assert 'elev_highestreturn_outlier_flag' in expanded['L4A']

    def test_no_stale_name_introduced_when_resolved_under_correct_version(self):
        # Sanity: when the keyword resolves under the correct version from
        # the start (explicit --gedi-version, or matching the fallback),
        # the purge must not remove anything that belongs there.
        h3_logger = H3BuildLogger({'L2B': ['minimal']}, version=2, dir='/tmp/_test_expand_correct_version')
        expanded = _expand_product_vars(dict(h3_logger.product_vars), soc_files=[], version=2)
        assert 'l2b_quality_flag' in expanded['L2B']
        assert 'l2b_quality_flag_rel3' not in expanded['L2B']

    def test_explicit_unrelated_variable_untouched_by_purge(self):
        # An explicit, non-preset variable list that happens to sit alongside
        # a real product with quality-flag injection should keep its own
        # requested names -- the purge only ever targets known essential/
        # quality-flag names from _GEDI_L2A_ESSENTIALS / _PRODUCT_QUALITY_FLAGS.
        product_vars = {'L2B': ['cover', 'pai']}
        expanded = _expand_product_vars(product_vars, soc_files=[], version=3)
        assert 'cover' in expanded['L2B']
        assert 'pai' in expanded['L2B']
        assert 'l2b_quality_flag_rel3' in expanded['L2B']
