"""H3 level mismatch guard on H3BuildLogger resume.

Contract: changing h3_resolution_level or h3_partition_level between the
existing build log and a re-invocation of ``gh3_build`` would silently
corrupt the database layout (partition directory names + per-shot index
column are baked from these levels). The guard mirrors the existing
``gedi_version`` mismatch check — raise GediValidationError, never
silently override.

Argparse defaults are ``None`` so naked-resume on a non-default database
(e.g. one built with -h3r 9 -h3p 2) does not falsely trigger the guard.
"""

import pytest

from gedih3.logger import H3BuildLogger
from gedih3.exceptions import GediValidationError


class TestFreshBuildDefaults:

    def test_none_falls_back_to_canonical_12_3(self, tmp_path):
        lg = H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=None, part=None,
            dir=str(tmp_path),
        )
        assert lg.res == 12
        assert lg.part == 3
        assert lg.updating is False

    def test_explicit_values_honored_on_fresh_build(self, tmp_path):
        lg = H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=9, part=2,
            dir=str(tmp_path),
        )
        assert lg.res == 9
        assert lg.part == 2

    def test_partial_explicit_one_only(self, tmp_path):
        # Only res passed, part defaults
        lg = H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=15, part=None,
            dir=str(tmp_path),
        )
        assert lg.res == 15
        assert lg.part == 3


class TestNakedResumeLoadsFromLog:

    def test_naked_resume_recovers_log_levels(self, tmp_path):
        H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=9, part=2,
            dir=str(tmp_path),
        ).save_log('COMPLETED')

        lg = H3BuildLogger(
            product_vars=None, res=None, part=None,
            dir=str(tmp_path),
        )
        assert lg.res == 9
        assert lg.part == 2
        assert lg.updating is True

    def test_naked_resume_on_default_db(self, tmp_path):
        # Default levels round-trip correctly through naked resume
        H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=None, part=None,
            dir=str(tmp_path),
        ).save_log('COMPLETED')

        lg = H3BuildLogger(
            product_vars=None, res=None, part=None,
            dir=str(tmp_path),
        )
        assert lg.res == 12
        assert lg.part == 3


class TestResumeMismatchRaises:

    def _make_log(self, tmp_path, res, part):
        H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=res, part=part,
            dir=str(tmp_path),
        ).save_log('COMPLETED')

    def test_res_mismatch_raises(self, tmp_path):
        self._make_log(tmp_path, res=9, part=2)
        with pytest.raises(GediValidationError) as exc:
            H3BuildLogger(
                product_vars=None, res=12, part=2,
                dir=str(tmp_path),
            )
        msg = str(exc.value)
        assert 'H3 resolution mismatch' in msg
        assert 'h3_resolution_level=9' in msg
        assert '-h3r 12' in msg

    def test_part_mismatch_raises(self, tmp_path):
        self._make_log(tmp_path, res=9, part=2)
        with pytest.raises(GediValidationError) as exc:
            H3BuildLogger(
                product_vars=None, res=9, part=3,
                dir=str(tmp_path),
            )
        msg = str(exc.value)
        assert 'H3 partition mismatch' in msg
        assert 'h3_partition_level=2' in msg
        assert '-h3p 3' in msg

    def test_both_mismatched_raises_on_resolution_first(self, tmp_path):
        # The res check runs first; the error message should reflect
        # that — operator fixes one thing at a time.
        self._make_log(tmp_path, res=9, part=2)
        with pytest.raises(GediValidationError) as exc:
            H3BuildLogger(
                product_vars=None, res=10, part=4,
                dir=str(tmp_path),
            )
        assert 'H3 resolution mismatch' in str(exc.value)

    def test_matching_resume_no_raise(self, tmp_path):
        self._make_log(tmp_path, res=9, part=2)
        lg = H3BuildLogger(
            product_vars=None, res=9, part=2,
            dir=str(tmp_path),
        )
        assert lg.res == 9
        assert lg.part == 2


class TestResumeWithOneArgPassed:
    """User passes -h3r but not -h3p (or vice versa) — only the passed
    one is validated; the other loads from the log unchecked."""

    def _make_log(self, tmp_path):
        H3BuildLogger(
            product_vars={'L2A': ['rh_098']}, res=9, part=2,
            dir=str(tmp_path),
        ).save_log('COMPLETED')

    def test_passing_matching_h3r_only(self, tmp_path):
        self._make_log(tmp_path)
        lg = H3BuildLogger(
            product_vars=None, res=9, part=None,
            dir=str(tmp_path),
        )
        assert lg.res == 9
        assert lg.part == 2

    def test_passing_mismatched_h3r_only_raises(self, tmp_path):
        self._make_log(tmp_path)
        with pytest.raises(GediValidationError):
            H3BuildLogger(
                product_vars=None, res=10, part=None,
                dir=str(tmp_path),
            )

    def test_passing_matching_h3p_only(self, tmp_path):
        self._make_log(tmp_path)
        lg = H3BuildLogger(
            product_vars=None, res=None, part=2,
            dir=str(tmp_path),
        )
        assert lg.res == 9
        assert lg.part == 2

    def test_passing_mismatched_h3p_only_raises(self, tmp_path):
        self._make_log(tmp_path)
        with pytest.raises(GediValidationError):
            H3BuildLogger(
                product_vars=None, res=None, part=4,
                dir=str(tmp_path),
            )
