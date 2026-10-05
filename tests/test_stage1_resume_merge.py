"""A build stopped after Stage 1 but before its merge recorded progress.

Resume reconcile flips such granules INDEXED from their completion sentinels,
so Stage 1 has nothing left to read; the fragments it wrote live only in tmp
and must still be merged. The sentinels are trusted only when the scope the
resumed CLI fingerprints (from the build log it reloads) equals the one the
interrupted run recorded.
"""
import copy
import os

import pytest

from gedih3.config import GEDI_BEAMS
from test_merge_failed_recovery import N_SHOTS, _client, _db_shots, _write_granule  # noqa: F401

# Explicit names, not a preset: the build expands them (essentials, quality
# flags) in the build log's own dict, which is what a resume reads back.
RAW_VARS = {'L2A': ['rh_098', 'sensitivity'], 'L4A': ['agbd']}
GRANULE = (101, 1, 201)


def _interrupted_then_resumed(tmp_dir, monkeypatch):
    """Run Stage 1 for one granule, stop before the merge, then resume like
    the CLI does. Returns ``(resumed logger, h3_dir, load calls on resume)``."""
    import gedih3.gh3builder as gb
    from gedih3.logger import H3BuildLogger

    soc_dir, h3_dir, tmp = (os.path.join(tmp_dir, n) for n in ('soc', 'db', 'tmp'))
    parquet_dir = os.path.join(tmp, 'partitions')
    kw = dict(res=12, part=3, soc_source=soc_dir, version=2, tmp_dir=tmp, h3_dir=h3_dir)
    _write_granule(soc_dir, *GRANULE)

    log = H3BuildLogger(product_vars=copy.deepcopy(RAW_VARS), dir=h3_dir, res=12, part=3, version=2)
    log.register_pending_granules([dict(zip(('orbit', 'granule', 'track'), GRANULE))])

    def _stop(*a, **k):
        raise KeyboardInterrupt
    monkeypatch.setattr(gb, '_merge_and_finalize', _stop)
    with pytest.raises(KeyboardInterrupt):
        gb.build_h3db(product_vars=log.get_product_vars(), **kw)
    log.save_log('INTERRUPTED')
    monkeypatch.undo()
    assert len(gb._scan_complete_sentinels(parquet_dir)) == len(GEDI_BEAMS)

    # Resume: the CLI reloads the log and fingerprints its variables.
    log = H3BuildLogger(product_vars=None, dir=h3_dir, res=12, part=3, version=2)
    gb._reconcile_granules_from_disk(
        h3_dir, log, tmp_dir=parquet_dir,
        expected_scope=gb._stage1_scope_fingerprint(None, 12, 3, log.get_product_vars()),
    )
    loads = []
    orig = gb.load_h5_merged
    monkeypatch.setattr(gb, 'load_h5_merged', lambda *a, **k: loads.append(1) or orig(*a, **k))
    gb.build_h3db(product_vars=log.get_product_vars(), skip_granules=log.get_finished_granules(), **kw)
    return log, h3_dir, loads


def test_resume_trusts_sentinels_of_expanded_variables(tmp_dir, _client, monkeypatch):
    """The recorded scope matches the resumed log's (expanded) variables, so
    reconcile flips the granule and Stage 1 reads nothing again."""
    log, _, loads = _interrupted_then_resumed(tmp_dir, monkeypatch)
    status = {(g['orbit'], g['granule'], g['track']): g['status'] for g in log.granule_info}
    assert status[GRANULE] == 'INDEXED'
    assert loads == []


def test_fragments_left_by_stage1_are_merged_when_nothing_is_pending(tmp_dir, _client, monkeypatch):
    """Every granule INDEXED by reconcile, so no Stage 1 runs: the fragments
    in tmp are still merged instead of being stranded there."""
    _, h3_dir, _ = _interrupted_then_resumed(tmp_dir, monkeypatch)
    shots = _db_shots(h3_dir)
    assert len(shots) == len(set(shots)) == len(GEDI_BEAMS) * N_SHOTS


class TestUnmergedFragmentProbe:

    def test_emptied_dirs_are_not_fragments(self, tmp_dir):
        from gedih3.gh3builder import _has_unmerged_fragments
        os.makedirs(os.path.join(tmp_dir, 'h3_03=830001fffffffff', 'year=2020'))
        os.makedirs(os.path.join(tmp_dir, 'h3_03=830002fffffffff'))
        assert not _has_unmerged_fragments(tmp_dir)

    def test_one_fragment_is_found(self, tmp_dir):
        from gedih3.gh3builder import _has_unmerged_fragments
        ydir = os.path.join(tmp_dir, 'h3_03=830001fffffffff', 'year=2020')
        os.makedirs(ydir)
        open(os.path.join(ydir, 'O00101_G01_T00201.BEAM0000.parquet'), 'wb').close()
        assert _has_unmerged_fragments(tmp_dir)

    def test_missing_dir(self, tmp_dir):
        from gedih3.gh3builder import _has_unmerged_fragments
        assert not _has_unmerged_fragments(os.path.join(tmp_dir, 'nope'))


class TestDiscardUnscopedSentinels:
    """Sentinels with no scope record, left after a COMPLETED build by an
    older gedih3, are discarded rather than adopted."""

    def _tree(self, tmp_dir, scoped):
        from gedih3.gh3builder import _check_scope_fingerprint, _emit_complete_sentinel
        tmp = os.path.join(tmp_dir, 'partitions')
        os.makedirs(tmp)
        if scoped:
            _check_scope_fingerprint(tmp, 'fp')
        _emit_complete_sentinel(tmp, 'O00101_G01_T00201.BEAM0000')
        return tmp

    def test_unscoped_set_is_discarded(self, tmp_dir):
        from gedih3.gh3builder import _discard_unscoped_sentinels, _scan_complete_sentinels
        tmp = self._tree(tmp_dir, scoped=False)
        assert _discard_unscoped_sentinels(tmp)
        assert _scan_complete_sentinels(tmp) == set()

    def test_scoped_set_is_left_to_the_scope_check(self, tmp_dir):
        from gedih3.gh3builder import _discard_unscoped_sentinels, _scan_complete_sentinels
        tmp = self._tree(tmp_dir, scoped=True)
        assert not _discard_unscoped_sentinels(tmp)
        assert len(_scan_complete_sentinels(tmp)) == 1

    def test_no_sentinels_is_a_noop(self, tmp_dir):
        from gedih3.gh3builder import _discard_unscoped_sentinels
        assert not _discard_unscoped_sentinels(os.path.join(tmp_dir, 'partitions'))
