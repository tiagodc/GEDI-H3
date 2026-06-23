"""Regression tests for gh3_update's source-database query-filter fallback.

The target dataset's stored ``query_filter`` is built against the database it was
extracted from.  When ``gh3_update`` adds columns from a *different* database that
lacks some of the filtered columns — e.g. an L2A/L4A quality-filtered dataset
having L1B columns joined from an L1B-only database — applying the filter to that
source raised ``UndefinedVariableError`` and aborted the whole update.

The left join on ``shot_number`` already restricts source rows to the
(already-filtered) target shots, so filtering the source is only a memory
optimization.  When the filter references a column absent from the source
database, gh3_update now loads the source unfiltered and relies on the join.
"""
import logging

import numpy as np
import pandas as pd

from gedih3.cli.gh3_update import _query_required_names


class TestQueryRequiredNames:
    def test_backtick_quality_columns(self):
        q = "`quality_flag_l2a` == 1 & `degrade_flag_l2a` == 0 & `l4_quality_flag_l4a` == 1"
        assert _query_required_names(q) == {
            'quality_flag_l2a', 'degrade_flag_l2a', 'l4_quality_flag_l4a'
        }

    def test_string_literals_not_mistaken_for_columns(self):
        q = "datetime >= '2020-01-01' & datetime <= '2020-12-31'"
        assert _query_required_names(q) == {'datetime'}

    def test_double_quoted_literal_stripped(self):
        q = 'name == "Brazil" & `agbd_l4a` > 5'
        names = _query_required_names(q)
        assert 'Brazil' not in names
        assert {'name', 'agbd_l4a'} <= names

    def test_keywords_excluded(self):
        q = "`a` == 1 and `b` == 0"
        assert _query_required_names(q) == {'a', 'b'}

    def test_empty_filter(self):
        assert _query_required_names(None) == set()
        assert _query_required_names('') == set()


class TestFilterFallbackDecision:
    """Mirror the applicability check in _update_from_database: a filter is
    applied to the source only when every name it references is loadable."""

    @staticmethod
    def _missing(query_filter, source_columns, sn_col='shot_number'):
        loadable = set(source_columns) | {sn_col}
        return sorted(_query_required_names(query_filter) - loadable)

    def test_filter_dropped_when_columns_absent(self):
        # L1B-only source lacks the L4A/L4C quality flags the filter references.
        q = "`quality_flag_l2a` == 1 & `l4_quality_flag_l4a` == 1 & `wsci_quality_flag_l4c` == 1"
        l1b_cols = ['quality_flag_l2a', 'noise_mean_corrected_l1b', 'datetime']
        assert self._missing(q, l1b_cols) == ['l4_quality_flag_l4a', 'wsci_quality_flag_l4c']

    def test_filter_kept_when_all_columns_present(self):
        q = "`quality_flag_l2a` == 1 & `degrade_flag_l2a` == 0"
        full_cols = ['quality_flag_l2a', 'degrade_flag_l2a', 'agbd_l4a']
        assert self._missing(q, full_cols) == []

    def test_shot_number_always_loadable(self):
        # Beam filters reference shot_number, which isn't always in h3_columns
        # but is always loadable — must not trigger a needless fallback.
        q = "(shot_number % 10000000000000 // 100000000000) > 3"
        assert self._missing(q, ['agbd_l4a'], sn_col='shot_number') == []


def test_update_from_database_drops_inapplicable_filter(monkeypatch, tmp_path):
    """End-to-end: an inapplicable filter is dropped (effective_filter=None) and
    the original filter is preserved in metadata; safe_query is never called with
    the bad filter."""
    import json
    import gedih3.gh3driver as gh3
    import gedih3.cli.gh3_update as upd

    # Target dataset: one H3 partition file with shots, extracted under an
    # L4A/L4C quality filter the L1B source can't satisfy.
    part_id = '83a891fffffffff'
    ds = tmp_path / 'dataset'
    ds.mkdir()
    pd.DataFrame({
        'shot_number': np.array([101, 102, 103], dtype=np.int64),
        'agbd_l4a': [10.0, 20.0, 30.0],
    }).to_parquet(ds / f'{part_id}.parquet')

    query_filter = "`quality_flag_l2a` == 1 & `l4_quality_flag_l4a` == 1"
    meta = {
        'index_type': 'h3',
        'query_filter': query_filter,
        'columns': ['shot_number', 'agbd_l4a'],
        'source_database': '/fake/l1b_db',
    }
    (ds / 'gedih3_dataset.json').write_text(json.dumps(meta))

    # L1B-only source: has quality_flag_l2a but NOT l4_quality_flag_l4a.
    source_cols = ['shot_number', 'noise_mean_corrected_l1b', 'quality_flag_l2a']

    captured = {'safe_query_filters': []}

    def fake_read_meta(field, gh3_root_dir=None):
        return {'h3_columns': source_cols, 'h3_partition_level': 3}[field]

    def fake_load_hex(h3_dir, columns=None, **kwargs):
        # Source only knows about shots 102, 103 (102 -> a value).
        return pd.DataFrame({
            'shot_number': np.array([102, 103], dtype=np.int64),
            'noise_mean_corrected_l1b': [205.0, 206.0],
        })

    def fake_safe_query(df, query_str):
        captured['safe_query_filters'].append(query_str)
        return df

    monkeypatch.setattr(gh3, 'gh3_read_meta', fake_read_meta)
    monkeypatch.setattr(gh3, 'gh3_load_hex', fake_load_hex)
    monkeypatch.setattr('gedih3.cliutils.safe_query', fake_safe_query)

    import argparse
    args = argparse.Namespace(database='/fake/l1b_db', list=['noise_mean_corrected_l1b'],
                              L1B=None, L2A=None, L2B=None, L4A=None, L4C=None)

    monkeypatch.setattr(upd, '_update_h3_partitions',
                        lambda *a, **k: captured.update(passed_filter=a[6]))
    # Avoid spinning a real dask cluster / collect_columns plumbing: stub the
    # column resolution and Client.
    monkeypatch.setattr('gedih3.cliutils.collect_columns',
                        lambda a, available_columns=None: ['noise_mean_corrected_l1b'])
    monkeypatch.setattr('gedih3.utils.smart_exists', lambda p: True)

    import contextlib
    class _FakeClient(contextlib.AbstractContextManager):
        dashboard_link = 'http://x'
        def __exit__(self, *a):
            return False
    monkeypatch.setattr('dask.distributed.Client', lambda **k: _FakeClient())
    monkeypatch.setattr('gedih3.cliutils.parse_dask_args', lambda a: {})

    upd._update_from_database(args, str(ds), meta, logging.getLogger('test'))

    # The inapplicable filter must have been dropped before reaching the updater.
    assert captured['passed_filter'] is None
    # Metadata still records the original provenance filter.
    out_meta = json.loads((ds / 'gedih3_dataset.json').read_text())
    assert out_meta['query_filter'] == query_filter
