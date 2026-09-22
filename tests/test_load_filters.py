"""`filters=` — pyarrow predicate pushdown in `egi_load` and `gh3_load`.

Contract under test:

1. `filters=` is honored end-to-end and is row-equivalent to filtering the
   unfiltered result in pandas (`query=`), for both the H3-database branch
   and the simplified-dataset branch.
2. The predicate reaches the parquet reader (real pushdown), it is not a
   post-read mask.
3. Predicate columns need not be listed in `columns=` and never leak into
   the output frame.
4. Conjunctive lists, DNF lists and `pyarrow` Expressions all compose (AND)
   with the reader's internal bbox predicate — on all three read strategies
   (`point`, `coord_filter`, `fallback`).
5. `region=` and `filters=` together keep BOTH pushdowns: the region bbox
   prefilter stays on, and results are identical to the legacy full-read +
   in-memory clip. This is the combination that used to silently drop the
   bbox pushdown in `gh3_load`.
6. Formats with no pushdown reader reject `filters=` loudly rather than
   silently returning unfiltered rows.
"""
from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point, box

import gedih3 as g
import gedih3.gh3driver as gh3
from gedih3.exceptions import GediValidationError

_CELL = "830e41fffffffff"


def _cell_points(cell, n=60):
    """~n level-12 cells inside `cell` via a lat/lng grid probe.

    Same probe trick as tests/test_bbox_index.py — enumerating the real
    children of a level-3 cell costs ~18 s per call.
    """
    import h3
    boundary = h3.cell_to_boundary(cell)
    la0, la1 = min(p[0] for p in boundary), max(p[0] for p in boundary)
    lo0, lo1 = min(p[1] for p in boundary), max(p[1] for p in boundary)
    out, seen = [], set()
    steps = 14
    for i in range(1, steps):
        for j in range(1, steps):
            la = la0 + (la1 - la0) * i / steps
            lo = lo0 + (lo1 - lo0) * j / steps
            c12 = h3.latlng_to_cell(la, lo, 12)
            if c12 not in seen and h3.cell_to_parent(c12, 3) == cell:
                seen.add(c12)
                out.append(c12)
    return out[:n]


def _frame(children, *, geometry_encoding=None):
    import h3
    lat = np.array([h3.cell_to_latlng(c)[0] for c in children])
    lng = np.array([h3.cell_to_latlng(c)[1] for c in children])
    n = len(children)
    return gpd.GeoDataFrame(
        {
            "agbd_l4a": np.linspace(5.0, 300.0, n),
            # 1 on even positions, 0 on odd — a predicate that keeps ~half
            "quality_l4a": np.arange(n) % 2,
            "lat_lowestmode_l2a": lat,
            "lon_lowestmode_l2a": lng,
            "geometry": [Point(x, y) for x, y in zip(lng, lat)],
        },
        index=pd.Index(list(children), name="h3_12"),
        crs=4326,
    )


@pytest.fixture(scope="module")
def mini_db(tmp_path_factory):
    """One level-3 partition x two years, WKB geometry + L2A coordinate
    columns — i.e. the `coord_filter` strategy, which is what the real
    v3 database uses."""
    import pyarrow.parquet as pq

    root = str(tmp_path_factory.mktemp("db"))
    children = _cell_points(_CELL)
    halves = {2019: children[: len(children) // 2], 2020: children[len(children) // 2:]}
    sample = None
    for year, kids in halves.items():
        ydir = os.path.join(root, f"h3_03={_CELL}", f"year={year}")
        os.makedirs(ydir, exist_ok=True)
        sample = os.path.join(ydir, f"{_CELL}.{year}.0.parquet")
        _frame(kids).to_parquet(sample)

    schema = pq.read_schema(sample)
    log = {
        "h3_resolution_level": 12,
        "h3_partition_level": 3,
        "h3_partition_ids": [_CELL],
        "h3_columns": list(schema.names),
        "h3_columns_dtypes": {n: str(schema.field(n).type) for n in schema.names},
    }
    with open(os.path.join(root, "gedih3_build_log.json"), "w") as fh:
        json.dump(log, fh)
    return root


@pytest.fixture(scope="module")
def region():
    import h3
    lat, lng = h3.cell_to_latlng(_CELL)
    return [lng - 0.6, lat - 0.6, lng + 0.6, lat + 0.6]


def _load(root, region, **kw):
    return g.egi_load(source=root, region=region, index_level=6,
                      partition_level=12, lazy=True, **kw).compute()


# ------------------------------------------------------- equivalence

def test_filters_match_the_pandas_equivalent(mini_db, region):
    """`filters=[(c, op, v)]` must select exactly what the same predicate
    selects post-hoc — pushdown may not change the answer."""
    full = _load(mini_db, region)
    assert len(full) > 0
    assert full["quality_l4a"].nunique() == 2, "fixture must exercise both branches"

    got = _load(mini_db, region, filters=[("quality_l4a", "==", 1)])
    exp = full[full["quality_l4a"] == 1]

    assert len(got) == len(exp) < len(full)
    assert sorted(got["agbd_l4a"].round(6)) == sorted(exp["agbd_l4a"].round(6))


def test_filters_and_query_agree(mini_db, region):
    """`filters=` (pushdown) and `query=` (post-read pandas) are two spellings
    of the same predicate and must return the same rows."""
    f = _load(mini_db, region, filters=[("agbd_l4a", ">", 100.0)])
    q = _load(mini_db, region, query="agbd_l4a > 100.0")
    assert len(f) > 0
    pd.testing.assert_frame_equal(
        f.sort_values("agbd_l4a").reset_index(drop=True),
        q.sort_values("agbd_l4a").reset_index(drop=True),
    )


def test_multiple_predicates_are_anded(mini_db, region):
    got = _load(mini_db, region,
                filters=[("quality_l4a", "==", 1), ("agbd_l4a", ">", 100.0)])
    full = _load(mini_db, region)
    exp = full[(full["quality_l4a"] == 1) & (full["agbd_l4a"] > 100.0)]
    assert 0 < len(got) == len(exp)


def test_dnf_filters_compose_with_the_bbox_predicate(mini_db, region):
    """A DNF (OR-of-ANDs) spec cannot be concatenated with the tile's bbox
    predicate — it has to be lifted to an Expression. Regression guard for
    `_combine_filters`."""
    dnf = [[("agbd_l4a", "<", 50.0)], [("agbd_l4a", ">", 250.0)]]
    got = _load(mini_db, region, filters=dnf)
    full = _load(mini_db, region)
    exp = full[(full["agbd_l4a"] < 50.0) | (full["agbd_l4a"] > 250.0)]
    assert 0 < len(got) == len(exp)


def test_expression_filters_are_accepted(mini_db, region):
    import pyarrow.compute as pc
    got = _load(mini_db, region, filters=pc.field("quality_l4a") == 1)
    exp = _load(mini_db, region, filters=[("quality_l4a", "==", 1)])
    assert 0 < len(got) == len(exp)


# ------------------------------------------------------- pushdown proof

def test_predicate_reaches_the_parquet_reader(mini_db, region, monkeypatch):
    """The filter must be handed to the reader, not applied after the read."""
    seen = []
    orig = gh3._read_parquet_bbox

    def spy(path, **kw):
        seen.append(kw.get("extra_filters"))
        return orig(path, **kw)

    monkeypatch.setattr(gh3, "_read_parquet_bbox", spy)
    _load(mini_db, region, filters=[("quality_l4a", "==", 1)])

    assert seen, "the bbox read path was never taken"
    assert all(s == [("quality_l4a", "==", 1)] for s in seen)


# --------------------------------------------------- column hygiene

def test_predicate_column_need_not_be_requested(mini_db, region):
    """Filtering on a column absent from `columns=` works and does not leak
    that column into the output (which would desync the dask meta)."""
    got = _load(mini_db, region, columns=["agbd_l4a"],
                filters=[("quality_l4a", "==", 1)])
    exp = _load(mini_db, region, filters=[("quality_l4a", "==", 1)])

    assert "quality_l4a" not in got.columns
    assert 0 < len(got) == len(exp)


def test_meta_matches_computed_columns(mini_db, region):
    """The dask `_meta` must still describe the computed frame once filters
    are in play (a leaked predicate column shows up here first)."""
    ddf = g.egi_load(source=mini_db, region=region, index_level=6,
                     partition_level=12, columns=["agbd_l4a"],
                     filters=[("quality_l4a", "==", 1)])
    out = ddf.compute()
    assert list(ddf.columns) == list(out.columns)


# --------------------------------------------- read-strategy matrix

@pytest.mark.parametrize("strategy", ["point", "coord_filter", "fallback"])
def test_all_bbox_strategies_compose_with_filters(tmp_path, strategy):
    """Every `_read_parquet_bbox` route must AND the caller's predicate with
    its own bbox predicate — and clip to the same bbox either way."""
    children = _cell_points(_CELL)
    gdf = _frame(children)
    lo0, la0, lo1, la1 = gdf.total_bounds
    # half-width bbox: excludes part of the data on every strategy
    half = (lo0, la0, (lo0 + lo1) / 2, la1)

    path = str(tmp_path / f"{strategy}.parquet")
    if strategy == "point":
        gdf.to_parquet(path, geometry_encoding="geoarrow")
        lat_col = lon_col = None
    elif strategy == "coord_filter":
        gdf.to_parquet(path)
        lat_col, lon_col = "lat_lowestmode_l2a", "lon_lowestmode_l2a"
    else:  # fallback — no coordinate columns to push down on
        gdf.drop(columns=["lat_lowestmode_l2a", "lon_lowestmode_l2a"]).to_parquet(path)
        lat_col = lon_col = None

    def _read(filters):
        return gh3._read_parquet_bbox(
            path, bbox_4326=half, clip_box=box(*half), columns=None, geo=True,
            strategy=strategy, lat_col=lat_col, lon_col=lon_col,
            extra_filters=filters,
        )

    base = _read(None)
    got = _read([("quality_l4a", "==", 1)])
    exp = base[base["quality_l4a"] == 1]

    assert 0 < len(base) < len(gdf), "bbox did not bite — test is not meaningful"
    assert len(got) == len(exp) < len(base)
    assert sorted(got["agbd_l4a"].round(6)) == sorted(exp["agbd_l4a"].round(6))


def test_missing_predicate_column_raises(tmp_path):
    """A schema-drifted file that cannot answer the predicate must fail loudly.
    Degrading to an unfiltered read here would look like a successful query
    and return rows the caller asked to exclude."""
    children = _cell_points(_CELL)
    path = str(tmp_path / "drift.parquet")
    _frame(children).drop(columns=["quality_l4a"]).to_parquet(path)

    with pytest.raises(Exception) as exc:
        gh3._read_parquet_bbox(
            path, bbox_4326=(-180, -90, 180, 90), clip_box=box(-180, -90, 180, 90),
            columns=None, geo=True, strategy="coord_filter",
            lat_col="lat_lowestmode_l2a", lon_col="lon_lowestmode_l2a",
            extra_filters=[("quality_l4a", "==", 1)],
        )
    assert "quality_l4a" in str(exc.value)


# --------------------------------------------- gh3_load: region + filters

def _spy_bbox_reads(monkeypatch):
    """Record the (bbox, extra_filters) every `_read_parquet_bbox` call gets."""
    seen = []
    orig = gh3._read_parquet_bbox

    def spy(path, **kw):
        seen.append((kw.get("bbox_4326"), kw.get("extra_filters")))
        return orig(path, **kw)

    monkeypatch.setattr(gh3, "_read_parquet_bbox", spy)
    return seen


def test_gh3_load_keeps_bbox_pushdown_alongside_filters(mini_db, region, monkeypatch):
    """`region=` + `filters=` must keep BOTH pushdowns. Passing filters used
    to disable the region bbox prefilter and fall back to a full read."""
    seen = _spy_bbox_reads(monkeypatch)
    got = g.gh3_load(mini_db, region=region,
                     filters=[("quality_l4a", "==", 1)], lazy=False)

    assert seen, "region bbox pushdown was skipped when filters were passed"
    assert all(b is not None for b, _ in seen), "bbox predicate lost"
    assert all(f == [("quality_l4a", "==", 1)] for _, f in seen), "user predicate lost"
    assert len(got) > 0


def test_gh3_load_region_and_filters_match_the_manual_answer(mini_db, region):
    """Both pushdowns are supersets of the exact clip + mask, so the answer
    must equal region-clip-then-filter done by hand."""
    full = g.gh3_load(mini_db, region=region, lazy=False)
    exp = full[full["quality_l4a"] == 1]

    got = g.gh3_load(mini_db, region=region,
                     filters=[("quality_l4a", "==", 1)], lazy=False)

    assert 0 < len(got) == len(exp) < len(full)
    assert sorted(got["agbd_l4a"].round(6)) == sorted(exp["agbd_l4a"].round(6))


def test_gh3_load_region_and_dnf_filters(mini_db, region):
    """DNF + the bbox predicate — the case plain list concatenation breaks."""
    full = g.gh3_load(mini_db, region=region, lazy=False)
    dnf = [[("agbd_l4a", "<", 50.0)], [("agbd_l4a", ">", 250.0)]]
    got = g.gh3_load(mini_db, region=region, filters=dnf, lazy=False)
    exp = full[(full["agbd_l4a"] < 50.0) | (full["agbd_l4a"] > 250.0)]
    assert 0 < len(got) == len(exp)


def test_gh3_load_bbox_index_still_skips_files_with_filters(mini_db, region, monkeypatch):
    """The a-priori year-file skip (`_bbox_index.parquet`) must survive the
    filters path too — it is part of the same region pushdown."""
    g.gh3_build_bbox_index(mini_db)
    try:
        seen = _spy_bbox_reads(monkeypatch)
        g.gh3_load(mini_db, region=region,
                   filters=[("quality_l4a", "==", 1)], lazy=False)
        assert seen and all(b is not None for b, _ in seen)
    finally:
        idx = os.path.join(mini_db, "_bbox_index.parquet")
        if os.path.exists(idx):
            os.remove(idx)


# ------------------------------------------------ simplified datasets

def test_filters_on_a_simplified_parquet_dataset(tmp_path):
    """The dataset branch (`_load_dataset`) pushes the predicate into every
    per-file task of the from_map graph."""
    root = tmp_path / "ds"
    root.mkdir()
    children = _cell_points(_CELL)
    _frame(children[: len(children) // 2]).to_parquet(root / "part0.parquet")
    _frame(children[len(children) // 2:]).to_parquet(root / "part1.parquet")

    ddf = gh3._load_dataset(str(root), lazy=True,
                            filters=[("quality_l4a", "==", 1)])
    got = ddf.compute()
    full = gh3._load_dataset(str(root), lazy=True).compute()

    assert 0 < len(got) == len(full[full["quality_l4a"] == 1]) < len(full)
    assert list(ddf.columns) == list(got.columns)


def test_non_parquet_formats_reject_filters():
    """Silently ignoring a predicate would hand back unfiltered rows the
    caller believes are filtered — that must be an error, not a no-op."""
    for fmt in ("feather", "gpkg"):
        with pytest.raises(GediValidationError, match="parquet"):
            gh3._check_filters_supported(fmt, [("a", "==", 1)])
    # no-ops
    gh3._check_filters_supported("parquet", [("a", "==", 1)])
    gh3._check_filters_supported("feather", None)


# ------------------------------------------------------- unit: combine

def test_combine_filters_identities_and_and():
    import pyarrow.parquet as pq

    conj = [("a", ">", 1)]
    assert gh3._combine_filters(conj, None) == conj
    assert gh3._combine_filters(conj, []) == conj
    assert gh3._combine_filters(None, conj) == conj
    assert gh3._combine_filters([], conj) == conj

    combined = gh3._combine_filters(conj, [[("b", "<", 2)], [("c", "==", 3)]])
    expected = (pq.filters_to_expression(conj)
                & pq.filters_to_expression([[("b", "<", 2)], [("c", "==", 3)]]))
    assert combined.equals(expected)
