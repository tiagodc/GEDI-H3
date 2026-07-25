"""The data-bbox index (`_bbox_index.parquet`) and its consumers.

The index materializes the true data envelope of every partition year-file
from existing parquet row-group statistics (footer-only scan). Query paths
use it to skip files a region / EGI tile provably cannot touch. The contract
under test:

1. The scanner reproduces the real coordinate envelope, and degrades to a
   NULL bbox (= "unknown, keep the file") when stats or columns are missing.
2. Pruning NEVER changes an answer — loads with and without the index are
   row-identical, for both `gh3_load(region=...)` and `egi_load`.
3. The index actually skips reads (not just a no-op decoration).
4. The all-files-skipped edge returns a correctly-shaped empty frame.
5. The `_prepare_egi_loading` candidate restriction is equivalence-preserving.
6. `get_children` vectorization is behavior-identical.
"""
from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Point

import gedih3 as g
import gedih3.gh3driver as gh3
from gedih3.config import BBOX_INDEX_FILENAME

_CELL_A = "830e41fffffffff"


def _neighbor_cell():
    import h3
    return sorted(c for c in h3.grid_disk(_CELL_A, 1) if c != _CELL_A)[0]


def _cell_points(cell, n=80):
    """~n level-12 cells inside `cell` via a lat/lng grid probe.

    Deliberately avoids `h3.cell_to_children(cell, 12)` — that enumerates
    ~40M descendants of a level-3 cell (~18 s per call) just to slice off
    80 of them, which made every fixture instantiation cost ~35 s.
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


def _write_year_file(root, cell, year, children):
    import h3
    lat = np.array([h3.cell_to_latlng(c)[0] for c in children])
    lng = np.array([h3.cell_to_latlng(c)[1] for c in children])
    ydir = os.path.join(root, f"h3_03={cell}", f"year={year}")
    os.makedirs(ydir, exist_ok=True)
    path = os.path.join(ydir, f"{cell}.{year}.0.parquet")
    gdf = gpd.GeoDataFrame(
        {
            "rh_098_l2a": np.linspace(5.0, 30.0, len(children)),
            "lat_lowestmode_l2a": lat,
            "lon_lowestmode_l2a": lng,
            "geometry": [Point(x, y) for x, y in zip(lng, lat)],
        },
        index=pd.Index(list(children), name="h3_12"),
        crs=4326,
    )
    gdf.to_parquet(path)
    return path, (lng.min(), lat.min(), lng.max(), lat.max())


@pytest.fixture
def mini_db(tmp_path):
    """Two partitions x two years; within each partition the years hold
    geographically separated halves (south=2019, north=2020) so a query bbox
    can be answered by one year file — the situation the index exploits."""
    import h3
    import pyarrow.parquet as pq

    root = str(tmp_path / "db")
    os.makedirs(root)
    cells = [_CELL_A, _neighbor_cell()]
    bboxes = {}
    sample = None
    for cell in cells:
        children = _cell_points(cell)
        children.sort(key=lambda c: h3.cell_to_latlng(c)[0])  # by latitude
        south, north = children[: len(children) // 2], children[len(children) // 2:]
        p1, b1 = _write_year_file(root, cell, 2019, south)
        p2, b2 = _write_year_file(root, cell, 2020, north)
        bboxes[(cell, 2019)] = b1
        bboxes[(cell, 2020)] = b2
        sample = p2

    schema = pq.read_schema(sample)
    log = {
        "h3_resolution_level": 12,
        "h3_partition_level": 3,
        "h3_partition_ids": cells,
        "h3_columns": list(schema.names),
        "h3_columns_dtypes": {n: str(schema.field(n).type) for n in schema.names},
    }
    with open(os.path.join(root, "gedih3_build_log.json"), "w") as fh:
        json.dump(log, fh)
    return root, cells, bboxes


def _rows(df):
    return sorted(zip(df["rh_098_l2a"].round(4), df.geometry.x.round(8),
                      df.geometry.y.round(8)))


# ------------------------------------------------------------------ scanner

def test_scan_file_bbox_matches_data(mini_db, tmp_path):
    root, cells, bboxes = mini_db
    cell = cells[0]
    path = os.path.join(root, f"h3_03={cell}", "year=2019",
                        f"{cell}.2019.0.parquet")
    rec = gh3._scan_file_bbox((path, gh3._bbox_index_key(path),
                               "lat_lowestmode_l2a", "lon_lowestmode_l2a"))
    exp = bboxes[(cell, 2019)]
    assert rec["n_rows"] > 0
    assert rec["lon_min"] == pytest.approx(exp[0])
    assert rec["lat_min"] == pytest.approx(exp[1])
    assert rec["lon_max"] == pytest.approx(exp[2])
    assert rec["lat_max"] == pytest.approx(exp[3])


def test_scan_missing_columns_gives_null_bbox(tmp_path):
    p = tmp_path / "x.parquet"
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(p)
    rec = gh3._scan_file_bbox((str(p), "x.parquet", "lat_nope", "lon_nope"))
    assert rec["lon_min"] is None and rec["n_rows"] == 3


def test_scan_unreadable_file_is_failsafe(tmp_path):
    p = tmp_path / "junk.parquet"
    p.write_bytes(b"not a parquet")
    rec = gh3._scan_file_bbox((str(p), "junk.parquet", "a", "b"))
    assert rec["lon_min"] is None  # unknown -> consumers keep the file


# ---------------------------------------------------------- build and load

def test_build_and_load_index(mini_db):
    root, cells, bboxes = mini_db
    opath = g.gh3_build_bbox_index(root)
    assert os.path.basename(opath) == BBOX_INDEX_FILENAME
    idx = gh3._load_bbox_index(root)
    assert len(idx) == 4
    key = f"h3_03={cells[0]}/year=2019/{cells[0]}.2019.0.parquet"
    assert idx[key] == pytest.approx(bboxes[(cells[0], 2019)])

    # missing sidecar -> None (fail-safe fallback), and cache revalidates
    os.remove(opath)
    assert gh3._load_bbox_index(root) is None
    g.gh3_build_bbox_index(root)
    assert gh3._load_bbox_index(root) is not None


def test_build_rejects_remote_roots():
    from gedih3.exceptions import GediValidationError
    with pytest.raises(GediValidationError):
        g.gh3_build_bbox_index("s3://bucket/db")


# ------------------------------------------------------------- gh3_load

def _count_bbox_reads(monkeypatch):
    calls = []
    orig = gh3._read_parquet_bbox

    def counting(path, **kw):
        calls.append(str(path))
        return orig(path, **kw)

    monkeypatch.setattr(gh3, "_read_parquet_bbox", counting)
    return calls


def test_gh3_load_region_identical_with_and_without_index(mini_db, monkeypatch):
    import h3
    root, cells, _ = mini_db
    lat, lng = h3.cell_to_latlng(cells[0])
    region = [lng - 0.4, lat - 0.4, lng + 0.4, lat + 0.4]

    baseline_full = g.gh3_load(root, lazy=False)
    manual = baseline_full.clip(gh3.gpd.GeoDataFrame(
        geometry=[__import__("shapely.geometry", fromlist=["box"]).box(*region)],
        crs=4326))

    no_index = g.gh3_load(root, region=region, lazy=False)
    assert _rows(no_index) == _rows(manual)
    assert len(no_index) > 0

    g.gh3_build_bbox_index(root)
    calls = _count_bbox_reads(monkeypatch)
    with_index = g.gh3_load(root, region=region, lazy=False)
    assert _rows(with_index) == _rows(no_index)
    assert calls, "bbox pushdown path was not used"


def test_gh3_load_index_actually_skips_files(mini_db, monkeypatch):
    """A bbox covering only the 2020 (north) half must skip the 2019 files."""
    import h3
    root, cells, bboxes = mini_db
    g.gh3_build_bbox_index(root)

    b2020 = bboxes[(cells[0], 2020)]
    b2019 = bboxes[(cells[0], 2019)]
    # strictly north of cell A's 2019 band (the neighbor partition's bands
    # sit at other latitudes — only cell A's own split is deterministic)
    lo = b2019[3] + 1e-6
    region = [b2020[0] - 0.01, lo, b2020[2] + 0.01, b2020[3] + 0.01]
    assert lo < b2020[3], "fixture must leave some 2020 rows above the split"

    calls = _count_bbox_reads(monkeypatch)
    got = g.gh3_load(root, region=region, lazy=False)
    assert len(got) > 0
    a2019 = f"h3_03={cells[0]}/year=2019"
    assert not any(a2019 in c.replace(os.sep, '/') for c in calls), \
        "cell A's 2019 file was read despite a disjoint data bbox"


def test_gh3_load_all_files_skipped_returns_empty_with_schema(mini_db):
    """Region inside the partition CELL but outside all DATA -> partition is
    selected (ring-1 cell math) yet every year file is index-skipped; the
    result must be an empty frame with the normal schema."""
    import h3
    root, cells, bboxes = mini_db
    g.gh3_build_bbox_index(root)

    # a spot inside cell A's polygon south of all data is not guaranteed;
    # instead go far outside every data bbox but keep a cell selected via
    # ring-1: use a box just outside the global data envelope.
    lat_min = min(b[1] for b in bboxes.values())
    lon_min = min(b[0] for b in bboxes.values())
    region = [lon_min - 0.5, lat_min - 0.5, lon_min - 0.4, lat_min - 0.4]

    got = g.gh3_load(root, region=region, lazy=False)
    ref = g.gh3_load(root, lazy=False)
    assert len(got) == 0
    assert set(got.columns) == set(ref.columns)


# --------------------------------------------------------------- egi_load

def test_egi_load_identical_with_and_without_index(mini_db, monkeypatch):
    import h3
    root, cells, _ = mini_db
    lat, lng = h3.cell_to_latlng(cells[0])
    region = [lng - 0.4, lat - 0.4, lng + 0.4, lat + 0.4]

    def _get(with_index):
        idx_path = os.path.join(root, BBOX_INDEX_FILENAME)
        if with_index:
            g.gh3_build_bbox_index(root)
        elif os.path.exists(idx_path):
            os.remove(idx_path)
        out = g.egi_load(source=root, region=region, index_level=6,
                         partition_level=12, lazy=True).compute()
        return out

    without = _get(False)
    withidx = _get(True)
    assert len(without) > 0
    a = without.sort_values("rh_098_l2a").reset_index()
    b = withidx.sort_values("rh_098_l2a").reset_index()
    pd.testing.assert_frame_equal(a, b)


def test_egi_tile_loader_skips_disjoint_files(mini_db, monkeypatch):
    """The tile-task skip: a year file whose data envelope is disjoint from
    the tile bbox is never opened, and the result is identical to reading
    everything. (End-to-end the tile bbox is a full 160 km square that can
    legitimately cover both year bands of this small fixture, so the skip is
    pinned at the task level where the geometry is controlled.)"""
    from pyproj import Transformer

    root, cells, bboxes = mini_db
    g.gh3_build_bbox_index(root)
    idx = gh3._load_bbox_index(root)

    b2019 = bboxes[(cells[0], 2019)]
    b2020 = bboxes[(cells[0], 2020)]
    lo = b2019[3] + 1e-6
    wgs84 = (b2020[0] - 0.01, lo, b2020[2] + 0.01, b2020[3] + 0.01)
    t = Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True)
    x0, y0 = t.transform(wgs84[0], wgs84[1])
    x1, y1 = t.transform(wgs84[2], wgs84[3])
    tile_bbox = (x0, y0, x1, y1)

    fb = {k: v for k, v in idx.items() if k.startswith(f"h3_03={cells[0]}/")}

    def _run(file_bboxes):
        return gh3._load_egi_tile_from_h3(
            tile_bbox, [cells[0]], root, "h3_03", None, None,
            index_level=6, partition_level=12, set_index=False,
            bbox_strategy="coord_filter",
            bbox_lat_col="lat_lowestmode_l2a",
            bbox_lon_col="lon_lowestmode_l2a",
            file_bboxes=file_bboxes,
        )

    calls = _count_bbox_reads(monkeypatch)
    with_skip = _run(fb)
    reads_with = list(calls)
    calls.clear()
    without = _run(None)
    reads_without = list(calls)

    a2019 = f"h3_03={cells[0]}/year=2019"
    assert any(a2019 in c.replace(os.sep, "/") for c in reads_without)
    assert not any(a2019 in c.replace(os.sep, "/") for c in reads_with), \
        "disjoint 2019 file was opened despite the index"
    assert len(with_skip) > 0
    a = with_skip.sort_values("rh_098_l2a").reset_index(drop=True)
    b = without.sort_values("rh_098_l2a").reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b)


# --------------------------------------------- driver-side prep equivalence

def test_prepare_egi_loading_restriction_is_equivalent(mini_db):
    """Restricting the H3 geometry build to tile-reachable candidates must
    not change the EGI->H3 mapping."""
    import h3
    from gedih3 import egi
    from gedih3.h3utils import h3_parts_to_gdf

    root, cells, _ = mini_db
    lat, lng = h3.cell_to_latlng(cells[0])
    region = gpd.GeoDataFrame(
        geometry=[__import__("shapely.geometry", fromlist=["box"]).box(
            lng - 0.4, lat - 0.4, lng + 0.4, lat + 0.4)], crs=4326)

    egi_tiles, egi_to_h3, part_col, _ = gh3._prepare_egi_loading(region, root)

    # ground truth: same intersection against the FULL partition list
    full = egi.egi_h3_intersection(egi_tiles, h3_parts_to_gdf(cells))
    assert {k: sorted(v) for k, v in egi_to_h3.items()} == \
           {k: sorted(v) for k, v in full.items()}


# ------------------------------------------------------------ get_children

def test_get_children_vectorized_is_consistent():
    from gedih3 import egi

    from gedih3.egi.config import OUTER_RES, RESOLUTIONS

    parent = int(egi.to_hash(1000000.0, 2000000.0, level=12))
    kids = egi.get_children(np.uint64(parent), children_level=7)
    expected = round(OUTER_RES / RESOLUTIONS[7]) ** 2
    assert len(kids) == expected  # silently-fewer children would be a loss
    assert len(kids) == len(set(int(k) for k in kids))
    levels = {int(egi.get_level(np.uint64(k))) for k in kids}
    assert levels == {7}
    # to_parent is bit-exact for levels >= 7: every child folds back
    parents = egi.to_parent(np.array(kids, dtype=np.uint64), 12)
    assert set(int(p) for p in np.atleast_1d(parents)) == {parent}


def test_index_ignored_when_build_log_is_newer(mini_db):
    """The self-guard: every producer saves the build log, so an index
    older than the log may describe stale envelopes — it must be treated
    as absent (fail-safe), never trusted."""
    import time

    root, _, _ = mini_db
    g.gh3_build_bbox_index(root)
    assert gh3._load_bbox_index(root) is not None

    time.sleep(0.02)
    log = os.path.join(root, "gedih3_build_log.json")
    os.utime(log, None)  # a producer touched the database after the index
    assert gh3._load_bbox_index(root) is None

    g.gh3_build_bbox_index(root)  # rebuild -> trusted again
    assert gh3._load_bbox_index(root) is not None


def test_invalidate_bbox_index_helper(mini_db):
    """The producer-side invalidation used by _merge_and_finalize (at merge
    ENTRY, before any partition write) and gh3_doctor --fix."""
    root, _, _ = mini_db
    g.gh3_build_bbox_index(root)
    assert gh3.invalidate_bbox_index(root) is True
    assert not os.path.exists(os.path.join(root, BBOX_INDEX_FILENAME))
    assert gh3._load_bbox_index(root) is None
    assert gh3.invalidate_bbox_index(root) is False  # idempotent


def test_smart_glob_never_sweeps_root_sidecars(mini_db):
    """`**/*.parquet` matches root files (recursive matches zero dirs); a
    consumer treating the bbox index as a data partition is a silent-
    corruption class. smart_glob's fallback must drop root sidecars."""
    from gedih3.utils import smart_glob

    root, _, _ = mini_db
    g.gh3_build_bbox_index(root)
    hits = smart_glob(os.path.join(root, "**/*.parquet"), recursive=True)
    assert hits, "expected data files"
    assert not any(os.path.basename(h).startswith("_") for h in hits)
    # explicit requests for underscore names still work
    direct = smart_glob(os.path.join(root, "_bbox_index.parquet"))
    assert len(direct) == 1


def test_coord_filter_degrades_on_schema_drifted_file(mini_db):
    """A file lacking the DB-wide predicate columns must not turn a region
    query into a hard crash — the read degrades to the geometric fallback
    for that file, matching the scanner's NULL-envelope degradation."""
    import h3 as h3lib
    root, cells, bboxes = mini_db

    # strip the predicate columns from one year file (plain pyarrow file
    # rewrite — gpd/pandas would hive-infer h3_03/year into the file)
    import pyarrow.parquet as pq
    p = os.path.join(root, f"h3_03={cells[0]}", "year=2019",
                     f"{cells[0]}.2019.0.parquet")
    t = pq.ParquetFile(p).read()  # physical columns only, no hive inference
    t = t.drop_columns(["lat_lowestmode_l2a", "lon_lowestmode_l2a"])
    pq.write_table(t, p)

    # Explicit projection: full-schema loads of a drifted DB already fail
    # on main (uniform-schema invariant — concat order/columns diverge);
    # the contract here is only that the REGION path must not crash where
    # the projected non-region path works.
    lat, lng = h3lib.cell_to_latlng(cells[0])
    region = [lng - 0.4, lat - 0.4, lng + 0.4, lat + 0.4]
    got = g.gh3_load(root, region=region, columns=["rh_098_l2a"], lazy=False)
    ref = g.gh3_load(root, columns=["rh_098_l2a", "geometry"], lazy=False).clip(
        gpd.GeoDataFrame(geometry=[__import__("shapely.geometry",
                                              fromlist=["box"]).box(*region)],
                         crs=4326))
    assert _rows(got) == _rows(ref)
    assert len(got) > 0


def test_fallback_strategy_when_db_has_no_predicate_columns(tmp_path):
    """A DB without lat/lon_lowestmode columns takes the 'fallback' bbox
    strategy (full read + geometric clip) and still answers correctly."""
    import json as _json
    import h3 as h3lib
    import pyarrow.parquet as pq
    from shapely.geometry import box as _box

    root = str(tmp_path / "nolatlon")
    os.makedirs(root)
    cell = _CELL_A
    children = _cell_points(cell)
    lat = np.array([h3lib.cell_to_latlng(c)[0] for c in children])
    lng = np.array([h3lib.cell_to_latlng(c)[1] for c in children])
    ydir = os.path.join(root, f"h3_03={cell}", "year=2020")
    os.makedirs(ydir)
    path = os.path.join(ydir, f"{cell}.2020.0.parquet")
    gpd.GeoDataFrame(
        {"rh_098_l2a": np.linspace(1.0, 9.0, len(children))},
        geometry=[Point(x, y) for x, y in zip(lng, lat)],
        index=pd.Index(list(children), name="h3_12"), crs=4326,
    ).to_parquet(path)
    schema = pq.read_schema(path)
    with open(os.path.join(root, "gedih3_build_log.json"), "w") as fh:
        _json.dump({
            "h3_resolution_level": 12, "h3_partition_level": 3,
            "h3_partition_ids": [cell],
            "h3_columns": list(schema.names),
            "h3_columns_dtypes": {n: str(schema.field(n).type)
                                  for n in schema.names},
        }, fh)

    la, lo = h3lib.cell_to_latlng(cell)
    region = [lo - 0.2, la - 0.2, lo + 0.2, la + 0.2]
    got = g.gh3_load(root, region=region, lazy=False)
    ref = g.gh3_load(root, lazy=False).clip(
        gpd.GeoDataFrame(geometry=[_box(*region)], crs=4326))
    assert len(got) > 0
    assert _rows(got) == _rows(ref)


def test_egi_load_wires_task_bboxes(mini_db, monkeypatch):
    """The driver must hand each EGI tile task its per-partition envelope
    dict — equality alone cannot distinguish working skips from a silent
    no-op in the wiring."""
    import h3 as h3lib
    root, cells, _ = mini_db
    g.gh3_build_bbox_index(root)

    captured = []
    orig = gh3._load_egi_tile_from_h3

    def spy(*args, **kw):
        captured.append(kw.get("file_bboxes"))
        return orig(*args, **kw)

    monkeypatch.setattr(gh3, "_load_egi_tile_from_h3", spy)
    lat, lng = h3lib.cell_to_latlng(cells[0])
    region = [lng - 0.4, lat - 0.4, lng + 0.4, lat + 0.4]
    out = g.egi_load(source=root, region=region, index_level=6,
                     partition_level=12, lazy=True).compute()
    assert len(out) > 0
    assert captured and all(fb for fb in captured), \
        "tile tasks did not receive their envelope dicts"


def test_gh3_load_projected_columns_survive_empty_file_results(mini_db):
    """A bbox-emptied file must not leak the helper lat/lon predicate
    columns into the graph (0-row frames previously skipped the drop and
    desynced the dask meta)."""
    root, cells, bboxes = mini_db
    b2019 = bboxes[(cells[0], 2019)]
    b2020 = bboxes[(cells[0], 2020)]
    lo = b2019[3] + 1e-6
    region = [b2020[0] - 0.01, lo, b2020[2] + 0.01, b2020[3] + 0.01]
    got = g.gh3_load(root, region=region, columns=["rh_098_l2a"], lazy=False)
    assert len(got) > 0
    assert "lat_lowestmode_l2a" not in got.columns
