"""Regression test for the egi_load() Dask meta ↔ partition schema mismatch.

Bug history: ``egi_load(columns=None)`` (the load-everything path) read each
H3 partition file with ``gpd.read_parquet(path)`` and no explicit column list.
On a hive-partitioned database (``h3_03=<cell>/year=<yyyy>/*.parquet``),
geopandas/pyarrow infer the partition columns ``h3_03`` and ``year`` from the
directory path and inject them into every returned frame. The declared Dask
``_meta``, however, is built from ``pq.read_schema`` on a single file, which
never sees the hive columns — so each computed partition carried
``['h3_03', 'year']`` that the meta lacked (and the meta carried ``h3_12`` as a
plain column that the computed partition — after ``set_index`` on the EGI
index — lacked). At ``.compute()`` Dask's ``check_matching_columns`` raised::

    ValueError: The columns in the computed data do not match the columns in
    the provided metadata.
      Extra:   ['h3_03', 'year']
      Missing: ['h3_12']

The fix resolves ``columns=None`` to the concrete data-column list from the DB
schema before the read, which (a) suppresses geopandas' hive inference by
passing an explicit column list and (b) routes columns=None through the same
final output-projection machinery as an explicit request — so the lazy meta
and every computed partition agree exactly, and no H3 partition artifacts leak
into the EGI output.
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

# A real H3 level-3 cell (same one used by test_h3_load_meta.py).
_H3_03_CELL = "830e41fffffffff"


def _build_hive_h3_db(root: str, n: int = 60) -> list:
    """Create a minimal hive-partitioned H3 database on disk.

    Mirrors the on-disk layout the builder produces: one parquet per
    ``h3_03=<cell>/year=<yyyy>/`` directory, indexed by a named ``h3_12``
    index, with the partition column ``h3_03`` living ONLY in the directory
    name (never a physical column) — which is precisely what makes geopandas
    infer it at read time.
    """
    import h3

    children = list(h3.cell_to_children(_H3_03_CELL, 12))[:n]
    lat = np.array([h3.cell_to_latlng(c)[0] for c in children])
    lng = np.array([h3.cell_to_latlng(c)[1] for c in children])

    ydir = os.path.join(root, f"h3_03={_H3_03_CELL}", "year=2019")
    os.makedirs(ydir)
    pq_path = os.path.join(ydir, f"{_H3_03_CELL}.2019.0.parquet")

    gdf = gpd.GeoDataFrame(
        {
            "rh_098_l2a": np.linspace(5.0, 30.0, len(children)),
            "geometry": [Point(x, y) for x, y in zip(lng, lat)],
        },
        index=pd.Index(children, name="h3_12"),
        crs=4326,
    )
    gdf.to_parquet(pq_path)

    # Build-log sidecar with exactly the fields the egi load path reads.
    import pyarrow.parquet as pq

    schema = pq.read_schema(pq_path)
    log = {
        "h3_resolution_level": 12,
        "h3_partition_level": 3,
        "h3_partition_ids": [_H3_03_CELL],
        "h3_columns": list(schema.names),
        "h3_columns_dtypes": {n: str(schema.field(n).type) for n in schema.names},
    }
    with open(os.path.join(root, "gedih3_build_log.json"), "w") as fh:
        json.dump(log, fh)

    return [lng.min(), lat.min(), lng.max(), lat.max()]


def test_egi_load_columns_none_meta_matches_partition(tmp_path):
    """``egi_load(columns=None)`` must produce a lazy meta whose columns match
    every computed partition, with no ``h3_03``/``year`` hive artifacts."""
    pytest.importorskip("h3")

    root = str(tmp_path / "db")
    bbox = _build_hive_h3_db(root)
    region = [bbox[0] - 0.05, bbox[1] - 0.05, bbox[2] + 0.05, bbox[3] + 0.05]

    ddf = g.egi_load(
        source=root, region=region, columns=None,
        index_level=1, partition_level=12, lazy=True,
    )

    meta_cols = set(ddf._meta.reset_index().columns)

    # No H3 partition artifacts in the declared schema.
    assert not any(str(c).startswith("h3_") or c == "year" for c in meta_cols), (
        f"declared meta must not carry H3 hive columns; got {sorted(meta_cols)}"
    )

    # The core invariant: computing a partition must not trip Dask's
    # check_matching_columns. Pre-fix this raised ValueError with
    # Extra=['h3_03','year'], Missing=['h3_12'].
    part = ddf.get_partition(0).compute()
    assert len(part) > 0, "fixture partition should carry rows"

    data_cols = set(part.reset_index().columns)
    assert not any(str(c).startswith("h3_") or c == "year" for c in data_cols), (
        f"computed partition must not leak H3 hive columns; got {sorted(data_cols)}"
    )
    assert data_cols == meta_cols, (
        "lazy meta and computed partition columns must match exactly.\n"
        f"  in DATA not META: {sorted(data_cols - meta_cols)}\n"
        f"  in META not DATA: {sorted(meta_cols - data_cols)}"
    )

    # The requested data variable survives the round-trip.
    assert "rh_098_l2a" in data_cols
