"""Regression tests for the DuckDB <-> GeoDataFrame round-trip in sqlutils.

Covers the ``duck_to_gdf`` geometry-column bug: when the source geometry
column is already named ``"geometry"`` (the default), the old code created the
GeoDataFrame's active geometry from that column and then *dropped* it by name,
silently returning a frame with no geometry. The fix only drops the source
column when its name differs from ``"geometry"``.
"""
import duckdb
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from gedih3.sqlutils import duck_to_gdf, gdf_to_duck


@pytest.fixture
def con():
    """A DuckDB connection with the spatial extension, or skip if unavailable.

    The spatial extension is fetched from the network on first install; in an
    offline environment without a cached copy this test cannot run.
    """
    c = duckdb.connect()
    try:
        c.install_extension("spatial")
        c.load_extension("spatial")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"DuckDB spatial extension unavailable: {exc}")
    return c


def test_duck_to_gdf_preserves_default_geometry_column(con):
    """Round-trip with the default 'geometry' column must keep the geometry."""
    poly = box(-50, -10, -49.5, -9.5)
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"], "geometry": [poly, poly]}, crs="EPSG:4326"
    )

    rel = gdf_to_duck(con, gdf)
    out = duck_to_gdf(rel)

    assert "geometry" in out.columns
    assert out.geometry.name == "geometry"
    assert len(out) == 2
    assert not out.geometry.isna().any()


def test_duck_to_gdf_drops_renamed_source_column(con):
    """A non-'geometry' source column is consumed into the active geometry."""
    poly = box(-50, -10, -49.5, -9.5)
    gdf = gpd.GeoDataFrame(
        {"name": ["a"], "geom": [poly]}, geometry="geom", crs="EPSG:4326"
    )

    rel = gdf_to_duck(con, gdf, geometry_columns=["geom"])
    out = duck_to_gdf(rel, geometry_columns=["geom"])

    assert "geom" not in out.columns
    assert "geometry" in out.columns
    assert not out.geometry.isna().any()


def test_gdf_to_duck_handles_new_pandas_str_dtype(con):
    """New-style pandas 'str' dtype columns must load without error."""
    poly = box(-50, -10, -49.5, -9.5)
    gdf = gpd.GeoDataFrame(
        {"name": pd.array(["a", "b"], dtype="str"), "geometry": [poly, poly]},
        crs="EPSG:4326",
    )

    rel = gdf_to_duck(con, gdf)
    out = duck_to_gdf(rel)

    assert list(out["name"]) == ["a", "b"]
    assert "geometry" in out.columns
