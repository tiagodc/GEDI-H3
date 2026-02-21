import h3
import duckdb
import geopandas as gpd
import os
import warnings

from typing import List
from shapely.geometry import Polygon # typing only

from .config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
from .utils import get_system_resources

def init_duckdb(threads=None, memory_limit=None, temp_directory=None, max_temp_size=None):
    temp_directory = temp_directory if temp_directory is not None else f"{GH3_DEFAULT_TMP_DIR}/duckdb"
    os.makedirs(temp_directory, exist_ok=True)

    cpus, ram, storage = get_system_resources(disk_path=temp_directory)
    memory_limit = memory_limit if memory_limit is not None else int(ram * 0.75)
    max_temp_size = max_temp_size if max_temp_size is not None else storage // 4
    threads = threads if threads is not None else max(1, cpus // 4)

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET parquet_metadata_cache = true;")
    con.execute(f"SET memory_limit='{memory_limit}GB';")
    con.execute(f"SET temp_directory='{temp_directory}';")
    con.execute(f"SET max_temp_directory_size='{max_temp_size}GB';")
    con.execute(f"PRAGMA threads={threads};")
    return con

def attach_ducklake_db(con, name='gedi_dl'):
    """Attach existing ducklake database located in GH3_DEFAULT_H3_DIR.

    Once this function is called, the gedi data can be queried using
        `SELECT ... FROM {name}.data`
    """
    con.sql(f"""--sql
        ATTACH 'ducklake:{GH3_DEFAULT_H3_DIR}/gedi.ducklake' AS {name} (READ_ONLY);
        USE {name};
    """)

def shape_to_filter(shp: Polygon, resolution: int = 3):
    """Convert a shapely geometry to H3 cells at the given resolution."""
    h3shape = h3.geo_to_h3shape(shp)
    h3_cells = h3.h3shape_to_cells_experimental(h3shape, resolution, 'overlap')
    return "h3_03 = ANY({})".format(list(h3_cells))

def duck_to_gdf(
    table, geometry_columns=["geometry"], crs="EPSG:4326"
) -> gpd.GeoDataFrame:
    """Convert a DuckDB table to a GeoDataFrame.
    If multiple geometry columns are specified,
    the first will be set as the active geometry.
    """
    for geom_col in geometry_columns:
        if geom_col not in table.columns:
            raise ValueError(f"Column '{geom_col}' not found in table.")
    replace_cols = ", ".join(
        [f"ST_AsHEXWKB({col}) AS {col}" for col in geometry_columns]
    )
    df = table.select(f"* REPLACE ({replace_cols})").to_df()
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.GeoSeries.from_wkb(df[geometry_columns[0]]),
        crs=crs,
    )
    gdf.drop(columns=[geometry_columns[0]], inplace=True)
    if len(geometry_columns) > 1:
        for geom_col in geometry_columns[1:]:
            gdf[geom_col] = gpd.GeoSeries.from_wkb(df[geom_col])
    return gdf


def gdf_to_duck(
    con,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    geometry_columns: List[str] = ["geometry"],
):
    """Load a GeoDataFrame into a DuckDB table."""
    # Convert geometries to WKT
    gdf_tmp = gdf.copy()
    with warnings.catch_warnings():
        # ignore that the df now has a geometry column of strings
        warnings.simplefilter("ignore")
        for col in geometry_columns:
            gdf_tmp[col] = gdf_tmp[col].to_wkt()

    replace_cols = ", ".join(
        [f"ST_GeomFromText({col}) AS {col}" for col in geometry_columns]
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * REPLACE ({replace_cols})
        FROM gdf_tmp
    """)