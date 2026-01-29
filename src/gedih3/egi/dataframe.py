"""
EGI (EASE Grid Index) DataFrame Module

This module provides operations for integrating EGI spatial indexing with
pandas and GeoPandas DataFrames:
- Adding EGI indices to DataFrames based on coordinates
- Converting between resolution levels (to_parent)
- Aggregation functions with spatial grouping
- Conversion to/from GeoDataFrames
"""
from typing import Callable, Dict, List, Optional, Union
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import geopandas as gpd

from .config import (
    OUTER_LEVEL, EGI_CRS, EGI_CRS_STRING, egi_col_name, validate_level
)
from .core import to_hash, to_parent as _to_parent, get_level
from .spatial import to_geodataframe


def egi_dataframe(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    x_col: str = 'lon_lowestmode',
    y_col: str = 'lat_lowestmode',
    level: int = 1,
    in_epsg: int = 4326,
    set_index: bool = True
) -> gpd.GeoDataFrame:
    """
    Add EGI spatial index to a DataFrame based on coordinate columns.

    This is the primary function for converting GEDI shot data to EGI-indexed format.
    It reprojects coordinates to EPSG:6933 and computes EGI hashes at the specified
    resolution level.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input data with coordinate columns or Point geometries
    x_col : str
        Name of longitude/X column (default: 'lon_lowestmode')
    y_col : str
        Name of latitude/Y column (default: 'lat_lowestmode')
    level : int
        EGI resolution level (1-12), default=1 (finest)
    in_epsg : int
        EPSG code of input coordinates (default: 4326 for WGS84)
    set_index : bool
        If True, set the EGI column as the DataFrame index

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with EGI column added and optionally set as index

    Examples
    --------
    >>> # Add EGI index to GEDI shots
    >>> gedi_df = pd.read_parquet("gedi_shots.parquet")
    >>> egi_df = egi_dataframe(gedi_df, level=6)  # ~1km resolution
    >>>
    >>> # Using existing GeoDataFrame
    >>> gdf = gpd.read_file("points.gpkg")
    >>> egi_gdf = egi_dataframe(gdf, level=6)
    """
    validate_level(level)

    # Handle GeoDataFrame with Point geometry
    if isinstance(df, gpd.GeoDataFrame) and df.geom_type.iloc[0] == 'Point':
        gdf = df
    else:
        # Create GeoDataFrame from coordinate columns
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[x_col], df[y_col]),
            crs=f'EPSG:{in_epsg}'
        )

    # Reproject to EASE-Grid 2.0 if needed
    if gdf.crs.to_epsg() != EGI_CRS:
        gdf = gdf.to_crs(epsg=EGI_CRS)

    # Compute EGI hashes
    egi_col = egi_col_name(level)
    gdf[egi_col] = np.uint64([
        to_hash(x, y, level)
        for x, y in zip(gdf.geometry.x, gdf.geometry.y)
    ])

    if set_index:
        gdf = gdf.set_index(egi_col)

    return gdf


def egi_dataframe_vectorized(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    x_col: str = 'lon_lowestmode',
    y_col: str = 'lat_lowestmode',
    level: int = 1,
    in_epsg: int = 4326,
    set_index: bool = True
) -> gpd.GeoDataFrame:
    """
    Add EGI spatial index using vectorized operations (faster for large datasets).

    This is an optimized version of egi_dataframe() that uses numpy vectorization
    for better performance on large datasets.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input data with coordinate columns
    x_col : str
        Name of longitude/X column
    y_col : str
        Name of latitude/Y column
    level : int
        EGI resolution level (1-12)
    in_epsg : int
        EPSG code of input coordinates
    set_index : bool
        If True, set the EGI column as index

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with EGI column added
    """
    from pyproj import Transformer

    validate_level(level)

    # Get coordinates
    if isinstance(df, gpd.GeoDataFrame) and df.geom_type.iloc[0] == 'Point':
        if df.crs.to_epsg() == EGI_CRS:
            x = df.geometry.x.values
            y = df.geometry.y.values
        else:
            transformer = Transformer.from_crs(df.crs, f'EPSG:{EGI_CRS}', always_xy=True)
            x, y = transformer.transform(df.geometry.x.values, df.geometry.y.values)
        gdf = df
    else:
        x = df[x_col].values
        y = df[y_col].values

        if in_epsg != EGI_CRS:
            transformer = Transformer.from_crs(f'EPSG:{in_epsg}', f'EPSG:{EGI_CRS}', always_xy=True)
            x, y = transformer.transform(x, y)

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[x_col], df[y_col]),
            crs=f'EPSG:{in_epsg}'
        )

    # Vectorized hash computation
    egi_col = egi_col_name(level)
    gdf[egi_col] = to_hash(np.asarray(x), np.asarray(y), level)

    if set_index:
        gdf = gdf.set_index(egi_col)

    return gdf


def egi_to_parent(
    gdf: Union[pd.DataFrame, gpd.GeoDataFrame],
    parent_level: int = OUTER_LEVEL,
    set_index: bool = True
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Convert EGI-indexed DataFrame to a coarser resolution level.

    Parameters
    ----------
    gdf : DataFrame or GeoDataFrame
        EGI-indexed DataFrame (index must be EGI hash)
    parent_level : int
        Target coarser resolution level
    set_index : bool
        If True, replace the index with the parent level

    Returns
    -------
    DataFrame or GeoDataFrame
        DataFrame with parent-level EGI column/index

    Examples
    --------
    >>> # Aggregate from level 1 to level 6
    >>> parent_df = egi_to_parent(fine_df, parent_level=6)
    """
    validate_level(parent_level)

    # Get current level from index
    current_level = int(gdf.index[0] // np.uint64(1e18))
    if parent_level <= current_level:
        return gdf

    parent_col = egi_col_name(parent_level)

    # Compute parent hashes
    parent_hashes = np.uint64([_to_parent(i, parent_level) for i in gdf.index.to_numpy()])
    gdf = gdf.assign(**{parent_col: parent_hashes})

    if set_index:
        gdf = gdf.reset_index().set_index(parent_col)

    return gdf


def egi_to_parent_vectorized(
    gdf: Union[pd.DataFrame, gpd.GeoDataFrame],
    parent_level: int = OUTER_LEVEL,
    set_index: bool = True
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Convert EGI-indexed DataFrame to coarser resolution (vectorized version).

    This is an optimized version using numpy vectorization.

    Parameters
    ----------
    gdf : DataFrame or GeoDataFrame
        EGI-indexed DataFrame
    parent_level : int
        Target coarser resolution level
    set_index : bool
        If True, replace the index with parent level

    Returns
    -------
    DataFrame or GeoDataFrame
        DataFrame with parent-level EGI column/index
    """
    validate_level(parent_level)

    current_level = int(gdf.index[0] // np.uint64(1e18))
    if parent_level <= current_level:
        return gdf

    parent_col = egi_col_name(parent_level)

    # Vectorized parent hash computation
    parent_hashes = _to_parent(gdf.index.to_numpy().astype(np.uint64), parent_level)
    gdf = gdf.assign(**{parent_col: parent_hashes})

    if set_index:
        gdf = gdf.reset_index().set_index(parent_col)

    return gdf


def egi_to_geo(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    polygons: bool = True
) -> gpd.GeoDataFrame:
    """
    Add geometry to an EGI-indexed DataFrame.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        EGI-indexed DataFrame
    polygons : bool
        If True, use polygon geometries; if False, use point centroids

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with geometry column added

    Examples
    --------
    >>> # Add polygon geometries for visualization
    >>> gdf = egi_to_geo(aggregated_df, polygons=True)
    """
    geom_gdf = to_geodataframe(df.index.to_numpy(), return_polygons=polygons)
    gdf = gpd.GeoDataFrame(df, geometry=geom_gdf.geometry.values, crs=EGI_CRS_STRING)
    return gdf


def egi_aggregate(
    gdf: Union[pd.DataFrame, gpd.GeoDataFrame],
    mapper: Union[str, List[str], Dict, Callable] = 'mean',
    return_geometry: bool = True,
    geom_points: bool = False
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Aggregate EGI-indexed DataFrame by spatial index.

    Parameters
    ----------
    gdf : DataFrame or GeoDataFrame
        EGI-indexed DataFrame
    mapper : str, list, dict, or callable
        Aggregation specification:
        - str: Single aggregation function (e.g., 'mean', 'sum', 'count')
        - list: Multiple functions ['mean', 'std', 'count']
        - dict: Per-column specification {'col1': 'mean', 'col2': ['min', 'max']}
        - callable: Custom aggregation function
    return_geometry : bool
        If True, return GeoDataFrame with geometry
    geom_points : bool
        If True and return_geometry, use point centroids instead of polygons

    Returns
    -------
    DataFrame or GeoDataFrame
        Aggregated data, optionally with geometry

    Examples
    --------
    >>> # Simple mean aggregation
    >>> agg_df = egi_aggregate(shots_df, mapper='mean')
    >>>
    >>> # Multiple aggregations
    >>> agg_df = egi_aggregate(shots_df, mapper=['mean', 'std', 'count'])
    >>>
    >>> # Per-column specification
    >>> agg_df = egi_aggregate(shots_df, mapper={'agbd': 'mean', 'rh_098': ['mean', 'std']})
    """
    # Remove geometry column if present (will be regenerated)
    if 'geometry' in gdf.columns:
        gdf = gdf.drop(columns='geometry')

    # Perform aggregation
    if callable(mapper):
        df_agg = pd.DataFrame(gdf.groupby(level=0).apply(mapper))
        # Handle MultiIndex from apply
        if isinstance(df_agg.index, pd.MultiIndex):
            df_agg.index = df_agg.index.get_level_values(0)
    else:
        df_agg = gdf.groupby(level=0).agg(mapper)

    # Add geometry if requested
    if return_geometry:
        geom = to_geodataframe(df_agg.index.to_numpy(), return_polygons=not geom_points)

        # Flatten MultiIndex columns if present
        if isinstance(df_agg.columns, pd.MultiIndex):
            df_agg.columns = ['_'.join(str(c) for c in col) for col in df_agg.columns]

        df_agg = geom.join(df_agg)

    return df_agg


def egi_col_from_df(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Optional[str]:
    """
    Find the EGI column in a DataFrame.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        DataFrame to search

    Returns
    -------
    str or None
        Name of the EGI column, or None if not found
    """
    # Check index name
    if df.index.name and df.index.name.startswith('egi'):
        return df.index.name

    # Check columns
    egi_cols = [col for col in df.columns if str(col).startswith('egi')]
    if egi_cols:
        return sorted(egi_cols)[0]

    return None


def egi_get_level_from_df(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Optional[int]:
    """
    Get the EGI resolution level from a DataFrame's index.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        EGI-indexed DataFrame

    Returns
    -------
    int or None
        Resolution level, or None if not EGI-indexed
    """
    col = egi_col_from_df(df)
    if col is None:
        return None

    # Extract level from column name (e.g., 'egi06' -> 6)
    try:
        return int(col[3:])
    except ValueError:
        # Fall back to examining the index values
        if df.index.name and df.index.name.startswith('egi'):
            return int(get_level(np.uint64(df.index[0])))
        return None
