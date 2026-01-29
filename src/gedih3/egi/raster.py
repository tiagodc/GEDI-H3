"""
EGI (EASE Grid Index) Raster Module

This module provides rasterization functions for converting EGI-indexed
GeoDataFrames to raster (xarray/GeoTIFF) format. The native alignment of
EGI with EASE-Grid 2.0 allows for direct rasterization without resampling.
"""
from typing import List, Optional, Union
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from rasterio import transform

from .config import LIMITS, RESOLUTIONS, OUTER_LEVEL, EGI_CRS_STRING, get_resolution
from .core import get_level
from .spatial import pixel_shape
from .dataframe import egi_to_parent


def geodf_to_raster(
    geodf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    fill_value: float = np.nan
) -> xr.Dataset:
    """
    Convert EGI-indexed GeoDataFrame to raster (xarray Dataset).

    This function creates a raster aligned to the EASE-Grid 2.0 projection,
    with each EGI pixel mapped to a corresponding raster cell. The native
    alignment avoids any resampling artifacts.

    Parameters
    ----------
    geodf : GeoDataFrame
        EGI-indexed GeoDataFrame with polygon geometries
    columns : list of str, optional
        Columns to rasterize. If None, all numeric columns are used.
    fill_value : float
        Value for pixels with no data (default: NaN)

    Returns
    -------
    xr.Dataset
        Raster dataset with one data variable per column

    Examples
    --------
    >>> # Rasterize aggregated GEDI data
    >>> raster = geodf_to_raster(agg_gdf, columns=['agbd_mean', 'rh_098_mean'])
    >>> raster.rio.to_raster("output.tif")
    """
    from geocube.api.core import make_geocube

    # Get level and resolution from index
    level = int(geodf.index[0] // np.uint64(1e18))
    res = RESOLUTIONS[level]

    # Calculate alignment offset to ensure pixels align with EGI grid
    bound_x = round(LIMITS['lon_w'] % res, 6)
    bound_y = round(LIMITS['lat_s'] % res, 6)

    # Create raster using geocube
    img = make_geocube(
        geodf.reset_index(),
        measurements=columns,
        resolution=(-res, res),
        align=(bound_y, bound_x),
        fill=fill_value
    )

    # Determine the outer tile for proper extent
    _df = geodf.sample(100) if len(geodf) > 100 else geodf
    pid = egi_to_parent(_df, OUTER_LEVEL).index.value_counts().idxmax()

    # Get the outer tile bounds and dimensions
    left, bottom, right, top = pixel_shape(pid).bounds
    height = int((top - bottom) / res)
    width = int((right - left) / res)
    trf = transform.from_bounds(left, bottom, right, top, width, height)

    # Reproject to exact tile extent
    img = img.rio.reproject(
        img.rio.crs,
        shape=(height, width),
        transform=trf
    )

    return img


def rasterize_partition(
    gdf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    include_egi_id: bool = True
) -> pd.Series:
    """
    Rasterize a single EGI partition (for use with Dask map_partitions).

    This function is designed to be used with Dask's map_partitions for
    parallel rasterization of large datasets.

    Parameters
    ----------
    gdf : GeoDataFrame
        EGI-indexed GeoDataFrame partition
    columns : list of str, optional
        Columns to rasterize
    include_egi_id : bool
        If True, include outer tile ID in raster attributes

    Returns
    -------
    pd.Series
        Series containing xarray DataArray(s)

    Examples
    --------
    >>> # With Dask
    >>> rasters = ddf.map_partitions(rasterize_partition, meta=pd.Series(dtype=object))
    """
    if len(gdf) == 0:
        return pd.Series(dtype=object)

    try:
        xras = geodf_to_raster(gdf, columns=columns)

        if include_egi_id:
            # Get the dominant outer tile ID
            _df = gdf.sample(100) if len(gdf) > 100 else gdf
            egi_id = egi_to_parent(_df, OUTER_LEVEL).index.value_counts().idxmax()

            # Add tile ID as attribute
            for var in list(xras.data_vars):
                xras[var] = xras[var].assign_attrs(egi12_id=egi_id)

        return pd.Series(xras)
    except Exception:
        return pd.Series(dtype=object)


def export_raster(
    xras: xr.Dataset,
    output_path: str,
    compress: str = 'LZW',
    tiled: bool = True,
    blocksize: int = 256,
    bigtiff: bool = True
) -> str:
    """
    Export xarray Dataset to GeoTIFF file.

    Parameters
    ----------
    xras : xr.Dataset
        Raster dataset to export
    output_path : str
        Output file path
    compress : str
        Compression method ('LZW', 'ZSTD', 'DEFLATE', None)
    tiled : bool
        Use tiled output format
    blocksize : int
        Tile block size in pixels
    bigtiff : bool
        Use BigTIFF format for large files

    Returns
    -------
    str
        Output file path
    """
    xras.rio.to_raster(
        output_path,
        compress=compress,
        TILED='YES' if tiled else 'NO',
        BLOCKXSIZE=blocksize,
        BLOCKYSIZE=blocksize,
        BIGTIFF='YES' if bigtiff else 'NO'
    )
    return output_path


def merge_raster_partitions(
    raster_series: pd.Series,
    output_path: Optional[str] = None
) -> xr.Dataset:
    """
    Merge multiple raster partitions into a single raster.

    Parameters
    ----------
    raster_series : pd.Series
        Series of xarray DataArrays from rasterize_partition
    output_path : str, optional
        If provided, save merged raster to this path

    Returns
    -------
    xr.Dataset
        Merged raster dataset
    """
    from rioxarray import merge

    # Filter out empty partitions
    valid_rasters = raster_series[raster_series.apply(lambda x: hasattr(x, 'shape') and all(np.array(x.shape) > 1))]

    if len(valid_rasters) == 0:
        raise ValueError("No valid raster partitions to merge")

    # Group by outer tile and merge within each tile
    merged_tiles = []
    for idx in valid_rasters.index.unique():
        tile_rasters = valid_rasters.loc[idx]
        if isinstance(tile_rasters, pd.Series):
            merged = merge.merge_arrays(tile_rasters.tolist())
        else:
            merged = tile_rasters
        merged_tiles.append(merged)

    # Merge all tiles
    result = xr.merge(merged_tiles)

    if output_path:
        export_raster(result, output_path)

    return result


def get_raster_profile(
    level: int,
    bounds: tuple,
    crs: str = EGI_CRS_STRING
) -> dict:
    """
    Generate a rasterio profile for EGI raster output.

    Parameters
    ----------
    level : int
        EGI resolution level
    bounds : tuple
        Raster bounds (left, bottom, right, top)
    crs : str
        Coordinate reference system

    Returns
    -------
    dict
        Rasterio profile dictionary
    """
    res = get_resolution(level)
    left, bottom, right, top = bounds
    width = int((right - left) / res)
    height = int((top - bottom) / res)

    return {
        'driver': 'GTiff',
        'dtype': 'float32',
        'width': width,
        'height': height,
        'count': 1,
        'crs': crs,
        'transform': transform.from_bounds(left, bottom, right, top, width, height),
        'compress': 'lzw',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256
    }
