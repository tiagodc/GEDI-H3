"""
H3 Hexagon Rasterization Module

This module provides functions for converting H3-indexed GeoDataFrames
to raster format. Unlike EGI (which has native square pixels), H3 hexagons
require interpolation/resampling for raster output.

The workflow:
1. Determine appropriate resolution from H3 level
2. Find optimal UTM zone for the data extent
3. Rasterize using geocube with bilinear interpolation
4. Reproject to target CRS (default: EPSG:4326)
"""
from typing import List, Optional, Tuple, Union
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import h3
import pyproj
from geocube.api.core import make_geocube

from .config import H3_RASTER_CRS, get_geotiff_options


def get_h3_resolution_meters(h3_level: int) -> float:
    """
    Get the approximate pixel resolution in meters for an H3 level.

    Uses the average hexagon edge length * 2 (diameter) as the pixel size.

    Parameters
    ----------
    h3_level : int
        H3 resolution level (0-15)

    Returns
    -------
    float
        Approximate pixel size in meters
    """
    return h3.average_hexagon_edge_length(h3_level, 'm') * 2


def get_optimal_utm(gdf: gpd.GeoDataFrame) -> int:
    """
    Find the optimal UTM zone EPSG code for a GeoDataFrame's extent.

    Parameters
    ----------
    gdf : GeoDataFrame
        Input GeoDataFrame (any CRS)

    Returns
    -------
    int
        EPSG code for the optimal UTM zone
    """
    # Ensure we have bounds in WGS84
    if gdf.crs.to_epsg() != 4326:
        bounds = gdf.to_crs(4326).total_bounds
    else:
        bounds = gdf.total_bounds

    # Query UTM CRS for the area of interest
    aoi = pyproj.aoi.AreaOfInterest(*bounds)
    utm_list = pyproj.database.query_utm_crs_info(
        datum_name='WGS 84',
        area_of_interest=aoi
    )

    if not utm_list:
        # Fallback to WGS84 Pseudo-Mercator
        return 3857

    return utm_list[0].code


def h3_to_raster(
    gdf: gpd.GeoDataFrame,
    resolution: Optional[Tuple[float, float]] = None,
    columns: Optional[List[str]] = None,
    fill_value: float = np.nan,
    output_crs: str = H3_RASTER_CRS
) -> xr.Dataset:
    """
    Convert H3-indexed GeoDataFrame to raster (xarray Dataset).

    This function rasterizes H3 hexagon data using geocube, with automatic
    resolution determination based on the H3 level. The data is first
    reprojected to UTM for accurate rasterization, then to the output CRS.

    Parameters
    ----------
    gdf : GeoDataFrame
        H3-indexed GeoDataFrame with polygon geometries
    resolution : tuple of float, optional
        Output resolution as (y_res, x_res) in target CRS units.
        If None, automatically determined from H3 level.
    columns : list of str, optional
        Columns to rasterize. If None, all numeric columns are used.
    fill_value : float
        Value for pixels with no data (default: NaN)
    output_crs : str
        Output coordinate reference system (default: EPSG:4326)

    Returns
    -------
    xr.Dataset
        Raster dataset with one data variable per column

    Examples
    --------
    >>> # Rasterize H3 aggregated data
    >>> raster = h3_to_raster(h3_gdf, columns=['agbd_mean'])
    >>> raster.rio.to_raster("output.tif")
    """
    if gdf.empty:
        raise ValueError("Cannot rasterize empty GeoDataFrame")

    # Get H3 level and partition ID
    h3_index = gdf.index[0] if gdf.index.name and gdf.index.name.startswith('h3_') else None
    if h3_index is None and len(gdf.columns) > 0:
        h3_cols = [c for c in gdf.columns if str(c).startswith('h3_')]
        if h3_cols:
            h3_index = gdf[h3_cols[0]].iloc[0]

    if h3_index:
        h3_level = h3.get_resolution(h3_index)
        partition_id = h3.cell_to_parent(h3_index, 3)
    else:
        h3_level = 12  # Default to finest level
        partition_id = None

    # Determine resolution if not provided
    if resolution is None:
        res_meters = get_h3_resolution_meters(h3_level)
        # Get optimal UTM for accurate resolution
        utm_epsg = get_optimal_utm(gdf)

        # Create raster in UTM
        gdf_utm = gdf.to_crs(epsg=utm_epsg)
        xras = make_geocube(
            gdf_utm.reset_index(),
            measurements=columns,
            resolution=(-res_meters, res_meters),
            fill=fill_value
        )

        # Reproject to output CRS
        xras = xras.rio.reproject(output_crs)
        xres, yres = xras.rio.resolution()
        resolution = (yres, xres)
    else:
        # Use provided resolution directly
        xras = make_geocube(
            gdf.reset_index(),
            measurements=columns,
            resolution=resolution,
            fill=fill_value
        )

    # Add metadata
    xras = xras.assign_attrs(
        h3_level=h3_level,
        source='gedih3'
    )
    if partition_id:
        xras = xras.assign_attrs(h3_03_id=partition_id)
        for var in list(xras.data_vars):
            xras[var] = xras[var].assign_attrs(h3_03_id=partition_id)

    return xras


def rasterize_h3_partition(
    gdf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    output_crs: str = H3_RASTER_CRS,
    include_partition_id: bool = True
) -> pd.Series:
    """
    Rasterize a single H3 partition (for use with Dask map_partitions).

    This function is designed to be used with Dask's map_partitions for
    parallel rasterization of large datasets.

    Parameters
    ----------
    gdf : GeoDataFrame
        H3-indexed GeoDataFrame partition
    columns : list of str, optional
        Columns to rasterize
    output_crs : str
        Output coordinate reference system
    include_partition_id : bool
        If True, include H3 partition ID in raster attributes

    Returns
    -------
    pd.Series
        Series containing xarray DataArray(s)

    Examples
    --------
    >>> # With Dask
    >>> rasters = ddf.map_partitions(rasterize_h3_partition, meta=pd.Series(dtype=object))
    """
    if gdf.empty or len(gdf) == 0:
        return pd.Series(dtype=object)

    try:
        xras = h3_to_raster(gdf, columns=columns, output_crs=output_crs)

        if include_partition_id:
            # Get the H3 level 3 partition ID
            h3_index = gdf.index[0] if gdf.index.name and gdf.index.name.startswith('h3_') else None
            if h3_index:
                partition_id = h3.cell_to_parent(h3_index, 3)
                for var in list(xras.data_vars):
                    xras[var] = xras[var].assign_attrs(h3_03_id=partition_id)

        return pd.Series([xras])
    except Exception:
        return pd.Series(dtype=object)


def compute_raster_mosaic(
    raster_series: pd.Series,
    show_progress: bool = False
) -> xr.Dataset:
    """
    Merge multiple raster partitions into a single mosaic.

    Parameters
    ----------
    raster_series : pd.Series
        Series of xarray Datasets from rasterize_h3_partition
    show_progress : bool
        If True, show progress during computation

    Returns
    -------
    xr.Dataset
        Merged raster mosaic
    """
    from rioxarray import merge as rio_merge

    # Filter out empty/invalid partitions
    valid = raster_series.apply(
        lambda x: hasattr(x, 'data_vars') and len(x.data_vars) > 0
    )
    valid_rasters = raster_series[valid]

    if len(valid_rasters) == 0:
        raise ValueError("No valid raster partitions to merge")

    if len(valid_rasters) == 1:
        return valid_rasters.iloc[0]

    # Merge all partitions
    raster_list = valid_rasters.tolist()
    merged = xr.merge([
        rio_merge.merge_arrays([r[var] for r in raster_list if var in r])
        for var in raster_list[0].data_vars
    ])

    return merged


def get_h3_raster_resolution(
    gdf: gpd.GeoDataFrame,
    npartitions: int = 1
) -> Tuple[float, float]:
    """
    Determine the appropriate raster resolution for H3 data.

    Parameters
    ----------
    gdf : GeoDataFrame or dask GeoDataFrame
        H3-indexed data
    npartitions : int
        Number of partitions to sample (for dask)

    Returns
    -------
    tuple
        (y_resolution, x_resolution) in degrees (EPSG:4326)
    """
    # Get sample data
    if hasattr(gdf, 'npartitions'):
        sample = gdf.head(npartitions=min(gdf.npartitions, npartitions))
    else:
        sample = gdf

    if sample.empty:
        raise ValueError("Cannot determine resolution from empty GeoDataFrame")

    # Get H3 level
    h3_index = sample.index[0] if sample.index.name and sample.index.name.startswith('h3_') else None
    if h3_index is None:
        h3_cols = [c for c in sample.columns if str(c).startswith('h3_')]
        if h3_cols:
            h3_index = sample[h3_cols[0]].iloc[0]

    if h3_index is None:
        raise ValueError("Could not find H3 index in GeoDataFrame")

    h3_level = h3.get_resolution(h3_index)
    res_meters = get_h3_resolution_meters(h3_level)

    # Get UTM for accurate conversion
    utm_epsg = get_optimal_utm(sample)

    # Create a small test raster to get the resolution in degrees
    sample_utm = sample.head(10).to_crs(epsg=utm_epsg)
    test_raster = make_geocube(
        sample_utm.reset_index(),
        resolution=(-res_meters, res_meters)
    )
    test_raster = test_raster.rio.reproject(H3_RASTER_CRS)

    return test_raster.rio.resolution()
