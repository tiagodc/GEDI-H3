# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
EGI (EASE Grid Index) Raster Module

This module provides rasterization functions for converting EGI-indexed
GeoDataFrames to raster (xarray/GeoTIFF) format. The native alignment of
EGI with EASE-Grid 2.0 allows for direct rasterization without resampling.
"""
import logging
from typing import List, Optional
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import rioxarray  # Register .rio accessor for xarray
from affine import Affine
from rasterio import transform
from .config import RESOLUTIONS, OUTER_RES, OUTER_LEVEL, EGI_CRS_STRING, get_resolution
from .spatial import pixel_shape
from ..cliutils import filter_raster_columns as _filter_raster_columns
from ..exceptions import GediRasterizationError

logger = logging.getLogger(__name__)


def geodf_to_raster(
    geodf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    fill_value: float = np.nan,
    outer_tile: Optional[int] = None
) -> xr.Dataset:
    """
    Convert EGI-indexed GeoDataFrame to raster (xarray Dataset).

    This function creates a raster aligned to the EASE-Grid 2.0 projection,
    with each EGI pixel mapped directly to a corresponding raster cell using
    direct index assignment (no interpolation or extrapolation).

    The raster covers exactly ONE level-12 outer tile. When that tile is
    given explicitly (``outer_tile``), stray pixels from any other tile are
    skipped with a WARNING — their inner indices are positions within their
    own tile and would land at wrong map coordinates in this tile's grid
    (this protects legacy datasets extracted before the ``to_hash``
    boundary-overflow carry and the ``egi_load`` spillover filter, which can
    still hold cross-tile rows). When no tile is given, the input must
    resolve to a single tile; a genuine multi-tile input raises
    ``GediRasterizationError`` instead of guessing which tile to keep —
    split by outer tile first (``rasterize_partition``) to keep every pixel.

    Parameters
    ----------
    geodf : GeoDataFrame
        EGI-indexed GeoDataFrame (index must be EGI hash values)
    columns : list of str, optional
        Columns to rasterize. If None, all numeric columns are used.
        Internal columns (egi indices, h3 indices) are automatically excluded.
    fill_value : float
        Value for pixels with no data (default: NaN)
    outer_tile : int, optional
        Level-12 EGI hash of the tile to rasterize. Callers that know their
        tile (partition writers, ``rasterize_partition``) should always pass
        it. When None, the input must resolve to a single outer tile; a
        multi-tile input with no ``outer_tile`` hint raises
        ``GediRasterizationError`` rather than guessing which tile to keep —
        split by outer tile first (``rasterize_partition``) so no pixels are
        dropped.

    Raises
    ------
    GediRasterizationError
        If the input spans more than one outer tile and no ``outer_tile`` is
        given (the function rasterizes exactly one tile and will not silently
        drop the rest).

    Returns
    -------
    xr.Dataset
        Raster dataset with one data variable per column

    Examples
    --------
    >>> # Rasterize aggregated GEDI data
    >>> raster = geodf_to_raster(agg_gdf, columns=['agbd_mean', 'rh_098_mean'])
    >>> raster.rio.to_raster("output.tif")

    Notes
    -----
    This implementation uses direct index-based pixel assignment:
    1. Decode each EGI hash to get pixel indices within the outer tile
    2. Create a raster array with exact tile dimensions
    3. Assign values directly to pixel locations

    This guarantees one polygon = one pixel with no extrapolation.
    """
    from .core import from_hash

    # Handle empty GeoDataFrames
    if len(geodf) == 0:
        return xr.Dataset()

    # Filter out internal columns from rasterization
    columns = _filter_raster_columns(columns, geodf)
    if columns is None or len(columns) == 0:
        raise GediRasterizationError("No columns to rasterize. Provide numeric columns or check input data.")

    # Get EGI level and resolution from index
    egi_hashes = np.asarray(geodf.index.values, dtype=np.uint64)
    level = int(egi_hashes[0] // np.uint64(1e18))
    res = RESOLUTIONS[level]

    # Determine the target outer tile. ``outer_ids`` packs px_outer*1000 +
    # py_outer for every input pixel (same digit layout from_hash decodes).
    outer_ids = (egi_hashes % np.uint64(10**18)) // np.uint64(10**12)
    uniq_outer, outer_counts = np.unique(outer_ids, return_counts=True)
    if outer_tile is not None:
        pid = np.uint64(outer_tile)
        target_outer = (pid % np.uint64(10**18)) // np.uint64(10**12)
    elif len(uniq_outer) == 1:
        # No hint, but the input resolves to exactly one tile — that tile is
        # determined, not guessed.
        target_outer = uniq_outer[0]
        pid = (np.uint64(OUTER_LEVEL) * np.uint64(10**18)
               + target_outer * np.uint64(10**12))
    else:
        # Multi-tile input with no explicit target. Refuse rather than pick a
        # winner and silently drop the rest: this function rasterizes exactly
        # one outer tile, so callers must split by tile first
        # (``rasterize_partition``) or pass the tile they want explicitly.
        raise GediRasterizationError(
            f"geodf_to_raster received {len(uniq_outer)} outer tiles but "
            f"rasterizes exactly one. Split the input by outer tile "
            f"(egi.rasterize_partition) so every pixel is kept, or pass "
            f"outer_tile=<level-12 hash> to select the tile to rasterize. "
            f"Tiles present: {sorted(int(t) for t in uniq_outer)}."
        )

    n_dropped = int(outer_counts[uniq_outer != target_outer].sum())
    if n_dropped:
        logger.warning(
            f"geodf_to_raster: input spans {len(uniq_outer)} outer tiles; "
            f"rasterizing tile {int(pid)} and skipping {n_dropped} pixel(s) "
            f"from other tile(s). Single-tile input is expected — mixed "
            f"tiles indicate a legacy dataset (pre boundary-overflow fix) "
            f"or a multi-tile API call; split by outer tile "
            f"(rasterize_partition) to keep every pixel."
        )

    # Get outer tile bounds
    left, bottom, _, _ = pixel_shape(pid).bounds
    pixels_per_tile = int(round(OUTER_RES / res))

    # Always use the full (unclamped) tile extent for coordinates and the
    # affine transform.  Tiles at the eastern CRS boundary have a clamped
    # `right`, which makes from_bounds() produce a smaller x_cell (~900 m
    # instead of ~1001 m). Using bottom + OUTER_RES as the canonical top
    # guarantees every tile carries the same pixel size regardless of
    # CRS clipping.
    tile_top = bottom + OUTER_RES

    # Create coordinate arrays for xarray
    # Y coordinates go from top to bottom (north-up convention)
    y_coords = tile_top - res * (np.arange(pixels_per_tile) + 0.5)
    x_coords = left + res * (np.arange(pixels_per_tile) + 0.5)

    # Extract outer tile indices from the dominant tile
    _, _, px_outer_tile, py_outer_tile, _, _ = from_hash(np.uint64(pid))

    # Create data arrays for each column
    data_vars = {}
    for col in columns:
        # Initialize raster with fill value
        raster_data = np.full((pixels_per_tile, pixels_per_tile), fill_value, dtype=np.float32)

        # Get column values
        values = geodf[col].values

        # Decode each hash and assign values to pixels
        for i, (egi_hash, value) in enumerate(zip(egi_hashes, values)):
            _, _, px_outer, py_outer, px_inner, py_inner = from_hash(np.uint64(egi_hash))

            # Only process pixels that belong to this outer tile
            if px_outer == px_outer_tile and py_outer == py_outer_tile:
                # Row index: from top (y inverted for north-up)
                row = pixels_per_tile - 1 - int(py_inner)
                col_idx = int(px_inner)

                if 0 <= row < pixels_per_tile and 0 <= col_idx < pixels_per_tile:
                    raster_data[row, col_idx] = value

        # Create DataArray
        da = xr.DataArray(
            raster_data,
            dims=['y', 'x'],
            coords={'y': y_coords, 'x': x_coords},
            name=col
        )
        data_vars[col] = da

    # Create Dataset
    ds = xr.Dataset(data_vars)

    # Set CRS
    ds = ds.rio.write_crs(EGI_CRS_STRING)

    # Set NoData on each variable so GeoTIFF exports mask empty pixels correctly
    for var in ds.data_vars:
        if np.issubdtype(ds[var].dtype, np.floating):
            ds[var] = ds[var].rio.write_nodata(np.nan)

    # Set transform using canonical EGI pixel size
    trf = Affine(res, 0.0, left, 0.0, -res, tile_top)
    ds = ds.rio.write_transform(trf)

    return ds


def rasterize_partition(
    gdf: gpd.GeoDataFrame,
    columns: Optional[List[str]] = None,
    include_egi_id: bool = True
) -> pd.Series:
    """
    Rasterize a single EGI partition (for use with Dask map_partitions).

    This function splits the partition by outer tile and rasterizes each
    tile separately to ensure proper alignment and avoid mixing data from
    different tiles.

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
        Series containing xarray Dataset(s), one per outer tile

    Examples
    --------
    >>> # With Dask
    >>> rasters = ddf.map_partitions(rasterize_partition, meta=pd.Series(dtype=object))
    """
    if len(gdf) == 0:
        return pd.Series(dtype=object)

    import logging
    logger = logging.getLogger(__name__)

    try:
        # Split data by outer tile to ensure proper rasterization
        # Each outer tile will be rasterized separately
        egi_hashes = np.asarray(gdf.index.values, dtype=np.uint64)
        outer_tiles = (egi_hashes // np.uint64(1e12)) * np.uint64(1e12)
        unique_outer = np.unique(outer_tiles)

        results = []
        for outer_tile in unique_outer:
            # Filter data for this outer tile
            mask = outer_tiles == outer_tile
            tile_gdf = gdf.iloc[mask]

            if len(tile_gdf) == 0:
                continue

            try:
                # Tile ID at level 12 (consistent regardless of data level) —
                # passed to geodf_to_raster as the explicit target so no
                # tile inference happens, and reused as the raster attribute.
                p_outer = outer_tile % np.uint64(1e18) // np.uint64(1e12)
                egi12_id = int(np.uint64(OUTER_LEVEL * 1e18) + np.uint64(p_outer * 1e12))

                # Rasterize this tile's data
                xras = geodf_to_raster(tile_gdf, columns=columns, outer_tile=egi12_id)

                if len(xras.data_vars) > 0:
                    for var in list(xras.data_vars):
                        xras[var] = xras[var].assign_attrs(egi12_id=egi12_id)

                    results.append(xras)
            except Exception as e:
                logger.debug(f"Rasterization failed for tile {outer_tile}: {e}")
                continue

        if not results:
            return pd.Series(dtype=object)

        return pd.Series(results)

    except Exception as e:
        logger.debug(f"Rasterization failed: {e}")
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
        Series of xarray Datasets from rasterize_partition
    output_path : str, optional
        If provided, save merged raster to this path

    Returns
    -------
    xr.Dataset
        Merged raster dataset
    """
    from rioxarray.merge import merge_datasets

    # Filter out empty partitions and None values
    def is_valid_raster(x):
        if x is None:
            return False
        if isinstance(x, xr.Dataset):
            return len(x.data_vars) > 0
        if hasattr(x, 'shape'):
            return all(s > 0 for s in x.shape)
        return False

    valid_rasters = [r for r in raster_series if is_valid_raster(r)]

    if len(valid_rasters) == 0:
        raise ValueError("No valid raster partitions to merge")

    # Merge all rasters
    # Use merge_datasets which handles overlapping regions correctly
    # IMPORTANT: Use nodata=np.nan to ensure gaps are filled with NaN, not 0
    result = merge_datasets(valid_rasters, nodata=np.nan)

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
