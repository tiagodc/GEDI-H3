#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

"""
Raster Image Sampling at GEDI Shot Locations

Sample raster pixel values at true GEDI shot coordinates (Points), with
optional moving-window statistics (sum, mean, median, mode).

Supports single raster files, VRT mosaics, and tile directories (auto-mosaicked
via VRT). Works with both H3 databases and simplified datasets from gh3_extract.
"""

import os
import re
import logging
import glob as _glob

import numpy as np
import pandas as pd
import geopandas as gpd

from .exceptions import GediImageSamplingError

logger = logging.getLogger(__name__)

# Window operation IDs (matches legacy 3-digit spec)
_WINDOW_OPS = {0: 'sum', 1: 'mean', 2: 'median', 3: 'mode', 4: 'std', 5: 'min', 6: 'max', 7: 'count', 8: 'range'}


# =============================================================================
# VRT Resolution
# =============================================================================

def resolve_raster_source(image_path, file_format='tif', odir=None):
    """Resolve image input to a single raster source.

    - Single file (.tif, .vrt, etc.) -> return as-is
    - Directory of tiles -> build VRT mosaic, return VRT path

    Parameters
    ----------
    image_path : str
        Path to raster file, VRT file, or directory of tiles
    file_format : str
        File extension to glob when image_path is a directory
    odir : str, optional
        Output directory for saving VRT beside output. If provided, VRT is
        saved as ``{odir}/{tiles_dirname}.vrt`` instead of inside the tiles
        directory.

    Returns
    -------
    tuple
        (raster_path, is_temp_vrt, tile_count)

    Raises
    ------
    GediImageSamplingError
        If no valid raster found
    """
    # Remote URLs are valid GDAL sources — pass through directly
    if image_path.startswith(('http://', 'https://', 's3://', '/vsicurl/', '/vsis3/')):
        return image_path, False, 1

    if os.path.isfile(image_path):
        return image_path, False, 1

    if not os.path.isdir(image_path):
        raise GediImageSamplingError(f"Image path not found: {image_path}")

    # Directory: glob for tiles
    tiles = sorted(_glob.glob(os.path.join(image_path, f'*.{file_format}')))
    if not tiles:
        raise GediImageSamplingError(
            f"No .{file_format} files found in {image_path}"
        )

    if len(tiles) == 1:
        return tiles[0], False, 1

    # Build VRT mosaic — save beside output dir with tiles directory basename
    from .raster.export import build_vrt

    tiles_basename = os.path.basename(os.path.normpath(image_path))
    if odir:
        os.makedirs(odir, exist_ok=True)
        vrt_path = os.path.join(odir, f'{tiles_basename}.vrt')
    else:
        vrt_path = os.path.join(image_path, '_gedih3_mosaic.vrt')
    build_vrt(tiles, vrt_path)
    logger.info(f"Built VRT mosaic from {len(tiles)} tiles: {vrt_path}")
    return vrt_path, True, len(tiles)


# =============================================================================
# Raster Metadata
# =============================================================================

def get_raster_info(raster_path):
    """Read raster metadata (CRS, bounds, resolution, bands, nodata).

    Also computes bounds in WGS84 (EPSG:4326) for spatial filtering of GEDI data.

    Parameters
    ----------
    raster_path : str
        Path to raster file or VRT

    Returns
    -------
    dict
        Keys: crs, bounds, bounds_wgs84, resolution, shape, band_count,
              band_names, nodata
    """
    import rioxarray

    with rioxarray.open_rasterio(raster_path) as ras:
        crs = ras.rio.crs
        bounds = ras.rio.bounds()  # (minx, miny, maxx, maxy)
        resolution = ras.rio.resolution()  # (x_res, y_res)
        shape = (ras.sizes.get('y', ras.shape[-2]), ras.sizes.get('x', ras.shape[-1]))
        band_count = ras.sizes.get('band', ras.shape[0]) if len(ras.shape) == 3 else 1
        nodata = ras.rio.nodata

        # Band names from long_name attribute or fallback to b0, b1, ...
        if hasattr(ras, 'long_name') and ras.long_name is not None:
            long_name = ras.long_name
            if isinstance(long_name, str):
                band_names = [long_name]
            else:
                band_names = list(long_name)
        else:
            band_names = [f'b{i}' for i in range(band_count)]

    # Compute WGS84 bounds for spatial filtering
    from pyproj import Transformer

    if crs and not crs.to_epsg() == 4326:
        transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
        x_coords = [bounds[0], bounds[2], bounds[0], bounds[2]]
        y_coords = [bounds[1], bounds[1], bounds[3], bounds[3]]
        lons, lats = transformer.transform(x_coords, y_coords)
        bounds_wgs84 = (min(lons), min(lats), max(lons), max(lats))
    else:
        bounds_wgs84 = bounds

    return {
        'crs': crs,
        'bounds': bounds,
        'bounds_wgs84': bounds_wgs84,
        'resolution': resolution,
        'shape': shape,
        'band_count': band_count,
        'band_names': band_names,
        'nodata': nodata,
    }


# =============================================================================
# Window Operations (ported from legacy, with median fix)
# =============================================================================

def _window_sum(data, size):
    """Sum within a moving window."""
    from scipy.ndimage import convolve
    kernel = np.ones((size, size))
    return convolve(data, kernel, mode='constant', cval=0)


def _window_mean(data, size):
    """Mean within a moving window."""
    from scipy.ndimage import convolve
    kernel = np.ones((size, size)) / (size * size)
    return convolve(data, kernel, mode='nearest')


def _window_median(data, size):
    """Median within a moving window."""
    from scipy.ndimage import percentile_filter
    return percentile_filter(data, 50, size=(size, size), mode='nearest')


def _window_mode(data, size):
    """Mode within a moving window (optimized binary-mask convolution).

    For each unique value, creates a binary mask and convolves with a ones
    kernel to count occurrences in the window. Tracks the value with the
    highest count at each pixel.
    """
    from scipy.ndimage import convolve

    if not np.issubdtype(data.dtype, np.integer):
        data = data.astype(int)

    kernel = np.ones((size, size))
    uvals = np.unique(data)
    result = np.full_like(data, uvals[0])
    best_count = convolve((data == uvals[0]).astype(np.uint8), kernel, mode='constant', cval=0)

    for u in uvals[1:]:
        count = convolve((data == u).astype(np.uint8), kernel, mode='constant', cval=0)
        mask = count > best_count
        result[mask] = u
        best_count[mask] = count[mask]

    return result


def _window_std(data, size):
    """Std deviation within a moving window via sum-of-squares (exact, O(n))."""
    from scipy.ndimage import convolve
    kernel = np.ones((size, size)) / (size * size)
    d = data.astype(float)
    mean = convolve(d, kernel, mode='nearest')
    mean_sq = convolve(d ** 2, kernel, mode='nearest')
    return np.sqrt(np.maximum(0.0, mean_sq - mean ** 2))


def _window_min(data, size):
    """Minimum within a moving window."""
    from scipy.ndimage import minimum_filter
    return minimum_filter(data, size=(size, size), mode='nearest')


def _window_max(data, size):
    """Maximum within a moving window."""
    from scipy.ndimage import maximum_filter
    return maximum_filter(data, size=(size, size), mode='nearest')


def _window_count(data, size):
    """Count of valid (non-NaN/non-zero) pixels within a moving window."""
    from scipy.ndimage import convolve
    kernel = np.ones((size, size))
    valid = (data != 0).astype(np.uint16) if not np.issubdtype(data.dtype, np.floating) \
        else np.isfinite(data).astype(np.uint16)
    return convolve(valid, kernel, mode='constant', cval=0)


def _window_range(data, size):
    """Range (max - min) within a moving window."""
    from scipy.ndimage import maximum_filter, minimum_filter
    return maximum_filter(data, size=(size, size), mode='nearest') \
         - minimum_filter(data, size=(size, size), mode='nearest')


_WINDOW_FUNCS = {
    'sum': _window_sum,
    'mean': _window_mean,
    'median': _window_median,
    'mode': _window_mode,
    'std': _window_std,
    'min': _window_min,
    'max': _window_max,
    'count': _window_count,
    'range': _window_range,
}


# =============================================================================
# Window Spec Parsing
# =============================================================================

def parse_window_specs(specs):
    """Parse legacy 3-digit format for window operations.

    Format: each spec is a 3-character string 'BZO' where:
    - B = band number (0-indexed)
    - Z = window size (1-9, must be odd)
    - O = operation ID (0=sum, 1=mean, 2=median, 3=mode, 4=std, 5=min, 6=max, 7=count, 8=range)

    Parameters
    ----------
    specs : list of str
        Window spec strings, e.g. ['033', '151']

    Returns
    -------
    list of dict
        Each dict has: band (int), size (int), op (str), name (str)

    Raises
    ------
    GediImageSamplingError
        If any spec is invalid
    """
    if specs is None:
        return []

    result = []
    for spec in specs:
        spec = str(spec)
        if len(spec) != 3:
            raise GediImageSamplingError(
                f"Window spec must be 3 digits (band/size/op), got '{spec}'"
            )
        try:
            band = int(spec[0])
            size = int(spec[1])
            op_id = int(spec[2])
        except ValueError:
            raise GediImageSamplingError(
                f"Window spec must be 3 digits (band/size/op), got '{spec}'"
            )

        if size < 1 or size > 9 or size % 2 == 0:
            raise GediImageSamplingError(
                f"Window size must be odd 1-9, got {size} in spec '{spec}'"
            )
        if op_id not in _WINDOW_OPS:
            raise GediImageSamplingError(
                f"Window op must be 0-8, got {op_id} in spec '{spec}'"
            )

        op_name = _WINDOW_OPS[op_id]
        result.append({
            'band': band,
            'size': size,
            'op': op_name,
            'name': f'b{band}_{op_name}_{size}x{size}',
        })

    return result


# =============================================================================
# Core Sampling Function (called via map_partitions)
# =============================================================================

def sample_raster_at_points(df, raster_path, band_names=None,
                            window_ops=None, fillna=None, dropna=False,
                            geo=False, partition_col=None, band_indices=None,
                            all_band_names=None, pixel_distance=False):
    """Sample raster values at GEDI shot locations within a single partition.

    Designed to be called via Dask map_partitions. For each partition:
    1. Extract true point coordinates from geometry column
    2. Compute partition bbox with buffer for window operations
    3. Open raster, clip to bbox (reads only relevant VRT tiles)
    4. Reproject points to raster CRS if needed
    5. Sample nearest pixel for each shot
    6. Compute relative_pixel_distance (if pixel_distance=True)
    7. Apply window operations if specified

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input partition with geometry column (Point geometries)
    raster_path : str
        Path to raster file or VRT
    band_names : list of str, optional
        Names for the output band columns. When ``band_indices`` is provided,
        should have length == len(band_indices).
    window_ops : list of dict, optional
        Parsed window operation specs from parse_window_specs().
        Band indices in window_ops refer to original raster bands (0-indexed).
    fillna : float, optional
        Value to fill raster NaN/NoData before sampling
    dropna : bool
        Drop rows where all band columns are NaN
    geo : bool
        Include geometry in output
    partition_col : str, optional
        Partition column name to preserve in output
    band_indices : list of int, optional
        0-based indices of raster bands to sample. If None, all bands are sampled.
    all_band_names : list of str, optional
        Full list of raster band names (all bands). Used for resolving window
        operation column names when ``band_indices`` selects a subset. If None,
        defaults to ``band_names``.
    pixel_distance : bool
        If True, include ``relative_pixel_distance`` column in output.
        Defaults to False.

    Returns
    -------
    DataFrame or GeoDataFrame
        Sampled data with band columns, optional relative_pixel_distance,
        and optional window operation columns
    """
    import xarray as xr
    import rioxarray

    if df is None or (hasattr(df, 'empty') and df.empty) or len(df) == 0:
        spatial_cols = None
        if partition_col:
            spatial_cols = {partition_col: 'object'}
        return _empty_sampling_result(band_names, window_ops, geo, partition_col, spatial_cols=spatial_cols,
                                      pixel_distance=pixel_distance)

    # --- Extract coordinates ---
    if 'geometry' in df.columns and hasattr(df['geometry'], 'geom_type'):
        geom = df['geometry']
        # Ensure points, not other geometry types
        pts_lon = geom.x.values
        pts_lat = geom.y.values
    else:
        # Fallback to coordinate columns
        from .cliutils import find_coordinate_column
        lon_col = find_coordinate_column(df.columns, 'lon_lowestmode')
        lat_col = find_coordinate_column(df.columns, 'lat_lowestmode')
        if lon_col is None or lat_col is None:
            raise GediImageSamplingError(
                "Cannot extract coordinates: no geometry column and no "
                "lon_lowestmode/lat_lowestmode columns found"
            )
        pts_lon = df[lon_col].values
        pts_lat = df[lat_col].values

    if len(pts_lon) == 0:
        spatial_cols = _detect_spatial_cols(df) or ({partition_col: 'object'} if partition_col else None)
        return _empty_sampling_result(band_names, window_ops, geo, partition_col, spatial_cols=spatial_cols,
                                      pixel_distance=pixel_distance)

    # --- Resolve band selection ---
    # Build merged band index set (selected + window-referenced) for efficient loading
    if band_indices is not None:
        window_band_set = {wop['band'] for wop in (window_ops or [])}
        load_indices = sorted(set(band_indices) | window_band_set)
        load_map = {orig: pos for pos, orig in enumerate(load_indices)}
    else:
        load_indices = None
        load_map = None

    # --- Open raster and clip to partition bbox ---
    max_win_size = max((w['size'] for w in window_ops), default=0) if window_ops else 0

    with rioxarray.open_rasterio(raster_path, masked=True) as ras:
        ras_crs = ras.rio.crs
        ras_bounds = ras.rio.bounds()  # (minx, miny, maxx, maxy)
        ras_res = np.abs(ras.rio.resolution()).mean()
        ras_nodata = ras.rio.nodata

        # Determine band names from raster if not provided
        if band_names is None:
            if hasattr(ras, 'long_name') and ras.long_name is not None:
                ln = ras.long_name
                band_names = [ln] if isinstance(ln, str) else list(ln)
            else:
                nb = ras.sizes.get('band', ras.shape[0]) if len(ras.shape) == 3 else 1
                band_names = [f'b{i}' for i in range(nb)]

        # Ensure all_band_names is set (for window column naming)
        # Must be set after band_names auto-detection from raster
        if all_band_names is None:
            all_band_names = band_names

        # Reproject points to raster CRS if needed
        if ras_crs and ras_crs.to_epsg() != 4326:
            from pyproj import Transformer
            transformer = Transformer.from_crs('EPSG:4326', ras_crs, always_xy=True)
            pts_x, pts_y = transformer.transform(pts_lon, pts_lat)
        else:
            pts_x = pts_lon
            pts_y = pts_lat

        # Identify shots inside raster bounds
        inside_mask = (
            (pts_x >= ras_bounds[0]) & (pts_x <= ras_bounds[2]) &
            (pts_y >= ras_bounds[1]) & (pts_y <= ras_bounds[3])
        )

        # Initialize output columns
        n_shots = len(df)
        band_values = {bn: np.full(n_shots, np.nan) for bn in band_names}
        distances = np.full(n_shots, np.nan)
        window_values = {}
        if window_ops:
            for wop in window_ops:
                col_name = _resolve_window_col_name(wop, all_band_names)
                window_values[col_name] = np.full(n_shots, np.nan)

        if not inside_mask.any():
            # No shots inside raster — return all NaN
            pass
        else:
            # Compute bbox of inside points with buffer
            inside_x = pts_x[inside_mask]
            inside_y = pts_y[inside_mask]
            buffer = max_win_size * ras_res if max_win_size > 0 else ras_res
            clip_minx = float(inside_x.min()) - buffer
            clip_miny = float(inside_y.min()) - buffer
            clip_maxx = float(inside_x.max()) + buffer
            clip_maxy = float(inside_y.max()) + buffer

            # Clip raster to bbox (efficient for VRT — reads only overlapping tiles)
            try:
                ras_clip = ras.rio.clip_box(
                    minx=clip_minx, miny=clip_miny,
                    maxx=clip_maxx, maxy=clip_maxy
                )
            except Exception:
                # clip_box can fail if bbox doesn't overlap at all
                ras_clip = None

            if ras_clip is not None and ras_clip.sizes.get('x', 0) > 0 and ras_clip.sizes.get('y', 0) > 0:
                if fillna is not None:
                    ras_clip = ras_clip.fillna(fillna)

                # Select only needed bands when band_indices is provided
                if load_indices is not None and len(ras_clip.shape) == 3:
                    ras_clip = ras_clip.isel(band=load_indices)

                # Sample nearest pixel for inside shots
                tgt_x = xr.DataArray(inside_x, dims='points')
                tgt_y = xr.DataArray(inside_y, dims='points')
                sampled = ras_clip.sel(x=tgt_x, y=tgt_y, method='nearest')

                # Extract band values
                inside_idx = np.where(inside_mask)[0]
                if len(ras_clip.shape) == 3:
                    if band_indices is not None:
                        # Map selected band indices to positions in the loaded subset
                        for i, bn in enumerate(band_names):
                            pos = load_map[band_indices[i]]
                            band_values[bn][inside_idx] = sampled.isel(band=pos).values
                    else:
                        for i, bn in enumerate(band_names):
                            band_values[bn][inside_idx] = sampled.isel(band=i).values
                else:
                    band_values[band_names[0]][inside_idx] = sampled.values

                # Compute relative_pixel_distance
                # Get actual sampled x,y coordinates
                sampled_x = sampled.coords['x'].values
                sampled_y = sampled.coords['y'].values
                offset_x = np.abs(inside_x - sampled_x)
                offset_y = np.abs(inside_y - sampled_y)
                dist = (offset_x + offset_y) / 2.0 / ras_res
                distances[inside_idx] = dist

                # Apply window operations
                if window_ops:
                    for wop in window_ops:
                        band_idx = wop['band']
                        if len(ras_clip.shape) == 3:
                            # Use load_map to find position in loaded band subset
                            pos = load_map[band_idx] if load_map else band_idx
                            band_data = ras_clip.isel(band=pos).values.copy()
                        else:
                            band_data = ras_clip.values.copy()
                            if len(band_data.shape) == 3:
                                band_data = band_data[0]

                        # Handle nodata: replace with NaN for window ops
                        if ras_nodata is not None and fillna is None:
                            band_data = band_data.astype(float)
                            band_data[band_data == ras_nodata] = np.nan

                        # Apply window function
                        wfunc = _WINDOW_FUNCS[wop['op']]
                        filtered = wfunc(band_data, wop['size'])

                        # Sample filtered raster at inside points
                        filtered_xr = xr.DataArray(
                            filtered,
                            coords={'y': ras_clip.coords['y'], 'x': ras_clip.coords['x']},
                            dims=['y', 'x']
                        )
                        w_sampled = filtered_xr.sel(x=tgt_x, y=tgt_y, method='nearest')
                        col_name = _resolve_window_col_name(wop, all_band_names)
                        window_values[col_name][inside_idx] = w_sampled.values

    # --- Build output DataFrame ---
    out = {}

    # Preserve all spatial index columns (h3_XX, egiXX)
    # Check DataFrame index first
    if df.index.name and re.match(r'^(h3_\d{2}|egi\d{2})$', str(df.index.name)):
        out[df.index.name] = df.index.values
    # Check columns
    for col in df.columns:
        if re.match(r'^(h3_\d{2}|egi\d{2})$', str(col)):
            out[str(col)] = df[col].values
    # Warn if partition_col was specified but not found
    if partition_col and partition_col not in out:
        logger.warning(f"Partition column '{partition_col}' not found in data")

    # shot_number
    sn_col = None
    for c in df.columns:
        if c.startswith('shot_number'):
            sn_col = c
            break
    if sn_col:
        out['shot_number'] = df[sn_col].values
    else:
        logger.warning(
            "shot_number column not found in partition — output will lack shot identifiers. "
            "Use an H3 database or gh3_extract output as the data source."
        )

    # Band values
    out.update(band_values)

    # Distance
    if pixel_distance:
        out['relative_pixel_distance'] = distances

    # Window values
    out.update(window_values)

    result = pd.DataFrame(out)

    # Geometry
    if geo and 'geometry' in df.columns:
        result = gpd.GeoDataFrame(result, geometry=df['geometry'].values, crs='EPSG:4326')

    # dropna: drop rows where ALL band columns are NaN
    if dropna:
        result = result.dropna(subset=band_names, how='all')

    # Set finest spatial column as the DataFrame index
    idx_col = _finest_spatial_col(result.columns)
    if idx_col:
        result = result.set_index(idx_col)

    return result


def _resolve_window_col_name(wop, band_names):
    """Resolve a window op's output column name using actual band names."""
    bname = band_names[wop['band']] if band_names and wop['band'] < len(band_names) else f"b{wop['band']}"
    return wop['name'].replace(f"b{wop['band']}_", f"{bname}_")


def _detect_spatial_cols(df):
    """Detect all spatial index columns (h3_XX, egiXX) from a DataFrame.

    Inspects both columns and index name for spatial patterns.

    Returns
    -------
    dict
        Mapping of column_name -> dtype string for all detected spatial columns.
    """
    spatial = {}

    # Check DataFrame index
    if df.index.name and re.match(r'^(h3_\d{2}|egi\d{2})$', str(df.index.name)):
        spatial[df.index.name] = str(df.index.dtype)

    # Check columns
    for col in df.columns:
        if re.match(r'^(h3_\d{2}|egi\d{2})$', str(col)):
            spatial[str(col)] = str(df[col].dtype)

    return spatial


def _finest_spatial_col(col_names):
    """Return the finest-resolution spatial column name from a list.

    For H3: highest level number is finest (h3_12 > h3_03).
    For EGI: lowest level number is finest (egi01 < egi12).
    Returns None if no spatial columns found.
    """
    h3_cols = sorted([c for c in col_names if re.match(r'^h3_\d{2}$', str(c))], reverse=True)
    egi_cols = sorted([c for c in col_names if re.match(r'^egi\d{2}$', str(c))])
    if h3_cols:
        return h3_cols[0]
    if egi_cols:
        return egi_cols[0]
    return None


def _empty_sampling_result(band_names, window_ops, geo, partition_col,
                           spatial_cols=None, all_band_names=None,
                           pixel_distance=False):
    """Return empty DataFrame matching the sampling output schema.

    Parameters
    ----------
    band_names : list of str
        Band column names for output
    window_ops : list of dict
        Parsed window specs
    geo : bool
        Include geometry column
    partition_col : str or None
        Partition column name (included in spatial_cols if provided)
    spatial_cols : dict, optional
        Dict of spatial column name -> dtype to include in output.
        If None, falls back to including just partition_col.
    all_band_names : list of str, optional
        Full raster band names for window column naming. Defaults to band_names.
    """
    if all_band_names is None:
        all_band_names = band_names
    cols = {}
    if spatial_cols:
        for col_name, dtype in spatial_cols.items():
            cols[col_name] = pd.Series(dtype=dtype)
    elif partition_col:
        cols[partition_col] = pd.Series(dtype=str)
    # uint64 matches the on-disk GEDI shot_number dtype. Using int64 here lets
    # Dask reconcile a mixed (int64-meta, uint64-data) graph by upcasting to
    # float64 across the whole column — and float64 has only ~15 significant
    # digits, while shot_numbers are 19+ digit integers, so the cast silently
    # collapses thousands of distinct shots onto the same float value. That
    # corrupts every downstream merge-on-shot_number and produces a Cartesian
    # blowup (one float-collapsed key → many real shots).
    cols['shot_number'] = pd.Series(dtype='uint64')
    if band_names:
        for bn in band_names:
            cols[bn] = pd.Series(dtype='float64')
    if pixel_distance:
        cols['relative_pixel_distance'] = pd.Series(dtype='float64')
    if window_ops:
        for wop in window_ops:
            col_name = _resolve_window_col_name(wop, all_band_names)
            cols[col_name] = pd.Series(dtype='float64')
    if geo:
        result = gpd.GeoDataFrame(cols, geometry=gpd.GeoSeries(dtype='geometry'))
    else:
        result = pd.DataFrame(cols)

    # Set finest spatial column as index (matches sample_raster_at_points behavior)
    idx_col = _finest_spatial_col(result.columns)
    if idx_col:
        result = result.set_index(idx_col)

    return result


# =============================================================================
# Meta computation for Dask map_partitions
# =============================================================================

def _compute_sampling_meta(band_names, window_ops, geo, partition_col,
                           spatial_cols=None, all_band_names=None,
                           pixel_distance=False):
    """Build empty DataFrame with correct schema for Dask map_partitions meta.

    Parameters
    ----------
    band_names : list of str
        Band column names for output
    window_ops : list of dict
        Parsed window specs
    geo : bool
        Include geometry column
    partition_col : str or None
        Partition column name
    spatial_cols : dict, optional
        Dict of spatial column name -> dtype to include in output schema
    all_band_names : list of str, optional
        Full raster band names for window column naming. Defaults to band_names.

    Returns
    -------
    DataFrame or GeoDataFrame
        Empty frame matching output schema
    """
    return _empty_sampling_result(band_names, window_ops, geo, partition_col,
                                  spatial_cols=spatial_cols, all_band_names=all_band_names,
                                  pixel_distance=pixel_distance)


# =============================================================================
# High-Level Python API
# =============================================================================

def from_image(image_path, data_source=None, region=None,
               query=None, band_names=None, band_indices=None,
               window_ops=None, fillna=None,
               dropna=False, geo=False, file_format='tif',
               pixel_distance=False):
    """Sample raster values at GEDI shot locations.

    Supports two input modes with different ROI logic:

    Mode 1 - H3 database (via data_source pointing to an H3 database):
      ROI = image boundaries (intersected with user region if provided).
      Only H3 partitions overlapping the image are loaded.

    Mode 2 - Simplified dataset (via data_source pointing to a simplified dataset):
      ROI = entire dataset (all tiles loaded regardless of image coverage).
      Shots outside image bounds get NaN values.

    In both cases, uses TRUE shot geometry for coordinate extraction.

    Parameters
    ----------
    image_path : str
        Path to raster file, VRT, or tile directory
    data_source : str, optional
        Path to H3 database or simplified dataset directory
    region : GeoDataFrame or bbox, optional
        Additional spatial filter
    query : str, optional
        Pandas query string for filtering shots
    band_names : list of str, optional
        Custom band names for output columns. When ``band_indices`` is
        provided, should have length == len(band_indices).
    band_indices : list of int, optional
        0-based indices of raster bands to sample. If None, all bands.
    window_ops : list of dict, optional
        Parsed window specs from parse_window_specs()
    fillna : float, optional
        Value to fill raster NaN/NoData
    dropna : bool
        Drop rows where all band columns are NaN
    geo : bool
        Include geometry in output
    file_format : str
        Tile file extension for directory input
    pixel_distance : bool
        If True, include ``relative_pixel_distance`` column in output.
        Defaults to False.

    Returns
    -------
    dask.dataframe.DataFrame
        Sampled data as Dask DataFrame

    Raises
    ------
    GediImageSamplingError
        If inputs are invalid or sampling fails
    """
    import dask.dataframe
    import gedih3.gh3driver as gh3

    if data_source is None:
        raise GediImageSamplingError(
            "Must provide data_source (H3 database or simplified dataset)"
        )

    # Resolve raster source
    raster_path, is_vrt, tile_count = resolve_raster_source(image_path, file_format)
    raster_info = get_raster_info(raster_path)
    logger.info(f"Raster: {raster_path} ({raster_info['band_count']} bands, "
                f"CRS={raster_info['crs']})")

    # Resolve band names
    all_band_names = raster_info['band_names']
    if band_names is None:
        if band_indices is not None:
            band_names = [all_band_names[i] for i in band_indices]
        else:
            band_names = all_band_names

    # Detect input type and load data
    from .cliutils import get_dataset_index_info
    ds_info = get_dataset_index_info(data_source)

    if ds_info['source_type'] == 'h3_database':
        # Mode 1: H3 database — ROI = image bounds (intersected with user region)
        from shapely.geometry import box

        img_box = box(*raster_info['bounds_wgs84'])
        if region is not None:
            if hasattr(region, 'geometry'):
                roi = gpd.GeoDataFrame(geometry=[img_box], crs='EPSG:4326')
                roi = gpd.overlay(roi, region.to_crs('EPSG:4326'), how='intersection')
            else:
                roi = gpd.GeoDataFrame(geometry=[img_box], crs='EPSG:4326')
        else:
            roi = gpd.GeoDataFrame(geometry=[img_box], crs='EPSG:4326')

        columns = ['geometry']  # Always need geometry for coordinate extraction
        ddf = gh3.gh3_load(source=data_source, columns=columns, region=roi, query=query)
        # Detect partition column
        part_level = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=data_source)
        from .cliutils import h3_col_name
        partition_col = h3_col_name(part_level)
    else:
        # Mode 2: Simplified dataset — load all tiles
        ddf = gh3.gh3_load(data_source)
        if query:
            ddf = ddf.query(query)

        # Detect partition column from dataset (ds_info already computed above)
        if ds_info['index_type'] == 'egi':
            from .egi.config import egi_col_name
            part_level = ds_info.get('partition_level') or ds_info.get('egi_partition_level')
            partition_col = egi_col_name(part_level) if part_level else None
        elif ds_info['index_type'] == 'h3':
            from .cliutils import h3_col_name
            part_level = ds_info.get('partition_level') or ds_info.get('h3_partition_level')
            partition_col = h3_col_name(part_level) if part_level else None
        else:
            partition_col = None

    # Validate geometry is available
    if 'geometry' not in ddf.columns:
        raise GediImageSamplingError(
            "Input data must contain geometry column for coordinate extraction. "
            "For simplified datasets, re-extract with the -g flag."
        )

    # Compute meta for map_partitions
    meta = _compute_sampling_meta(band_names, window_ops, geo, partition_col,
                                  all_band_names=all_band_names if band_indices else None,
                                  pixel_distance=pixel_distance)

    # Apply sampling via map_partitions
    result = ddf.map_partitions(
        sample_raster_at_points,
        raster_path=raster_path,
        band_names=band_names,
        window_ops=window_ops,
        fillna=fillna,
        dropna=dropna,
        geo=geo,
        partition_col=partition_col,
        band_indices=band_indices,
        all_band_names=all_band_names if band_indices else None,
        pixel_distance=pixel_distance,
        meta=meta
    )

    return result
