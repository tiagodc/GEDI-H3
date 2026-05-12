#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
Vector Polygon Spatial Join at GEDI Shot Locations

Spatially join polygon attributes (e.g., ecoregion names, administrative
boundaries) to GEDI shot locations based on spatial containment.

Supports shapefiles, GeoPackages, GeoJSON, and GeoParquet. Works with both
H3 databases and simplified datasets from gh3_extract.
"""

import os
import re
import logging
import glob as _glob

import pandas as pd
import geopandas as gpd

from .exceptions import GediSpatialJoinError

logger = logging.getLogger(__name__)

# Supported vector file extensions
_VECTOR_EXTENSIONS = ('*.shp', '*.gpkg', '*.geojson', '*.parquet', '*.geoparquet')

# Worker-level cache for polygon data (same pattern as raster file opening in imgutils)
_VECTOR_CACHE = {}


# =============================================================================
# Vector Source Resolution
# =============================================================================

def resolve_vector_source(vector_path, file_format='*'):
    """Resolve vector input to a single file path.

    - Single file -> return as-is
    - Directory -> glob for vector files

    Parameters
    ----------
    vector_path : str
        Path to vector file or directory of vector files
    file_format : str
        File extension to glob when vector_path is a directory.
        Use '*' to search all supported formats.

    Returns
    -------
    tuple
        (file_path, file_count)

    Raises
    ------
    GediSpatialJoinError
        If no valid vector file found
    """
    if os.path.isfile(vector_path):
        return vector_path, 1

    if not os.path.isdir(vector_path):
        raise GediSpatialJoinError(f"Vector path not found: {vector_path}")

    # Directory: glob for vector files
    if file_format == '*':
        files = []
        for ext in _VECTOR_EXTENSIONS:
            files.extend(_glob.glob(os.path.join(vector_path, ext)))
        files = sorted(set(files))
    else:
        fmt = file_format.lstrip('.')
        files = sorted(_glob.glob(os.path.join(vector_path, f'*.{fmt}')))

    if not files:
        raise GediSpatialJoinError(
            f"No vector files found in {vector_path}"
        )

    if len(files) == 1:
        return files[0], 1

    # Multiple files found — use the first one and warn
    logger.warning(
        f"Multiple vector files found in {vector_path}, using first: "
        f"{os.path.basename(files[0])}"
    )
    return files[0], len(files)


# =============================================================================
# Vector Metadata
# =============================================================================

def get_vector_info(vector_path):
    """Read vector file metadata without loading all features.

    Parameters
    ----------
    vector_path : str
        Path to vector file

    Returns
    -------
    dict
        Keys: crs, bounds_wgs84, columns, feature_count, geometry_type
    """
    import fiona

    with fiona.open(vector_path) as src:
        crs = src.crs
        bounds = src.bounds  # (minx, miny, maxx, maxy)
        feature_count = len(src)
        schema = src.schema
        geometry_type = schema['geometry']
        columns = list(schema['properties'].keys())

    # Compute WGS84 bounds for spatial filtering
    from pyproj import CRS, Transformer

    src_crs = CRS.from_user_input(crs)
    if src_crs.to_epsg() != 4326:
        transformer = Transformer.from_crs(src_crs, 'EPSG:4326', always_xy=True)
        x_coords = [bounds[0], bounds[2], bounds[0], bounds[2]]
        y_coords = [bounds[1], bounds[1], bounds[3], bounds[3]]
        lons, lats = transformer.transform(x_coords, y_coords)
        bounds_wgs84 = (min(lons), min(lats), max(lons), max(lats))
    else:
        bounds_wgs84 = bounds

    return {
        'crs': str(src_crs),
        'bounds_wgs84': bounds_wgs84,
        'columns': columns,
        'feature_count': feature_count,
        'geometry_type': geometry_type,
    }


# =============================================================================
# Vector Loading (with worker-level cache)
# =============================================================================

def load_vector(vector_path, columns=None, to_crs=4326):
    """Load polygon GeoDataFrame, filter columns, reproject to WGS84.

    Parameters
    ----------
    vector_path : str
        Path to vector file
    columns : list of str, optional
        Polygon attribute columns to include. If None, all columns.
    to_crs : int or str
        Target CRS (default: EPSG:4326 for WGS84)

    Returns
    -------
    GeoDataFrame
        Polygon geometries in target CRS with spatial index

    Raises
    ------
    GediSpatialJoinError
        If file contains non-polygon geometries
    """
    gdf = gpd.read_file(vector_path)

    # Validate geometry types
    geom_types = set(gdf.geometry.geom_type)
    valid_types = {'Polygon', 'MultiPolygon'}
    invalid = geom_types - valid_types
    if invalid:
        raise GediSpatialJoinError(
            f"Vector file contains unsupported geometry types: {invalid}. "
            f"Only Polygon and MultiPolygon are supported."
        )

    # Filter columns
    if columns is not None:
        missing = [c for c in columns if c not in gdf.columns]
        if missing:
            raise GediSpatialJoinError(
                f"Columns not found in vector file: {missing}. "
                f"Available: {list(gdf.columns.drop('geometry'))}"
            )
        gdf = gdf[columns + ['geometry']]

    # Reproject
    if gdf.crs is not None and gdf.crs.to_epsg() != to_crs:
        gdf = gdf.to_crs(epsg=to_crs)
    elif gdf.crs is None:
        logger.warning("Vector file has no CRS defined, assuming EPSG:4326")
        gdf = gdf.set_crs(epsg=4326)

    # Ensure spatial index is built
    gdf.sindex  # noqa: B018

    return gdf


def _get_cached_polygons(vector_path, columns=None):
    """Get polygon GeoDataFrame with worker-level caching.

    Each Dask worker loads the polygon file once and reuses it across
    partitions. Same pattern as raster file opening in imgutils.py.

    Parameters
    ----------
    vector_path : str
        Path to vector file
    columns : list of str or None
        Columns to include (used as part of cache key)

    Returns
    -------
    GeoDataFrame
        Cached polygon data
    """
    # Cache key includes columns to handle different column selections
    cache_key = (vector_path, tuple(columns) if columns else None)

    if cache_key not in _VECTOR_CACHE:
        _VECTOR_CACHE[cache_key] = load_vector(vector_path, columns=columns)

    return _VECTOR_CACHE[cache_key]


# =============================================================================
# Spatial column detection (reuse from imgutils)
# =============================================================================

def _detect_spatial_cols(df):
    """Detect all spatial index columns (h3_XX, egiXX) from a DataFrame.

    Inspects both columns and index name for spatial patterns.
    """
    from .imgutils import _detect_spatial_cols
    return _detect_spatial_cols(df)


def _finest_spatial_col(col_names):
    """Return the finest-resolution spatial column name from a list."""
    from .imgutils import _finest_spatial_col
    return _finest_spatial_col(col_names)


# =============================================================================
# Core Spatial Join Function (called via map_partitions)
# =============================================================================

def join_polygons_to_points(df, vector_path, join_columns=None,
                            predicate='within', how='left', prefix=None,
                            partition_col=None, geo=False):
    """Spatially join polygon attributes to GEDI shot locations within a partition.

    Designed to be called via Dask map_partitions. For each partition:
    1. Load polygons via worker-level cache
    2. Build GeoDataFrame from partition points
    3. Perform spatial join (gpd.sjoin)
    4. Cleanup: drop index_right, apply column prefix, handle conflicts

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input partition with geometry column (Point geometries)
    vector_path : str
        Path to polygon vector file
    join_columns : list of str, optional
        Polygon attribute columns to include. If None, all polygon columns.
    predicate : str
        Spatial join predicate: 'within' or 'intersects'
    how : str
        Join type: 'left' (keep all shots) or 'inner' (matched only)
    prefix : str, optional
        Prefix to add to polygon column names (avoids conflicts)
    partition_col : str, optional
        Partition column name to preserve in output
    geo : bool
        Include geometry in output

    Returns
    -------
    DataFrame or GeoDataFrame
        Joined data with polygon attribute columns
    """
    if df is None or (hasattr(df, 'empty') and df.empty) or len(df) == 0:
        return _empty_join_result(
            join_columns, prefix, geo, partition_col,
            spatial_cols={partition_col: 'object'} if partition_col else None
        )

    # Load polygons (cached per worker)
    polygons = _get_cached_polygons(vector_path, columns=join_columns)

    # Determine polygon columns to include
    poly_cols = [c for c in polygons.columns if c != 'geometry']

    # Build GeoDataFrame from partition if not already
    if not isinstance(df, gpd.GeoDataFrame):
        if 'geometry' not in df.columns:
            raise GediSpatialJoinError(
                "Partition has no geometry column for spatial join"
            )
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    else:
        gdf = df
        if gdf.crs is None:
            gdf = gdf.set_crs('EPSG:4326')

    # Check for column conflicts before join
    existing_cols = set(gdf.columns) - {'geometry'}
    conflicts = existing_cols & set(poly_cols)
    if conflicts and prefix is None:
        raise GediSpatialJoinError(
            f"Column name conflicts between GEDI data and polygon file: "
            f"{sorted(conflicts)}. Use the -x/--prefix flag to add a prefix "
            f"to polygon column names."
        )

    # Perform spatial join
    try:
        joined = gpd.sjoin(gdf, polygons, how=how, predicate=predicate)
    except Exception as e:
        raise GediSpatialJoinError(f"Spatial join failed: {e}")

    # Cleanup: drop index_right column added by sjoin
    if 'index_right' in joined.columns:
        joined = joined.drop(columns=['index_right'])

    # Warn about duplicates (overlapping polygons)
    n_orig = len(gdf)
    n_joined = len(joined)
    if n_joined > n_orig:
        logger.debug(
            f"Spatial join produced {n_joined - n_orig} extra rows from "
            f"overlapping polygons (partition had {n_orig} shots)"
        )

    # Apply column prefix if specified
    if prefix:
        rename_map = {c: f"{prefix}{c}" for c in poly_cols if c in joined.columns}
        joined = joined.rename(columns=rename_map)
        poly_cols = [rename_map.get(c, c) for c in poly_cols]

    # --- Build output DataFrame ---
    out = {}

    # Preserve all spatial index columns (h3_XX, egiXX)
    if joined.index.name and re.match(r'^(h3_\d{2}|egi\d{2})$', str(joined.index.name)):
        out[joined.index.name] = joined.index.values
    for col in joined.columns:
        if re.match(r'^(h3_\d{2}|egi\d{2})$', str(col)):
            out[str(col)] = joined[col].values
    if partition_col and partition_col not in out:
        logger.warning(f"Partition column '{partition_col}' not found in data")

    # shot_number
    sn_col = None
    for c in joined.columns:
        if c.startswith('shot_number'):
            sn_col = c
            break
    if sn_col:
        out['shot_number'] = joined[sn_col].values
    else:
        logger.warning(
            "shot_number column not found in partition — output will lack shot identifiers. "
            "Use an H3 database or gh3_extract output as the data source."
        )

    # Polygon attribute columns
    for c in poly_cols:
        if c in joined.columns:
            out[c] = joined[c].values

    result = pd.DataFrame(out)

    # Geometry
    if geo and 'geometry' in joined.columns:
        result = gpd.GeoDataFrame(result, geometry=joined['geometry'].values, crs='EPSG:4326')

    # Set finest spatial column as the DataFrame index
    idx_col = _finest_spatial_col(result.columns)
    if idx_col:
        result = result.set_index(idx_col)

    return result


# =============================================================================
# Meta computation for Dask map_partitions
# =============================================================================

def _empty_join_result(join_columns, prefix, geo, partition_col,
                       spatial_cols=None):
    """Return empty DataFrame matching the join output schema.

    Parameters
    ----------
    join_columns : list of str or None
        Polygon attribute column names
    prefix : str or None
        Column name prefix
    geo : bool
        Include geometry column
    partition_col : str or None
        Partition column name
    spatial_cols : dict, optional
        Dict of spatial column name -> dtype
    """
    cols = {}
    if spatial_cols:
        for col_name, dtype in spatial_cols.items():
            cols[col_name] = pd.Series(dtype=dtype)
    elif partition_col:
        cols[partition_col] = pd.Series(dtype='object')
    cols['shot_number'] = pd.Series(dtype='int64')

    if join_columns:
        prefixed = [f"{prefix}{c}" if prefix else c for c in join_columns]
        for c in prefixed:
            cols[c] = pd.Series(dtype='object')

    if geo:
        result = gpd.GeoDataFrame(cols, geometry=gpd.GeoSeries(dtype='geometry'))
    else:
        result = pd.DataFrame(cols)

    idx_col = _finest_spatial_col(result.columns)
    if idx_col:
        result = result.set_index(idx_col)

    return result


def _compute_join_meta(join_columns, polygon_dtypes, prefix, geo,
                       partition_col, spatial_cols=None):
    """Build empty DataFrame with correct schema for Dask map_partitions meta.

    Parameters
    ----------
    join_columns : list of str or None
        Polygon attribute column names
    polygon_dtypes : dict
        Mapping of polygon column name -> dtype (from polygon GeoDataFrame)
    prefix : str or None
        Column name prefix
    geo : bool
        Include geometry column
    partition_col : str or None
        Partition column name
    spatial_cols : dict, optional
        Dict of spatial column name -> dtype

    Returns
    -------
    DataFrame or GeoDataFrame
        Empty frame matching output schema
    """
    cols = {}
    if spatial_cols:
        for col_name, dtype in spatial_cols.items():
            cols[col_name] = pd.Series(dtype=dtype)
    elif partition_col:
        cols[partition_col] = pd.Series(dtype='object')
    cols['shot_number'] = pd.Series(dtype='int64')

    if join_columns and polygon_dtypes:
        for c in join_columns:
            col_name = f"{prefix}{c}" if prefix else c
            dtype = polygon_dtypes.get(c, 'object')
            cols[col_name] = pd.Series(dtype=dtype)

    if geo:
        result = gpd.GeoDataFrame(cols, geometry=gpd.GeoSeries(dtype='geometry'))
    else:
        result = pd.DataFrame(cols)

    idx_col = _finest_spatial_col(result.columns)
    if idx_col:
        result = result.set_index(idx_col)

    return result
