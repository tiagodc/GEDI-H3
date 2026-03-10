"""
EGI (EASE Grid Index) Module

A spatial indexing system for GEDI data using the EASE-Grid 2.0 projection (EPSG:6933).
This module provides square pixel indexing compatible with GEDI L4B products.

Notes
-----
- 12 resolution levels from ~1m to ~160km
- Hash-based coordinate encoding for efficient storage and queries
- Native alignment with GEDI L4B raster products
- Seamless integration with pandas/GeoPandas DataFrames
- Direct rasterization without resampling artifacts

**Resolution levels:** Level 1 ~1m · Level 4 ~100m (NISAR) · Level 5 ~200m (BIOMASS) ·
Level 6 ~1km (GEDI baseline) · Level 7 ~2km · Level 8 ~10km · Level 12 ~160km (partition).
See ``egi.RESOLUTIONS`` for full table.

Examples
--------
>>> import gedih3.egi as egi
>>> gdf = egi.egi_dataframe(shots_df, level=6)   # ~1km resolution
>>> coarse_gdf = egi.egi_to_parent(gdf, parent_level=8)
>>> agg_gdf = egi.egi_aggregate(gdf, mapper='mean')
>>> raster = egi.geodf_to_raster(agg_gdf, columns=['agbd_mean'])
"""

# Configuration constants
from .config import (
    # Numeric constants
    UINT_MAX,
    INT_MAX,
    LIMITS,
    EGI_RES6,
    RESOLUTIONS,
    OUTER_RES,
    OUTER_LEVEL,
    EGI_CRS,
    EGI_CRS_STRING,
    # Utility functions
    egi_col_name,
    validate_level,
    get_resolution,
    get_level_from_resolution,
)

# Core hash functions
from .core import (
    hasher,
    to_hash,
    from_hash,
    get_level,
    get_scale,
    to_parent,
    get_children,
    pixels_per_tile,
    validate_hash,
)

# Spatial operations
from .spatial import (
    check_crs_limits,
    pixel_coordinate,
    pixel_coordinates,
    pixel_shape,
    pixel_ring,
    aoi_tiles,
    to_geodataframe,
    egi_h3_intersection,
)

# DataFrame operations
from .dataframe import (
    egi_dataframe,
    egi_dataframe_vectorized,
    egi_to_parent,
    egi_to_parent_vectorized,
    egi_to_geo,
    egi_aggregate,
    egi_col_from_df,
    egi_get_level_from_df,
)

# Rasterization
from .raster import (
    geodf_to_raster,
    rasterize_partition,
    export_raster,
    merge_raster_partitions,
    get_raster_profile,
)

__all__ = [
    # Config
    'UINT_MAX',
    'INT_MAX',
    'LIMITS',
    'EGI_RES6',
    'RESOLUTIONS',
    'OUTER_RES',
    'OUTER_LEVEL',
    'EGI_CRS',
    'EGI_CRS_STRING',
    'egi_col_name',
    'validate_level',
    'get_resolution',
    'get_level_from_resolution',
    # Core
    'hasher',
    'to_hash',
    'from_hash',
    'get_level',
    'get_scale',
    'to_parent',
    'get_children',
    'pixels_per_tile',
    'validate_hash',
    # Spatial
    'check_crs_limits',
    'pixel_coordinate',
    'pixel_coordinates',
    'pixel_shape',
    'pixel_ring',
    'aoi_tiles',
    'to_geodataframe',
    'egi_h3_intersection',
    # DataFrame
    'egi_dataframe',
    'egi_dataframe_vectorized',
    'egi_to_parent',
    'egi_to_parent_vectorized',
    'egi_to_geo',
    'egi_aggregate',
    'egi_col_from_df',
    'egi_get_level_from_df',
    # Raster
    'geodf_to_raster',
    'rasterize_partition',
    'export_raster',
    'merge_raster_partitions',
    'get_raster_profile',
]
