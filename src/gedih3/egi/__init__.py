"""
EGI (EASE Grid Index) Module

A spatial indexing system for GEDI data using the EASE-Grid 2.0 projection (EPSG:6933).
This module provides square pixel indexing compatible with GEDI L4B products.

Key Features
------------
- 12 resolution levels from ~1m to ~160km
- Hash-based coordinate encoding for efficient storage and queries
- Native alignment with GEDI L4B raster products
- Seamless integration with pandas/GeoPandas DataFrames
- Direct rasterization without resampling artifacts

Basic Usage
-----------
>>> import gedih3.egi as egi
>>>
>>> # Add EGI index to a DataFrame
>>> gdf = egi.egi_dataframe(shots_df, level=6)  # ~1km resolution
>>>
>>> # Convert to coarser resolution
>>> coarse_gdf = egi.egi_to_parent(gdf, parent_level=8)
>>>
>>> # Aggregate data spatially
>>> agg_gdf = egi.egi_aggregate(gdf, mapper='mean')
>>>
>>> # Rasterize for GIS output
>>> raster = egi.geodf_to_raster(agg_gdf, columns=['agbd_mean'])

Resolution Levels
-----------------
Level 1:  ~1m     (finest resolution)
Level 4:  ~100m   (NISAR compatible)
Level 5:  ~200m   (BIOMASS compatible)
Level 6:  ~1km    (GEDI baseline)
Level 7:  ~2km    (GEDI threshold)
Level 8:  ~10km   (GEDI wall-to-wall)
Level 12: ~160km  (partition level)

For detailed resolution values, see: egi.RESOLUTIONS
"""

# Configuration constants
from .config import (
    EGI_CRS,
    EGI_CRS_STRING,
    EGI_RES6,
    INT_MAX,
    LIMITS,
    OUTER_LEVEL,
    OUTER_RES,
    RESOLUTIONS,
    # Numeric constants
    UINT_MAX,
    # Utility functions
    egi_col_name,
    get_level_from_resolution,
    get_resolution,
    validate_level,
)

# Core hash functions
from .core import (
    from_hash,
    get_children,
    get_level,
    get_scale,
    hasher,
    pixels_per_tile,
    to_hash,
    to_parent,
    validate_hash,
)

# DataFrame operations
from .dataframe import (
    egi_aggregate,
    egi_col_from_df,
    egi_dataframe,
    egi_dataframe_vectorized,
    egi_get_level_from_df,
    egi_to_geo,
    egi_to_parent,
    egi_to_parent_vectorized,
)

# Rasterization
from .raster import (
    export_raster,
    geodf_to_raster,
    get_raster_profile,
    merge_raster_partitions,
    rasterize_partition,
)

# Spatial operations
from .spatial import (
    aoi_tiles,
    check_crs_limits,
    egi_h3_intersection,
    pixel_coordinate,
    pixel_coordinates,
    pixel_ring,
    pixel_shape,
    to_geodataframe,
)

__all__ = [
    # Config
    "UINT_MAX",
    "INT_MAX",
    "LIMITS",
    "EGI_RES6",
    "RESOLUTIONS",
    "OUTER_RES",
    "OUTER_LEVEL",
    "EGI_CRS",
    "EGI_CRS_STRING",
    "egi_col_name",
    "validate_level",
    "get_resolution",
    "get_level_from_resolution",
    # Core
    "hasher",
    "to_hash",
    "from_hash",
    "get_level",
    "get_scale",
    "to_parent",
    "get_children",
    "pixels_per_tile",
    "validate_hash",
    # Spatial
    "check_crs_limits",
    "pixel_coordinate",
    "pixel_coordinates",
    "pixel_shape",
    "pixel_ring",
    "aoi_tiles",
    "to_geodataframe",
    "egi_h3_intersection",
    # DataFrame
    "egi_dataframe",
    "egi_dataframe_vectorized",
    "egi_to_parent",
    "egi_to_parent_vectorized",
    "egi_to_geo",
    "egi_aggregate",
    "egi_col_from_df",
    "egi_get_level_from_df",
    # Raster
    "geodf_to_raster",
    "rasterize_partition",
    "export_raster",
    "merge_raster_partitions",
    "get_raster_profile",
]
