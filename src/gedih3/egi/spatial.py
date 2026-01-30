"""
EGI (EASE Grid Index) Spatial Module

This module provides spatial operations for EGI indices including:
- Coordinate extraction from hashes
- Geometry generation (points and polygons)
- Neighbor finding (pixel ring)
- Child pixel enumeration
- Area of Interest (AOI) tile generation
"""
from typing import List, Optional, Tuple, Union
import numpy as np
from numpy.typing import NDArray
import geopandas as gpd
from shapely.geometry import box, Point, Polygon

from .config import (
    LIMITS, RESOLUTIONS, OUTER_RES, OUTER_LEVEL, EGI_CRS_STRING,
    egi_col_name, validate_level
)
from .core import from_hash, to_hash, hasher, pixels_per_tile


def check_crs_limits(
    x: Union[float, NDArray[np.floating]],
    y: Union[float, NDArray[np.floating]]
) -> Tuple[Union[float, NDArray[np.floating]], Union[float, NDArray[np.floating]]]:
    """
    Clamp coordinates to EPSG:6933 valid bounds.

    Parameters
    ----------
    x : float or array
        X coordinate(s) in EPSG:6933
    y : float or array
        Y coordinate(s) in EPSG:6933

    Returns
    -------
    tuple
        (x, y) clamped to valid bounds
    """
    if isinstance(x, np.ndarray):
        x = np.clip(x, LIMITS['lon_w'], LIMITS['lon_e'])
        y = np.clip(y, LIMITS['lat_s'], LIMITS['lat_n'])
    else:
        x = min(max(x, LIMITS['lon_w']), LIMITS['lon_e'])
        y = min(max(y, LIMITS['lat_s']), LIMITS['lat_n'])
    return x, y


def pixel_coordinate(
    uint_hash: np.uint64,
    center: bool = True,
    return_point: bool = False
) -> Union[Tuple[float, float], Point]:
    """
    Get the coordinate of an EGI pixel.

    Parameters
    ----------
    uint_hash : uint64
        EGI hash value
    center : bool
        If True, return pixel center; if False, return lower-left corner
    return_point : bool
        If True, return a Shapely Point; if False, return (x, y) tuple

    Returns
    -------
    tuple or Point
        (x, y) coordinates in EPSG:6933, or Shapely Point

    Examples
    --------
    >>> x, y = pixel_coordinate(hash_val)
    >>> point = pixel_coordinate(hash_val, return_point=True)
    """
    level, scale, px_outer, py_outer, px_inner, py_inner = from_hash(uint_hash)

    # Calculate absolute coordinates from tile + pixel position
    px = scale * px_inner + OUTER_RES * px_outer + LIMITS['lon_w']
    py = scale * py_inner + OUTER_RES * py_outer + LIMITS['lat_s']

    if center:
        px += scale / 2
        py += scale / 2
        px, py = check_crs_limits(px, py)

    if return_point:
        return Point(px, py)
    return float(px), float(py)


def pixel_coordinates(
    uint_hash: NDArray[np.uint64],
    center: bool = True
) -> Tuple[NDArray[np.floating], NDArray[np.floating]]:
    """
    Get coordinates for multiple EGI pixels (vectorized).

    Parameters
    ----------
    uint_hash : array of uint64
        EGI hash values
    center : bool
        If True, return pixel centers; if False, return lower-left corners

    Returns
    -------
    tuple
        (x_array, y_array) coordinates in EPSG:6933
    """
    level, scale, px_outer, py_outer, px_inner, py_inner = from_hash(uint_hash)

    # Vectorized coordinate calculation
    px = scale * px_inner + OUTER_RES * px_outer + LIMITS['lon_w']
    py = scale * py_inner + OUTER_RES * py_outer + LIMITS['lat_s']

    if center:
        px = px + scale / 2
        py = py + scale / 2
        px, py = check_crs_limits(px, py)

    return px.astype(np.float64), py.astype(np.float64)


def pixel_shape(uint_hash: np.uint64) -> Polygon:
    """
    Get the bounding polygon of an EGI pixel.

    Parameters
    ----------
    uint_hash : uint64
        EGI hash value

    Returns
    -------
    Polygon
        Shapely polygon representing the pixel bounds

    Examples
    --------
    >>> geom = pixel_shape(hash_val)
    >>> area_m2 = geom.area
    """
    level = int(np.uint64(uint_hash) // np.uint64(1e18))
    scale = RESOLUTIONS[level]

    px0, py0 = pixel_coordinate(uint_hash, center=False)
    px1 = px0 + scale
    py1 = py0 + scale

    px1, py1 = check_crs_limits(px1, py1)

    return box(px0, py0, px1, py1)


def pixel_ring(
    uint_hash: np.uint64,
    include_input: bool = False
) -> List[np.uint64]:
    """
    Get the 8 neighboring pixels (ring) around an EGI pixel.

    Parameters
    ----------
    uint_hash : uint64
        EGI hash value (center pixel)
    include_input : bool
        If True, include the input pixel in the result

    Returns
    -------
    list of uint64
        EGI hashes of neighboring pixels (up to 8)

    Notes
    -----
    Pixels at the edge of the projection bounds will have fewer than 8 neighbors.
    The function correctly handles tile boundary crossings.
    """
    level, scale, px_outer, py_outer, px_inner, py_inner = from_hash(uint_hash)
    level = int(level)
    max_pix = int(pixels_per_tile(uint_hash)) - 1
    max_tile_x = int((LIMITS['lon_e'] - LIMITS['lon_w']) // OUTER_RES)
    max_tile_y = int((LIMITS['lat_n'] - LIMITS['lat_s']) // OUTER_RES)

    neighbors = []
    for i in range(-1, 2):
        for j in range(-1, 2):
            if i == 0 and j == 0:
                continue

            pxo = int(px_outer)
            pyo = int(py_outer)
            pxi = int(px_inner) + i
            pyi = int(py_inner) + j

            # Handle tile boundary crossing for X
            if pxi < 0:
                pxo -= 1
                pxi = max_pix
            elif pxi > max_pix:
                pxo += 1
                pxi = 0

            # Handle tile boundary crossing for Y
            if pyi < 0:
                pyo -= 1
                pyi = max_pix
            elif pyi > max_pix:
                pyo += 1
                pyi = 0

            # Only include if within valid tile bounds
            if 0 <= pxo < max_tile_x and 0 <= pyo < max_tile_y:
                neighbor_hash = hasher(level, np.uint16(pxo), np.uint16(pyo),
                                      np.uint32(pxi), np.uint32(pyi))
                neighbors.append(neighbor_hash)

    if include_input:
        neighbors.append(uint_hash)

    return neighbors


def get_children(
    uint_hash: np.uint64,
    children_level: int = -1
) -> List[np.uint64]:
    """
    Get all child pixels at a finer resolution.

    Parameters
    ----------
    uint_hash : uint64
        EGI hash value (parent pixel)
    children_level : int
        Target resolution level for children.
        Negative values are relative to parent level (e.g., -1 = one level finer)

    Returns
    -------
    list of uint64
        EGI hashes of all child pixels

    Examples
    --------
    >>> # Get children one level finer
    >>> children = get_children(parent_hash, children_level=-1)
    >>>
    >>> # Get all level-1 children of a level-6 pixel
    >>> fine_children = get_children(level6_hash, children_level=1)
    """
    level = int(np.uint64(uint_hash) // np.uint64(1e18))
    minx, miny, maxx, maxy = pixel_shape(uint_hash).bounds

    if children_level < 0:
        children_level = level + children_level

    validate_level(children_level)
    if children_level >= level:
        raise ValueError(
            f"Children level ({children_level}) must be finer than parent level ({level})"
        )

    children_scale = RESOLUTIONS[children_level]
    offset = children_scale / 2

    # Generate grid of center points within parent pixel
    px = np.arange(minx + offset, maxx, children_scale)
    py = np.arange(miny + offset, maxy, children_scale)

    center_points = np.stack(np.meshgrid(px, py)).reshape(2, -1).T
    egi_ids = [to_hash(x, y, children_level) for x, y in center_points]

    return egi_ids


def aoi_tiles(region: Optional[gpd.GeoDataFrame] = None) -> gpd.GeoDataFrame:
    """
    Generate outer tiles (level 12) covering an area of interest.

    Parameters
    ----------
    region : GeoDataFrame, optional
        Area of interest. If None, returns all global tiles.

    Returns
    -------
    GeoDataFrame
        Tiles indexed by EGI level-12 hash with polygon geometries

    Raises
    ------
    ValueError
        If region has no CRS defined

    Examples
    --------
    >>> # Get tiles for a specific region
    >>> region = gpd.read_file("study_area.shp")
    >>> tiles = aoi_tiles(region)
    >>>
    >>> # Get all global tiles
    >>> all_tiles = aoi_tiles()
    """
    # Calculate number of tiles in each dimension
    xn = int((LIMITS['lon_e'] - LIMITS['lon_w']) // OUTER_RES)
    yn = int((LIMITS['lat_n'] - LIMITS['lat_s']) // OUTER_RES)

    # Generate all tile indices
    pairs = np.stack(np.meshgrid(range(xn + 1), range(yn + 1))).reshape(2, -1)

    # Create outer-level hashes (level 12, no inner pixels)
    outer_ids = np.uint64(
        OUTER_LEVEL * np.uint64(1e18) +
        pairs[0] * np.uint64(1e15) +
        pairs[1] * np.uint64(1e12)
    )

    # Create GeoDataFrame with tile geometries
    tiles = to_geodataframe(outer_ids, return_polygons=True)

    if region is not None:
        if not region.crs:
            raise ValueError('Input region has no CRS defined')

        # Reproject region to EGI CRS
        reg = region.to_crs(EGI_CRS_STRING)

        # Find tiles that intersect the region
        is_in = tiles.geometry.apply(lambda x: reg.intersects(x).any())
        tiles = tiles[is_in]

    return tiles


def to_geodataframe(
    uint_hash_iter: Union[List[np.uint64], NDArray[np.uint64]],
    return_polygons: bool = True
) -> gpd.GeoDataFrame:
    """
    Convert EGI hashes to a GeoDataFrame.

    Parameters
    ----------
    uint_hash_iter : list or array of uint64
        EGI hash values
    return_polygons : bool
        If True, return polygon geometries; if False, return point centroids

    Returns
    -------
    GeoDataFrame
        GeoDataFrame indexed by EGI hash with geometry column

    Examples
    --------
    >>> gdf = to_geodataframe(egi_hashes, return_polygons=True)
    """
    uint_hash_arr = np.asarray(uint_hash_iter, dtype=np.uint64)

    # Handle empty arrays (e.g., during Dask operations on empty partitions)
    if len(uint_hash_arr) == 0:
        # Return empty GeoDataFrame with correct structure
        # Use a default level since we can't infer from empty array
        idx_name = 'egi_hash'
        return gpd.GeoDataFrame(
            {idx_name: np.array([], dtype=np.uint64)},
            geometry=[],
            crs=EGI_CRS_STRING
        ).set_index(idx_name)

    if return_polygons:
        geometries = [pixel_shape(h) for h in uint_hash_arr]
    else:
        geometries = [pixel_coordinate(h, center=True, return_point=True) for h in uint_hash_arr]

    # Determine column name from first hash's level
    level = int(uint_hash_arr[0] // np.uint64(1e18))
    idx_name = egi_col_name(level)

    gdf = gpd.GeoDataFrame(
        {idx_name: uint_hash_arr},
        geometry=geometries,
        crs=EGI_CRS_STRING
    ).set_index(idx_name)

    return gdf
