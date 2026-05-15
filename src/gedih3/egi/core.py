"""
EGI (EASE Grid Index) Core Module

This module provides the fundamental hash encoding and decoding functions for
the EGI spatial indexing system. The hash encodes spatial location and resolution
level into a single uint64 value.

Hash Structure (uint64):
    level * 1e18 + px_outer * 1e15 + py_outer * 1e12 + px_inner * 1e6 + py_inner

    - level (1-12): Resolution level encoded in digits 19-20
    - px_outer: Outer tile X index (0-215) encoded in digits 16-18
    - py_outer: Outer tile Y index (0-90) encoded in digits 13-15
    - px_inner: Inner pixel X index within tile encoded in digits 7-12
    - py_inner: Inner pixel Y index within tile encoded in digits 1-6

Data Type Considerations:
    - Hash values MUST use np.uint64 for 64-bit precision
    - Intermediate calculations use appropriate uint16/uint32 to prevent overflow
    - Coordinate divisions must use exact type casting to preserve precision
"""
from typing import List, Tuple, Union
import numpy as np
from numpy.typing import NDArray

from .config import LIMITS, RESOLUTIONS, OUTER_RES, validate_level


def hasher(
    level: Union[int, NDArray[np.integer]],
    px_outer: Union[int, NDArray[np.uint16]],
    py_outer: Union[int, NDArray[np.uint16]],
    px_inner: Union[int, NDArray[np.uint32]],
    py_inner: Union[int, NDArray[np.uint32]]
) -> Union[np.uint64, NDArray[np.uint64]]:
    """
    Construct EGI hash from component parts.

    This is the core hash construction function. It combines level, outer tile
    coordinates, and inner pixel coordinates into a single uint64 hash.

    Parameters
    ----------
    level : int or array
        Resolution level (1-12)
    px_outer : int or array
        Outer tile X index (0-215)
    py_outer : int or array
        Outer tile Y index (0-90)
    px_inner : int or array
        Inner pixel X index within tile
    py_inner : int or array
        Inner pixel Y index within tile

    Returns
    -------
    np.uint64 or ndarray of uint64
        EGI hash value(s)

    Notes
    -----
    All inputs must be the same shape or broadcastable. The function preserves
    exact integer arithmetic using uint64 throughout.
    """
    # Convert to uint64 for multiplication to prevent overflow
    uint_hash = (
        np.uint64(level) * np.uint64(1e18) +
        np.uint64(px_outer) * np.uint64(1e15) +
        np.uint64(py_outer) * np.uint64(1e12) +
        np.uint64(px_inner) * np.uint64(1e6) +
        np.uint64(py_inner)
    )
    return uint_hash


def to_hash(
    x: Union[float, NDArray[np.floating]],
    y: Union[float, NDArray[np.floating]],
    level: int = 1
) -> Union[np.uint64, NDArray[np.uint64]]:
    """
    Convert EPSG:6933 coordinates to EGI hash.

    Parameters
    ----------
    x : float or array
        X coordinate(s) in EPSG:6933 (meters from origin)
    y : float or array
        Y coordinate(s) in EPSG:6933 (meters from origin)
    level : int
        Target EGI resolution level (1-12), default=1

    Returns
    -------
    np.uint64 or ndarray of uint64
        EGI hash value(s)

    Examples
    --------
    >>> # Single coordinate
    >>> hash_val = to_hash(-8000000.0, 4000000.0, level=6)
    >>>
    >>> # Array of coordinates
    >>> x = np.array([-8000000.0, -7000000.0])
    >>> y = np.array([4000000.0, 3500000.0])
    >>> hashes = to_hash(x, y, level=6)

    Notes
    -----
    Outer-tile and inner-pixel indices are derived independently in the float
    domain — matching the reference easegridindex grid alignment so existing
    on-disk data stays interpretable — but we then carry any boundary overflow
    from ``px_inner`` into ``px_outer``. ``OUTER_RES`` (~160143.2 m) is not
    exactly representable in float64, so for coordinates at or just below a
    tile boundary, ``x_offset // OUTER_RES`` rounds down to tile N-1 while
    ``x_offset % OUTER_RES // scale`` returns ``SCALE_FACTOR`` (one past the
    valid inner range). Left uncorrected, those out-of-range inner indices
    propagate through ``to_parent`` as spurious non-zero inner indices at
    coarser levels, producing boundary-shadow cells at every level. The carry
    folds the overflow back into the proper outer tile so ``px_inner`` is
    always in ``[0, SCALE_FACTOR)``. Non-boundary inputs are unaffected.

    Coordinates must be in EPSG:6933 projection. Do NOT use this function
    with WGS84 coordinates - reproject first.
    """
    validate_level(level)
    scale = RESOLUTIONS[level]
    scale_factor = round(OUTER_RES / scale)  # integer pixels per outer tile at this level

    # Calculate coordinate offsets from grid origin
    x_offset = x - LIMITS['lon_w']
    y_offset = y - LIMITS['lat_s']

    # Outer-tile and inner-pixel indices, both via the same OUTER_RES grid the
    # reference easegridindex uses. This preserves backward-compatible cell
    # alignment for existing data — we only diverge for boundary overflow below.
    px_outer = (x_offset // OUTER_RES).astype(np.int64) if hasattr(x_offset, 'astype') else np.int64(x_offset // OUTER_RES)
    py_outer = (y_offset // OUTER_RES).astype(np.int64) if hasattr(y_offset, 'astype') else np.int64(y_offset // OUTER_RES)
    px_inner = (x_offset % OUTER_RES // scale).astype(np.int64) if hasattr(x_offset, 'astype') else np.int64(x_offset % OUTER_RES // scale)
    py_inner = (y_offset % OUTER_RES // scale).astype(np.int64) if hasattr(y_offset, 'astype') else np.int64(y_offset % OUTER_RES // scale)

    # Boundary overflow carry: at tile boundaries, the independent float ops
    # leave px_inner == scale_factor (one past valid). Fold that overflow into
    # the next outer tile so the hash always satisfies 0 <= inner < scale_factor.
    px_carry = px_inner >= scale_factor
    py_carry = py_inner >= scale_factor
    px_outer = np.where(px_carry, px_outer + 1, px_outer)
    py_outer = np.where(py_carry, py_outer + 1, py_outer)
    px_inner = np.where(px_carry, 0, px_inner)
    py_inner = np.where(py_carry, 0, py_inner)

    return hasher(
        level,
        np.uint16(px_outer),
        np.uint16(py_outer),
        np.uint32(px_inner),
        np.uint32(py_inner),
    )


def from_hash(
    uint_hash: Union[np.uint64, NDArray[np.uint64]]
) -> Tuple[
    Union[int, NDArray[np.integer]],
    Union[float, NDArray[np.floating]],
    Union[np.uint16, NDArray[np.uint16]],
    Union[np.uint16, NDArray[np.uint16]],
    Union[np.uint32, NDArray[np.uint32]],
    Union[np.uint32, NDArray[np.uint32]]
]:
    """
    Decode EGI hash into its component parts.

    Parameters
    ----------
    uint_hash : uint64 or array of uint64
        EGI hash value(s) to decode

    Returns
    -------
    tuple
        (level, scale, px_outer, py_outer, px_inner, py_inner)
        - level: Resolution level (1-12)
        - scale: Pixel size in meters
        - px_outer: Outer tile X index
        - py_outer: Outer tile Y index
        - px_inner: Inner pixel X index
        - py_inner: Inner pixel Y index

    Examples
    --------
    >>> level, scale, px_o, py_o, px_i, py_i = from_hash(hash_val)
    """
    uint_hash = np.uint64(uint_hash)

    # Extract level from highest digits
    level = uint_hash // np.uint64(1e18)

    # Handle scalar vs array for resolution lookup
    if np.ndim(level) == 0:
        scale = RESOLUTIONS.get(int(level))
    else:
        scale = np.array([RESOLUTIONS.get(int(lv)) for lv in level])

    # Extract inner pixel coordinates (lower digits)
    py_inner = np.uint32(uint_hash % np.uint32(1e6))
    px_inner = np.uint32(uint_hash % np.uint64(1e12) // np.uint64(1e6))

    # Extract outer tile coordinates (middle digits)
    py_outer = np.uint16(uint_hash % np.uint64(1e15) // np.uint64(1e12))
    px_outer = np.uint16(uint_hash % np.uint64(1e18) // np.uint64(1e15))

    return level, scale, px_outer, py_outer, px_inner, py_inner


def get_level(uint_hash: Union[np.uint64, NDArray[np.uint64]]) -> Union[int, NDArray[np.integer]]:
    """
    Extract the resolution level from an EGI hash.

    Parameters
    ----------
    uint_hash : uint64 or array of uint64
        EGI hash value(s)

    Returns
    -------
    int or array
        Resolution level(s)
    """
    return np.uint64(uint_hash) // np.uint64(1e18)


def get_scale(uint_hash: Union[np.uint64, NDArray[np.uint64]]) -> Union[float, NDArray[np.floating]]:
    """
    Get the pixel size in meters for an EGI hash.

    Parameters
    ----------
    uint_hash : uint64 or array of uint64
        EGI hash value(s)

    Returns
    -------
    float or array
        Pixel size(s) in meters
    """
    level = get_level(uint_hash)
    if np.ndim(level) == 0:
        return RESOLUTIONS.get(int(level))
    return np.array([RESOLUTIONS.get(int(lv)) for lv in level])


def to_parent(
    uint_hash: Union[np.uint64, NDArray[np.uint64]],
    parent_level: int
) -> Union[np.uint64, NDArray[np.uint64]]:
    """
    Convert EGI hash to a coarser (parent) resolution level.

    This function rescales the inner pixel coordinates to the parent resolution
    while preserving the outer tile coordinates.

    Parameters
    ----------
    uint_hash : uint64 or array of uint64
        EGI hash value(s) to convert
    parent_level : int
        Target parent resolution level (must be >= current level)

    Returns
    -------
    uint64 or array of uint64
        EGI hash(es) at parent resolution

    Raises
    ------
    ValueError
        If parent_level is finer than current level

    Examples
    --------
    >>> # Convert from level 1 to level 6
    >>> parent_hash = to_parent(fine_hash, parent_level=6)
    """
    validate_level(parent_level)

    uint_hash = np.uint64(uint_hash)
    level = get_level(uint_hash)

    # Validate that we're going to coarser resolution
    if np.ndim(level) == 0:
        if int(level) > parent_level:
            raise ValueError(
                f"Cannot convert to finer resolution. Current level: {level}, "
                f"requested parent level: {parent_level}"
            )
        current_scale = RESOLUTIONS[int(level)]
    else:
        if np.any(level > parent_level):
            raise ValueError(
                f"Cannot convert to finer resolution. Some hashes have level > {parent_level}"
            )
        current_scale = np.array([RESOLUTIONS[int(lv)] for lv in level])

    parent_scale = RESOLUTIONS[parent_level]
    scale_factor = round(parent_scale / current_scale) if np.ndim(current_scale) == 0 else np.round(parent_scale / current_scale)

    # Rescale inner pixel coordinates
    py_inner = np.uint32(uint_hash % np.uint32(1e6) // scale_factor)
    px_inner = np.uint64(uint_hash % np.uint64(1e12) // np.uint32(1e6) // scale_factor)

    # Preserve outer tile coordinates
    p_outer = uint_hash % np.uint64(1e18) // np.uint64(1e12)

    # Reconstruct hash at parent level
    re_uint_hash = (
        np.uint64(parent_level) * np.uint64(1e18) +
        np.uint64(p_outer) * np.uint64(1e12) +
        np.uint64(px_inner) * np.uint64(1e6) +
        py_inner
    )

    return re_uint_hash


def get_children(
    uint_hash: Union[np.uint64, NDArray[np.uint64]],
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

    Raises
    ------
    ValueError
        If children_level is not finer than parent level

    Examples
    --------
    >>> children = get_children(parent_hash, children_level=-1)
    >>> fine_children = get_children(level6_hash, children_level=1)
    """
    uint_hash = np.uint64(uint_hash)
    level, scale, px_outer, py_outer, px_inner, py_inner = from_hash(uint_hash)
    level = int(level)

    if children_level < 0:
        children_level = level + children_level

    validate_level(children_level)
    if children_level >= level:
        raise ValueError(
            f"Children level ({children_level}) must be finer than parent level ({level})"
        )

    # Compute pixel bounds directly (no Shapely dependency)
    minx = float(scale) * float(px_inner) + OUTER_RES * float(px_outer) + LIMITS['lon_w']
    miny = float(scale) * float(py_inner) + OUTER_RES * float(py_outer) + LIMITS['lat_s']
    maxx = minx + float(scale)
    maxy = miny + float(scale)

    children_scale = RESOLUTIONS[children_level]
    offset = children_scale / 2

    # Generate grid of center points within parent pixel
    px = np.arange(minx + offset, maxx, children_scale)
    py = np.arange(miny + offset, maxy, children_scale)

    center_points = np.stack(np.meshgrid(px, py)).reshape(2, -1).T
    return [to_hash(x, y, children_level) for x, y in center_points]


def pixels_per_tile(uint_hash_or_level: Union[np.uint64, int]) -> Union[int, float]:
    """
    Calculate the number of pixels per outer tile at a given level.

    Parameters
    ----------
    uint_hash_or_level : uint64 or int
        Either an EGI hash or a resolution level (1-12)

    Returns
    -------
    int or float
        Number of pixels along one edge of an outer tile
    """
    if uint_hash_or_level <= 12:
        # Input is a level
        level = int(uint_hash_or_level)
    else:
        # Input is a hash
        level = int(np.uint64(uint_hash_or_level) // np.uint64(1e18))

    scale = RESOLUTIONS[level]
    return OUTER_RES / scale


def validate_hash(uint_hash: Union[np.uint64, NDArray[np.uint64]]) -> bool:
    """
    Validate that an EGI hash is well-formed.

    Parameters
    ----------
    uint_hash : uint64 or array of uint64
        EGI hash value(s) to validate

    Returns
    -------
    bool
        True if valid, False otherwise
    """
    uint_hash = np.uint64(uint_hash)
    level = get_level(uint_hash)

    if np.ndim(level) == 0:
        return 1 <= int(level) <= 12
    return np.all((level >= 1) & (level <= 12))
