# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
EGI (EASE Grid Index) Configuration Module

This module defines constants and configuration for the EASE-Grid 2.0 (EPSG:6933)
spatial indexing system used for GEDI L4B-compatible square pixel outputs.

The EGI system provides:
- 12 resolution levels (1m to 160km)
- Perfect alignment with GEDI L4B products at level 6 (~1km)
- Native raster output without resampling artifacts
- Hash-based coordinate encoding for efficient spatial indexing

Reference:
- EASE-Grid 2.0: https://nsidc.org/ease/ease-grid-projection-gt
- EPSG:6933: WGS 84 / NSIDC EASE-Grid 2.0 Global
"""
from typing import Dict

# Integer limits for hash validation
UINT_MAX: int = 18_446_744_073_709_551_615  # 2^64 - 1
INT_MAX: int = 9_223_372_036_854_775_807     # 2^63 - 1

# EPSG:6933 projected coordinate bounds (meters)
# These define the valid extent of the EASE-Grid 2.0 projection
LIMITS: Dict[str, float] = {
    'lat_s': -7_314_540.830638599582016,   # Southern bound (meters)
    'lat_n':  7_314_540.830638599582016,   # Northern bound (meters)
    'lon_w': -17_367_530.445161499083042,  # Western bound (meters)
    'lon_e':  17_367_530.445161499083042,  # Eastern bound (meters)
}

# Base resolution at level 6 (GEDI baseline ~1km)
EGI_RES6: float = 1000.89502334956

# Resolution lookup table: level -> pixel size in meters
# Levels 1-5 are finer than GEDI baseline (higher resolution)
# Levels 7-12 are coarser than GEDI baseline (lower resolution)
RESOLUTIONS: Dict[int, float] = {
    1:  round(EGI_RES6 / 1000, 6),   # ~1m (finest)
    2:  round(EGI_RES6 / 200, 6),    # ~5m
    3:  round(EGI_RES6 / 40, 6),     # ~25m
    4:  round(EGI_RES6 / 10, 6),     # ~100m (NISAR compatible)
    5:  round(EGI_RES6 / 5, 6),      # ~200m (BIOMASS compatible)
    6:  round(EGI_RES6, 6),          # ~1km (GEDI baseline)
    7:  round(EGI_RES6 * 2, 6),      # ~2km (GEDI threshold)
    8:  round(EGI_RES6 * 10, 6),     # ~10km (GEDI wall-to-wall)
    9:  round(EGI_RES6 * 20, 6),     # ~20km
    10: round(EGI_RES6 * 40, 6),     # ~40km
    11: round(EGI_RES6 * 80, 6),     # ~80km
    12: round(EGI_RES6 * 160, 6),    # ~160km (coarsest, partition level)
}

# Outer tile resolution (coarsest level, used for partitioning)
OUTER_RES: float = max(RESOLUTIONS.values())
OUTER_LEVEL: int = max(RESOLUTIONS.keys())

# EPSG code for EASE-Grid 2.0
EGI_CRS: int = 6933

# Coordinate reference system string
EGI_CRS_STRING: str = "EPSG:6933"

# Column naming convention for EGI indices
def egi_col_name(level: int) -> str:
    """
    Generate standard EGI column name for a given resolution level.

    Parameters
    ----------
    level : int
        EGI resolution level (1-12)

    Returns
    -------
    str
        Column name in format 'egi{level:02d}' (e.g., 'egi01', 'egi12')
    """
    return f'egi{level:02d}'


def validate_level(level: int) -> None:
    """
    Validate that a level is within the supported range.

    Parameters
    ----------
    level : int
        EGI resolution level to validate

    Raises
    ------
    ValueError
        If level is not between 1 and 12
    """
    if not 1 <= level <= 12:
        raise ValueError(f"EGI level must be between 1 and 12, got {level}")


def get_resolution(level: int) -> float:
    """
    Get the pixel size in meters for a given EGI level.

    Parameters
    ----------
    level : int
        EGI resolution level (1-12)

    Returns
    -------
    float
        Pixel size in meters

    Raises
    ------
    ValueError
        If level is not between 1 and 12
    """
    validate_level(level)
    return RESOLUTIONS[level]


def get_level_from_resolution(resolution: float, tolerance: float = 0.01) -> int:
    """
    Find the EGI level closest to a given resolution.

    Parameters
    ----------
    resolution : float
        Target resolution in meters
    tolerance : float
        Relative tolerance for matching (default 1%)

    Returns
    -------
    int
        EGI level with closest matching resolution

    Raises
    ------
    ValueError
        If no level matches within tolerance
    """
    for level, res in RESOLUTIONS.items():
        if abs(res - resolution) / res < tolerance:
            return level

    raise ValueError(
        f"No EGI level matches resolution {resolution}m within {tolerance*100}% tolerance. "
        f"Available resolutions: {list(RESOLUTIONS.values())}"
    )
