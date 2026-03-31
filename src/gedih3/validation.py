#! python
"""
gedih3 Validation Module

Parameter validation functions for H3, EGI, and other critical operations.
These functions raise descriptive exceptions to help users fix configuration issues.

Usage:
    from gedih3.validation import validate_h3_params, validate_egi_level

    validate_h3_params(res=12, part=3)  # Raises H3ValidationError if invalid
"""

import os
from typing import Union, List, Dict, Optional, Any

from .exceptions import (
    H3ValidationError,
    EGIValidationError,
    GediProductError,
    GediVariableError,
    GediFileError,
    GediDatabaseNotFoundError,
)


# =============================================================================
# H3 Parameter Validation
# =============================================================================

H3_MIN_RESOLUTION = 0
H3_MAX_RESOLUTION = 15


def validate_h3_resolution(res: int, param_name: str = 'resolution') -> int:
    """
    Validate H3 resolution is within valid range [0, 15].

    Parameters
    ----------
    res : int
        H3 resolution level
    param_name : str
        Parameter name for error messages

    Returns
    -------
    int
        Validated resolution

    Raises
    ------
    H3ValidationError
        If resolution is out of range or not an integer
    """
    if not isinstance(res, int):
        raise H3ValidationError(
            f"H3 {param_name} must be an integer, got {type(res).__name__}",
            param_name=param_name,
            value=res
        )

    if not H3_MIN_RESOLUTION <= res <= H3_MAX_RESOLUTION:
        raise H3ValidationError(
            f"H3 {param_name} must be between {H3_MIN_RESOLUTION} and {H3_MAX_RESOLUTION}, got {res}",
            param_name=param_name,
            value=res
        )

    return res


def validate_h3_params(res: int, part: int) -> tuple:
    """
    Validate H3 resolution and partition parameters.

    Parameters
    ----------
    res : int
        H3 resolution level for indexing (0-15)
    part : int
        H3 resolution level for partitioning (0-15, must be <= res)

    Returns
    -------
    tuple
        (res, part) validated values

    Raises
    ------
    H3ValidationError
        If parameters are invalid

    Examples
    --------
    >>> validate_h3_params(12, 3)  # OK
    (12, 3)
    >>> validate_h3_params(3, 12)  # Raises H3ValidationError
    """
    res = validate_h3_resolution(res, 'resolution')
    part = validate_h3_resolution(part, 'partition')

    if part > res:
        raise H3ValidationError(
            f"H3 partition level ({part}) must be <= resolution level ({res}). "
            f"Partition cells are used to group shots, so they must be coarser than index cells.",
            param_name='partition',
            value=part
        )

    return res, part


def validate_h3_cell(cell: str, expected_res: Optional[int] = None) -> str:
    """
    Validate an H3 cell index string.

    Parameters
    ----------
    cell : str
        H3 cell index (hexadecimal string)
    expected_res : int, optional
        Expected resolution level

    Returns
    -------
    str
        Validated cell index

    Raises
    ------
    H3ValidationError
        If cell is invalid
    """
    import h3

    if not isinstance(cell, str):
        raise H3ValidationError(
            f"H3 cell must be a string, got {type(cell).__name__}",
            param_name='cell',
            value=cell
        )

    if not h3.is_valid_cell(cell):
        raise H3ValidationError(
            f"Invalid H3 cell index: {cell}",
            param_name='cell',
            value=cell
        )

    if expected_res is not None:
        actual_res = h3.get_resolution(cell)
        if actual_res != expected_res:
            raise H3ValidationError(
                f"H3 cell has resolution {actual_res}, expected {expected_res}",
                param_name='cell',
                value=cell
            )

    return cell


# =============================================================================
# EGI Parameter Validation
# =============================================================================

EGI_MIN_LEVEL = 1
EGI_MAX_LEVEL = 12


def validate_egi_level(level: int, param_name: str = 'level') -> int:
    """
    Validate EGI level is within valid range [1, 12].

    Parameters
    ----------
    level : int
        EGI resolution level
    param_name : str
        Parameter name for error messages

    Returns
    -------
    int
        Validated level

    Raises
    ------
    EGIValidationError
        If level is out of range
    """
    if not isinstance(level, int):
        raise EGIValidationError(
            f"EGI {param_name} must be an integer, got {type(level).__name__}",
            param_name=param_name,
            value=level
        )

    if not EGI_MIN_LEVEL <= level <= EGI_MAX_LEVEL:
        raise EGIValidationError(
            f"EGI {param_name} must be between {EGI_MIN_LEVEL} and {EGI_MAX_LEVEL}, got {level}",
            param_name=param_name,
            value=level
        )

    return level


# =============================================================================
# GEDI Product Validation
# =============================================================================

VALID_PRODUCTS = {'L1B', 'L2A', 'L2B', 'L3', 'L4A', 'L4B', 'L4C'}


def validate_product(product: str) -> str:
    """
    Validate GEDI product identifier.

    Parameters
    ----------
    product : str
        GEDI product code (e.g., 'L2A', 'L4A')

    Returns
    -------
    str
        Normalized product code (uppercase)

    Raises
    ------
    GediProductError
        If product is invalid
    """
    if not isinstance(product, str):
        raise GediProductError(
            f"Product must be a string, got {type(product).__name__}"
        )

    product_upper = product.upper()
    if product_upper not in VALID_PRODUCTS:
        raise GediProductError(
            f"Invalid GEDI product: {product}. "
            f"Valid products are: {', '.join(sorted(VALID_PRODUCTS))}"
        )

    return product_upper


def validate_product_vars(product_vars: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Validate product variables dictionary.

    Parameters
    ----------
    product_vars : dict
        Dictionary mapping product codes to variable specifications

    Returns
    -------
    dict
        Validated product variables

    Raises
    ------
    GediProductError
        If product is invalid
    GediVariableError
        If variable specification is invalid
    """
    if not isinstance(product_vars, dict):
        raise GediProductError(
            f"product_vars must be a dictionary, got {type(product_vars).__name__}"
        )

    validated = {}
    for product, vars_spec in product_vars.items():
        product = validate_product(product)

        if vars_spec is None:
            validated[product] = None
        elif isinstance(vars_spec, str):
            validated[product] = [vars_spec]
        elif isinstance(vars_spec, list):
            if not all(isinstance(v, str) for v in vars_spec):
                raise GediVariableError(
                    f"Variables for {product} must be strings, got: {vars_spec}"
                )
            validated[product] = vars_spec
        else:
            raise GediVariableError(
                f"Variables for {product} must be a string, list, or None, "
                f"got {type(vars_spec).__name__}"
            )

    return validated


# =============================================================================
# File/Path Validation
# =============================================================================

def validate_file_exists(path: str, file_type: str = 'file') -> str:
    """
    Validate that a file exists.

    Parameters
    ----------
    path : str
        File path to validate
    file_type : str
        Description for error message (e.g., 'HDF5 file', 'database')

    Returns
    -------
    str
        Validated path

    Raises
    ------
    GediFileError
        If file does not exist
    """
    if not os.path.exists(path):
        raise GediFileError(f"{file_type} not found: {path}")
    return path


def validate_directory_exists(path: str, create: bool = False) -> str:
    """
    Validate that a directory exists, optionally creating it.

    Parameters
    ----------
    path : str
        Directory path to validate
    create : bool
        If True, create directory if it doesn't exist

    Returns
    -------
    str
        Validated path

    Raises
    ------
    GediFileError
        If directory does not exist and create=False
    """
    if not os.path.exists(path):
        if create:
            os.makedirs(path, exist_ok=True)
        else:
            raise GediFileError(f"Directory not found: {path}")

    if not os.path.isdir(path):
        raise GediFileError(f"Path exists but is not a directory: {path}")

    return path


def validate_database_path(db_path: str) -> str:
    """
    Validate H3 database path exists and appears valid.

    Parameters
    ----------
    db_path : str
        Path to H3 database directory

    Returns
    -------
    str
        Validated path

    Raises
    ------
    GediDatabaseNotFoundError
        If database directory doesn't exist or appears invalid
    """
    from .utils import smart_exists, smart_isdir, smart_glob

    if not smart_exists(db_path):
        raise GediDatabaseNotFoundError(
            f"H3 database not found: {db_path}"
        )

    if not smart_isdir(db_path):
        raise GediDatabaseNotFoundError(
            f"H3 database path is not a directory: {db_path}"
        )

    # Check for H3 partition directories or parquet files
    from .utils import smart_join
    h3_dirs = smart_glob(smart_join(db_path, 'h3_*/'))
    parquet_files = smart_glob(smart_join(db_path, '**/*.parquet'), recursive=True)

    if not h3_dirs and not parquet_files:
        raise GediDatabaseNotFoundError(
            f"H3 database appears empty or invalid: {db_path}. "
            f"No H3 partition directories or parquet files found."
        )

    return db_path


# =============================================================================
# Coordinate Validation
# =============================================================================

def validate_coordinates(lat: float, lon: float) -> tuple:
    """
    Validate latitude and longitude coordinates.

    Parameters
    ----------
    lat : float
        Latitude in degrees
    lon : float
        Longitude in degrees

    Returns
    -------
    tuple
        (lat, lon) validated coordinates

    Raises
    ------
    ValueError
        If coordinates are out of range
    """
    if not -90 <= lat <= 90:
        raise ValueError(f"Latitude must be between -90 and 90, got {lat}")
    if not -180 <= lon <= 180:
        raise ValueError(f"Longitude must be between -180 and 180, got {lon}")
    return lat, lon


def validate_bbox(bbox: Union[list, tuple]) -> tuple:
    """
    Validate bounding box coordinates.

    Parameters
    ----------
    bbox : list or tuple
        Bounding box as (west, south, east, north) or [west, south, east, north]

    Returns
    -------
    tuple
        Validated (west, south, east, north)

    Raises
    ------
    ValueError
        If bbox is invalid
    """
    if not isinstance(bbox, (list, tuple)):
        raise ValueError(f"Bounding box must be a list or tuple, got {type(bbox).__name__}")

    if len(bbox) != 4:
        raise ValueError(f"Bounding box must have 4 elements (W, S, E, N), got {len(bbox)}")

    west, south, east, north = bbox

    # Validate coordinates
    validate_coordinates(south, west)
    validate_coordinates(north, east)

    if south > north:
        raise ValueError(f"South ({south}) must be <= North ({north})")

    # Note: west > east is valid for antimeridian-crossing bboxes

    return (west, south, east, north)
