"""
Raster Module Configuration

This module defines constants and defaults for raster output operations
including GeoTIFF compression, tiling, and format settings.
"""
from typing import Dict, Any

# Default GeoTIFF export options
GEOTIFF_DEFAULTS: Dict[str, Any] = {
    'compress': 'LZW',
    'tiled': True,
    'blockxsize': 256,
    'blockysize': 256,
    'bigtiff': True,
}

# Compression options (for user selection)
COMPRESSION_OPTIONS = ['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE']

# Supported raster output formats
RASTER_FORMATS = ['tif', 'tiff', 'geotiff', 'nc', 'netcdf']

# Time units for temporal aggregation
TIME_UNITS = ['years', 'months', 'weeks', 'days']

# Default CRS for H3 raster output (WGS84)
H3_RASTER_CRS = 'EPSG:4326'

# GEDI mission start date (used for delta_time conversion)
GEDI_START_DATE_STR = '2018-01-01'


def get_geotiff_options(
    compress: str = 'LZW',
    tiled: bool = True,
    blocksize: int = 256,
    bigtiff: bool = True
) -> Dict[str, Any]:
    """
    Generate rasterio GeoTIFF export options.

    Parameters
    ----------
    compress : str
        Compression method ('LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE')
    tiled : bool
        Use tiled output format
    blocksize : int
        Tile block size in pixels
    bigtiff : bool
        Use BigTIFF format for large files

    Returns
    -------
    dict
        Options dictionary for rio.to_raster()
    """
    return {
        'compress': compress if compress != 'NONE' else None,
        'TILED': 'YES' if tiled else 'NO',
        'BLOCKXSIZE': blocksize,
        'BLOCKYSIZE': blocksize,
        'BIGTIFF': 'YES' if bigtiff else 'NO',
    }


def is_raster_format(fmt: str) -> bool:
    """Check if a format string indicates raster output."""
    return fmt.lower() in RASTER_FORMATS
