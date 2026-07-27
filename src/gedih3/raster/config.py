# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

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
    'cog': True,
}

# Cloud Optimized GeoTIFF is the default layout for image output: internal
# tiling plus overviews, so a viewer or a range-reading client fetches only
# the bytes it needs. A COG is a valid GeoTIFF, so nothing that could read
# the old output stops working; the cost is the overview pyramid on disk
# (~30% on a dense tile).
COG_DEFAULTS: Dict[str, Any] = {
    'overviews': 'AUTO',
    'resampling': 'AVERAGE',
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


def get_cog_options(
    compress: str = 'LZW',
    blocksize: int = 256,
    bigtiff: bool = True,
    overviews: str = 'AUTO',
    resampling: str = 'AVERAGE'
) -> Dict[str, Any]:
    """
    Generate rasterio creation options for the GDAL COG driver.

    The COG driver takes its own option names — ``BLOCKSIZE`` rather than
    ``TILED``/``BLOCKXSIZE``, and it is always tiled. Passing the GeoTIFF
    option names instead is silently ignored and you get the driver defaults
    (512-pixel blocks), which is why this is a separate builder rather than a
    flag on :func:`get_geotiff_options`.

    Parameters
    ----------
    compress : str
        Compression method ('LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE')
    blocksize : int
        Internal tile size in pixels
    bigtiff : bool
        Use BigTIFF for large files
    overviews : str
        COG ``OVERVIEWS`` policy — 'AUTO' builds the pyramid when the image is
        larger than one block, 'NONE' skips it
    resampling : str
        Resampling used to build the overviews

    Returns
    -------
    dict
        Options for ``rio.to_raster()``, including ``driver='COG'``
    """
    return {
        'driver': 'COG',
        'COMPRESS': compress,
        'BLOCKSIZE': blocksize,
        'BIGTIFF': 'YES' if bigtiff else 'NO',
        'OVERVIEWS': overviews,
        'RESAMPLING': resampling,
    }


def is_raster_format(fmt: str) -> bool:
    """Check if a format string indicates raster output."""
    return fmt.lower() in RASTER_FORMATS
