"""
Raster Module

This module provides comprehensive rasterization capabilities for GEDI data,
supporting both H3 hexagon and EGI square pixel outputs.

Key Features
------------
- H3 hexagon to raster conversion with automatic resolution detection
- Time-series raster generation (years, months, weeks, days)
- GeoTIFF export with compression, tiling, and BigTIFF support
- Batch and parallel rasterization for large datasets
- Integration with Dask for distributed processing

Basic Usage
-----------
>>> from gedih3 import raster
>>>
>>> # Rasterize H3 data
>>> from gedih3.raster import h3_to_raster
>>> ras = h3_to_raster(h3_gdf, columns=['agbd_mean'])
>>> ras.rio.to_raster("output.tif")
>>>
>>> # Generate time-series rasters
>>> from gedih3.raster import TimeSeriesRasterizer
>>> ts = TimeSeriesRasterizer(data, time_col='datetime', target_level=6)
>>> for raster, suffix in ts.generate('2020-01-01', '2023-01-01', 1, 'years'):
...     raster.rio.to_raster(f"output_{suffix}.tif")

For EGI (square pixel) rasterization, use the `gedih3.egi` module instead,
which provides native raster alignment without interpolation.
"""

# Configuration
from .config import (
    COMPRESSION_OPTIONS,
    GEDI_START_DATE_STR,
    GEOTIFF_DEFAULTS,
    H3_RASTER_CRS,
    RASTER_FORMATS,
    TIME_UNITS,
    get_geotiff_options,
    is_raster_format,
)

# Export utilities
from .export import (
    build_vrt,
    compute_raster_stats,
    export_raster,
    export_raster_partition,
    merge_and_export_rasters,
    rasterize_and_export_partitions,
)

# H3 rasterization
from .h3_raster import (
    compute_raster_mosaic,
    get_h3_raster_resolution,
    get_h3_resolution_meters,
    get_optimal_utm,
    h3_to_raster,
    rasterize_h3_partition,
)

# Time-series generation
from .timeseries import (
    GEDI_START_DATE,
    TimeSeriesRasterizer,
    build_temporal_query,
    convert_delta_time_to_datetime,
    filter_by_time_range,
    generate_time_windows,
    parse_datetime_column,
)

__all__ = [
    # Config
    "GEOTIFF_DEFAULTS",
    "COMPRESSION_OPTIONS",
    "RASTER_FORMATS",
    "TIME_UNITS",
    "H3_RASTER_CRS",
    "GEDI_START_DATE_STR",
    "get_geotiff_options",
    "is_raster_format",
    # H3 raster
    "get_h3_resolution_meters",
    "get_optimal_utm",
    "h3_to_raster",
    "rasterize_h3_partition",
    "compute_raster_mosaic",
    "get_h3_raster_resolution",
    # Time-series
    "GEDI_START_DATE",
    "parse_datetime_column",
    "convert_delta_time_to_datetime",
    "generate_time_windows",
    "filter_by_time_range",
    "build_temporal_query",
    "TimeSeriesRasterizer",
    # Export
    "export_raster",
    "export_raster_partition",
    "rasterize_and_export_partitions",
    "merge_and_export_rasters",
    "compute_raster_stats",
    "build_vrt",
]
