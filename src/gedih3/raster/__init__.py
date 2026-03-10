"""
Raster Module

This module provides comprehensive rasterization capabilities for GEDI data,
supporting both H3 hexagon and EGI square pixel outputs.

Notes
-----
- H3 hexagon to raster conversion with automatic resolution detection
- Time-series raster generation (years, months, weeks, days)
- GeoTIFF export with compression, tiling, and BigTIFF support
- Batch and parallel rasterization for large datasets
- Integration with Dask for distributed processing

For EGI (square pixel) rasterization, use the ``gedih3.egi`` module instead,
which provides native raster alignment without interpolation.

Examples
--------
>>> from gedih3.raster import h3_to_raster
>>> ras = h3_to_raster(h3_gdf, columns=['agbd_mean'])
>>> ras.rio.to_raster("output.tif")
>>>
>>> from gedih3.raster import TimeSeriesRasterizer
>>> ts = TimeSeriesRasterizer(data, time_col='datetime', target_level=6)
>>> for raster, suffix in ts.generate('2020-01-01', '2023-01-01', 1, 'years'):
...     raster.rio.to_raster(f"output_{suffix}.tif")
"""

# Configuration
from .config import (
    GEOTIFF_DEFAULTS,
    COMPRESSION_OPTIONS,
    RASTER_FORMATS,
    TIME_UNITS,
    H3_RASTER_CRS,
    GEDI_START_DATE_STR,
    get_geotiff_options,
    is_raster_format,
)

# H3 rasterization
from .h3_raster import (
    get_h3_resolution_meters,
    get_optimal_utm,
    h3_to_raster,
    rasterize_h3_partition,
    compute_raster_mosaic,
    get_h3_raster_resolution,
)

# Time-series generation
from .timeseries import (
    GEDI_START_DATE,
    parse_datetime_column,
    convert_delta_time_to_datetime,
    generate_time_windows,
    filter_by_time_range,
    build_temporal_query,
    TimeSeriesRasterizer,
)

# Export utilities
from .export import (
    export_raster,
    export_raster_partition,
    rasterize_and_export_partitions,
    merge_and_export_rasters,
    compute_raster_stats,
    build_vrt,
)

__all__ = [
    # Config
    'GEOTIFF_DEFAULTS',
    'COMPRESSION_OPTIONS',
    'RASTER_FORMATS',
    'TIME_UNITS',
    'H3_RASTER_CRS',
    'GEDI_START_DATE_STR',
    'get_geotiff_options',
    'is_raster_format',
    # H3 raster
    'get_h3_resolution_meters',
    'get_optimal_utm',
    'h3_to_raster',
    'rasterize_h3_partition',
    'compute_raster_mosaic',
    'get_h3_raster_resolution',
    # Time-series
    'GEDI_START_DATE',
    'parse_datetime_column',
    'convert_delta_time_to_datetime',
    'generate_time_windows',
    'filter_by_time_range',
    'build_temporal_query',
    'TimeSeriesRasterizer',
    # Export
    'export_raster',
    'export_raster_partition',
    'rasterize_and_export_partitions',
    'merge_and_export_rasters',
    'compute_raster_stats',
    'build_vrt',
]
