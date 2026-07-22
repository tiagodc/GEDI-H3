# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
Raster Export Module

This module provides utilities for exporting raster data to various formats,
with support for GeoTIFF compression, tiling, and batch operations.
"""
from typing import Dict, List, Optional, Union
import logging
import os
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import dask
import dask.dataframe
from dask.distributed import progress as dask_progress

from .config import get_geotiff_options, is_raster_format
from ..exceptions import GediRasterizationError
from ..utils import AtomicFileWriter

logger = logging.getLogger(__name__)


def export_raster(
    xras: xr.Dataset,
    output_path: str,
    compress: str = 'LZW',
    tiled: bool = True,
    blocksize: int = 256,
    bigtiff: bool = True
) -> str:
    """
    Export xarray Dataset to GeoTIFF file.

    Parameters
    ----------
    xras : xr.Dataset
        Raster dataset to export
    output_path : str
        Output file path
    compress : str
        Compression method ('LZW', 'ZSTD', 'DEFLATE', 'NONE')
    tiled : bool
        Use tiled output format
    blocksize : int
        Tile block size in pixels
    bigtiff : bool
        Use BigTIFF format for large files

    Returns
    -------
    str
        Output file path
    """
    options = get_geotiff_options(compress, tiled, blocksize, bigtiff)

    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    xras.rio.to_raster(output_path, **options)
    return output_path


def export_raster_partition(
    data: Union[pd.Series, xr.Dataset],
    output_dir: str,
    fmt: str = 'tif',
    compress: str = 'LZW',
    partition_id_attr: Optional[str] = None
) -> str:
    """
    Export raster partition(s) to file(s).

    This function handles the case where a Series may contain multiple
    rasters from different spatial tiles. Each raster is exported to
    its own file based on its tile ID attribute.

    Parameters
    ----------
    data : pd.Series or xr.Dataset
        Raster data (Series of DataArrays/Datasets or single Dataset)
    output_dir : str
        Output directory
    fmt : str
        Output format ('tif', 'nc')
    compress : str
        Compression method for GeoTIFF
    partition_id_attr : str, optional
        Attribute name containing partition ID for filename

    Returns
    -------
    str
        Output file path(s), comma-separated if multiple files written
    """
    import re
    os.makedirs(output_dir, exist_ok=True)

    if isinstance(data, pd.Series):
        if len(data) == 0:
            return ''
        # Series of Datasets - export each separately
        valid_rasters = [x for x in data if hasattr(x, 'data_vars') and len(x.data_vars) > 0]
        if not valid_rasters:
            return ''

        # Export each raster to its own file based on its tile ID
        output_paths = []
        for xras in valid_rasters:
            path = _export_single_raster(xras, output_dir, fmt, compress, partition_id_attr)
            if path:
                output_paths.append(path)

        return ','.join(output_paths) if output_paths else ''

    elif isinstance(data, xr.Dataset):
        return _export_single_raster(data, output_dir, fmt, compress, partition_id_attr)

    return ''


def _export_single_raster(
    xras: xr.Dataset,
    output_dir: str,
    fmt: str,
    compress: str,
    partition_id_attr: Optional[str] = None
) -> str:
    """
    Export a single xarray Dataset to file.

    Parameters
    ----------
    xras : xr.Dataset
        Raster data
    output_dir : str
        Output directory
    fmt : str
        Output format
    compress : str
        Compression method
    partition_id_attr : str, optional
        Attribute name for partition ID

    Returns
    -------
    str
        Output file path
    """
    import re

    if len(xras.data_vars) == 0:
        return ''

    # Determine output filename from attributes
    basename = 'raster'

    for var in list(xras.data_vars)[:1]:
        attrs = xras[var].attrs
        # Look for any H3 partition ID attribute (h3_XX_id pattern)
        h3_part_attrs = [k for k in attrs.keys() if re.match(r'h3_\d{2}_id$', str(k))]
        if h3_part_attrs:
            basename = str(attrs[h3_part_attrs[0]])
            break
        elif 'egi12_id' in attrs:
            basename = str(attrs['egi12_id'])
            break
        # Look for any EGI partition ID attribute (egiXX_id pattern)
        egi_part_attrs = [k for k in attrs.keys() if re.match(r'egi\d+_id$', str(k))]
        if egi_part_attrs:
            basename = str(attrs[egi_part_attrs[0]])
            break
        elif partition_id_attr and partition_id_attr in attrs:
            basename = str(attrs[partition_id_attr])
            break

    # Add extension
    output_path = os.path.join(output_dir, f"{basename}.{fmt}")

    if fmt in ('tif', 'tiff', 'geotiff'):
        options = get_geotiff_options(compress)
        xras.rio.to_raster(output_path, **options)
    elif fmt in ('nc', 'netcdf'):
        xras.to_netcdf(output_path)
    else:
        raise GediRasterizationError(f"Unsupported raster format: {fmt}")

    return output_path


def rasterize_and_export_partitions(
    gdf: Union[gpd.GeoDataFrame, dask.dataframe.DataFrame],
    output_dir: str,
    rasterize_func,
    columns: Optional[List[str]] = None,
    fmt: str = 'tif',
    compress: str = 'LZW',
    show_progress: bool = True,
    **rasterize_kwargs
) -> List[str]:
    """
    Rasterize and export GeoDataFrame partitions to individual files.

    Parameters
    ----------
    gdf : GeoDataFrame or dask GeoDataFrame
        Input spatially-indexed data
    output_dir : str
        Output directory for raster files
    rasterize_func : callable
        Function to rasterize each partition (e.g., rasterize_h3_partition)
    columns : list of str, optional
        Columns to include in rasterization
    fmt : str
        Output format ('tif', 'nc')
    compress : str
        Compression method for GeoTIFF
    show_progress : bool
        Show Dask progress bar

    Returns
    -------
    list of str
        Paths to output files
    """
    os.makedirs(output_dir, exist_ok=True)

    if hasattr(gdf, 'npartitions'):
        # Dask GeoDataFrame
        raster_parts = gdf.map_partitions(
            rasterize_func,
            columns=columns,
            **rasterize_kwargs,
            meta=pd.Series(dtype=object)
        )

        export_func = lambda x: export_raster_partition(
            x, output_dir, fmt=fmt, compress=compress
        )

        paths = raster_parts.map_partitions(
            export_func,
            meta=pd.Series(dtype=str)
        )

        # Driver-side gather instead of .compute() — the optimizer's
        # RepartitionToFewer collapse step wedges on tunneled meshes past
        # ~1500 partitions in dask >= 2025.2.
        from ..utils import dask_safe_collect
        try:
            result = list(dask_safe_collect(paths, show_progress=show_progress))
        except (ValueError, ImportError):
            # No distributed client — fall back to legacy compute.
            result = paths.compute().tolist()
    else:
        # Regular GeoDataFrame
        raster = rasterize_func(gdf, columns=columns, **rasterize_kwargs)
        path = export_raster_partition(raster, output_dir, fmt=fmt, compress=compress)
        result = [path] if path else []

    # Build VRT mosaic from output tiles
    # Split comma-separated paths (from partitions producing multiple tiles)
    all_paths = []
    for p in result:
        if p:
            all_paths.extend(p.split(','))
    tif_files = [p for p in all_paths if p.endswith('.tif')]
    if len(tif_files) > 1:
        vrt_path = os.path.join(output_dir, 'mosaic.vrt')
        build_vrt_safe(tif_files, vrt_path)

    return result


# numpy dtype name -> GDAL type name, as spelled in VRT XML. Explicit table
# rather than a rasterio private helper: the mapping is stable and the failure
# mode of a wrong guess (silently misread pixels) is worse than a KeyError.
_GDAL_TYPENAME = {
    'uint8': 'Byte', 'int8': 'Int8', 'uint16': 'UInt16', 'int16': 'Int16',
    'uint32': 'UInt32', 'int32': 'Int32', 'uint64': 'UInt64', 'int64': 'Int64',
    'float32': 'Float32', 'float64': 'Float64',
    'complex_int16': 'CInt16', 'complex64': 'CFloat32', 'complex128': 'CFloat64',
}


def build_vrt(tif_files, vrt_path):
    """Build a GDAL VRT file mosaicking a list of GeoTIFF tiles.

    Uses the ``osgeo.gdal`` bindings when they are importable, and falls back
    to :func:`build_vrt_xml` otherwise. The GDAL bindings are not pip-installable
    without a version-matched system libgdal, so pip-only installs take the
    fallback; conda / HPC installs keep the authoritative backend.

    Parameters
    ----------
    tif_files : list of str
        Paths to input GeoTIFF files
    vrt_path : str
        Output VRT file path

    Returns
    -------
    str
        Path to the written VRT
    """
    try:
        from osgeo import gdal
    except ImportError:
        return build_vrt_xml(tif_files, vrt_path)

    gdal.UseExceptions()
    # Pin the VRT resolution to the first tile's pixel size.  Without an
    # explicit resolution, gdal.BuildVRT defaults to 'average'.
    ds = gdal.Open(tif_files[0])
    gt = ds.GetGeoTransform()
    ds = None
    opts = gdal.BuildVRTOptions(resolution='user', xRes=gt[1], yRes=abs(gt[5]))
    gdal.BuildVRT(vrt_path, tif_files, options=opts)
    return vrt_path


def build_vrt_xml(tif_files, vrt_path):
    """Write a VRT mosaic as XML using rasterio only — no ``osgeo`` bindings.

    Handles the narrow case gedih3's own writers emit: north-up GeoTIFF tiles
    sharing one CRS and one pixel size. Anything outside that (rotated
    geotransforms, mixed CRS) raises rather than emitting a subtly wrong
    mosaic — use the ``osgeo`` backend for the general case.

    As with :func:`build_vrt`, the mosaic resolution is pinned to the first
    tile so partial edge tiles cannot drag it off the canonical grid.

    Parameters
    ----------
    tif_files : list of str
        Paths to input GeoTIFF files
    vrt_path : str
        Output VRT file path

    Returns
    -------
    str
        Path to the written VRT

    Raises
    ------
    GediRasterizationError
        If the tiles are rotated, span multiple CRS, or use a dtype with no
        GDAL equivalent.
    """
    import rasterio

    if not tif_files:
        raise GediRasterizationError("build_vrt_xml requires at least one input tile")

    profiles = []
    for path in tif_files:
        with rasterio.open(path) as src:
            profiles.append({
                'path': path, 'transform': src.transform, 'width': src.width,
                'height': src.height, 'count': src.count, 'dtypes': src.dtypes,
                'nodata': src.nodata, 'crs': src.crs, 'bounds': src.bounds,
                'block': src.block_shapes[0],
                'colorinterp': [ci.name.capitalize() for ci in src.colorinterp],
            })

    first = profiles[0]

    # Preconditions — refuse loudly instead of writing a wrong mosaic.
    for p in profiles:
        if p['transform'].b or p['transform'].d:
            raise GediRasterizationError(
                f"build_vrt_xml cannot mosaic rotated rasters [file={p['path']}]; "
                "install the GDAL Python bindings for the general case"
            )
        if p['crs'] != first['crs']:
            raise GediRasterizationError(
                f"build_vrt_xml requires a single CRS across tiles: "
                f"{first['crs']} vs {p['crs']} [file={p['path']}]"
            )
    unknown = {d for p in profiles for d in p['dtypes']} - set(_GDAL_TYPENAME)
    if unknown:
        raise GediRasterizationError(f"No GDAL type name for dtype(s): {sorted(unknown)}")

    xres = first['transform'].a
    yres = abs(first['transform'].e)

    left = min(p['bounds'].left for p in profiles)
    right = max(p['bounds'].right for p in profiles)
    bottom = min(p['bounds'].bottom for p in profiles)
    top = max(p['bounds'].top for p in profiles)

    root = ET.Element('VRTDataset',
                      rasterXSize=str(int(round((right - left) / xres))),
                      rasterYSize=str(int(round((top - bottom) / yres))))
    if first['crs'] is not None:
        ET.SubElement(root, 'SRS').text = first['crs'].to_wkt()
    ET.SubElement(root, 'GeoTransform').text = ', '.join(
        f'{v:.16e}' for v in (left, xres, 0.0, top, 0.0, -yres))

    vrt_dir = os.path.dirname(os.path.abspath(vrt_path))

    for band in range(1, first['count'] + 1):
        vband = ET.SubElement(root, 'VRTRasterBand',
                              dataType=_GDAL_TYPENAME[first['dtypes'][band - 1]],
                              band=str(band))
        if first['nodata'] is not None:
            ET.SubElement(vband, 'NoDataValue').text = repr(first['nodata'])
        ET.SubElement(vband, 'ColorInterp').text = first['colorinterp'][band - 1]

        for p in profiles:
            source = ET.SubElement(vband, 'ComplexSource')
            filename = ET.SubElement(source, 'SourceFilename', relativeToVRT='1')
            filename.text = os.path.relpath(os.path.abspath(p['path']), vrt_dir)
            ET.SubElement(source, 'SourceBand').text = str(band)
            ET.SubElement(source, 'SourceProperties',
                          RasterXSize=str(p['width']), RasterYSize=str(p['height']),
                          DataType=_GDAL_TYPENAME[p['dtypes'][band - 1]],
                          BlockXSize=str(p['block'][1]), BlockYSize=str(p['block'][0]))
            ET.SubElement(source, 'SrcRect', xOff='0', yOff='0',
                          xSize=str(p['width']), ySize=str(p['height']))
            ET.SubElement(source, 'DstRect',
                          xOff=str(int(round((p['bounds'].left - left) / xres))),
                          yOff=str(int(round((top - p['bounds'].top) / yres))),
                          xSize=str(int(round(p['width'] * p['transform'].a / xres))),
                          ySize=str(int(round(p['height'] * abs(p['transform'].e) / yres))))
            if p['nodata'] is not None:
                ET.SubElement(source, 'NODATA').text = repr(p['nodata'])

    ET.indent(root, space='  ')
    with AtomicFileWriter(vrt_path) as tmp_path:
        with open(tmp_path, 'w') as handle:
            handle.write(ET.tostring(root, encoding='unicode'))
    return vrt_path


def build_vrt_safe(tif_files, vrt_path):
    """Build a VRT mosaic, downgrading any failure to a warning.

    The ``.tif`` tiles are the deliverable; the VRT is a convenience mosaic
    over them. A missing GDAL backend or an unmosaicable tile set must not
    discard a completed rasterization job.

    Parameters
    ----------
    tif_files : list of str
        Paths to input GeoTIFF files
    vrt_path : str
        Output VRT file path

    Returns
    -------
    str or None
        Path to the written VRT, or None if it could not be built.
    """
    try:
        return build_vrt(tif_files, vrt_path)
    except Exception as exc:
        logger.warning(
            f"Could not build VRT mosaic: {type(exc).__name__}: {exc} "
            f"[vrt={vrt_path}] — the {len(tif_files)} GeoTIFF tiles are unaffected"
        )
        return None


def merge_and_export_rasters(
    gdf: Union[gpd.GeoDataFrame, dask.dataframe.DataFrame],
    output_path: str,
    rasterize_func,
    columns: Optional[List[str]] = None,
    compress: str = 'LZW',
    show_progress: bool = True,
    **rasterize_kwargs
) -> str:
    """
    Rasterize all partitions and merge into a single output file.

    Parameters
    ----------
    gdf : GeoDataFrame or dask GeoDataFrame
        Input spatially-indexed data
    output_path : str
        Output file path
    rasterize_func : callable
        Function to rasterize each partition
    columns : list of str, optional
        Columns to include in rasterization
    compress : str
        Compression method for GeoTIFF
    show_progress : bool
        Show Dask progress bar

    Returns
    -------
    str
        Path to output file
    """
    from rioxarray.merge import merge_datasets

    if hasattr(gdf, 'npartitions'):
        # Dask GeoDataFrame - rasterize partitions in parallel
        raster_parts = gdf.map_partitions(
            rasterize_func,
            columns=columns,
            **rasterize_kwargs,
            meta=pd.Series(dtype=object)
        )

        # Driver-side gather instead of .compute() — see dask_safe_collect
        # docstring; the optimizer's RepartitionToFewer collapse wedges on
        # tunneled meshes past ~1500 partitions in dask >= 2025.2.
        from ..utils import dask_safe_collect
        try:
            rasters = dask_safe_collect(raster_parts, show_progress=show_progress)
        except (ValueError, ImportError):
            rasters = raster_parts.compute()

        # Filter valid rasters - handle both Series results and direct Dataset results
        valid_rasters = []
        for r in rasters:
            if isinstance(r, pd.Series):
                # rasterize_func returns Series containing Dataset
                for item in r:
                    if hasattr(item, 'data_vars') and len(item.data_vars) > 0:
                        valid_rasters.append(item)
            elif hasattr(r, 'data_vars') and len(r.data_vars) > 0:
                valid_rasters.append(r)

        if not valid_rasters:
            raise GediRasterizationError("No valid rasters to merge")

        if len(valid_rasters) == 1:
            merged = valid_rasters[0]
        else:
            # Use merge_datasets which properly handles non-overlapping tiles
            # by creating a combined extent and filling NoData where tiles don't overlap
            # IMPORTANT: Use nodata=np.nan to ensure gaps are filled with NaN, not 0
            merged = merge_datasets(valid_rasters, nodata=np.nan)
    else:
        # Single GeoDataFrame
        merged = rasterize_func(gdf, columns=columns, **rasterize_kwargs)
        if isinstance(merged, pd.Series) and len(merged) > 0:
            merged = merged.iloc[0]

    # Export
    return export_raster(merged, output_path, compress=compress)


def compute_raster_stats(xras: xr.Dataset) -> Dict[str, Dict[str, float]]:
    """
    Compute basic statistics for each variable in a raster dataset.

    Parameters
    ----------
    xras : xr.Dataset
        Input raster dataset

    Returns
    -------
    dict
        Statistics for each variable: {var_name: {min, max, mean, std, count}}
    """
    stats = {}
    for var in xras.data_vars:
        data = xras[var].values
        valid = ~np.isnan(data)
        stats[var] = {
            'min': float(np.nanmin(data)) if valid.any() else np.nan,
            'max': float(np.nanmax(data)) if valid.any() else np.nan,
            'mean': float(np.nanmean(data)) if valid.any() else np.nan,
            'std': float(np.nanstd(data)) if valid.any() else np.nan,
            'count': int(valid.sum()),
        }
    return stats
