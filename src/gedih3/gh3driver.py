import os, glob, h3
import pandas as pd
import geopandas as gpd
import dask.dataframe
import dask_geopandas

from .config import GH3_DEFAULT_H3_DIR, configure_environment
from .utils import json_read, json_write, now, get_package_version, is_parquet
from .h3utils import intersect_h3_geometries, fix_h3_geometry

def gh3_set_db_path(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    os.environ['GH3_DEFAULT_H3_DIR'] = gh3_root_dir
    configure_environment()

def gh3_list_files(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    return glob.glob(os.path.join(gh3_root_dir, '**', '*.parquet'), recursive=True)

def gh3_list_parts(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    files = glob.glob(os.path.join(gh3_root_dir, 'h3_*/'))
    h3_ids = [i.split('=')[-1].rstrip('/') for i in files]
    return h3_ids

def gh3_read_meta(var, gh3_root_dir=GH3_DEFAULT_H3_DIR):
    meta_path = os.path.join(gh3_root_dir, "gedih3_build_log.json")
    meta = json_read(meta_path)
    return meta.get(var)

def gh3_write_meta(opath, **kwargs):
    h3_partition_ids = gh3_list_parts(gh3_root_dir=opath)
    ddf = dask_geopandas.read_parquet(opath, gather_spatial_partitions=False, ignore_metadata_file=False)
    
    extracted_meta = {
        "metadata": {
            "package_version": get_package_version()
        },
        "h3_resolution_level": int(ddf.index.name[-2:]),
        "h3_partition_level": h3.get_resolution(h3_partition_ids[0]),        
        "h3_partition_ids": h3_partition_ids,
        "h3_columns": sorted(ddf.columns.tolist()),
        "last_modified": now()
    }
        
    extracted_meta.update(kwargs)
    
    meta_path = os.path.join(opath, "gedih3_build_log.json")
    json_write(extracted_meta, meta_path, rewrite=True)
    return meta_path        

def gh3_part_from_df(df):
    h3_cols = [col for col in df.columns if col.startswith('h3_')]
    return sorted(h3_cols)[0]

def gh3_reindex(df):
    if (h3_id := df.index.name) < (h3_col := gh3_part_from_df(df)):
        kwargs = {}
        if isinstance(df, (dask.dataframe.DataFrame, dask_geopandas.GeoDataFrame)):
            kwargs['sort'] = False
        rdf = df.reset_index().set_index(h3_col, **kwargs)
        rdf[h3_id] = rdf[h3_id].astype(str)
        return rdf
    return df        

def gh3_aggregate_func(df, res, agg='mean', cols=None, **kwargs):
    import h3pandas
    df = gh3_reindex(df)
    h3col = f"h3_{res:02d}"

    if df.index.name == h3col:
        g = df.groupby(h3col, observed=True)
    else:
        g = df.h3.h3_to_parent(resolution=res).groupby(h3col, observed=True)
    
    if cols is not None:
        g = g[cols]
    out = g.apply(agg, include_groups=False, **kwargs) if callable(agg) else g.agg(agg)

    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ['_'.join(map(str, col)).strip() for col in out.columns.values]

    if isinstance(out.index, pd.MultiIndex):
        out.index = out.index.get_level_values(0)
    return out

def gh3_add_geometry(df):
    geo = [fix_h3_geometry(i) for i in df.index]
    gdf = gpd.GeoDataFrame(df, geometry=geo, crs=4326)
    return gdf

def gh3_load_hex(d, **kwargs):
    files = glob.glob(os.path.join(d, '**/*.parquet'), recursive=True)
    return gpd.read_parquet(files, **kwargs)

def gh3_load(columns=None, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR, from_map=False): 
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir)
    
    h3_filter = {}
    out_cols = None
    if columns is not None:
        if h3_part_col not in columns:
            columns.append(h3_part_col)

        out_cols = columns.copy()
        
        if query is not None:
            available_cols = gh3_read_meta("h3_columns", gh3_root_dir=gh3_dir)
            q_cols = [col for col in available_cols if col in query]
            columns = list(set(columns + q_cols))
        
        h3_filter['columns'] = columns

    if region is not None:
        h3_ids = intersect_h3_geometries(region, h3_ids=h3_ids)
        h3_filter['filters'] = [(h3_part_col,'in',h3_ids)]

        if 'columns' in h3_filter:
            if 'geometry' not in h3_filter['columns']:
                h3_filter['columns'].append('geometry')        
                
    if from_map:
        if region is None:
            h3_dirs = sorted(glob.glob(os.path.join(gh3_dir, f"{h3_part_col}=*/")))
            h3_ids = [os.path.basename(i.rstrip('/')).replace(f'{h3_part_col}=', '') for i in h3_dirs]
        else:
            h3_ids = sorted(h3_ids)
            h3_dirs = [os.path.join(gh3_dir, f"{h3_part_col}={hid}/") for hid in h3_ids]
        
        divs = h3_ids + h3_ids[-1:]
        
        _meta = gh3_load_hex(h3_dirs[0], **h3_filter)
        ddf = dask.dataframe.from_map(gh3_load_hex, h3_dirs, **h3_filter, meta=_meta)
        ddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')
        ddf = ddf.reset_index().set_index(h3_part_col, sort=False, sorted=True, divisions=divs)
    else:
        ddf = dask_geopandas.read_parquet(gh3_dir, 
                                        calculate_divisions=False, 
                                        split_row_groups=False, 
                                        aggregate_files=False, 
                                        gather_spatial_partitions=False, 
                                        ignore_metadata_file=False, 
                                        **h3_filter)
        
        ddf[h3_part_col] = ddf[h3_part_col].astype(str)

    if query is not None:
        ddf = ddf.query(query)
        if out_cols is not None:
            ddf = ddf[out_cols]
    
    if region is not None:
        ddf = ddf.clip(region)
    
    return ddf

def gh3_aggregate(gh3_df, target_res=5, agg='mean', columns=None, query=None, add_geometry=True, repartition=False, **kwargs):
    _meta = gh3_aggregate_func(df=gh3_df.head(npartitions=min(gh3_df.npartitions, 10)), res=target_res, agg=agg, cols=columns, **kwargs)

    if query is not None:
        gh3_df = gh3_df.query(query)
    
    h3part = gh3_part_from_df(gh3_reindex(gh3_df))
    h3agg = f"h3_{target_res:02d}"
    
    _meta[h3part] = h3part
    _meta = _meta.reset_index().set_index([h3part, h3agg])
    
    agg_df = gh3_df.groupby(h3part, observed=True).apply(gh3_aggregate_func, res=target_res, agg=agg, cols=columns, meta=_meta, **kwargs)
    agg_df = agg_df.reset_index().set_index(h3agg, sort=False)
    
    if add_geometry:
        _gmeta = gpd.GeoDataFrame(columns=agg_df._meta.columns.tolist() + ['geometry'], geometry='geometry', crs=4326)
        agg_df = agg_df.map_partitions(gh3_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)
            
    if repartition:
        gh3_parts = gh3_df.index if gh3_df.index.name == h3part else gh3_df[h3part]
        uparts = sorted(gh3_parts.unique().compute().tolist())
        agg_df.index = agg_df.index.rename(h3agg)
        agg_df = agg_df.reset_index().set_index(h3part, sort=False, divisions=uparts + uparts[-1:])
        agg_df = agg_df.reset_index().set_index(h3agg, sort=False)

    agg_df.index = agg_df.index.astype(str)
    return agg_df


def gh3_export_part(df, odir, fmt='parquet', is_file_path=False):
    if df.empty:
        return ''
    
    import h3pandas
    os.makedirs(odir, exist_ok=True)    
    
    if is_file_path:
        odir = odir.rstrip('/')
        opath = f"{odir}.{fmt}" if not odir.endswith(fmt) else odir
    else:
        if hasattr(df, 'name') and df.name.startswith('h3_'):
            oname = df.name
        else:
            h3_partition_level = gh3_part_from_df(df)
            oname = df[h3_partition_level].iloc[0]
        
        opath = os.path.join(odir, f"{oname}.{fmt}")
    
    if is_parquet(opath):
        df.to_parquet(opath)
    elif fmt == 'feather':
        df.to_feather(opath)
    elif fmt in ('geojson', 'gpkg', 'shp'):
        df.to_file(opath)
    elif fmt == 'txt':
        df.to_csv(opath, sep='\t')
    elif fmt == 'csv':
        df.to_csv(opath)
    elif fmt in ('h5', 'hdf5'):
        df.to_hdf(opath, key='GEDI', mode='w')
    else:
        raise ValueError(f"Unsupported export format: {fmt}")

    return opath


# ============================================================================
# EGI (EASE Grid Index) Support
# ============================================================================
# The following functions provide square-pixel indexing using EASE-Grid 2.0
# (EPSG:6933) for GEDI L4B-compatible outputs.

def egi_aggregate_func(df, level, agg='mean', cols=None, x_col='lon_lowestmode', y_col='lat_lowestmode', **kwargs):
    """
    Aggregate H3-indexed DataFrame to EGI (EASE Grid Index) pixels.

    This function converts H3-indexed GEDI data to EGI square pixels,
    which are compatible with GEDI L4B products and standard raster formats.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        H3-indexed GEDI data
    level : int
        Target EGI resolution level (1-12)
    agg : str, list, dict, or callable
        Aggregation specification (same as pandas groupby.agg)
    cols : list, optional
        Columns to aggregate (numeric columns only)
    x_col : str
        Longitude column name (default: 'lon_lowestmode')
    y_col : str
        Latitude column name (default: 'lat_lowestmode')
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    DataFrame or GeoDataFrame
        EGI-indexed aggregated data
    """
    from . import egi

    # Ensure we have the coordinate columns
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"Coordinate columns '{x_col}' and '{y_col}' required for EGI conversion")

    # Add EGI index to the data
    egi_df = egi.egi_dataframe(df, x_col=x_col, y_col=y_col, level=level, set_index=True)

    # Remove geometry if present (will be regenerated)
    if 'geometry' in egi_df.columns:
        egi_df = pd.DataFrame(egi_df.drop(columns='geometry'))

    # Filter to requested columns
    if cols is not None:
        egi_df = egi_df[[c for c in cols if c in egi_df.columns]]

    # Aggregate
    if callable(agg):
        agg_df = pd.DataFrame(egi_df.groupby(level=0).apply(agg, include_groups=False, **kwargs))
        if isinstance(agg_df.index, pd.MultiIndex):
            agg_df.index = agg_df.index.get_level_values(0)
    else:
        agg_df = egi_df.groupby(level=0).agg(agg, **kwargs)

    # Flatten MultiIndex columns
    if isinstance(agg_df.columns, pd.MultiIndex):
        agg_df.columns = ['_'.join(map(str, col)).strip() for col in agg_df.columns.values]

    return agg_df


def egi_add_geometry(df, polygons=True):
    """
    Add EGI pixel geometry to an EGI-indexed DataFrame.

    Parameters
    ----------
    df : DataFrame
        EGI-indexed DataFrame
    polygons : bool
        If True, use polygon geometries; if False, use centroids

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with geometry column
    """
    from . import egi
    return egi.egi_to_geo(df, polygons=polygons)


def egi_aggregate(gh3_df, target_level=6, agg='mean', columns=None, query=None,
                  add_geometry=True, x_col='lon_lowestmode', y_col='lat_lowestmode',
                  repartition=False, **kwargs):
    """
    Aggregate H3-indexed GEDI data to EGI (EASE Grid Index) square pixels.

    This is the main function for converting H3 hexagon data to EGI square
    pixels for GEDI L4B-compatible outputs and rasterization.

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        H3-indexed GEDI data loaded via gh3_load()
    target_level : int
        Target EGI resolution level (1-12):
        - Level 6 (~1km): GEDI baseline
        - Level 7 (~2km): GEDI threshold
        - Level 8 (~10km): GEDI wall-to-wall
    agg : str, list, dict, or callable
        Aggregation specification (same as pandas groupby.agg)
    columns : list, optional
        Columns to aggregate (if None, all numeric columns)
    query : str, optional
        Pandas query string for filtering before aggregation
    add_geometry : bool
        If True, add pixel polygon geometries to output
    x_col : str
        Longitude column name for coordinate lookup
    y_col : str
        Latitude column name for coordinate lookup
    repartition : bool
        If True, repartition by outer EGI tile for export
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        EGI-indexed aggregated data

    Examples
    --------
    >>> # Load H3 data and aggregate to ~1km EGI pixels
    >>> ddf = gh3.gh3_load(columns=['agbd_l4a', 'rh_098_l2a'], region=study_area)
    >>> egi_agg = gh3.egi_aggregate(ddf, target_level=6, agg='mean')
    >>>
    >>> # Rasterize the result
    >>> from gedih3 import egi
    >>> raster = egi.geodf_to_raster(egi_agg.compute())
    """
    from . import egi

    # Validate level
    egi.validate_level(target_level)

    # Build metadata from a sample
    nparts = min(gh3_df.npartitions, 10)
    _meta = egi_aggregate_func(
        df=gh3_df.head(npartitions=nparts),
        level=target_level,
        agg=agg,
        cols=columns,
        x_col=x_col,
        y_col=y_col,
        **kwargs
    )

    if query is not None:
        gh3_df = gh3_df.query(query)

    # Get H3 partition column
    h3part = gh3_part_from_df(gh3_reindex(gh3_df))
    egi_col = egi.egi_col_name(target_level)

    # Update meta with partition info
    _meta[h3part] = h3part
    _meta = _meta.reset_index().set_index([h3part, egi_col])

    # Apply aggregation per H3 partition
    agg_df = gh3_df.groupby(h3part, observed=True).apply(
        egi_aggregate_func,
        level=target_level,
        agg=agg,
        cols=columns,
        x_col=x_col,
        y_col=y_col,
        meta=_meta,
        **kwargs
    )
    agg_df = agg_df.reset_index().set_index(egi_col, sort=False)

    # Add geometry if requested
    if add_geometry:
        _gmeta = gpd.GeoDataFrame(
            columns=agg_df._meta.columns.tolist() + ['geometry'],
            geometry='geometry',
            crs=egi.EGI_CRS_STRING
        )
        agg_df = agg_df.map_partitions(egi_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    # Repartition by outer EGI tile if requested
    if repartition:
        egi_outer_col = egi.egi_col_name(egi.OUTER_LEVEL)
        agg_df = agg_df.map_partitions(
            lambda x: egi.egi_to_parent(x, parent_level=egi.OUTER_LEVEL, set_index=False)
        )
        gh3_parts = gh3_df.index if gh3_df.index.name == h3part else gh3_df[h3part]
        uparts = sorted(gh3_parts.unique().compute().tolist())
        agg_df.index = agg_df.index.rename(egi_col)
        agg_df = agg_df.reset_index().set_index(h3part, sort=False, divisions=uparts + uparts[-1:])
        agg_df = agg_df.reset_index().set_index(egi_col, sort=False)

    return agg_df


def egi_export_part(df, odir, fmt='parquet', is_file_path=False):
    """
    Export a single EGI partition to file.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        EGI-indexed data partition
    odir : str
        Output directory or file path
    fmt : str
        Output format ('parquet', 'gpkg', 'geojson', 'tif', etc.)
    is_file_path : bool
        If True, odir is treated as a complete file path

    Returns
    -------
    str
        Output file path
    """
    from . import egi
    import numpy as np

    if df.empty:
        return ''

    os.makedirs(odir, exist_ok=True)

    if is_file_path:
        odir = odir.rstrip('/')
        opath = f"{odir}.{fmt}" if not odir.endswith(fmt) else odir
    else:
        # Determine output filename from EGI outer tile
        if hasattr(df, 'name') and str(df.name).startswith('egi'):
            oname = str(df.name)
        else:
            # Get dominant outer tile ID
            _df = df.sample(100) if len(df) > 100 else df
            outer_df = egi.egi_to_parent(_df, egi.OUTER_LEVEL)
            oname = str(outer_df.index.value_counts().idxmax())

        opath = os.path.join(odir, f"{oname}.{fmt}")

    # Handle raster export
    if fmt in ('tif', 'tiff', 'geotiff'):
        raster = egi.geodf_to_raster(df)
        egi.export_raster(raster, opath)
        return opath

    # Handle vector/tabular export
    if is_parquet(opath):
        df.to_parquet(opath)
    elif fmt == 'feather':
        df.to_feather(opath)
    elif fmt in ('geojson', 'gpkg', 'shp'):
        df.to_file(opath)
    elif fmt == 'txt':
        df.to_csv(opath, sep='\t')
    elif fmt == 'csv':
        df.to_csv(opath)
    elif fmt in ('h5', 'hdf5'):
        df.to_hdf(opath, key='GEDI', mode='w')
    else:
        raise ValueError(f"Unsupported export format: {fmt}")

    return opath


def is_egi_indexed(df):
    """
    Check if a DataFrame is EGI-indexed.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        DataFrame to check

    Returns
    -------
    bool
        True if EGI-indexed, False otherwise
    """
    if df.index.name and str(df.index.name).startswith('egi'):
        return True
    egi_cols = [col for col in df.columns if str(col).startswith('egi')]
    return len(egi_cols) > 0


def get_spatial_index_type(df):
    """
    Determine the spatial index type of a DataFrame.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        DataFrame to check

    Returns
    -------
    str
        'h3', 'egi', or None
    """
    # Check index name
    if df.index.name:
        if str(df.index.name).startswith('h3_'):
            return 'h3'
        if str(df.index.name).startswith('egi'):
            return 'egi'

    # Check columns
    h3_cols = [col for col in df.columns if str(col).startswith('h3_')]
    egi_cols = [col for col in df.columns if str(col).startswith('egi')]

    if egi_cols:
        return 'egi'
    if h3_cols:
        return 'h3'

    return None


# ============================================================================
# Rasterization Support
# ============================================================================

def gh3_to_raster(
    gdf,
    columns=None,
    output_path=None,
    compress='LZW'
):
    """
    Convert H3-indexed GeoDataFrame to raster.

    This is a convenience function that wraps the raster module's
    h3_to_raster function with sensible defaults.

    Parameters
    ----------
    gdf : GeoDataFrame
        H3-indexed GeoDataFrame with polygon geometries
    columns : list of str, optional
        Columns to rasterize. If None, all numeric columns.
    output_path : str, optional
        If provided, save raster to this path
    compress : str
        Compression method for GeoTIFF

    Returns
    -------
    xr.Dataset
        Raster dataset

    Examples
    --------
    >>> # Rasterize aggregated data
    >>> raster = gh3_to_raster(agg_gdf)
    >>> raster.rio.to_raster("output.tif")
    >>>
    >>> # Or save directly
    >>> raster = gh3_to_raster(agg_gdf, output_path="output.tif")
    """
    from .raster import h3_to_raster, export_raster

    xras = h3_to_raster(gdf, columns=columns)

    if output_path:
        export_raster(xras, output_path, compress=compress)

    return xras


def gh3_rasterize_partitions(
    ddf,
    output_dir,
    columns=None,
    compress='LZW',
    show_progress=True
):
    """
    Rasterize Dask GeoDataFrame partitions to individual GeoTIFF files.

    Parameters
    ----------
    ddf : dask GeoDataFrame
        H3-indexed Dask GeoDataFrame
    output_dir : str
        Output directory for raster files
    columns : list of str, optional
        Columns to rasterize
    compress : str
        Compression method for GeoTIFF
    show_progress : bool
        Show Dask progress bar

    Returns
    -------
    list of str
        Paths to output files
    """
    from .raster import rasterize_and_export_partitions, rasterize_h3_partition

    return rasterize_and_export_partitions(
        ddf, output_dir, rasterize_h3_partition,
        columns=columns, compress=compress, show_progress=show_progress
    )