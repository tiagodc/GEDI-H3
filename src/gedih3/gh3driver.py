import os, glob, h3
import numpy as np
import pandas as pd
import geopandas as gpd
import dask.dataframe
import dask_geopandas

from .config import GH3_DEFAULT_H3_DIR, configure_environment
from .utils import json_read, json_write, now, get_package_version, is_parquet
from .h3utils import intersect_h3_geometries, fix_h3_geometry
from .cliutils import filter_data_columns


def _find_coordinate_column(columns, base_name):
    """
    Find a coordinate column by base name, handling product suffixes.

    In the H3 database, coordinate columns may have product suffixes
    (e.g., 'lon_lowestmode_l2a' instead of 'lon_lowestmode').

    Parameters
    ----------
    columns : list-like
        Available column names
    base_name : str
        Base column name to search for (e.g., 'lon_lowestmode')

    Returns
    -------
    str or None
        Actual column name if found, None otherwise
    """
    columns = list(columns)

    # Exact match
    if base_name in columns:
        return base_name

    # Find columns starting with base_name
    matches = [c for c in columns if c.startswith(base_name)]

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        # Prefer _l2a suffix since coordinates typically come from L2A product
        l2a_matches = [c for c in matches if c.endswith('_l2a')]
        return l2a_matches[0] if l2a_matches else matches[0]

    return None

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

def gh3_write_dataset_meta(opath, index_type='h3', index_level=None, columns=None,
                           source_database=None, query_filter=None, tool=None, **kwargs):
    """
    Write simplified metadata for extracted/aggregated datasets.

    This creates a single metadata file for user-friendly outputs (not hive-partitioned),
    making it easy to understand and use the data outside of gedih3 tools.

    Parameters
    ----------
    opath : str
        Output directory path
    index_type : str
        Type of spatial index ('h3' or 'egi')
    index_level : int
        Resolution level of the index
    columns : list
        List of data columns
    source_database : str
        Path to source H3 database (if applicable)
    query_filter : str
        Query string used for filtering
    tool : str
        Name of the tool that created this dataset
    **kwargs
        Additional metadata to include
    """
    # List parquet files in output directory
    parquet_files = sorted(glob.glob(os.path.join(opath, '*.parquet')))
    file_names = [os.path.basename(f) for f in parquet_files]

    # Extract partition IDs from file names
    partition_ids = [os.path.splitext(f)[0] for f in file_names]

    meta = {
        "metadata": {
            "package_version": get_package_version(),
            "format": "simplified",
            "description": "User-friendly dataset for use with external tools (R, QGIS, etc.)"
        },
        "index_type": index_type,
        "index_level": index_level,
        "columns": sorted(columns) if columns else [],
        "partition_ids": partition_ids,
        "n_files": len(parquet_files),
        "source_database": source_database,
        "query_filter": query_filter,
        "tool": tool,
        "created": now()
    }

    meta.update(kwargs)

    meta_path = os.path.join(opath, "gedih3_dataset.json")
    json_write(meta, meta_path, rewrite=True)
    return meta_path


def gh3_load_dataset(dataset_path, columns=None, filters=None):
    """
    Load a simplified extracted/aggregated dataset.

    This function loads user-friendly datasets created by gh3_extract or gh3_aggregate,
    which consist of simple parquet files (not hive-partitioned).

    Parameters
    ----------
    dataset_path : str
        Path to the dataset directory
    columns : list, optional
        Columns to load (if None, load all)
    filters : list, optional
        PyArrow filters for predicate pushdown

    Returns
    -------
    GeoDataFrame or DataFrame
        Loaded data

    Examples
    --------
    >>> # Load aggregated dataset
    >>> gdf = gh3.gh3_load_dataset('/path/to/aggregated/')
    >>>
    >>> # Load specific columns
    >>> gdf = gh3.gh3_load_dataset('/path/to/extracted/', columns=['agbd_l4a', 'geometry'])
    """
    # Check if it's a file or directory
    if os.path.isfile(dataset_path):
        # Single file
        return gpd.read_parquet(dataset_path, columns=columns)

    # Directory - find parquet files
    parquet_files = sorted(glob.glob(os.path.join(dataset_path, '*.parquet')))

    if not parquet_files:
        # Check for hive-style structure (for backwards compatibility)
        parquet_files = sorted(glob.glob(os.path.join(dataset_path, '**/*.parquet'), recursive=True))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {dataset_path}")

    # Load and concatenate
    kwargs = {}
    if columns:
        kwargs['columns'] = columns
    if filters:
        kwargs['filters'] = filters

    # Try to load as GeoParquet first
    try:
        gdf = gpd.read_parquet(parquet_files, **kwargs)
        return gdf
    except Exception:
        # Fall back to pandas if no geometry
        import pyarrow.parquet as pq
        df = pq.read_table(parquet_files, **kwargs).to_pandas()
        return df


def gh3_load_dataset_lazy(dataset_path, columns=None):
    """
    Load a simplified dataset lazily as a Dask DataFrame.

    Parameters
    ----------
    dataset_path : str
        Path to the dataset directory
    columns : list, optional
        Columns to load. Geometry is automatically included if present in the source.

    Returns
    -------
    dask GeoDataFrame
        Lazy-loaded data
    """
    parquet_files = sorted(glob.glob(os.path.join(dataset_path, '*.parquet')))

    if not parquet_files:
        parquet_files = sorted(glob.glob(os.path.join(dataset_path, '**/*.parquet'), recursive=True))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {dataset_path}")

    # Check if geometry column exists in source
    import pyarrow.parquet as pq
    schema = pq.read_schema(parquet_files[0])
    has_geometry = 'geometry' in schema.names

    kwargs = {}
    if columns:
        # Ensure geometry is always included for GeoParquet files
        if has_geometry and 'geometry' not in columns:
            columns = list(columns) + ['geometry']
        kwargs['columns'] = columns

    # Load first file to get metadata
    _meta = gpd.read_parquet(parquet_files[0], **kwargs)

    ddf = dask.dataframe.from_map(
        lambda f: gpd.read_parquet(f, **kwargs),
        parquet_files,
        meta=_meta
    )

    return dask_geopandas.from_dask_dataframe(ddf, geometry='geometry') if 'geometry' in ddf.columns else ddf


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
    else:
        # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        filtered_cols = filter_data_columns(numeric_cols)
        if filtered_cols:
            g = g[filtered_cols]
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

def gh3_load_hex(d, part_col=None, **kwargs):
    files = glob.glob(os.path.join(d, '**/*.parquet'), recursive=True)
    cols = kwargs.get('columns')
    if cols is None or 'geometry' in cols:
        df = gpd.read_parquet(files, **kwargs)
    else:
        df = pd.read_parquet(files, **kwargs)
    # Add partition column from hive-style directory name (e.g., 'h3_03=abc123')
    if part_col:
        part_id = os.path.basename(d.rstrip('/')).split('=')[-1]
        if part_col not in df.columns and df.index.name != part_col:
            df[part_col] = part_id
    return df

def gh3_load(columns=None, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR, from_map=True): 
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

        # Remove partition column and filter from h3_filter (not in parquet files, derived from dir name)
        fm_filter = {k: v for k, v in h3_filter.items() if k != 'filters'}
        if 'columns' in fm_filter:
            fm_filter['columns'] = [c for c in fm_filter['columns'] if c != h3_part_col]

        _meta = gh3_load_hex(h3_dirs[0], part_col=h3_part_col, **fm_filter)
        ddf = dask.dataframe.from_map(gh3_load_hex, h3_dirs, part_col=h3_part_col, **fm_filter, meta=_meta)
        if 'geometry' in ddf.columns:
            ddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')
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
            # Remove index column from selection (it's the index, not a column)
            out_cols = [c for c in out_cols if c != ddf.index.name]
            ddf = ddf[out_cols]
    
    if region is not None:
        ddf = ddf.clip(region)
    
    return ddf

def gh3_aggregate(gh3_df, target_res=5, agg='mean', columns=None, query=None, add_geometry=True, repartition=False, **kwargs):
    """
    Aggregate H3-indexed GEDI data to a coarser H3 resolution.

    Uses map_partitions for efficient processing when data is loaded with
    from_map=True (each partition corresponds to a single H3 partition cell).

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        H3-indexed GEDI data loaded via gh3_load()
    target_res : int
        Target H3 resolution level (0-15, lower = coarser)
    agg : str, list, dict, or callable
        Aggregation specification (same as pandas groupby.agg)
    columns : list, optional
        Columns to aggregate (if None, all numeric columns)
    query : str, optional
        Pandas query string for filtering before aggregation
    add_geometry : bool
        If True, add H3 polygon geometries to output
    repartition : bool
        If True, repartition by H3 partition column for export
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        H3-indexed aggregated data
    """
    _meta = gh3_aggregate_func(df=gh3_df.head(npartitions=min(gh3_df.npartitions, 10)), res=target_res, agg=agg, cols=columns, **kwargs)

    if query is not None:
        gh3_df = gh3_df.query(query)

    h3part = gh3_part_from_df(gh3_reindex(gh3_df))
    h3agg = f"h3_{target_res:02d}"

    # Use map_partitions for efficient processing
    # Each partition corresponds to a single H3 partition cell when loaded with from_map=True
    agg_df = gh3_df.map_partitions(
        gh3_aggregate_func,
        res=target_res,
        agg=agg,
        cols=columns,
        meta=_meta,
        **kwargs
    )
    agg_df = agg_df.reset_index().set_index(h3agg, sort=False)
    
    if add_geometry:
        _gmeta = agg_df._meta.copy()
        _gmeta['geometry'] = gpd.GeoSeries([], crs=4326)
        _gmeta = gpd.GeoDataFrame(_gmeta, geometry='geometry', crs=4326)
        agg_df = agg_df.map_partitions(gh3_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    if repartition:
        h3part_res = int(h3part.split('_')[1])

        # Compute partition column from aggregated H3 cells
        def add_h3_parent(df, parent_col, parent_res, idx_col):
            df = df.reset_index()
            df[parent_col] = df[idx_col].apply(lambda x: h3.cell_to_parent(x, parent_res))
            return df.set_index(idx_col)

        _part_meta = agg_df._meta.copy()
        _part_meta = _part_meta.reset_index()
        _part_meta[h3part] = ''
        _part_meta = _part_meta.set_index(h3agg)

        agg_df = agg_df.map_partitions(add_h3_parent, parent_col=h3part, parent_res=h3part_res, idx_col=h3agg, meta=_part_meta)
        uparts = sorted(agg_df[h3part].unique().compute().tolist())
        agg_df = agg_df.reset_index().set_index(h3part, sort=False, divisions=uparts + uparts[-1:])
        agg_df = agg_df.reset_index().set_index(h3agg, sort=False)

    agg_df.index = agg_df.index.astype(str)
    return agg_df


def gh3_export_part(df, odir, fmt='parquet', is_file_path=False, part_col=None):
    """
    Export a single partition to file with a simple naming convention.

    Creates user-friendly output files named by partition ID (e.g., 'abc123.parquet'),
    not hive-style directories.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Data partition to export
    odir : str
        Output directory or file path
    fmt : str
        Output format ('parquet', 'gpkg', 'geojson', 'csv', etc.)
    is_file_path : bool
        If True, odir is treated as a complete file path
    part_col : str, optional
        Partition column name to use for naming. If None, auto-detect.

    Returns
    -------
    str
        Output file path
    """
    if df.empty:
        return ''

    os.makedirs(odir, exist_ok=True)

    if is_file_path:
        odir = odir.rstrip('/')
        opath = f"{odir}.{fmt}" if not odir.endswith(fmt) else odir
    else:
        # Determine output filename from partition ID
        oname = None

        # Check for explicit partition column
        if part_col and part_col in df.columns:
            oname = str(df[part_col].iloc[0])
        # Check for H3 partition columns
        elif not oname:
            h3_cols = [col for col in df.columns if col.startswith('h3_')]
            if h3_cols:
                part_col = sorted(h3_cols)[0]
                oname = str(df[part_col].iloc[0])
        # Check for EGI columns
        if not oname:
            egi_cols = [col for col in df.columns if str(col).startswith('egi')]
            if egi_cols:
                part_col = sorted(egi_cols)[0]
                oname = str(df[part_col].iloc[0])
        # Check index name
        if not oname and df.index.name:
            if df.index.name.startswith('h3_') or str(df.index.name).startswith('egi'):
                oname = str(df.index[0])

        # Fallback to generic name
        if not oname:
            oname = f"part_{hash(df.index[0]) % 10000:04d}"

        opath = os.path.join(odir, f"{oname}.{fmt}")

    # Export based on format
    if is_parquet(opath):
        # Use compression for parquet
        df.to_parquet(opath, compression='zstd')
    elif fmt == 'feather':
        df.to_feather(opath)
    elif fmt in ('geojson', 'gpkg', 'shp'):
        if isinstance(df, gpd.GeoDataFrame):
            df.to_file(opath)
        else:
            raise ValueError(f"Cannot export non-GeoDataFrame to {fmt}")
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
        H3-indexed GEDI data (GeoDataFrame with Point geometry preferred)
    level : int
        Target EGI resolution level (1-12)
    agg : str, list, dict, or callable
        Aggregation specification (same as pandas groupby.agg)
    cols : list, optional
        Columns to aggregate (numeric columns only)
    x_col : str
        Longitude column name (default: 'lon_lowestmode'). Only used if df is
        not a GeoDataFrame with Point geometry.
    y_col : str
        Latitude column name (default: 'lat_lowestmode'). Only used if df is
        not a GeoDataFrame with Point geometry.
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    DataFrame or GeoDataFrame
        EGI-indexed aggregated data
    """
    from . import egi

    # Check if input is a GeoDataFrame with Point geometry
    is_point_gdf = (
        isinstance(df, gpd.GeoDataFrame) and
        'geometry' in df.columns and
        len(df) > 0 and
        df.geom_type.iloc[0] == 'Point'
    )

    if not is_point_gdf:
        # Need coordinate columns - try to find them with potential product suffixes
        actual_x_col = _find_coordinate_column(df.columns, x_col)
        actual_y_col = _find_coordinate_column(df.columns, y_col)

        if actual_x_col is None or actual_y_col is None:
            raise ValueError(
                f"Coordinate columns for EGI conversion not found. "
                f"Either provide a GeoDataFrame with Point geometry, or ensure "
                f"columns matching '{x_col}*' and '{y_col}*' are included."
            )
        x_col, y_col = actual_x_col, actual_y_col

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


def _egi_add_index(df, level, x_col, y_col):
    """
    Add EGI index column to a DataFrame without aggregating.

    This is a per-row operation that can be used with map_partitions.
    """
    from . import egi

    # Check if input is a GeoDataFrame with Point geometry
    is_point_gdf = (
        isinstance(df, gpd.GeoDataFrame) and
        'geometry' in df.columns and
        len(df) > 0 and
        df.geom_type.iloc[0] == 'Point'
    )

    if not is_point_gdf:
        actual_x_col = _find_coordinate_column(df.columns, x_col)
        actual_y_col = _find_coordinate_column(df.columns, y_col)
        if actual_x_col is None or actual_y_col is None:
            raise ValueError(f"Coordinate columns not found: {x_col}, {y_col}")
        x_col, y_col = actual_x_col, actual_y_col

    # Add EGI index column (don't set as index yet - need to repartition first)
    return egi.egi_dataframe(df, x_col=x_col, y_col=y_col, level=level, set_index=False)


def _build_agg_meta(gh3_df, target_level, agg, columns, index_type='egi'):
    """
    Build metadata for aggregation result.

    Parameters
    ----------
    gh3_df : dask DataFrame
        Source DataFrame
    target_level : int
        Target resolution level
    agg : str, list, dict, or callable
        Aggregation specification
    columns : list or None
        Columns being aggregated
    index_type : str
        'egi' or 'h3'

    Returns
    -------
    pandas DataFrame
        Metadata template with correct index and column names
    """
    from . import egi

    if index_type == 'egi':
        idx_col = egi.egi_col_name(target_level)
    else:
        idx_col = f'h3_{target_level:02d}'

    # Get sample columns
    sample = gh3_df._meta
    if columns is not None:
        cols = [c for c in columns if c in sample.columns]
    else:
        # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
        numeric_cols = sample.select_dtypes(include=[np.number]).columns.tolist()
        cols = filter_data_columns(numeric_cols)

    # Build metadata with aggregated column names
    if isinstance(agg, dict):
        meta_cols = [f"{col}_{func}" for col, funcs in agg.items()
                     for func in (funcs if isinstance(funcs, list) else [funcs])]
    elif isinstance(agg, list):
        meta_cols = [f"{col}_{func}" for col in cols for func in agg]
    else:
        meta_cols = cols

    _meta = pd.DataFrame(columns=meta_cols, dtype=float)
    _meta.index = pd.Index([], dtype=np.uint64 if index_type == 'egi' else str, name=idx_col)
    return _meta


def egi_aggregate(gh3_df, target_level=6, agg='mean', columns=None, query=None,
                  add_geometry=True, x_col='lon_lowestmode', y_col='lat_lowestmode',
                  repartition=False, **kwargs):
    """
    Aggregate H3-indexed GEDI data to EGI (EASE Grid Index) square pixels.

    Uses a two-level strategy for efficiency:
    1. Repartition by EGI outer tile (coarse, ~19,656 keys)
    2. Local fine-grained aggregation within each partition

    This approach is much more efficient than shuffling by fine EGI pixels
    (which could have billions of unique keys) because:
    - The coarse shuffle uses only ~19,656 unique outer tile keys
    - Fine-grained aggregation happens locally with NO network communication
    - The EGI hash structure guarantees all fine pixels within an outer tile
      share the same outer coordinates, preserving spatial locality

    IMPORTANT: This function repartitions data by EGI outer tile before
    fine-grained aggregation to ensure all GEDI shots for each EGI pixel
    are aggregated together. This is necessary because H3 partitions are
    not self-contained - EGI pixels at H3 boundaries may span multiple
    H3 partitions.

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
        If True, keep output repartitioned by outer EGI tile for export
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        EGI-indexed aggregated data
    """
    from . import egi

    # Validate level
    egi.validate_level(target_level)
    egi_col = egi.egi_col_name(target_level)
    egi_outer_col = egi.egi_col_name(egi.OUTER_LEVEL)

    if query is not None:
        gh3_df = gh3_df.query(query)

    # =========================================================================
    # PHASE 1: Add EGI OUTER tile index (coarse partitioning key)
    # =========================================================================
    # Note: We keep geometry in gh3_df for now so add_outer_index can extract
    # coordinates from Point geometries. The function will drop geometry itself.
    # This adds the ~160km outer tile index to each shot for coarse repartitioning.
    # The outer tile has only ~19,656 unique values globally, making the shuffle
    # much more efficient than shuffling by fine EGI pixels (billions of keys).

    def add_outer_index(df, x_col, y_col, outer_level, egi_outer_col):
        """Add EGI outer tile index to DataFrame without creating geometry.

        Also stores projected coordinates for use in fine-grained aggregation.
        """
        from gedih3.egi.core import to_hash as _to_hash
        from pyproj import Transformer

        # Check if input is a GeoDataFrame with Point geometry
        is_point_gdf = (
            isinstance(df, gpd.GeoDataFrame) and
            'geometry' in df.columns and
            len(df) > 0 and
            df.geom_type.iloc[0] == 'Point'
        )

        if is_point_gdf:
            # Extract coordinates from geometry
            if df.crs.to_epsg() != 6933:
                transformer = Transformer.from_crs(df.crs, 'EPSG:6933', always_xy=True)
                x, y = transformer.transform(df.geometry.x.values, df.geometry.y.values)
            else:
                x, y = df.geometry.x.values, df.geometry.y.values
        else:
            # Use coordinate columns
            actual_x_col = _find_coordinate_column(df.columns, x_col)
            actual_y_col = _find_coordinate_column(df.columns, y_col)
            if actual_x_col is None or actual_y_col is None:
                raise ValueError(f"Coordinate columns not found: {x_col}, {y_col}")

            # Transform from WGS84 to EPSG:6933
            transformer = Transformer.from_crs('EPSG:4326', 'EPSG:6933', always_xy=True)
            x, y = transformer.transform(df[actual_x_col].values, df[actual_y_col].values)

        # Compute EGI outer tile hashes directly (no geometry creation)
        df = df.copy()
        df[egi_outer_col] = _to_hash(np.asarray(x), np.asarray(y), outer_level)

        # Store projected coordinates for fine-grained indexing (after shuffle)
        df['_egi_x'] = x
        df['_egi_y'] = y

        # Drop geometry column if present (we don't need it for shuffling)
        if 'geometry' in df.columns:
            df = df.drop(columns=['geometry'])

        return df

    # Build metadata for outer-indexed result (no geometry - dropped by add_outer_index)
    _outer_meta = gh3_df._meta.copy()
    if 'geometry' in _outer_meta.columns:
        _outer_meta = pd.DataFrame(_outer_meta.drop(columns=['geometry']))
    _outer_meta[egi_outer_col] = np.uint64(0)
    _outer_meta['_egi_x'] = np.float64(0)
    _outer_meta['_egi_y'] = np.float64(0)

    outer_indexed = gh3_df.map_partitions(
        add_outer_index,
        x_col=x_col,
        y_col=y_col,
        outer_level=egi.OUTER_LEVEL,
        egi_outer_col=egi_outer_col,
        meta=_outer_meta
    )

    # =========================================================================
    # PHASE 2: Repartition by EGI outer tile (coarse shuffle)
    # =========================================================================
    # This shuffle uses only ~19,656 unique keys (outer tiles) instead of
    # potentially billions of fine EGI pixels. This is the key efficiency gain.
    # After this shuffle, all data within each ~160km outer tile is co-located
    # in a single partition.

    outer_indexed = outer_indexed.set_index(egi_outer_col)

    # =========================================================================
    # PHASE 3: Local fine-grained aggregation within each partition
    # =========================================================================
    # Now each partition contains all data for its outer tile. We can compute
    # the fine EGI index and aggregate locally with NO network communication.

    def local_egi_aggregate(df, target_level, agg, columns, egi_col, **agg_kwargs):
        """Aggregate a single partition to fine EGI pixels.

        Uses pre-computed EPSG:6933 coordinates stored as _egi_x and _egi_y.
        """
        from gedih3.egi.core import to_hash as _to_hash

        if len(df) == 0:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(index=pd.Index([], dtype=np.uint64, name=egi_col))

        # Reset index to get outer tile as column (we don't need it anymore)
        df = df.reset_index(drop=True)

        # Use pre-computed projected coordinates from add_outer_index
        x = df['_egi_x'].values
        y = df['_egi_y'].values

        # Add fine EGI index directly (no geometry creation)
        df[egi_col] = _to_hash(np.asarray(x), np.asarray(y), target_level)
        df = df.set_index(egi_col)

        # Drop temporary coordinate columns
        df = df.drop(columns=['_egi_x', '_egi_y'], errors='ignore')

        # Filter columns for aggregation
        if columns is not None:
            agg_cols = [c for c in columns if c in df.columns]
            if agg_cols:
                df = df[agg_cols]
        else:
            # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            filtered_cols = filter_data_columns(numeric_cols)
            if filtered_cols:
                df = df[filtered_cols]

        # Local groupby aggregation (NO shuffle - all data is local!)
        if callable(agg):
            result = df.groupby(level=0).apply(agg, include_groups=False, **agg_kwargs)
            if isinstance(result.index, pd.MultiIndex):
                result.index = result.index.get_level_values(0)
        else:
            result = df.groupby(level=0).agg(agg, **agg_kwargs)

        # Flatten MultiIndex columns if present
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(map(str, col)).strip() for col in result.columns.values]

        return result

    # Build metadata for result
    _agg_meta = _build_agg_meta(gh3_df, target_level, agg, columns, index_type='egi')

    agg_df = outer_indexed.map_partitions(
        local_egi_aggregate,
        target_level=target_level,
        agg=agg,
        columns=columns,
        egi_col=egi_col,
        meta=_agg_meta,
        **kwargs
    )

    # =========================================================================
    # PHASE 4: Optional repartition by outer tile for export
    # =========================================================================
    # If requested, add back the outer tile column for organized export.
    # This is done BEFORE adding geometry to avoid corruption during shuffle.

    if repartition:
        def add_outer_from_fine(df, outer_col, outer_level):
            from gedih3.egi.core import to_parent as _to_parent
            if len(df) == 0:
                df[outer_col] = pd.Series([], dtype=np.uint64)
                return df
            df = df.reset_index()
            idx_col = df.columns[0]  # The EGI index column
            df[outer_col] = df[idx_col].apply(lambda x: _to_parent(x, outer_level))
            return df.set_index(idx_col)

        _part_meta = agg_df._meta.copy()
        _part_meta = _part_meta.reset_index()
        _part_meta[egi_outer_col] = np.uint64(0)
        _part_meta = _part_meta.set_index(egi_col)

        agg_df = agg_df.map_partitions(
            add_outer_from_fine,
            outer_col=egi_outer_col,
            outer_level=egi.OUTER_LEVEL,
            meta=_part_meta
        )

    # =========================================================================
    # PHASE 5: Add geometry AFTER all shuffles
    # =========================================================================
    # Geometry must be added last to avoid corruption during Dask serialization.

    if add_geometry:
        _gmeta = agg_df._meta.copy()
        _gmeta['geometry'] = gpd.GeoSeries([], crs=egi.EGI_CRS_STRING)
        _gmeta = gpd.GeoDataFrame(_gmeta, geometry='geometry', crs=egi.EGI_CRS_STRING)
        agg_df = agg_df.map_partitions(egi_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    return agg_df


def egi_export_part(df, odir, fmt='parquet', is_file_path=False):
    """
    Export a single EGI partition to file(s).

    This function handles the case where a partition may contain data from
    multiple EGI outer tiles by splitting the data and writing separate files
    for each outer tile.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        EGI-indexed data partition
    odir : str
        Output directory or file path
    fmt : str
        Output format ('parquet', 'gpkg', 'geojson', 'tif', etc.)
    is_file_path : bool
        If True, odir is treated as a complete file path (single output)

    Returns
    -------
    str
        Output file path(s) - comma-separated if multiple files written
    """
    from . import egi
    import numpy as np

    if df.empty:
        return ''

    os.makedirs(odir, exist_ok=True)

    if is_file_path:
        # Single file output mode - write all data to one file
        odir = odir.rstrip('/')
        opath = f"{odir}.{fmt}" if not odir.endswith(fmt) else odir
        return _write_egi_file(df, opath, fmt)

    # Multi-file mode: split by outer tile to ensure correct file organization
    # This handles the case where Dask partitions may contain data from multiple
    # EGI outer tiles (which can happen after shuffle operations)

    # Compute outer tile for each row (preserving original level)
    idx_array = df.index.to_numpy().astype(np.uint64)
    # Extract outer tile part (px_outer, py_outer) and mask out inner indices
    outer_tiles = (idx_array // np.uint64(1e12)) * np.uint64(1e12)

    # Find unique outer tiles in this partition
    unique_outer = np.unique(outer_tiles)

    output_paths = []
    for outer_tile in unique_outer:
        # Filter data for this outer tile
        mask = outer_tiles == outer_tile
        tile_df = df.iloc[mask]

        if len(tile_df) == 0:
            continue

        # Convert to level 12 (OUTER_LEVEL) for consistent filename
        # This extracts px_outer and py_outer and creates a level 12 hash
        p_outer = outer_tile % np.uint64(1e18) // np.uint64(1e12)
        outer_tile_12 = np.uint64(egi.OUTER_LEVEL * 1e18) + np.uint64(p_outer * 1e12)
        oname = str(outer_tile_12)
        opath = os.path.join(odir, f"{oname}.{fmt}")

        written_path = _write_egi_file(tile_df, opath, fmt)
        if written_path:
            output_paths.append(written_path)

    return ','.join(output_paths) if output_paths else ''


def _write_egi_file(df, opath, fmt):
    """
    Write EGI data to a file.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        EGI-indexed data
    opath : str
        Output file path
    fmt : str
        Output format

    Returns
    -------
    str
        Output file path, or empty string on failure
    """
    from . import egi

    if df.empty:
        return ''

    try:
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

    except Exception:
        return ''


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