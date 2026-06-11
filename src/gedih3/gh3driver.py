# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

import os, re, h3
import numpy as np
import pandas as pd
import geopandas as gpd
import dask.dataframe
import dask_geopandas


from .config import GH3_DEFAULT_H3_DIR, configure_environment, BUILD_LOG_FILENAME, DATASET_META_FILENAME
from .utils import (json_read, json_write, now, get_package_version, is_parquet,
                     smart_glob, smart_exists, smart_isdir, is_remote_path,
                     smart_open, generate_manifest, check_nan_only_columns,
                     smart_join, AtomicFileWriter, atomic_parquet_write,
                     dask_safe_wait, dask_safe_collect)
from .h3utils import intersect_h3_geometries, fix_h3_geometry
from .cliutils import find_coordinate_column, get_aggregatable_columns
from .exceptions import (GediValidationError, GediDatabaseNotFoundError, GediProcessingError,
                         GediSpatialError, GediVariableError)


def _resolve_columns(columns, path, info):
    """Expand fnmatch wildcards in ``columns`` against available column names.

    If no wildcard characters are present, returns ``columns`` unchanged.
    """
    if columns is None:
        return None
    if not any(any(c in col for c in ('*', '?', '[', ']')) for col in columns):
        return columns

    # Obtain available column names from the source
    if info['source_type'] == 'h3_database':
        available = gh3_read_meta("h3_columns", gh3_root_dir=path)
    else:
        from .cliutils import detect_dataset_format, read_dataset_schema, list_dataset_files
        fmt = detect_dataset_format(path)
        if smart_exists(path) and not smart_isdir(path):
            available, _ = read_dataset_schema(path, fmt)
        else:
            files = list_dataset_files(path, fmt=fmt)
            available, _ = read_dataset_schema(files[0], fmt)

    from .gedidriver import expand_var_wildcards
    return expand_var_wildcards(columns, available)


def _detect_source(source=None):
    """Resolve data source path and detect its type.

    Parameters
    ----------
    source : str, optional
        Path to any data source (H3 database, simplified dataset, parquet dir).

    Returns
    -------
    tuple
        (path, info_dict) where info_dict is from get_dataset_index_info().
    """
    from .cliutils import get_dataset_index_info

    path = source if source is not None else GH3_DEFAULT_H3_DIR
    info = get_dataset_index_info(path)
    return path, info

def gh3_set_db_path(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    os.environ['GH3_DEFAULT_H3_DIR'] = gh3_root_dir
    configure_environment()

def gh3_list_files(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    return smart_glob(smart_join(gh3_root_dir, '**', '*.parquet'), recursive=True)

def gh3_list_parts(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    files = smart_glob(smart_join(gh3_root_dir, 'h3_*/'))
    h3_ids = [i.split('=')[-1].rstrip('/') for i in files]
    return h3_ids

def gh3_read_meta(var, gh3_root_dir=GH3_DEFAULT_H3_DIR):
    meta_path = smart_join(gh3_root_dir, BUILD_LOG_FILENAME)
    meta = json_read(meta_path)
    return meta.get(var)

def gh3_write_meta(opath, **kwargs):
    h3_partition_ids = gh3_list_parts(gh3_root_dir=opath)
    storage_kwargs = {}
    if is_remote_path(opath):
        from .utils import get_storage_options
        protocol = opath.split('://')[0]
        storage_kwargs['storage_options'] = get_storage_options(protocol)
    ddf = dask_geopandas.read_parquet(opath, gather_spatial_partitions=False,
                                       ignore_metadata_file=False, **storage_kwargs)
    
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
    
    meta_path = smart_join(opath, BUILD_LOG_FILENAME)
    json_write(extracted_meta, meta_path, rewrite=True)
    return meta_path

def gh3_write_dataset_meta(opath, index_type='h3', index_level=None, columns=None,
                           source_database=None, query_filter=None, tool=None,
                           file_format='parquet', **kwargs):
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
    file_format : str
        Output file format (e.g. 'parquet', 'feather', 'gpkg')
    **kwargs
        Additional metadata to include
    """
    from .cliutils import list_dataset_files, PIPELINE_FORMATS

    # List data files in output directory
    if file_format in PIPELINE_FORMATS:
        try:
            data_files = list_dataset_files(opath, fmt=file_format)
        except (FileNotFoundError, GediDatabaseNotFoundError):
            data_files = []
    else:
        # Non-pipeline format: glob for whatever was written
        data_files = smart_glob(smart_join(opath, f'*.{file_format}'))

    file_names = [os.path.basename(f) for f in data_files]
    partition_ids = [os.path.splitext(f)[0] for f in file_names]

    meta = {
        "metadata": {
            "package_version": get_package_version(),
            "format": "simplified",
            "description": "User-friendly dataset for use with external tools (R, QGIS, etc.)"
        },
        "file_format": file_format,
        "index_type": index_type,
        "index_level": index_level,
        "columns": sorted(columns) if columns else [],
        "partition_ids": partition_ids,
        "n_files": len(data_files),
        "source_database": source_database,
        "query_filter": query_filter,
        "tool": tool,
        "created": now()
    }

    meta.update(kwargs)

    meta_path = smart_join(opath, DATASET_META_FILENAME)
    json_write(meta, meta_path, rewrite=True)

    # Generate manifest for accelerated file listing. The extract /
    # aggregate output is a flat directory of parquet files (not an H3
    # partition tree), so pass tree_shape='flat' to avoid scanning for
    # h3_NN=* partition dirs that don't exist.
    if not is_remote_path(opath):
        generate_manifest(opath, pattern='*.parquet', tree_shape='flat')

    return meta_path


def _detect_dataset_index_col(dataset_path):
    """Detect the expected index column from dataset metadata.

    Reads dataset metadata to determine the index column name.
    Returns None if metadata is missing or doesn't specify an index.
    """
    meta_path = smart_join(dataset_path, DATASET_META_FILENAME)
    if not smart_exists(meta_path):
        return None

    meta = json_read(meta_path)

    idx_type = meta.get('index_type')
    idx_level = meta.get('index_level')
    if idx_type == 'h3' and idx_level is not None:
        return f'h3_{int(idx_level):02d}'
    if idx_type == 'egi' and idx_level is not None:
        return f'egi{int(idx_level):02d}'

    return None


def _find_dataset_files(dataset_path, fmt):
    """Find data files in a dataset directory with hive-style fallback.

    Returns (data_files, fmt) tuple.
    """
    from .cliutils import list_dataset_files

    try:
        return list_dataset_files(dataset_path, fmt=fmt), fmt
    except FileNotFoundError:
        # Fallback: check for hive-style parquet structure
        hive_files = smart_glob(smart_join(dataset_path, '**/*.parquet'), recursive=True)
        if hive_files:
            return hive_files, 'parquet'
        raise GediDatabaseNotFoundError(f"No data files found in {dataset_path}")


def _load_dataset(path, columns=None, query=None, region=None, lazy=True, filters=None):
    """Internal: load from simplified dataset (H3 or EGI).

    Handles both eager and lazy loading, with query/region filtering.

    Parameters
    ----------
    path : str
        Path to the dataset directory or single file.
    columns : list, optional
        Columns to load.
    query : str, optional
        Pandas query string for filtering.
    region : GeoDataFrame or bbox, optional
        Spatial filter for clipping.
    lazy : bool
        If True, return Dask DataFrame. If False, return computed DataFrame.
    filters : list, optional
        PyArrow predicate pushdown filters (only for lazy=False + parquet).

    Returns
    -------
    dask GeoDataFrame or GeoDataFrame
    """
    from .cliutils import (detect_dataset_format, read_dataset_schema,
                           make_dataset_reader, _add_query_columns)

    # --- Eager mode ---
    if not lazy:
        # Single file
        if smart_exists(path) and not smart_isdir(path):
            ext = os.path.splitext(path)[1].lstrip('.').lower()
            fmt = ext if ext in ('parquet', 'feather', 'gpkg') else 'parquet'
            _, has_geo = read_dataset_schema(path, fmt)
            reader = make_dataset_reader(fmt, columns=columns, geo=has_geo)
            return reader(path)

        fmt = detect_dataset_format(path)
        data_files, fmt = _find_dataset_files(path, fmt)
        _, has_geo = read_dataset_schema(data_files[0], fmt)

        if fmt == 'parquet':
            kwargs = {}
            if columns:
                kwargs['columns'] = columns
            if filters:
                kwargs['filters'] = filters
            return _read_parquet_files(data_files, geo=has_geo, **kwargs)
        else:
            index_col = _detect_dataset_index_col(path)
            load_columns = columns
            if index_col and load_columns and index_col not in load_columns:
                load_columns = list(load_columns) + [index_col]

            reader = make_dataset_reader(fmt, columns=load_columns, geo=has_geo)
            dfs = [reader(f) for f in data_files]
            result = pd.concat(dfs)

            if index_col and result.index.name != index_col and index_col in result.columns:
                result = result.set_index(index_col)
            return result

    # --- Lazy mode ---
    fmt = detect_dataset_format(path)

    # Handle query-column expansion
    load_columns = columns
    query_only_cols = set()
    if query and columns:
        load_columns, query_only_cols = _add_query_columns(columns, query, path, fmt)

    data_files, fmt = _find_dataset_files(path, fmt)

    # Read schema from first file
    col_names, has_geometry = read_dataset_schema(data_files[0], fmt)

    # Detect expected index column from metadata
    index_col = _detect_dataset_index_col(path)

    load_cols = list(load_columns) if load_columns else None
    if load_cols:
        if has_geometry and 'geometry' not in load_cols:
            load_cols.append('geometry')
        if index_col and index_col not in load_cols and index_col in col_names:
            load_cols.append(index_col)

    # Build reader and metadata
    reader = make_dataset_reader(fmt, columns=load_cols, geo=has_geometry)
    _meta = reader(data_files[0])

    # Wrap reader to propagate storage credentials to Dask workers
    _scfg = None
    if is_remote_path(path):
        from .utils import _storage_options
        _scfg = dict(_storage_options)

    # Restore index for formats that don't preserve it (e.g. GPKG).
    # Crucially, do NOT restore when the file already produced a valid spatial
    # index — even if the sidecar's `index_level` says otherwise. Parquet/feather
    # preserve the pandas index in metadata; trusting a wrong sidecar over the
    # file-supplied index would silently demote h3_12 → h3_03 (the partition
    # column), and downstream `h3_to_parent(res=4)` would then attempt to find
    # an L4 parent of L3 cells (impossible). Defends against the cascading
    # sidecar-corruption class.
    import re as _re_idx
    _file_has_spatial_index = (
        _meta.index.name is not None and (
            _re_idx.match(r'^h3_\d{2}$', str(_meta.index.name))
            or _re_idx.match(r'^egi\d{2}$', str(_meta.index.name))
        )
    )
    needs_index_restore = (
        index_col
        and _meta.index.name != index_col
        and index_col in _meta.columns
        and not _file_has_spatial_index
    )
    if needs_index_restore:
        _meta = _meta.set_index(index_col)

        def read_and_set_index(f):
            _restore_storage_on_worker(_scfg)
            df = reader(f)
            return df.set_index(index_col)

        ddf = dask.dataframe.from_map(read_and_set_index, data_files, meta=_meta)
    else:
        if _scfg:
            _base_reader = reader
            def reader(f):
                _restore_storage_on_worker(_scfg)
                return _base_reader(f)

        ddf = dask.dataframe.from_map(reader, data_files, meta=_meta)

    if 'geometry' in ddf.columns:
        ddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')

    if query:
        ddf = ddf.query(query)
    if query_only_cols:
        keep = [c for c in ddf.columns if c not in query_only_cols]
        ddf = ddf[keep]
    if region is not None:
        ddf = ddf.clip(region)

    return ddf


def gh3_part_from_df(df):
    h3_cols = [col for col in df.columns if col.startswith('h3_')]
    return sorted(h3_cols)[0] if h3_cols else None

def gh3_reindex(df):
    h3_col = gh3_part_from_df(df)
    h3_id = df.index.name
    if h3_col is not None and h3_id is not None and h3_id < h3_col:
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
        active_cols = list(cols) if not isinstance(cols, str) else [cols]
        g = g[cols]
    elif callable(agg) or isinstance(agg, dict):
        # Callables and dicts handle column selection/naming themselves — pass everything.
        active_cols = [c for c in df.columns if c != h3col]
    else:
        # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
        filtered_cols = get_aggregatable_columns(df)
        active_cols = filtered_cols if filtered_cols else df.columns.tolist()
        if filtered_cols:
            g = g[filtered_cols]

    if callable(agg) and len(df) == 0:
        # pandas groupby.apply on an empty DataFrame does not call the function;
        # it returns an empty DataFrame with the *input* columns, which causes a
        # column mismatch when Dask validates map_partitions output against _meta.
        # Call the function directly with an empty DataFrame to infer the true schema.
        # Use df directly (preserves correct dtypes) — pd.DataFrame(columns=...) gives
        # object dtype, which breaks functions that call np.isfinite on the values.
        # Use active_cols (not g.obj.columns) — g.obj has all columns but apply only sees the selection.
        _typed = [c for c in active_cols if c in df.columns]
        _sample = df[_typed].iloc[0:0].copy() if _typed else pd.DataFrame(columns=active_cols)
        try:
            out = agg(_sample, **kwargs)
            out = out.iloc[0:0].copy()
            out.index = pd.Index([], name=h3col, dtype='object')
        except Exception:
            out = g.apply(agg, include_groups=False, **kwargs)
    elif callable(agg):
        out = g.apply(agg, include_groups=False, **kwargs)
    else:
        out = g.agg(agg)

    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ['_'.join(map(str, col)).strip() for col in out.columns.values]

    if isinstance(out.index, pd.MultiIndex):
        out.index = out.index.get_level_values(0)
    return out

def gh3_add_geometry(df):
    geo = [fix_h3_geometry(i) for i in df.index]
    gdf = gpd.GeoDataFrame(df, geometry=geo, crs=4326)
    return gdf

def _read_parquet_files(files, geo=True, **kwargs):
    """Read parquet file(s), handling remote paths correctly.

    PyArrow does not recognize http:// URIs natively. For remote paths,
    we use fsspec (via smart_open) to open files as file-like objects.
    """
    reader = gpd.read_parquet if geo else pd.read_parquet

    if isinstance(files, str):
        files = [files]

    remote = len(files) > 0 and is_remote_path(files[0])

    # Single file
    if len(files) == 1:
        if remote:
            with smart_open(files[0], 'rb') as fobj:
                return reader(fobj, **kwargs)
        return reader(files[0], **kwargs)

    # Multiple local files: pass list directly (PyArrow handles this)
    if not remote:
        return reader(files, **kwargs)

    # Multiple remote files: read each via fsspec, concat
    dfs = []
    for f in files:
        with smart_open(f, 'rb') as fobj:
            dfs.append(reader(fobj, **kwargs))
    return pd.concat(dfs)


_BBOX_STRATEGY_CACHE = {}


def _pick_bbox_strategy(sample_file):
    """Inspect ONE parquet file from the H3 db and pick the fastest read-time
    bbox-filter path supported by its encoding. Result is cached per-db.

    Returns
    -------
    (strategy, lat_col, lon_col) where strategy is one of:
      'point'        — GeoParquet point encoding (gpd.read_parquet(bbox=...) works directly)
      'coord_filter' — WKB encoding + L2A lat/lon columns present (use parquet
                       column-stats pushdown via filters=[(lat,...), (lon,...)])
      'fallback'     — neither available; caller must do full read + geometry.intersects clip

    Both fast paths are EXACT for point geometries: row groups whose stats
    don't overlap the bbox are pruned before decompression; within surviving
    row groups every row is evaluated by pyarrow during decode. No post-read
    clip needed for Point data (boundary-coincident shots are handled by the
    spillover filter in load_tile, not by clipping).

    The inspection is a single small read of GeoParquet metadata + the schema
    column list; cached by directory in ``_BBOX_STRATEGY_CACHE`` so subsequent
    calls within the same process don't repeat it.
    """
    import pyarrow.parquet as pq
    import json
    import re

    cache_key = os.path.dirname(sample_file)
    if cache_key in _BBOX_STRATEGY_CACHE:
        return _BBOX_STRATEGY_CACHE[cache_key]

    encoding = None
    try:
        md = pq.read_metadata(sample_file).metadata or {}
        gm = json.loads(md.get(b'geo', b'{}'))
        for _, gcol in gm.get('columns', {}).items():
            encoding = gcol.get('encoding')
            break
    except Exception:
        pass

    if encoding == 'point':
        result = ('point', None, None)
        _BBOX_STRATEGY_CACHE[cache_key] = result
        return result

    # WKB (or unknown geometry encoding): fall back on the canonical L2A
    # coordinate columns as filter predicates. The build path always carries
    # `lat_lowestmode` / `lon_lowestmode` (suffixed `_l2a` after product
    # variable expansion); they are the same lat/lon that the `geometry`
    # column is constructed from, so filtering on them is identical to
    # filtering on geometry for Point shots.
    lat_col = lon_col = None
    try:
        schema = pq.read_schema(sample_file)
        names = [f.name for f in schema]
        # Prefer L2A-suffixed; fall back to unsuffixed (older builds).
        for pat in (r'^lat_lowestmode_l2a$', r'^lat_lowestmode$'):
            for c in names:
                if re.match(pat, c):
                    lat_col = c
                    break
            if lat_col:
                break
        for pat in (r'^lon_lowestmode_l2a$', r'^lon_lowestmode$'):
            for c in names:
                if re.match(pat, c):
                    lon_col = c
                    break
            if lon_col:
                break
    except Exception:
        pass

    if lat_col and lon_col:
        result = ('coord_filter', lat_col, lon_col)
    else:
        result = ('fallback', None, None)
    _BBOX_STRATEGY_CACHE[cache_key] = result
    return result


def _read_parquet_bbox(path, *, bbox_4326, clip_box, columns, geo, strategy, lat_col, lon_col):
    """Single-file bbox-filtered parquet read, routed by `strategy`.

    All three paths return a DataFrame whose rows satisfy the bbox predicate
    EXACTLY (for Point geometries). The first two prune row groups at the
    parquet-stats layer so the peak working set is bounded by the
    bbox-clipped result; the fallback materializes the full column-projected
    file before clipping in memory.
    """
    if strategy == 'point':
        return gpd.read_parquet(path, bbox=bbox_4326, columns=columns)

    if strategy == 'coord_filter':
        x0, y0, x1, y1 = bbox_4326
        filt = [(lon_col, '>=', x0), (lon_col, '<=', x1),
                (lat_col, '>=', y0), (lat_col, '<=', y1)]
        # Pyarrow's `filters=` requires the predicate columns to be in the
        # read column list; append + drop them if the caller didn't ask for
        # them. The extra column is already on disk in the same row groups
        # we'd decode anyway, so the I/O cost is negligible.
        cols = list(columns) if columns else None
        extras = []
        if cols is not None:
            for c in (lat_col, lon_col):
                if c not in cols:
                    cols.append(c)
                    extras.append(c)
        reader = gpd.read_parquet if geo else pd.read_parquet
        df = reader(path, columns=cols, filters=filt)
        if extras and len(df) > 0:
            df = df.drop(columns=extras)
        return df

    # 'fallback' — full read + in-memory geometric clip. Last resort.
    df = gpd.read_parquet(path, columns=columns)
    if len(df) > 0:
        df = df[df.geometry.intersects(clip_box)]
    return df


def _restore_storage_on_worker(storage_cfg):
    """Restore storage credentials on Dask worker processes.

    Dask workers are separate processes that don't inherit the
    module-level ``_storage_options`` configured in the main process.
    This must be called at the start of any function that runs on a
    worker and needs remote filesystem access.
    """
    if not storage_cfg:
        return
    from .utils import _storage_options
    for protocol, opts in storage_cfg.items():
        if protocol not in _storage_options:
            _storage_options[protocol] = opts


# Pyarrow type strings stored by ``read_parquet_schema`` are mostly
# pandas-compatible as-is (``int64``, ``float32``, ``bool``, ``uint8``,
# …). The few names that differ between pyarrow and pandas dtype
# vocabularies map here; everything outside this table either round-
# trips directly or signals "fall back to a real parquet sample" via
# ``_meta_from_dtype_dict`` returning ``None``.
_PA_TO_PANDAS_DTYPE = {
    'double': 'float64',
    'float': 'float32',
    'halffloat': 'float16',
    'string': 'object',
    'large_string': 'object',
    'binary': 'object',
    'large_binary': 'object',
}


def _pa_dtype_to_pandas(s):
    """Translate a pyarrow dtype string into a pandas-compatible dtype.

    Returns ``None`` for types we cannot safely round-trip (list/struct/
    map/dictionary/extension), which signals the caller to fall back to
    sampling an actual parquet file.
    """
    s = (s or '').strip()
    if not s:
        return None
    if s in _PA_TO_PANDAS_DTYPE:
        return _PA_TO_PANDAS_DTYPE[s]
    if s.startswith('timestamp'):
        # timestamp[ns], timestamp[us, tz=UTC], etc. → pandas datetime64[ns]
        return 'datetime64[ns]'
    if s.startswith('date'):
        return 'datetime64[ns]'
    if s.startswith(('list', 'struct', 'map', 'dictionary', 'extension')):
        return None
    # int*/uint*/bool/decimal128/… are accepted by pandas as-is
    return s


def _meta_from_dtype_dict(col_dtypes, *, columns=None, part_col=None, index_name=None):
    """Construct an empty (Geo)DataFrame matching what
    :func:`gh3_load_hex` would return — built entirely from the cached
    ``h3_columns_dtypes`` build-log field, no parquet I/O.

    Returns ``None`` when the cache is missing/empty, contains a dtype
    the translator can't round-trip, or fails to cover a critical
    column the caller has explicitly requested — callers must fall
    back to ``gh3_load_hex(h3_dirs[0], …)`` in any of those cases.

    The ``index_name`` arg names the synthetic meta's index so the lazy
    ddf's metadata matches what each computed partition actually returns.
    Required: without it, ``ddf.index.name`` is ``None`` while every
    actual partition has a proper named index (``h3_12``). That mismatch
    is silent at load time but cascades into ``_detect_export_params``
    inferring the wrong ``index_level`` from the only h3 column present
    (the partition column), which then gets written into every simplified
    dataset's sidecar — and every later load of that sidecar destroys
    the real index on each partition via the "needs index restore" branch
    in ``_load_dataset``.

    "Critical column" coverage check:
      * ``shot_number`` is the universal GEDI shot identifier; every
        extraction / aggregation / audit pipeline relies on it. If the
        caller requested a column starting with ``shot_number`` but
        the cached dtype map doesn't carry it (legacy partition
        metadata, partial cache merge, etc.), we refuse to build the
        meta and force the caller through the sampling fallback —
        which DOES read it from a real partition. Building a
        shot_number-less meta from the cache would silently mis-shape
        the Dask graph for a downstream tool that's expecting it, so
        a fallback (one parquet sample read) is the safer trade.
    """
    if not col_dtypes:
        return None

    if columns is None:
        keep = list(col_dtypes.keys())
    else:
        # If shot_number was requested but the cache lacks it, drop
        # to the sampling path. shot_number is whitelisted as a
        # required identifier across every gedih3 pipeline; a
        # mis-typed or missing shot_number in the dask _meta would
        # silently break downstream joins/audits.
        requested_sn = [c for c in columns if str(c).startswith('shot_number')]
        cached_sn = [c for c in col_dtypes if str(c).startswith('shot_number')]
        if requested_sn and not cached_sn:
            return None
        keep = [c for c in columns if c in col_dtypes]

    # The h3 index column is stored as the pandas index in each parquet
    # (set_index('h3_<res>') before write), so the read-back partition has
    # it as the named index, NOT a column. Drop it from the column set
    # here and apply it as the index dtype below — otherwise the synthetic
    # meta carries h3_12 as both a column AND the (empty) index name,
    # while gh3_load_hex returns it as index only → Dask raises
    # "Missing: ['h3_12']" on the first compute.
    index_dtype = None
    if index_name and index_name in keep:
        index_dtype = _pa_dtype_to_pandas(col_dtypes[index_name])
        if index_dtype is None:
            return None
        keep = [c for c in keep if c != index_name]

    series = {}
    for c in keep:
        pd_dtype = _pa_dtype_to_pandas(col_dtypes[c])
        if pd_dtype is None:
            return None
        try:
            series[c] = pd.Series([], dtype=pd_dtype)
        except (TypeError, ValueError):
            return None

    df = pd.DataFrame(series)

    # gh3_load_hex normalizes tail columns to [part_col, year] order:
    # gpd.read_parquet infers hive partition columns in outer→inner path
    # order (h3_03 then year), and the explicit normalize step at the end
    # of gh3_load_hex guarantees this order for both geo and non-geo paths.
    # Mirror that canonical tail order here so from_map's meta matches.
    # Neither column is in h3_columns_dtypes (build records dtypes before
    # the partition split).
    if part_col and part_col not in df.columns:
        df[part_col] = pd.Series([], dtype='object')

    if 'year' not in df.columns:
        df['year'] = pd.Series([], dtype='int32')

    if 'geometry' in df.columns:
        df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)

    # Match the named index that the parquet reader produces at compute time.
    if index_name:
        try:
            df.index = pd.Index([], name=index_name, dtype=index_dtype) if index_dtype else pd.Index([], name=index_name)
        except (TypeError, ValueError):
            df.index = pd.Index([], name=index_name)

    return df


_YEAR_HIVE_RE = re.compile(r'year=(\d{4})')


def gh3_load_hex(d, part_col=None, _storage_cfg=None, **kwargs):
    _restore_storage_on_worker(_storage_cfg)
    files = smart_glob(smart_join(d, '**/*.parquet'), recursive=True)
    cols = kwargs.get('columns')
    use_geo = cols is None or 'geometry' in cols

    # Per-file read so we can attach the `year` hive partition column from
    # each file's path. pd.read_parquet on a LIST of files does NOT
    # reconstruct hive partition columns (only a directory read or
    # pyarrow.dataset would), so a list read would return data without
    # `year` while the synthetic Dask meta (built from h3_columns_dtypes)
    # always includes it — producing a "Missing: ['year']" mismatch on
    # every .compute(). Reading per file is the same I/O the list-read
    # would do internally; the only overhead is N small open() calls.
    parts = []
    for f in files:
        sub = _read_parquet_files([f], geo=use_geo, **kwargs)
        if 'year' not in sub.columns and sub.index.name != 'year':
            m = _YEAR_HIVE_RE.search(str(f))
            if m:
                sub['year'] = np.int32(m.group(1))
        parts.append(sub)

    if len(parts) == 0:
        df = _read_parquet_files(files, geo=use_geo, **kwargs)
    elif len(parts) == 1:
        df = parts[0]
    else:
        df = pd.concat(parts)
        if use_geo and not isinstance(df, gpd.GeoDataFrame) and 'geometry' in df.columns:
            df = gpd.GeoDataFrame(df, geometry='geometry', crs=4326)

    # Add partition column from hive-style directory name (e.g., 'h3_03=abc123')
    if part_col:
        part_id = os.path.basename(d.rstrip('/')).split('=')[-1]
        if part_col not in df.columns and df.index.name != part_col:
            df[part_col] = part_id

    # Normalize tail column order to [part_col, year] regardless of reader.
    # gpd.read_parquet infers hive columns from the path in outer→inner order
    # (h3_03 then year), while pd.read_parquet does not infer them at all and
    # the manual adds above produce the same order. _meta_from_dtype_dict relies
    # on this canonical order — keep them in sync.
    _tail = [c for c in [part_col, 'year'] if c and c in df.columns]
    if _tail:
        _other = [c for c in df.columns if c not in _tail]
        df = df[_other + _tail]

    return df

def _load_h3_database(columns=None, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR, from_map=True, filters=None):
    """Internal: load from H3 database (original gh3_load implementation)."""
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_index_level = gh3_read_meta("h3_resolution_level", gh3_root_dir=gh3_dir)
    h3_index_col = f"h3_{int(h3_index_level):02d}" if h3_index_level is not None else None
    h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir)

    h3_filter = {}
    out_cols = None
    if columns is not None:
        if h3_part_col not in columns:
            columns.append(h3_part_col)

        # Always include shot_number for observation-level identification
        available_cols = gh3_read_meta("h3_columns", gh3_root_dir=gh3_dir)
        sn_cols = [c for c in available_cols if c.startswith('shot_number')]
        for c in sn_cols:
            if c not in columns:
                columns.append(c)

        out_cols = columns.copy()

        if query is not None:
            q_cols = [col for col in available_cols if col in query]
            columns = list(set(columns + q_cols))

        h3_filter['columns'] = columns

    region_filters = None
    if region is not None:
        h3_ids = intersect_h3_geometries(region, h3_ids=h3_ids)
        region_filters = [(h3_part_col,'in',h3_ids)]

        if 'columns' in h3_filter:
            if 'geometry' not in h3_filter['columns']:
                h3_filter['columns'].append('geometry')

    # Combine the region partition filter (on h3_part_col, only meaningful for
    # the read_parquet branch) with the user-supplied pyarrow predicate filters
    # (on real data columns). Both forms are conjunctive lists of tuples, so an
    # AND-combination is plain concatenation.
    if region_filters is not None or filters is not None:
        h3_filter['filters'] = (region_filters or []) + (list(filters) if filters is not None else [])

    if from_map:
        if is_remote_path(gh3_dir) or region is not None:
            # For remote paths and spatial filters, construct paths directly from metadata
            # (avoids expensive directory listing over HTTP/S3)
            h3_ids = sorted(h3_ids)
            h3_dirs = [smart_join(gh3_dir, f"{h3_part_col}={hid}/") for hid in h3_ids]
        else:
            h3_dirs = smart_glob(smart_join(gh3_dir, f"{h3_part_col}=*/"))
            if not h3_dirs:
                h3_ids = sorted(h3_ids)
                h3_dirs = [smart_join(gh3_dir, f"{h3_part_col}={hid}/") for hid in h3_ids]
            else:
                h3_ids = [os.path.basename(i.rstrip('/')).replace(f'{h3_part_col}=', '') for i in h3_dirs]

        divs = h3_ids + h3_ids[-1:]

        # Remove partition column and filter from h3_filter (not in parquet files, derived from dir name)
        fm_filter = {k: v for k, v in h3_filter.items() if k != 'filters'}
        if 'columns' in fm_filter:
            fm_filter['columns'] = [c for c in fm_filter['columns'] if c != h3_part_col]

        # Re-attach the user's pyarrow predicate filters (on real data columns)
        # so they apply as per-file row-group pushdown during the from_map read.
        # The region partition filter is intentionally NOT re-added — it targets
        # h3_part_col, which lives in the directory name, not the parquet files,
        # and is already honored by the h3_dirs selection above.
        if filters is not None:
            fm_filter['filters'] = list(filters)

        # Pass storage credentials so Dask workers (separate processes) can
        # authenticate against remote filesystems.
        if is_remote_path(gh3_dir):
            from .utils import _storage_options
            fm_filter['_storage_cfg'] = dict(_storage_options)

        # Prefer the cached schema (zero parquet I/O) — falls back to
        # opening h3_dirs[0] when h3_columns_dtypes is missing (legacy
        # DB) or contains a dtype the translator can't round-trip.
        col_dtypes = gh3_read_meta("h3_columns_dtypes", gh3_root_dir=gh3_dir)
        _meta = _meta_from_dtype_dict(
            col_dtypes,
            columns=fm_filter.get('columns'),
            part_col=h3_part_col,
            index_name=h3_index_col,
        )
        if _meta is None:
            _meta = gh3_load_hex(h3_dirs[0], part_col=h3_part_col, **fm_filter)
        ddf = dask.dataframe.from_map(gh3_load_hex, h3_dirs, part_col=h3_part_col, **fm_filter, meta=_meta)
        if 'geometry' in ddf.columns:
            ddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')
    else:
        storage_kwargs = {}
        if is_remote_path(gh3_dir):
            from .utils import get_storage_options
            protocol = gh3_dir.split('://')[0]
            storage_kwargs['storage_options'] = get_storage_options(protocol)
        ddf = dask_geopandas.read_parquet(gh3_dir,
                                        calculate_divisions=False,
                                        split_row_groups=False,
                                        aggregate_files=False,
                                        gather_spatial_partitions=False,
                                        ignore_metadata_file=False,
                                        **storage_kwargs,
                                        **h3_filter)

        ddf[h3_part_col] = ddf[h3_part_col].astype(str)

    if query is not None:
        ddf = ddf.query(query)

    if region is not None and isinstance(ddf, dask_geopandas.GeoDataFrame):
        from shapely.geometry import box as shapely_box
        if isinstance(region, list):
            mask = gpd.GeoDataFrame(geometry=[shapely_box(*region)], crs=4326)
        elif isinstance(region, (gpd.GeoSeries, gpd.GeoDataFrame)):
            mask = region.to_crs(4326)
        else:
            mask = gpd.GeoDataFrame(geometry=[region], crs=4326)
        ddf = ddf.clip(mask)

    if query is not None and out_cols is not None:
        # Remove index column from selection (it's the index, not a column)
        out_cols = [c for c in out_cols if c != ddf.index.name]
        ddf = ddf[out_cols]

    return ddf


def gh3_load(source=None, *, columns=None, region=None, query=None,
             from_map=True, lazy=True, filters=None):
    """Load H3-indexed GEDI data from any source.

    Auto-detects whether the source is an H3 database, simplified dataset,
    or parquet directory and loads accordingly.

    Parameters
    ----------
    source : str, optional
        Path to data source (H3 database, simplified dataset, or parquet dir).
        If None, falls back to default H3 directory.
    columns : list, optional
        Columns to load.
    region : GeoDataFrame or bbox, optional
        Spatial filter.
    query : str, optional
        Pandas query string for filtering.
    from_map : bool
        Use from_map loading for H3 databases (default True).
    lazy : bool
        If True (default), return Dask DataFrame. If False, return computed
        pandas DataFrame.
    filters : list, optional
        PyArrow predicate pushdown filters (conjunctive list of
        ``(column, op, value)`` tuples), applied as per-file row-group pushdown
        during the read. Works for H3 databases and simplified parquet datasets,
        and combines (AND) with ``region`` when both are given.

    Returns
    -------
    dask GeoDataFrame or GeoDataFrame
        Loaded data (lazy by default, eager if lazy=False).

    Raises
    ------
    GediValidationError
        If the source is an EGI-indexed dataset (use ``egi_load()`` instead).
    GediDatabaseNotFoundError
        If no valid data source is found.

    Examples
    --------
    >>> import gedih3.gh3driver as gh3
    >>> ddf = gh3.gh3_load(
    ...     source='/path/to/h3_database',
    ...     columns=['agbd_l4a', 'rh_098_l2a'],
    ...     region='region.shp',
    ... )
    >>> ddf.compute().head()
    """
    path, info = _detect_source(source)
    columns = _resolve_columns(columns, path, info)

    if info.get('index_type') == 'egi':
        raise GediValidationError(
            f"Source '{path}' is an EGI-indexed dataset. Use egi_load() instead."
        )

    # Normalize region once so all downstream code (intersect_h3_geometries,
    # the dask-clip path, _load_dataset) sees a list / GeoDataFrame / shapely
    # geometry — mirroring what the CLI parse_region() produces. The
    # docstring example advertises ``region='region.shp'``; without this
    # normalization that example raises a confusing error 200 lines later.
    if isinstance(region, str):
        from .cliutils import parse_region
        region = parse_region(region)

    if info['source_type'] == 'h3_database':
        ddf = _load_h3_database(columns=columns, region=region, query=query,
                                gh3_dir=path, from_map=from_map, filters=filters)
    else:
        ddf = _load_dataset(path, columns=columns, query=query, region=region,
                            lazy=True, filters=filters)

    if not lazy:
        return dask_safe_collect(ddf)
    return ddf

def gh3_aggregate(gh3_df, target_res=5, agg='mean', columns=None, query=None, add_geometry=True, repartition=False, partition_level=None, **kwargs):
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
    partition_level : int, optional
        Explicit H3 partition level. Used as fallback when the DataFrame
        lacks h3_XX columns (e.g., loaded from a simplified dataset).
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        H3-indexed aggregated data.

    Raises
    ------
    H3ValidationError
        If ``target_res`` is not a valid H3 resolution (0–15).
    GediAggregationError
        If spatial aggregation fails.
    """
    # Infer output schema from the empty _meta DataFrame (no data read).
    # gh3_aggregate_func handles empty DataFrames correctly, returning the
    # right column names (including multi-agg suffixes) and index name.
    _meta = gh3_aggregate_func(df=gh3_df._meta, res=target_res, agg=agg, cols=columns, **kwargs)

    if query is not None:
        gh3_df = gh3_df.query(query)

    h3part = gh3_part_from_df(gh3_reindex(gh3_df))
    # Fallback: use explicit partition_level when no h3_XX columns detected
    if h3part is None and partition_level is not None and partition_level < target_res:
        h3part = f"h3_{partition_level:02d}"
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
    # gh3_aggregate_func returns data already indexed by h3agg (groupby result).
    # No set_index shuffle needed — the index is already correct.

    if add_geometry:
        _gmeta = agg_df._meta.copy()
        _gmeta['geometry'] = gpd.GeoSeries([], crs=4326)
        _gmeta = gpd.GeoDataFrame(_gmeta, geometry='geometry', crs=4326)
        agg_df = agg_df.map_partitions(gh3_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    if repartition and h3part is not None:
        h3part_res = int(h3part.split('_')[1])

        # Add partition column via map_partitions (no shuffle).
        # Each Dask partition already contains data from a single H3 parent
        # cell (from from_map loading), so part_col values are uniform within
        # each partition. Export uses part_col as a data column for file naming.
        def add_h3_parent(df, parent_col, parent_res):
            df = df.copy()
            df[parent_col] = [h3.cell_to_parent(x, parent_res) for x in df.index]
            return df

        _part_meta = agg_df._meta.copy()
        _part_meta[h3part] = ''

        agg_df = agg_df.map_partitions(add_h3_parent, parent_col=h3part, parent_res=h3part_res, meta=_part_meta)

    agg_df.index = agg_df.index.astype(str)
    return agg_df


def gh3_export_part(df, odir, fmt='parquet', is_file_path=False, part_col=None,
                    group_by_partition=False, naming_partition_level=None):
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
    group_by_partition : bool
        If True and part_col is specified, group data by partition column
        and write separate files for each unique partition ID within this
        Dask partition. Use this after shuffling data by partition column
        (via set_index) to ensure each unique partition ID is in exactly
        one Dask partition, avoiding file collision issues.
    naming_partition_level : int, optional
        H3 resolution level for deriving file names via cell_to_parent.
        Used when no partition column is available (e.g., aggregated data).

    Returns
    -------
    str
        Output file path(s). Comma-separated if multiple files written.
    """
    if df.empty:
        return ''

    check_nan_only_columns(df, context='Export partition: ')

    # When is_file_path=True (merge mode), `odir` is actually the user's
    # destination FILE path — creating it as a directory here turns the
    # final AtomicFileWriter.os.replace() into "Is a directory". The parent
    # dir is created by AtomicFileWriter.__enter__ anyway, so this is safe
    # to skip in that case.
    if not is_file_path:
        os.makedirs(odir, exist_ok=True)

    # Determine actual partition column
    actual_part_col = part_col
    if not actual_part_col:
        # Check for H3 partition columns
        h3_cols = [col for col in df.columns if col.startswith('h3_')]
        if h3_cols:
            actual_part_col = sorted(h3_cols)[0]
        else:
            # Check for EGI columns
            egi_cols = [col for col in df.columns if str(col).startswith('egi')]
            if egi_cols:
                actual_part_col = sorted(egi_cols)[0]

    # Handle grouped export (multiple output files per Dask partition)
    # After shuffle (set_index), each unique partition ID is in exactly one Dask
    # partition, so files won't be written by multiple workers. However, a single
    # Dask partition may contain multiple partition IDs that need separate files.
    if group_by_partition and actual_part_col and actual_part_col in df.columns:
        unique_parts = df[actual_part_col].unique()
        output_paths = []
        for part_id in unique_parts:
            part_df = df[df[actual_part_col] == part_id]
            oname = str(part_id)
            opath = smart_join(odir, f"{oname}.{fmt}")
            _write_dataframe(part_df, opath, fmt)
            output_paths.append(opath)
        return ','.join(output_paths)

    # Single file export (no grouping)
    if is_file_path:
        odir = odir.rstrip('/')
        # Append the format extension only if the user didn't already supply
        # any extension. Respects equivalences like `.h5` for fmt=hdf5 or
        # `.json` for fmt=geojson without rewriting them to `.h5.hdf5` etc.
        # User-typed extensions are taken at face value — the writer dispatch
        # below keys off `fmt`, not the path suffix.
        ext = os.path.splitext(odir)[1]
        opath = odir if ext else f"{odir}.{fmt}"
    else:
        # Determine output filename from partition ID
        oname = None

        # 1. Try partition column (raw data case)
        if actual_part_col and actual_part_col in df.columns:
            oname = str(df[actual_part_col].iloc[0])

        # 2. Try deriving from H3 index via cell_to_parent (aggregated data case)
        if not oname and naming_partition_level is not None and df.index.name:
            if str(df.index.name).startswith('h3_'):
                import h3
                oname = h3.cell_to_parent(str(df.index[0]), naming_partition_level)

        # 3. Fallback to index value
        if not oname and df.index.name:
            if str(df.index.name).startswith('h3_') or str(df.index.name).startswith('egi'):
                oname = str(df.index[0])

        # 4. Generic fallback
        if not oname:
            oname = f"part_{hash(df.index[0]) % 10000:04d}"

        opath = smart_join(odir, f"{oname}.{fmt}")

    _write_dataframe(df, opath, fmt)
    return opath


def _write_dataframe(df, opath, fmt):
    """Write a DataFrame to file in the specified format.

    Single-file formats (parquet/feather/csv/txt/h5) write through
    :class:`AtomicFileWriter` so a worker SIGKILL or disk-full mid-write
    does not leave a partial file at the final path. The
    geopandas-backed formats (geojson/gpkg/shp) bypass the atomic wrap
    because :meth:`GeoDataFrame.to_file` infers the OGR driver from the
    file extension and shapefile in particular emits multiple sidecars
    that a single tmp+rename cannot cover.
    """
    if fmt in ('geojson', 'gpkg', 'shp'):
        if not isinstance(df, gpd.GeoDataFrame):
            raise GediProcessingError(f"Cannot export non-GeoDataFrame to {fmt}")
        df.to_file(opath)
        return

    if is_parquet(opath):
        # Verify+retry around parquet writes — catches the GPFS/transient-IO
        # class where pyarrow commits a file whose data pages are corrupt
        # (footer intact, body bad). A plain AtomicFileWriter cannot detect it.
        atomic_parquet_write(df, opath, compression='zstd')
        return

    with AtomicFileWriter(opath) as tmp:
        if fmt == 'feather':
            df.to_feather(tmp)
        elif fmt == 'txt':
            df.to_csv(tmp, sep='\t')
        elif fmt == 'csv':
            df.to_csv(tmp)
        elif fmt in ('h5', 'hdf5'):
            df.to_hdf(tmp, key='GEDI', mode='w')
        else:
            raise GediProcessingError(f"Unsupported export format: {fmt}")


# ============================================================================
# Export API
# ============================================================================


def _detect_export_params(ddf, index_type=None):
    """
    Auto-detect export parameters from a Dask DataFrame.

    Inspects the DataFrame's index and columns to determine the spatial index
    type, partition column, and index level.

    Parameters
    ----------
    ddf : dask DataFrame or GeoDataFrame
        The data to export
    index_type : str, optional
        Override auto-detection: 'h3' or 'egi'. If None, auto-detect.

    Returns
    -------
    tuple
        (index_type, part_col, index_level, group_by_partition)
        - index_type: 'h3', 'egi', or None
        - part_col: partition column name (e.g. 'h3_03', 'egi12')
        - index_level: spatial index resolution level (int or None)
        - group_by_partition: whether to use group_by_partition in export
    """
    meta = ddf._meta if hasattr(ddf, '_meta') else ddf

    # Auto-detect index type if not provided
    if index_type is None:
        index_type = get_spatial_index_type(meta)

    if index_type == 'egi':
        import re
        # Find EGI partition column (coarsest = highest level number)
        egi_cols = sorted(
            [c for c in meta.columns if re.match(r'^egi\d{2}$', str(c))],
            key=lambda c: int(str(c).replace('egi', ''))
        )
        if egi_cols:
            part_col = egi_cols[-1]  # coarsest = highest level = partition
        else:
            part_col = None

        # Index level from index name
        idx_name = str(meta.index.name) if meta.index.name else ''
        if idx_name.startswith('egi'):
            index_level = int(idx_name.replace('egi', ''))
        elif egi_cols:
            # Finest EGI column
            index_level = int(str(egi_cols[0]).replace('egi', ''))
        else:
            index_level = None

        # EGI data after shuffle needs group_by_partition
        group_by_partition = True

    elif index_type == 'h3':
        import re
        # Find H3 partition column (coarsest = lowest level number)
        h3_cols = sorted(
            [c for c in meta.columns if re.match(r'^h3_\d{2}$', c)]
        )
        if h3_cols:
            part_col = h3_cols[0]  # coarsest = lowest level = partition
        else:
            part_col = None

        # Index level from index name
        idx_name = str(meta.index.name) if meta.index.name else ''
        if idx_name.startswith('h3_'):
            index_level = int(idx_name.replace('h3_', ''))
        elif h3_cols:
            index_level = int(h3_cols[-1].replace('h3_', ''))
        else:
            index_level = None

        group_by_partition = False

    else:
        part_col = None
        index_level = None
        group_by_partition = False

    return index_type, part_col, index_level, group_by_partition


def gh3_export(ddf, output, fmt='parquet', merge=False,
               show_progress=True, drop_internal=False,
               write_metadata=True, source_database=None,
               tool=None, h3_partition_level=None, **metadata_kwargs):
    """
    Export a Dask DataFrame to simplified flat files with metadata.

    This is the high-level export function that encapsulates the full export
    pipeline: persist, write partition files, and write dataset metadata.
    It replaces the boilerplate pattern of
    map_partitions + persist + progress + gh3_write_dataset_meta.

    Parameters
    ----------
    ddf : dask DataFrame or GeoDataFrame
        Data to export. Should already be persisted if it represents
        an expensive computation (e.g., aggregation result).
    output : str
        Output directory path
    fmt : str
        Output format ('parquet', 'feather', 'gpkg', etc.)
    merge : bool
        If True, compute and write a single merged file instead of
        per-partition files.
    show_progress : bool
        If True and a Dask distributed client is available, show progress bar.
    drop_internal : bool
        If True, drop internal columns (h3_XX, egiXX, _egi_x/y, shot_number*)
        before export. Default False — internal columns are kept so downstream
        tools can join on shot_number or spatial indexes.
    write_metadata : bool
        If True, write dataset metadata file.
    source_database : str, optional
        Path to source H3 database (recorded in metadata).
    tool : str, optional
        Name of the tool creating this dataset (recorded in metadata).
    h3_partition_level : int, optional
        H3 resolution level to use for naming output files. When provided,
        files are named by the parent cell at this level (via h3.cell_to_parent).
        Useful for aggregated data where the original partition column was lost.
        If None, auto-detected from source_database metadata when available.
    **metadata_kwargs
        Additional key-value pairs to include in the dataset metadata.
        Common keys: query_filter, aggregation, egi_index_level,
        egi_partition_level, h3_partition_level, image_source, etc.

    Returns
    -------
    list of str
        Paths to output files created.

    Examples
    --------
    >>> import gedih3.gh3driver as gh3
    >>> ddf = gh3.gh3_load(source='/db', columns=['agbd_l4a'], region='roi.shp')
    >>> gh3.gh3_export(ddf, '/tmp/test_export/')
    >>>
    >>> # Merged export
    >>> gh3.gh3_export(ddf, '/tmp/merged/', merge=True)
    >>>
    >>> # With metadata
    >>> gh3.gh3_export(ddf, '/tmp/out/', source_database='/db', tool='my_script',
    ...               query_filter='quality_flag == 1')
    """
    from .cliutils import is_internal_column

    # When merge=True the output is a single file path (gh3_export_part runs
    # with is_file_path=True); only its parent dir should be created. The
    # legacy unconditional makedirs(output) would turn that file path into a
    # directory and the subsequent AtomicFileWriter would fail with
    # "Is a directory" on the os.replace.
    if merge:
        parent = os.path.dirname(os.path.abspath(output)) or '.'
        os.makedirs(parent, exist_ok=True)
    else:
        os.makedirs(output, exist_ok=True)

    # Auto-detect spatial index parameters
    index_type, part_col, index_level, group_by_partition = _detect_export_params(ddf)

    # Choose the right export function based on index type
    if index_type == 'egi':
        export_func = egi_export_part
    else:
        export_func = gh3_export_part

    # Drop internal columns if requested (but preserve partition column for naming)
    if drop_internal:
        drop_cols = [c for c in ddf.columns if is_internal_column(c) and c != part_col]
        if drop_cols:
            ddf = ddf.drop(columns=drop_cols)

    # Determine naming partition level for H3 data (for aggregated data without partition column)
    naming_partition_level = None
    if index_type == 'h3' and part_col is None:
        if h3_partition_level is not None:
            naming_partition_level = h3_partition_level
        elif source_database:
            try:
                naming_partition_level = gh3_read_meta("h3_partition_level", gh3_root_dir=source_database)
            except Exception:
                pass

    # Export data
    if merge:
        # Driver-side concat instead of the optimizer's cluster-side collapse
        # (RepartitionToFewer(1) wedges on tunneled meshes past ~1500 parts).
        result_df = dask_safe_collect(ddf, show_progress=show_progress)
        opath = export_func(result_df, odir=output, fmt=fmt, is_file_path=True)
        ofiles = [opath] if opath else []
    else:
        import pandas as pd

        # Build export kwargs based on index type
        # egi_export_part handles splitting internally; gh3_export_part uses part_col/group_by_partition
        if index_type == 'egi':
            egi_partition_level = int(part_col.replace('egi', '')) if part_col else 12
            export_kwargs = dict(odir=output, fmt=fmt, partition_level=egi_partition_level)
        else:
            export_kwargs = dict(odir=output, fmt=fmt, part_col=part_col,
                                 group_by_partition=group_by_partition,
                                 naming_partition_level=naming_partition_level)

        write_task = ddf.map_partitions(
            export_func, **export_kwargs, meta=pd.Series(dtype=str)
        )

        # Wait for the per-partition writes (side effect only) and propagate
        # any worker exceptions, without going through ``.compute()`` — that
        # would trigger the optimizer's RepartitionToFewer step which wedges
        # on tunneled multi-node clusters past ~1500 partitions in dask
        # >= 2025.2. dask_safe_wait persists + waits + checks futures_of
        # for errors; same semantics, no fan-in collect step.
        write_task = write_task.persist()
        dask_safe_wait(write_task, show_progress=show_progress)

        ofiles = smart_glob(smart_join(output, f'*.{fmt}'))

    if not ofiles:
        raise GediProcessingError("No output files were created.")

    # Write dataset metadata.
    # Skip in merge mode: the output is a single self-contained file, not a
    # multi-file dataset directory. gh3_write_dataset_meta would try to drop
    # `gedih3_dataset.json` *inside* the output path (treating it as a dir)
    # and the downstream tools that consume the sidecar all look for it
    # inside a directory anyway, so a sibling file wouldn't be picked up.
    # The user explicitly chose -m for portability; no sidecar to manage.
    if write_metadata and not merge:
        columns = list(ddf.columns)
        # Forward h3_partition_level to metadata (it's a named param, not in **metadata_kwargs)
        if h3_partition_level is not None:
            metadata_kwargs.setdefault('h3_partition_level', h3_partition_level)
        gh3_write_dataset_meta(
            opath=output,
            index_type=index_type or 'unknown',
            index_level=index_level,
            columns=columns,
            source_database=source_database,
            tool=tool,
            file_format=fmt,
            **metadata_kwargs
        )

    return ofiles


# ============================================================================
# EGI (EASE Grid Index) Support
# ============================================================================
# The following functions provide square-pixel indexing using EASE-Grid 2.0
# (EPSG:6933) for GEDI L4B-compatible outputs.


def _prepare_egi_loading(region, gh3_dir, partition_level=12):
    """
    Prepare EGI↔H3 intersection for direct loading.

    This is the setup step for egi_load().

    Parameters
    ----------
    region : GeoDataFrame, list, or None
        Region filter. Can be a GeoDataFrame, a bbox list [W, S, E, N], or None.
    gh3_dir : str
        Path to H3 database directory
    partition_level : int
        EGI level for output partitioning (1-12, default=12). When < 12, each
        level-12 outer tile is expanded into its level-N children via get_children(),
        so each Dask partition corresponds to one level-N tile.

    Returns
    -------
    tuple
        (egi_tiles, egi_to_h3, h3_part_col, region_gdf) for use in tile loading.
        region_gdf is the region as GeoDataFrame (for clipping).
    """
    from . import egi
    from .h3utils import h3_parts_to_gdf
    from shapely.geometry import box

    # Get H3 partition info
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir)

    # Convert region to GeoDataFrame if needed
    region_gdf = None
    if region is not None:
        if isinstance(region, (list, tuple)):
            # bbox: [W, S, E, N] -> GeoDataFrame
            region_gdf = gpd.GeoDataFrame(
                geometry=[box(*region)],
                crs=4326
            )
        elif isinstance(region, gpd.GeoDataFrame):
            region_gdf = region
        elif isinstance(region, gpd.GeoSeries):
            region_gdf = gpd.GeoDataFrame(geometry=region)
        else:
            raise GediValidationError(f"region must be GeoDataFrame, bbox list, or None. Got {type(region)}")

    # Get level-12 outer EGI tiles for region
    egi_tiles = egi.aoi_tiles(region_gdf)
    if len(egi_tiles) == 0:
        raise GediSpatialError("No EGI tiles found for the specified region")

    # Expand to finer partition_level by subdividing each level-12 tile
    if partition_level < egi.OUTER_LEVEL:
        all_children = []
        for tile_hash in egi_tiles.index:
            all_children.extend(egi.get_children(tile_hash, children_level=partition_level))
        egi_tiles = egi.to_geodataframe(
            np.array(all_children, dtype=np.uint64), return_polygons=True
        )
        # Drop degenerate edge-of-grid tiles (clamped to zero area by check_crs_limits)
        egi_tiles = egi_tiles[egi_tiles.geometry.is_valid & (egi_tiles.geometry.area > 0)]

    # Get H3 partitions as GeoDataFrame
    h3_gdf = h3_parts_to_gdf(h3_ids)

    # Compute EGI → H3 intersection
    egi_to_h3 = egi.egi_h3_intersection(egi_tiles, h3_gdf)
    if not egi_to_h3:
        raise GediSpatialError("No H3 partitions intersect the EGI tiles")

    return egi_tiles, egi_to_h3, h3_part_col, region_gdf


def _load_egi_tile_from_h3(egi_bbox, h3_list, gh3_dir, h3_part_col, load_cols,
                            query, index_level, partition_level, set_index=True,
                            tile_egi_id=None,
                            bbox_strategy='fallback', bbox_lat_col=None, bbox_lon_col=None):
    """
    Load data for a single EGI tile from its intersecting H3 partitions.

    Streams one H3 partition at a time and reduces it (bbox clip → query →
    EGI indexing → spillover filter) before moving on to the next, then
    concatenates the reduced results. This caps peak per-task memory at
    ~one H3 partition's raw size plus the (much smaller) reduced output,
    independent of how many H3 partitions are in ``h3_list``. Without the
    streaming, the ring-1 expansion in ``egi_h3_intersection`` would load
    up to 7× more H3 partitions in parallel for each EGI tile and overflow
    20 GB workers on dense tropical L12 tiles (production observation:
    ``KilledWorker`` after 6 retries, ~1,500 tiles unwritten).

    Parameters
    ----------
    egi_bbox : tuple
        Bounding box (minx, miny, maxx, maxy) for the EGI tile in EPSG:6933
    h3_list : list
        List of H3 partition IDs that intersect this EGI tile (after the
        ring-1 expansion in ``egi_h3_intersection``).
    gh3_dir : str
        Path to H3 database directory
    h3_part_col : str
        H3 partition column name (e.g., 'h3_03')
    load_cols : list or None
        Columns to load
    query : str or None
        Pandas query string for filtering
    index_level : int
        EGI resolution level for fine indexing
    partition_level : int
        EGI level for partitioning
    set_index : bool
        If True, set EGI index column as DataFrame index (avoids later shuffle)
    tile_egi_id : int or np.uint64, optional
        If provided, rows whose ``egi_part_col`` doesn't match are dropped
        per H3 partition (before concat). This is the spillover filter that
        prevents the boundary-edge race where two neighbor tasks both write
        to the same canonical filename (last-writer-wins). When ``None``,
        no filter is applied (legacy behavior).

    Returns
    -------
    DataFrame or GeoDataFrame
        EGI-indexed data for this tile.
    """
    from gedih3 import egi as egi_mod
    from pyproj import Transformer
    from shapely.geometry import box

    egi_index_col = egi_mod.egi_col_name(index_level)
    egi_part_col = egi_mod.egi_col_name(partition_level)

    # Transform EGI bbox from EPSG:6933 to WGS84 for H3 data filtering.
    # EPSG:6933 is Lambert Cylindrical Equal Area, so an axis-aligned
    # rectangle in 6933 maps to an axis-aligned rectangle in 4326 (corner
    # transform is exact, no curvature loss).
    transformer = Transformer.from_crs('EPSG:6933', 'EPSG:4326', always_xy=True)
    minx, miny = transformer.transform(egi_bbox[0], egi_bbox[1])
    maxx, maxy = transformer.transform(egi_bbox[2], egi_bbox[3])
    wgs84_bbox = (minx, miny, maxx, maxy)
    clip_box = box(*wgs84_bbox)

    tile_uid = np.uint64(tile_egi_id) if tile_egi_id is not None else None

    def _reduce_one_h3(h3_id):
        """Load a single H3 partition, clip+filter, return reduced df or None.

        Inner loop streams year files (one parquet per year) one at a time
        and applies the full reduction pipeline (bbox clip → query → EGI
        indexing → spillover filter) before moving on, so the working set
        is bounded by one year file (~1 GB raw decompressed) instead of
        the full H3 partition (~5 GB). The reduced per-year chunks are
        small (clipped to the tile's geographic extent + filtered by
        partition column = only this tile's rows), so concatenating them
        at the end stays under a few hundred MB.

        Required because the H3 v3 database files use WKB encoding with a
        file-level bbox in metadata but no per-row covering-bbox column,
        which means ``gpd.read_parquet(bbox=...)`` always raises
        ``ValueError: Specifying 'bbox' not supported for this Parquet
        file`` and we fall through to a full read every time. Reading
        all year files together into one pyarrow buffer caused
        ``KilledWorker`` on dense high-latitude tiles where the orbit
        turnaround clusters >25M shots into a single L12 cell.
        """
        h3_path = smart_join(gh3_dir, f"{h3_part_col}={h3_id}")
        parquet_files = smart_glob(smart_join(h3_path, '*.parquet'))
        if not parquet_files:
            parquet_files = smart_glob(smart_join(h3_path, '**/*.parquet'), recursive=True)
        if not parquet_files:
            return None

        sub_chunks = []
        for pf in parquet_files:
            # One year file at a time. Encoding-aware routing: avoids the
            # try/except cost when we already know bbox-pushdown won't work
            # on this file's encoding, and uses parquet column-stats pushdown
            # (via `filters=`) on the lat/lon columns when the geometry is
            # plain WKB. Final memory bound is ~the bbox-clipped result, not
            # the full file.
            year_df = _read_parquet_bbox(
                pf, bbox_4326=wgs84_bbox, clip_box=clip_box,
                columns=load_cols, geo=True,
                strategy=bbox_strategy, lat_col=bbox_lat_col, lon_col=bbox_lon_col,
            )
            if len(year_df) == 0:
                continue

            if query:
                year_df = year_df.query(query).copy()
                if len(year_df) == 0:
                    continue

            # Compute EGI index + partition columns at the smallest possible
            # working-set size (one year, already bbox-clipped + query-filtered).
            year_df = egi_mod.egi_dataframe_vectorized(year_df, level=index_level, set_index=False)
            if partition_level == index_level:
                year_df[egi_part_col] = year_df[egi_index_col]
            else:
                year_df[egi_part_col] = egi_mod.to_parent(year_df[egi_index_col].values, partition_level)

            # Spillover filter per year file (same rationale as the original
            # per-H3 filter, applied earlier in the pipeline).
            if tile_uid is not None:
                year_df = year_df[year_df[egi_part_col].values == tile_uid]
                if len(year_df) == 0:
                    continue

            sub_chunks.append(year_df)

        if not sub_chunks:
            return None
        return pd.concat(sub_chunks, ignore_index=True) if len(sub_chunks) > 1 else sub_chunks[0]

    # Stream H3 partitions; only the reduced (post-filter) chunks accumulate.
    chunks = []
    for h3_id in h3_list:
        c = _reduce_one_h3(h3_id)
        if c is not None:
            chunks.append(c)

    if not chunks:
        # Return empty DataFrame with correct structure
        empty = pd.DataFrame(columns=load_cols or [])
        empty[egi_index_col] = pd.Series([], dtype=np.uint64)
        empty[egi_part_col] = pd.Series([], dtype=np.uint64)
        if set_index:
            empty = empty.set_index(egi_index_col)
        return empty

    df = pd.concat(chunks, ignore_index=True)

    # Set EGI index column as DataFrame index BEFORE reordering columns
    if set_index:
        df = df.set_index(egi_index_col)

    # Reorder columns: data cols, partition col, geometry last
    if 'geometry' in df.columns:
        special_cols = {'geometry', egi_part_col}
        if not set_index:
            special_cols.add(egi_index_col)
        data_cols = [c for c in df.columns if c not in special_cols]
        cols = data_cols + [egi_part_col, 'geometry']
        cols = [c for c in cols if c in df.columns]
        df = df[cols]

    return df


def _find_parquet_file(gh3_dir):
    """
    Find a parquet file in the H3 database for schema inspection.

    Searches through H3 partition directories to find one with parquet files.
    Handles nested hive structures (e.g., h3_03=xxx/year=yyyy/*.parquet).
    """
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_dirs = smart_glob(smart_join(gh3_dir, f"{h3_part_col}=*/"))

    if not h3_dirs:
        raise GediDatabaseNotFoundError(f"No H3 partition directories found in {gh3_dir}")

    # Find a directory that actually has parquet files (search recursively)
    for h3_dir_path in h3_dirs:
        # Try direct children first, then recursive
        parquet_files = smart_glob(smart_join(h3_dir_path, '*.parquet'))
        if not parquet_files:
            parquet_files = smart_glob(smart_join(h3_dir_path, '**/*.parquet'), recursive=True)
        if parquet_files:
            return parquet_files[0]

    raise GediValidationError(f"No parquet files found in any H3 partition directory in {gh3_dir}")


def _get_schema_columns(load_cols, gh3_dir, exclude_geometry=False):
    """
    Get schema and columns from H3 database parquet files.

    This is shared logic used by EGI metadata building functions.

    Parameters
    ----------
    load_cols : list or None
        Columns to load, or None for all columns
    gh3_dir : str
        Path to H3 database directory
    exclude_geometry : bool
        If True, exclude geometry column from result

    Returns
    -------
    tuple
        (schema, meta_cols) where schema is pyarrow schema and meta_cols is list of column names
    """
    import pyarrow.parquet as pq

    # Get schema from a parquet file in database
    parquet_file = _find_parquet_file(gh3_dir)
    if is_remote_path(parquet_file):
        with smart_open(parquet_file, 'rb') as fobj:
            schema = pq.read_schema(fobj)
    else:
        schema = pq.read_schema(parquet_file, memory_map=True)
    schema_cols = schema.names

    # Determine columns for metadata
    if load_cols is not None:
        meta_cols = [c for c in load_cols if c in schema_cols]
        if exclude_geometry:
            meta_cols = [c for c in meta_cols if c != 'geometry']
    else:
        meta_cols = [c for c in schema_cols if c != 'geometry']

    return schema, meta_cols


def _build_meta_dict_from_schema(schema, columns):
    """
    Build empty DataFrame column dict with correct dtypes from schema.

    Parameters
    ----------
    schema : pyarrow.Schema
        Schema from parquet file
    columns : list
        Column names to include

    Returns
    -------
    dict
        Dictionary mapping column names to empty pandas arrays with correct dtypes
    """
    meta_dict = {}
    for col in columns:
        if col == 'geometry':
            continue
        field_idx = schema.get_field_index(col)
        if field_idx >= 0:
            pa_type = schema.field(field_idx).type
            # Convert PyArrow type to pandas dtype
            try:
                meta_dict[col] = pd.array([], dtype=pa_type.to_pandas_dtype())
            except (NotImplementedError, TypeError):
                meta_dict[col] = pd.array([], dtype=object)
    return meta_dict


def _build_egi_load_meta(load_cols, gh3_dir, index_level, partition_level, include_geometry=True, set_index=True):
    """
    Build metadata for egi_load() without loading actual data.

    This avoids the metadata inference error when sample data is empty.
    """
    from . import egi

    egi_index_col = egi.egi_col_name(index_level)
    egi_part_col = egi.egi_col_name(partition_level)

    # Get schema and columns from database
    schema, meta_cols = _get_schema_columns(load_cols, gh3_dir, exclude_geometry=False)

    # Build empty DataFrame with correct dtypes
    meta_dict = _build_meta_dict_from_schema(schema, meta_cols)
    _meta = pd.DataFrame(meta_dict)

    # Add EGI columns
    _meta[egi_index_col] = pd.array([], dtype=np.uint64)
    _meta[egi_part_col] = pd.array([], dtype=np.uint64)

    # Add geometry column if requested
    if include_geometry:
        _meta = gpd.GeoDataFrame(_meta, geometry=gpd.GeoSeries([], crs=4326), crs=4326)

    # Set EGI index column as DataFrame index (matches final output structure)
    if set_index:
        _meta = _meta.set_index(egi_index_col)

    return _meta


def _load_egi_from_h3_database(columns=None, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR,
                               index_level=1, partition_level=12):
    """Internal: load H3 database directly into EGI partitions (original egi_load body)."""
    import dask
    from dask import dataframe as ddf
    from . import egi

    egi.validate_level(index_level)
    egi.validate_level(partition_level)

    # Prepare EGI↔H3 intersection (tiles at partition_level)
    egi_tiles, egi_to_h3, h3_part_col, region_gdf = _prepare_egi_loading(
        region, gh3_dir, partition_level=partition_level
    )

    # Track output columns (exclude query-only columns from final output)
    out_cols = None
    load_cols = columns.copy() if columns else None
    if load_cols is not None:
        # Always include shot_number for observation-level identification
        available_cols = gh3_read_meta("h3_columns", gh3_root_dir=gh3_dir)
        sn_cols = [c for c in available_cols if c.startswith('shot_number')]
        for c in sn_cols:
            if c not in load_cols:
                load_cols.append(c)

        # Save output columns before adding query-specific columns
        out_cols = load_cols.copy()

        # Ensure we have geometry for bbox filtering
        if 'geometry' not in load_cols:
            load_cols.append('geometry')
        if 'geometry' not in out_cols:
            out_cols.append('geometry')

        # Handle query columns (load but don't include in output)
        if query is not None:
            q_cols = [col for col in available_cols if col in query]
            load_cols = list(set(load_cols + q_cols))

    egi_index_col = egi.egi_col_name(index_level)
    egi_part_col = egi.egi_col_name(partition_level)

    # Build list of (egi_id, h3_list, egi_bbox) tuples for from_map
    tile_args = [
        (egi_id, h3_list, egi_tiles.loc[egi_id, 'geometry'].bounds)
        for egi_id, h3_list in egi_to_h3.items()
    ]

    # Capture storage credentials for Dask workers (separate processes)
    _scfg = None
    if is_remote_path(gh3_dir):
        from .utils import _storage_options
        _scfg = dict(_storage_options)

    # Detect the fastest bbox-filter strategy ONCE on the driver (one parquet
    # metadata read against a sample h3 partition file). The result is the
    # same for every file in the db, so we capture it here and pass it
    # through to every worker task — workers do NOT re-detect per file,
    # which would cost ~10ms × 210k file reads.
    _sample_pf = None
    for _hid in egi_to_h3:
        _hpath = smart_join(gh3_dir, f"{h3_part_col}={egi_to_h3[_hid][0]}")
        _files = smart_glob(smart_join(_hpath, '*.parquet')) or \
                 smart_glob(smart_join(_hpath, '**/*.parquet'), recursive=True)
        if _files:
            _sample_pf = _files[0]
            break
    if _sample_pf is not None:
        bbox_strategy, bbox_lat_col, bbox_lon_col = _pick_bbox_strategy(_sample_pf)
    else:
        bbox_strategy, bbox_lat_col, bbox_lon_col = 'fallback', None, None

    # Define loader function for from_map. set_index=True avoids a later
    # shuffle. tile_egi_id makes the loader stream + spillover-filter per
    # H3 partition (caps peak memory at ~1 H3 partition; without it, the
    # ring-1 expansion would OOM workers on dense tropical L12 tiles).
    def load_tile(args):
        _restore_storage_on_worker(_scfg)
        egi_id, h3_list, egi_bbox = args
        return _load_egi_tile_from_h3(
            egi_bbox, h3_list, gh3_dir, h3_part_col, load_cols,
            query, index_level, partition_level, set_index=True,
            tile_egi_id=egi_id,
            bbox_strategy=bbox_strategy,
            bbox_lat_col=bbox_lat_col,
            bbox_lon_col=bbox_lon_col,
        )

    # Build metadata from schema (avoids empty sample issue)
    # set_index=True because tile loader sets index (metadata must match)
    _meta = _build_egi_load_meta(load_cols, gh3_dir, index_level, partition_level, include_geometry=True, set_index=True)

    # Use from_map instead of from_delayed (from_delayed is deprecated)
    result = ddf.from_map(load_tile, tile_args, meta=_meta)

    # Convert to dask_geopandas GeoDataFrame
    if 'geometry' in result.columns:
        result = dask_geopandas.from_dask_dataframe(result, geometry='geometry')

    # Filter to output columns only (exclude query-only columns)
    if out_cols is not None:
        # Include partition column in output (index is already set)
        final_cols = [c for c in out_cols if c != egi_index_col] + [egi_part_col]
        # Filter to columns that exist
        final_cols = [c for c in final_cols if c in result.columns]
        result = result[final_cols]

    # Clip to ROI boundaries (like gh3_load does)
    # Data is in WGS84 (kept original CRS), so clip with region in WGS84
    if region_gdf is not None:
        region_wgs84 = region_gdf.to_crs(4326) if region_gdf.crs.to_epsg() != 4326 else region_gdf
        result = result.clip(region_wgs84)

    # Index is already set in tile loader - no shuffle needed!
    return result


def egi_load(source=None, *, columns=None, region=None, query=None,
             index_level=1, partition_level=12, lazy=True):
    """Load EGI-indexed GEDI data from any source.

    Auto-detects whether the source is an H3 database (direct EGI loading)
    or a simplified EGI dataset and loads accordingly.

    Parameters
    ----------
    source : str, optional
        Path to data source (H3 database or EGI dataset).
        If None, falls back to default H3 directory.
    columns : list, optional
        Columns to load.
    region : GeoDataFrame or bbox, optional
        Spatial filter.
    query : str, optional
        Pandas query string for filtering.
    index_level : int
        EGI resolution level for fine indexing (1-12, default=1 ~1m).
        Only used when loading from H3 database.
    partition_level : int
        EGI level for output partitioning (1-12, default=12 ~160km).
        Only used when loading from H3 database.
    lazy : bool
        If True (default), return Dask DataFrame. If False, return computed
        pandas DataFrame.

    Returns
    -------
    dask GeoDataFrame or GeoDataFrame
        EGI-indexed data (lazy by default, eager if lazy=False).

    Raises
    ------
    GediValidationError
        If source is an H3 dataset (use ``gh3_load()`` instead).
    GediDatabaseNotFoundError
        If no valid data source is found.
    EGIValidationError
        If ``index_level`` or ``partition_level`` is outside [1, 12].

    Examples
    --------
    >>> import gedih3.gh3driver as gh3
    >>> ddf = gh3.egi_load(
    ...     source='/path/to/h3_database',
    ...     columns=['agbd_l4a'],
    ...     region='region.shp',
    ...     index_level=1,
    ...     partition_level=12,
    ... )
    >>> agg = gh3.egi_aggregate(ddf, target_level=6, agg='mean')
    """
    path, info = _detect_source(source)
    columns = _resolve_columns(columns, path, info)

    if info['source_type'] == 'h3_database':
        # Direct EGI loading from H3 database (no shuffle)
        ddf = _load_egi_from_h3_database(
            columns=columns, region=region, query=query,
            gh3_dir=path, index_level=index_level, partition_level=partition_level
        )
    elif info.get('index_type') == 'egi':
        # Simplified EGI dataset
        ddf = _load_dataset(path, columns=columns, query=query, region=region, lazy=True)
    elif info.get('index_type') == 'h3':
        raise GediValidationError(
            f"Source '{path}' is an H3 dataset. Use gh3_load() for H3 data, "
            f"or load from an H3 database with egi_load() for direct EGI conversion."
        )
    else:
        # Parquet directory with unknown index — try loading as dataset
        ddf = _load_dataset(path, columns=columns, query=query, region=region, lazy=True)

    if not lazy:
        return dask_safe_collect(ddf)
    return ddf


def _egi_repartition(gh3_df, shuffle_level, x_col='lon_lowestmode', y_col='lat_lowestmode'):
    """
    Repartition H3-indexed data by EGI tiles for efficient H3->EGI conversion.

    This is an internal helper that handles the coordinate projection and shuffle
    step common to both egi_extract and egi_aggregate. It:

    1. Projects coordinates to EPSG:6933 and stores them as _egi_x, _egi_y
    2. Computes EGI hash at the specified shuffle level
    3. Shuffles data by that hash so all shots in each tile are co-located

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        H3-indexed GEDI data
    shuffle_level : int
        EGI level for shuffling (1-12). Higher levels = coarser tiles = fewer
        unique keys = more efficient shuffle. Level 12 has ~19,656 unique tiles.
    x_col : str
        Longitude column name for coordinate lookup
    y_col : str
        Latitude column name for coordinate lookup

    Returns
    -------
    dask DataFrame
        Data shuffled by EGI tile, with _egi_x, _egi_y columns for local indexing.
        Index is the EGI shuffle column (egiXX where XX is shuffle_level).
    """
    from . import egi

    egi.validate_level(shuffle_level)
    egi_shuffle_col = egi.egi_col_name(shuffle_level)

    def add_shuffle_index(df, x_col, y_col, shuffle_level, shuffle_col):
        """Add EGI shuffle index and store projected + original coordinates."""
        from gedih3.egi.core import to_hash as _to_hash
        from pyproj import Transformer

        if len(df) == 0:
            df = df.copy()
            df[shuffle_col] = pd.Series([], dtype=np.uint64)
            df['_egi_x'] = pd.Series([], dtype=np.float64)
            df['_egi_y'] = pd.Series([], dtype=np.float64)
            df['_wgs84_x'] = pd.Series([], dtype=np.float64)
            df['_wgs84_y'] = pd.Series([], dtype=np.float64)
            if 'geometry' in df.columns:
                df = df.drop(columns=['geometry'])
            return df

        # Check if input is a GeoDataFrame with Point geometry
        is_point_gdf = (
            isinstance(df, gpd.GeoDataFrame) and
            'geometry' in df.columns and
            len(df) > 0 and
            df.geom_type.iloc[0] == 'Point'
        )

        if is_point_gdf:
            # Extract WGS84 coordinates from geometry
            if df.crs is not None and df.crs.to_epsg() != 4326:
                # Transform to WGS84 first
                transformer_wgs = Transformer.from_crs(df.crs, 'EPSG:4326', always_xy=True)
                wgs84_x, wgs84_y = transformer_wgs.transform(df.geometry.x.values, df.geometry.y.values)
            else:
                wgs84_x, wgs84_y = df.geometry.x.values, df.geometry.y.values

            # Transform to EPSG:6933 for EGI hash computation
            transformer = Transformer.from_crs('EPSG:4326', 'EPSG:6933', always_xy=True)
            x, y = transformer.transform(wgs84_x, wgs84_y)
        else:
            # Use coordinate columns (assumed WGS84)
            actual_x_col = find_coordinate_column(df.columns, x_col)
            actual_y_col = find_coordinate_column(df.columns, y_col)
            if actual_x_col is None or actual_y_col is None:
                raise GediVariableError(f"Coordinate columns not found: {x_col}, {y_col}")

            wgs84_x = df[actual_x_col].values
            wgs84_y = df[actual_y_col].values

            # Transform from WGS84 to EPSG:6933
            transformer = Transformer.from_crs('EPSG:4326', 'EPSG:6933', always_xy=True)
            x, y = transformer.transform(wgs84_x, wgs84_y)

        # Compute EGI shuffle hash
        df = df.copy()
        df[shuffle_col] = _to_hash(np.asarray(x), np.asarray(y), shuffle_level)

        # Store projected coordinates for fine-grained indexing after shuffle
        df['_egi_x'] = x
        df['_egi_y'] = y

        # Store original WGS84 coordinates for geometry recreation
        df['_wgs84_x'] = wgs84_x
        df['_wgs84_y'] = wgs84_y

        # Drop geometry column (can be recreated later if needed)
        if 'geometry' in df.columns:
            df = df.drop(columns=['geometry'])

        return df

    # Build metadata
    _meta = gh3_df._meta.copy()
    if 'geometry' in _meta.columns:
        _meta = pd.DataFrame(_meta.drop(columns=['geometry']))
    _meta[egi_shuffle_col] = np.uint64(0)
    _meta['_egi_x'] = np.float64(0)
    _meta['_egi_y'] = np.float64(0)
    _meta['_wgs84_x'] = np.float64(0)
    _meta['_wgs84_y'] = np.float64(0)

    shuffled = gh3_df.map_partitions(
        add_shuffle_index,
        x_col=x_col,
        y_col=y_col,
        shuffle_level=shuffle_level,
        shuffle_col=egi_shuffle_col,
        meta=_meta
    )

    # Shuffle by EGI tile
    shuffled = shuffled.set_index(egi_shuffle_col)

    return shuffled


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
        actual_x_col = find_coordinate_column(df.columns, x_col)
        actual_y_col = find_coordinate_column(df.columns, y_col)

        if actual_x_col is None or actual_y_col is None:
            raise GediVariableError(
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

    # Filter to requested columns (skip for callable/dict — they handle selection themselves)
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


def _build_agg_meta(gh3_df, target_level, agg, columns, index_type='egi', **agg_kwargs):
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
    **agg_kwargs
        Extra kwargs forwarded to the aggregation callable when inferring meta.

    Returns
    -------
    pandas DataFrame
        Metadata template with correct index and column names
    """
    from . import egi

    if index_type == 'egi':
        idx_col = egi.egi_col_name(target_level)
        idx_dtype = np.uint64
    else:
        idx_col = f'h3_{target_level:02d}'
        idx_dtype = str

    sample = gh3_df._meta

    # Callable agg: the output schema is whatever the callable returns and
    # generally unrelated to the input column names. Invoke it on an empty
    # sample to infer the true result columns (mirrors gh3_aggregate_func's
    # H3 path at gh3driver.py:419-434).
    if callable(agg):
        if columns is not None:
            sample_cols = [c for c in columns if c in sample.columns]
            sample_input = sample[sample_cols].iloc[0:0].copy() if sample_cols else sample.iloc[0:0].copy()
        else:
            sample_input = sample.iloc[0:0].copy()
        try:
            result = agg(sample_input, **agg_kwargs)
            _meta = result.iloc[0:0].copy() if hasattr(result, 'iloc') else pd.DataFrame()
        except Exception:
            # Fallback: keep legacy behavior of echoing input column names.
            _meta = pd.DataFrame(columns=list(sample_input.columns), dtype=float)
        _meta.index = pd.Index([], dtype=idx_dtype, name=idx_col)
        return _meta

    if columns is not None:
        cols = [c for c in columns if c in sample.columns]
    else:
        # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
        cols = get_aggregatable_columns(sample)

    def _agg_name(func):
        """Get the name pandas uses for an aggregation function."""
        return func.__name__ if callable(func) else str(func)

    if isinstance(agg, dict):
        meta_cols = [f"{col}_{_agg_name(func)}" for col, funcs in agg.items()
                     for func in (funcs if isinstance(funcs, list) else [funcs])]
    elif isinstance(agg, list):
        meta_cols = [f"{col}_{_agg_name(func)}" for col in cols for func in agg]
    else:
        meta_cols = cols

    _meta = pd.DataFrame(columns=meta_cols, dtype=float)
    _meta.index = pd.Index([], dtype=idx_dtype, name=idx_col)
    return _meta


def _egi_aggregate_from_indexed(gh3_df, target_level, partition_level, agg,
                                 columns, add_geometry, repartition, **kwargs):
    """
    Aggregate EGI-indexed data (from egi_load) without shuffle.

    When input is already EGI-partitioned, aggregation is purely local:
    each partition is grouped by its EGI index and aggregated independently.

    If the input EGI level differs from target_level, hashes are coarsened
    via to_parent() before grouping. When egi_load() is called with
    index_level=target_level, this step is skipped entirely.

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        EGI-indexed data (index name like 'egi06')
    target_level : int
        Target EGI resolution level for aggregation
    partition_level : int
        EGI level for output partitioning
    agg : str, list, dict, or callable
        Aggregation specification
    columns : list or None
        Columns to aggregate (if None, all numeric columns)
    add_geometry : bool
        If True, add pixel polygon geometries to output
    repartition : bool
        If True, add partition column for organized export
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        EGI-indexed aggregated data
    """
    import dask
    from . import egi

    egi_col = egi.egi_col_name(target_level)
    egi_part_col = egi.egi_col_name(partition_level)

    # Read input EGI level from index name (e.g., 'egi06' -> 6)
    input_index_name = str(gh3_df.index.name)
    input_level = int(input_index_name.replace('egi', ''))
    needs_coarsen = (input_level != target_level)

    def local_aggregate(df, target_level, input_level, needs_coarsen,
                        agg, columns, egi_col, **agg_kwargs):
        """Aggregate a single EGI-indexed partition locally."""
        from gedih3.egi.core import to_parent as _to_parent

        if len(df) == 0:
            # Empty partition: pandas groupby.apply on empty input skips the
            # callable and echoes input columns, breaking dask's _meta check.
            # Invoke the callable directly on an empty frame to get the right
            # output schema (mirrors gh3_aggregate_func at gh3driver.py:419-434).
            if callable(agg):
                try:
                    out = agg(df.iloc[0:0].copy(), **agg_kwargs)
                    out = out.iloc[0:0].copy() if hasattr(out, 'iloc') else pd.DataFrame()
                    out.index = pd.Index([], dtype=np.uint64, name=egi_col)
                    return out
                except Exception:
                    pass
            return pd.DataFrame(index=pd.Index([], dtype=np.uint64, name=egi_col))

        # If input level != target level, coarsen index
        if needs_coarsen:
            df = df.reset_index()
            input_col = df.columns[0]  # The input EGI index column
            df[egi_col] = _to_parent(df[input_col].values, target_level)
            df = df.drop(columns=[input_col]).set_index(egi_col)
        elif df.index.name != egi_col:
            # Same level but different name shouldn't happen, but be safe
            df.index.name = egi_col

        # Filter columns for aggregation
        if columns is not None:
            agg_cols = [c for c in columns if c in df.columns]
            if agg_cols:
                df = df[agg_cols]
        elif callable(agg) or isinstance(agg, dict):
            # Callables / dicts manage column selection themselves — pass everything.
            pass
        else:
            filtered_cols = get_aggregatable_columns(df)
            if filtered_cols:
                df = df[filtered_cols]

        # Local groupby aggregation (no shuffle!)
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
    _agg_meta = _build_agg_meta(gh3_df, target_level, agg, columns, index_type='egi', **kwargs)

    agg_df = gh3_df.map_partitions(
        local_aggregate,
        target_level=target_level,
        input_level=input_level,
        needs_coarsen=needs_coarsen,
        agg=agg,
        columns=columns,
        egi_col=egi_col,
        meta=_agg_meta,
        **kwargs
    )

    # Add partition column for organized export
    if repartition:
        def add_partition_col(df, part_col, part_level):
            from gedih3.egi.core import to_parent as _to_parent
            if len(df) == 0:
                df[part_col] = pd.Series([], dtype=np.uint64)
                return df
            df = df.reset_index()
            idx_col = df.columns[0]
            df[part_col] = _to_parent(df[idx_col].values, part_level)
            return df.set_index(idx_col)

        _part_meta = agg_df._meta.copy()
        _part_meta = _part_meta.reset_index()
        _part_meta[egi_part_col] = np.uint64(0)
        _part_meta = _part_meta.set_index(egi_col)

        agg_df = agg_df.map_partitions(
            add_partition_col,
            part_col=egi_part_col,
            part_level=partition_level,
            meta=_part_meta
        )

    # Add geometry
    if add_geometry:
        _gmeta = agg_df._meta.copy()
        _gmeta['geometry'] = gpd.GeoSeries([], crs=egi.EGI_CRS_STRING)
        _gmeta = gpd.GeoDataFrame(_gmeta, geometry='geometry', crs=egi.EGI_CRS_STRING)
        agg_df = agg_df.map_partitions(egi_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    return agg_df


def egi_aggregate(gh3_df, target_level=6, agg='mean', columns=None, query=None,
                  add_geometry=True, x_col='lon_lowestmode', y_col='lat_lowestmode',
                  partition_level=12, repartition=False, **kwargs):
    """
    Aggregate GEDI data to EGI (EASE Grid Index) square pixels.

    Supports two input types:

    - **EGI-indexed** (from egi_load()): Fast path — no shuffle needed, aggregation
      is purely local within each partition.
    - **H3-indexed** (from gh3_load()): Shuffle path — data is repartitioned by EGI
      tiles before local aggregation.

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        GEDI data loaded via egi_load() (EGI-indexed) or gh3_load() (H3-indexed)
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
        Longitude column name for coordinate lookup (shuffle path only)
    y_col : str
        Latitude column name for coordinate lookup (shuffle path only)
    partition_level : int
        EGI level for output partitioning and data shuffling (1-12, default=12 ~160km).
        Higher levels = coarser tiles = fewer unique keys = more efficient shuffle.
        Use smaller values for regions with many variables to reduce file sizes.
    repartition : bool
        If True, add partition column for organized export
    **kwargs
        Additional arguments passed to aggregation function

    Returns
    -------
    dask GeoDataFrame
        EGI-indexed aggregated data
    """
    from . import egi

    # Validate levels
    egi.validate_level(target_level)
    egi.validate_level(partition_level)
    egi_col = egi.egi_col_name(target_level)
    egi_part_col = egi.egi_col_name(partition_level)

    if query is not None:
        gh3_df = gh3_df.query(query)

    # Fast path: input is already EGI-indexed (from egi_load)
    input_is_egi = (
        gh3_df.index.name is not None
        and str(gh3_df.index.name).startswith('egi')
    )
    if input_is_egi:
        return _egi_aggregate_from_indexed(
            gh3_df, target_level, partition_level, agg,
            columns, add_geometry, repartition, **kwargs
        )

    # Shuffle path: H3-indexed input needs repartitioning
    # Phase 1-2: Repartition by EGI partition level (shared helper)
    shuffled = _egi_repartition(gh3_df, partition_level, x_col, y_col)

    # Phase 3: Local fine-grained aggregation within each partition
    def local_egi_aggregate(df, target_level, agg, columns, egi_col, **agg_kwargs):
        """Aggregate a single partition to fine EGI pixels.

        Uses pre-computed EPSG:6933 coordinates stored as _egi_x and _egi_y.
        """
        from gedih3.egi.core import to_hash as _to_hash

        if len(df) == 0:
            # Empty partition: invoke callable on empty input to capture the
            # true output schema; otherwise return a bare empty frame.
            if callable(agg):
                empty = df.drop(columns=['_egi_x', '_egi_y'], errors='ignore').iloc[0:0].copy()
                try:
                    out = agg(empty, **agg_kwargs)
                    out = out.iloc[0:0].copy() if hasattr(out, 'iloc') else pd.DataFrame()
                    out.index = pd.Index([], dtype=np.uint64, name=egi_col)
                    return out
                except Exception:
                    pass
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
        elif callable(agg) or isinstance(agg, dict):
            # Callables / dicts manage column selection themselves — pass everything.
            pass
        else:
            # Filter out internal columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, geometry)
            filtered_cols = get_aggregatable_columns(df)
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
    _agg_meta = _build_agg_meta(gh3_df, target_level, agg, columns, index_type='egi', **kwargs)

    agg_df = shuffled.map_partitions(
        local_egi_aggregate,
        target_level=target_level,
        agg=agg,
        columns=columns,
        egi_col=egi_col,
        meta=_agg_meta,
        **kwargs
    )

    # Phase 4: Optional - add partition column for organized export
    if repartition:
        def add_partition_col(df, part_col, part_level):
            from gedih3.egi.core import to_parent as _to_parent
            if len(df) == 0:
                df[part_col] = pd.Series([], dtype=np.uint64)
                return df
            df = df.reset_index()
            idx_col = df.columns[0]  # The EGI index column
            df[part_col] = df[idx_col].apply(lambda x: _to_parent(x, part_level))
            return df.set_index(idx_col)

        _part_meta = agg_df._meta.copy()
        _part_meta = _part_meta.reset_index()
        _part_meta[egi_part_col] = np.uint64(0)
        _part_meta = _part_meta.set_index(egi_col)

        agg_df = agg_df.map_partitions(
            add_partition_col,
            part_col=egi_part_col,
            part_level=partition_level,
            meta=_part_meta
        )

    # Phase 5: Add geometry
    if add_geometry:
        _gmeta = agg_df._meta.copy()
        _gmeta['geometry'] = gpd.GeoSeries([], crs=egi.EGI_CRS_STRING)
        _gmeta = gpd.GeoDataFrame(_gmeta, geometry='geometry', crs=egi.EGI_CRS_STRING)
        agg_df = agg_df.map_partitions(egi_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    return agg_df


def egi_extract(gh3_df, index_level=1, partition_level=12,
                query=None, add_geometry=True, x_col='lon_lowestmode', y_col='lat_lowestmode'):
    """
    Extract H3-indexed GEDI data with EGI spatial indexing.

    This function converts H3-indexed GEDI shots to EGI-indexed data without
    aggregation. It repartitions data by EGI tiles for efficient H3->EGI conversion.

    Parameters
    ----------
    gh3_df : dask GeoDataFrame
        H3-indexed GEDI data loaded via gh3_load()
    index_level : int
        EGI resolution level for fine indexing (1-12, default=1 ~1m)
    partition_level : int
        EGI level for output file partitioning and shuffling (1-12, default=12 ~160km).
        Higher levels = coarser tiles = fewer unique keys = more efficient shuffle.
    query : str, optional
        Pandas query string for filtering before extraction
    add_geometry : bool
        If True, add Point geometries to output (in WGS84/EPSG:4326)
    x_col : str
        Longitude column name for coordinate lookup
    y_col : str
        Latitude column name for coordinate lookup

    Returns
    -------
    dask GeoDataFrame
        EGI-indexed data with all original columns plus EGI index columns
    """
    from . import egi

    # Validate levels
    egi.validate_level(index_level)
    egi.validate_level(partition_level)

    egi_index_col = egi.egi_col_name(index_level)
    egi_part_col = egi.egi_col_name(partition_level)

    if query is not None:
        gh3_df = gh3_df.query(query)

    # Phase 1-2: Repartition by EGI partition level
    shuffled = _egi_repartition(gh3_df, partition_level, x_col, y_col)

    # Phase 3: Add fine EGI index, partition columns, and optionally recreate geometry
    def add_egi_indices_and_geometry(df, index_level, partition_level, index_col, part_col, add_geom):
        """Add fine EGI index and partition columns, recreate geometry from WGS84 coords."""
        from gedih3.egi.core import to_hash as _to_hash, to_parent as _to_parent
        from shapely.geometry import Point

        if len(df) == 0:
            df = df.reset_index(drop=True)
            df[index_col] = pd.Series([], dtype=np.uint64)
            df[part_col] = pd.Series([], dtype=np.uint64)
            df = df.drop(columns=['_egi_x', '_egi_y', '_wgs84_x', '_wgs84_y'], errors='ignore')
            if add_geom:
                df = gpd.GeoDataFrame(df, geometry=[], crs=4326)
            df = df.set_index(index_col)
            return df

        # Reset index (drop shuffle column)
        df = df.reset_index(drop=True)

        # Use pre-computed projected coordinates for EGI hash
        x = df['_egi_x'].values
        y = df['_egi_y'].values

        # Compute fine EGI index
        df[index_col] = _to_hash(np.asarray(x), np.asarray(y), index_level)

        # Compute partition column (may be same as index or coarser)
        if partition_level == index_level:
            df[part_col] = df[index_col]
        else:
            df[part_col] = _to_parent(df[index_col].values, partition_level)

        # Recreate geometry from original WGS84 coordinates (not EGI pixel centers!)
        if add_geom:
            wgs84_x = df['_wgs84_x'].values
            wgs84_y = df['_wgs84_y'].values
            points = [Point(px, py) for px, py in zip(wgs84_x, wgs84_y)]
            df = gpd.GeoDataFrame(df, geometry=points, crs=4326)

        # Drop temporary coordinate columns
        df = df.drop(columns=['_egi_x', '_egi_y', '_wgs84_x', '_wgs84_y'], errors='ignore')

        # Set EGI index column as DataFrame index (matches direct load behavior)
        df = df.set_index(index_col)

        return df

    # Build metadata for result (with index set)
    _idx_meta = shuffled._meta.reset_index(drop=True)
    _idx_meta[egi_index_col] = np.uint64(0)
    _idx_meta[egi_part_col] = np.uint64(0)
    _idx_meta = _idx_meta.drop(columns=['_egi_x', '_egi_y', '_wgs84_x', '_wgs84_y'], errors='ignore')
    if add_geometry:
        _idx_meta = gpd.GeoDataFrame(_idx_meta, geometry=gpd.GeoSeries([], crs=4326), crs=4326)
    _idx_meta = _idx_meta.set_index(egi_index_col)

    extracted = shuffled.map_partitions(
        add_egi_indices_and_geometry,
        index_level=index_level,
        partition_level=partition_level,
        index_col=egi_index_col,
        part_col=egi_part_col,
        add_geom=add_geometry,
        meta=_idx_meta
    )

    # Convert to dask_geopandas if geometry was added
    if add_geometry and 'geometry' in extracted.columns:
        extracted = dask_geopandas.from_dask_dataframe(extracted, geometry='geometry')

    return extracted

def egi_export_part(df, odir, fmt='parquet', is_file_path=False, partition_level=12):
    """
    Export a single EGI partition to file(s).

    Splits the data by partition tile and writes one file per unique tile.
    File names are the EGI hash of the partition tile at the requested level.

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
    partition_level : int
        EGI level used for output file naming (1-12, default=12). Used as a
        fallback when no egiXX column is present in the DataFrame.

    Returns
    -------
    str
        Output file path(s) - comma-separated if multiple files written
    """
    from . import egi
    import numpy as np
    import re

    if df.empty:
        return ''

    # When is_file_path=True (merge mode), ``odir`` is actually the user's
    # destination FILE path — creating it as a directory here turns the
    # final AtomicFileWriter.os.replace() into "Is a directory". The parent
    # dir is created by AtomicFileWriter.__enter__ anyway, so this is safe
    # to skip in that case. Mirrors gh3_export_part's guard.
    if not is_file_path:
        os.makedirs(odir, exist_ok=True)

    if is_file_path:
        # Single file output mode - write all data to one file
        odir = odir.rstrip('/')
        opath = f"{odir}.{fmt}" if not odir.endswith(fmt) else odir
        return _write_egi_file(df, opath, fmt)

    # Multi-file mode: split by partition tile for correct file naming.
    # Prefer the egiXX column (present when drop_internal=False, i.e. CLI paths).
    # Fall back to computing partition tiles from the index via to_parent().
    egi_part_cols = sorted(
        [c for c in df.columns if re.match(r'^egi\d{2}$', str(c))],
        key=lambda c: int(str(c).replace('egi', ''))
    )
    if egi_part_cols:
        part_col_name = egi_part_cols[-1]   # coarsest egiXX = partition column
        part_hashes = df[part_col_name].to_numpy().astype(np.uint64)
    else:
        idx_array = df.index.to_numpy().astype(np.uint64)
        part_hashes = egi.to_parent(idx_array, partition_level)

    output_paths = []
    for part_hash in np.unique(part_hashes):
        mask = part_hashes == part_hash
        tile_df = df.iloc[mask]

        if len(tile_df) == 0:
            continue

        oname = str(part_hash)
        opath = smart_join(odir, f"{oname}.{fmt}")

        # Partitions at level <= 12 nest in exactly one outer tile — pass it
        # so raster outputs never fall back to tile inference.
        outer_tile = int(egi.to_parent(np.uint64(part_hash), egi.OUTER_LEVEL))
        written_path = _write_egi_file(tile_df, opath, fmt, outer_tile=outer_tile)
        if written_path:
            output_paths.append(written_path)

    return ','.join(output_paths) if output_paths else ''


def _write_egi_file(df, opath, fmt, outer_tile=None):
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
    outer_tile : int, optional
        Level-12 EGI hash of the data's outer tile, when the caller knows it
        (per-partition writes). Forwarded to ``geodf_to_raster`` so raster
        output targets the right tile without inference. None falls back to
        the deterministic majority pick (single-file merge mode, where the
        data may legitimately span tiles — only the dominant one is
        rasterized, with a warning).

    Returns
    -------
    str
        Output file path, or empty string on failure
    """
    from . import egi

    if df.empty:
        return ''

    # Handle raster export (rasterio writer handles its own atomicity)
    if fmt in ('tif', 'tiff', 'geotiff'):
        raster = egi.geodf_to_raster(df, outer_tile=outer_tile)
        egi.export_raster(raster, opath)
        return opath

    # Geo-vector formats infer the OGR driver from the file extension
    # and shp emits multiple sidecars — bypass the atomic wrapper for
    # those, like ``_write_dataframe`` does.
    if fmt in ('geojson', 'gpkg', 'shp'):
        df.to_file(opath)
        return opath

    if is_parquet(opath):
        # Verify+retry around parquet writes — catches the GPFS/transient-IO
        # class where pyarrow commits a file whose data pages are corrupt
        # (footer intact, body bad). A plain AtomicFileWriter cannot detect it.
        atomic_parquet_write(df, opath)
        return opath

    # Single-file non-parquet formats: write through AtomicFileWriter so a
    # worker SIGKILL or disk-full mid-write does not leave a partial file at
    # the final path. Errors propagate — caller decides resilience policy.
    with AtomicFileWriter(opath) as tmp:
        if fmt == 'feather':
            df.to_feather(tmp)
        elif fmt == 'txt':
            df.to_csv(tmp, sep='\t')
        elif fmt == 'csv':
            df.to_csv(tmp)
        elif fmt in ('h5', 'hdf5'):
            df.to_hdf(tmp, key='GEDI', mode='w')
        else:
            raise GediProcessingError(f"Unsupported export format: {fmt}")

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
    show_progress=True,
    partition_level=None
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
    partition_level : int, optional
        H3 partition level for grouping/naming tiles. If None, auto-detected
        from data columns or defaults to treating each partition as one tile.

    Returns
    -------
    list of str
        Paths to output files
    """
    from .raster import rasterize_and_export_partitions, rasterize_h3_partition

    return rasterize_and_export_partitions(
        ddf, output_dir, rasterize_h3_partition,
        columns=columns, compress=compress, show_progress=show_progress,
        partition_level=partition_level
    )


# ============================================================================
# Raster Sampling API
# ============================================================================


def gh3_sample_raster(image_path, data_source=None,
                      region=None, query=None, band_names=None,
                      band_indices=None, window_ops=None,
                      fillna=None, dropna=False, geo=False,
                      file_format='tif'):
    """
    Sample raster pixel values at GEDI shot locations.

    Thin wrapper around ``imgutils.from_image()`` for API discoverability.
    Returns a Dask DataFrame; use ``gh3_export()`` to save results.

    Parameters
    ----------
    image_path : str
        Path to raster file, VRT, or tile directory
    data_source : str, optional
        Path to H3 database or simplified dataset directory
    region : GeoDataFrame or bbox, optional
        Additional spatial filter
    query : str, optional
        Pandas query string for filtering shots
    band_names : list of str, optional
        Custom names for output band columns
    band_indices : list of int, optional
        Select specific bands by 0-based index
    window_ops : list of dict, optional
        Window operation specifications
    fillna : float, optional
        Fill NaN/NoData with this value
    dropna : bool
        If True, drop rows where all band columns are NaN
    geo : bool
        If True, include geometry in output
    file_format : str
        Raster file extension for tile directory globbing

    Returns
    -------
    dask DataFrame or GeoDataFrame
        Sampled raster values at GEDI shot locations

    Examples
    --------
    >>> import gedih3.gh3driver as gh3
    >>> ddf = gh3.gh3_sample_raster(
    ...     'dem.tif', data_source='/path/to/database',
    ...     band_names=['elevation'], geo=True
    ... )
    >>> gh3.gh3_export(ddf, '/tmp/sampled/')
    """
    from .imgutils import from_image

    return from_image(
        image_path=image_path,
        data_source=data_source,
        region=region,
        query=query,
        band_names=band_names,
        band_indices=band_indices,
        window_ops=window_ops,
        fillna=fillna,
        dropna=dropna,
        geo=geo,
        file_format=file_format,
    )