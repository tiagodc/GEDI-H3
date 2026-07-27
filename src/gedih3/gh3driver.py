# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

import os, re, h3
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import dask.dataframe
import dask_geopandas


from .config import GH3_DEFAULT_H3_DIR, configure_environment, BUILD_LOG_FILENAME, DATASET_META_FILENAME
from .utils import (json_read, json_read_cached, json_write, now, get_package_version, is_parquet,
                     smart_glob, smart_exists, smart_isfile, is_remote_path,
                     smart_open, smart_open_columnar, read_parquet_coalesced,
                     generate_manifest, check_nan_only_columns,
                     smart_join, AtomicFileWriter, atomic_parquet_write,
                     dask_safe_wait, dask_safe_collect)
from .h3utils import intersect_h3_geometries, fix_h3_geometry
from .cliutils import find_coordinate_column, get_aggregatable_columns, filter_data_columns
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
        if smart_isfile(path):
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
    from .utils import resolve_s3_source

    path = source if source is not None else GH3_DEFAULT_H3_DIR
    # s3://host:port/bucket/... carries its endpoint in the URL — the CLI
    # normalizes this in setup_storage; doing it here gives the Python API
    # (gh3_load / egi_load and everything routed through them) the same
    # capability. An already-configured endpoint always wins.
    path = resolve_s3_source(path)
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
    from .utils import resolve_s3_source
    gh3_root_dir = resolve_s3_source(gh3_root_dir)
    meta_path = smart_join(gh3_root_dir, BUILD_LOG_FILENAME)
    return json_read_cached(meta_path).get(var)


def gh3_select_partitions(source, region=None):
    """Return the H3 partition cell IDs of a database that may hold shots for ``region``.

    This is the canonical, overhang-safe way to determine which partition
    files a region touches, and the function external consumers should use
    when reading the H3 database **directly** (i.e. not through
    :func:`gh3_load`). It applies the same ring-1 expansion ``gh3_load`` uses
    internally.

    H3 partitions are *not* geometrically inclusive of the shots stored in
    them: a shot is filed under ``cell_to_parent(latlng_to_cell(lon, lat,
    index_res), partition_res)``, whose polygon a boundary shot can sit
    outside of by up to ~0.18 x the partition's edge length (~11 km at
    partition level 3). Selecting partitions by an *exact* polygon
    intersection therefore silently drops boundary shots (~5% of a boundary
    partition's shots observed in production). The ring-1 expansion applied
    here closes that gap; it is guaranteed sufficient because the overhang is
    far smaller than one cell width. See the "Parent/Child Nesting Caveat" in
    the H3 Indexing docs for the full explanation.

    Parameters
    ----------
    source : str
        Path to an H3 database (the directory holding
        ``gedih3_build_log.json``) or a simplified dataset (the directory
        holding ``gedih3_dataset.json`` — H3 or EGI). Datasets answer
        through the same safety-gated selection the loaders use
        (:func:`_select_dataset_files`): when the sidecar cannot prove the
        filenames bound their files, every partition is returned rather
        than risking an under-selection.
    region : str | list | GeoDataFrame | GeoSeries | shapely geometry, optional
        ROI as a vector-file path / ``"W,S,E,N"`` string, ``[W, S, E, N]``
        bbox list, geopandas object, or shapely geometry (EPSG:4326). When
        ``None`` (default), every partition is returned.

    Returns
    -------
    list[str]
        Sorted partition IDs: H3 cell IDs at the database's partition level,
        or the dataset's file-basename partition IDs (H3 cells / EGI hashes).
        Empty when no dataset partition intersects the region.

    Raises
    ------
    GediDatabaseNotFoundError
        If ``source`` has no readable build log / partition list.

    Examples
    --------
    >>> ids = gh3_select_partitions('/data/h3db', region='roi.shp')
    >>> ids[:2]
    ['83804cfffffffff', '838041fffffffff']
    >>> # Map ids -> files when reading outside gh3_load:
    >>> import glob
    >>> files = [f for i in ids
    ...          for f in glob.glob(f'/data/h3db/h3_03={i}/year=*/*.parquet')]
    """
    from .utils import resolve_s3_source
    source = resolve_s3_source(source)
    try:
        h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=source)
    except (FileNotFoundError, OSError):
        h3_ids = None

    if not h3_ids:
        # Not an H3 database — maybe a simplified dataset (H3 or EGI).
        # Answer from the same safety-gated machinery the loaders use, so
        # this helper and gh3_load/egi_load can never disagree.
        if smart_exists(smart_join(source, DATASET_META_FILENAME)):
            from .cliutils import detect_dataset_format
            fmt = detect_dataset_format(source)
            data_files, _ = _find_dataset_files(source, fmt)
            if region is not None:
                data_files = _select_dataset_files(
                    data_files, region, dataset_path=source,
                    keep_schema_file=False)
            return sorted({os.path.splitext(os.path.basename(f))[0]
                           for f in data_files})
        raise GediDatabaseNotFoundError(
            f"No H3 partition list found in {source} "
            f"(missing or empty 'h3_partition_ids' in {BUILD_LOG_FILENAME})."
        )
    if region is None:
        return sorted(h3_ids)
    return intersect_h3_geometries(region, h3_ids=h3_ids)

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

    meta = json_read_cached(meta_path)

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


def _dataset_partition_kind(part_ids):
    """Classify dataset partition IDs as ``'h3'``, ``'egi'`` or ``None``.

    A simplified dataset's file basenames *are* its partition IDs
    (``gh3_write_dataset_meta`` derives ``partition_ids`` from them), so
    the file list is where candidates come from. Parsing alone is NOT
    proof that a name *bounds* its file's contents — that proof comes
    from the sidecar level check in :func:`_dataset_prune_is_safe`.
    ``None`` means "these names are not partition IDs" (e.g. a
    hive-style tree), which callers must treat as "cannot prune".
    """
    if not part_ids:
        return None

    # EGI first: its hashes are decimal uint64 strings, and feeding one to
    # h3.is_valid_cell raises OverflowError rather than returning False.
    if all(p.isdigit() for p in part_ids):
        try:
            import numpy as _np
            from . import egi
            return 'egi' if all(egi.validate_hash(_np.uint64(p)) for p in part_ids) else None
        except (ValueError, OverflowError, TypeError):
            return None

    try:
        import h3
        return 'h3' if all(h3.is_valid_cell(p) for p in part_ids) else None
    except (ValueError, OverflowError, TypeError):
        return None


def _dataset_prune_is_safe(dataset_path, kind, part_ids):
    """True when each file's basename provably *bounds* its contents.

    A basename that parses as a partition ID is not enough:
    ``gh3_export_part`` has naming branches that name a file after its
    FIRST ROW's cell (e.g. when a source without a sidecar collapses the
    partition level onto the index level), and pruning on such a name
    silently drops every row outside it. The checkable proof that names
    are bounding partitions is the dataset sidecar declaring a partition
    level *strictly coarser* than the index level — then each file is
    one spatial partition and its rows nest inside the named cell/tile
    (H3 modulo the child overhang covered by ring-1; EGI exactly).

    Refuses (returns False → caller reads everything) when the sidecar
    is missing, the levels are absent or not strictly ordered, the IDs
    are not all at the declared coarser level, or files were renamed /
    added since the sidecar recorded ``partition_ids``.
    """
    if not dataset_path:
        return False
    meta_path = smart_join(dataset_path, DATASET_META_FILENAME)
    if not smart_exists(meta_path):
        return False
    meta = json_read_cached(meta_path)
    if meta.get('index_type') != kind:
        return False
    idx_level = meta.get('index_level')
    if idx_level is None:
        return False
    known = meta.get('partition_ids')
    if known and not set(part_ids).issubset(set(known)):
        return False

    if kind == 'h3':
        import h3
        levels = {h3.get_resolution(p) for p in part_ids}
        # H3: finer index = higher resolution number
        return len(levels) == 1 and levels.pop() < int(idx_level)

    import numpy as _np
    from . import egi
    levels = {int(v) for v in _np.atleast_1d(
        egi.get_level(_np.array(part_ids, dtype=_np.uint64)))}
    # EGI: coarser = higher level number (level 12 ~ 160 km is coarsest)
    return len(levels) == 1 and levels.pop() > int(idx_level)


def _select_dataset_files(data_files, region, logger=None, dataset_path=None,
                          keep_schema_file=True):
    """Subset a dataset's files to those whose partition may hold ``region``.

    A simplified dataset is one file per spatial partition, so the region
    answers which files can possibly contribute *before* anything is read.
    Without this a 1-degree query against a global dataset schedules every
    partition and clips rows afterwards: measured 25 of 12,461 partitions
    for a real ROI, i.e. ~500x more remote reads than needed. The H3
    database path has always pruned this way (``_load_h3_database``); this
    brings datasets — H3 and EGI, local and remote — in line.

    Selection is deliberately conservative, because under-selection is a
    silent data-loss class:

    * **H3** uses :func:`intersect_h3_geometries`, whose ``expand_ring=1``
      default covers the parent/child overhang (a shot can sit outside the
      polygon of the partition it is stored in).
    * **EGI** intersects the partition squares themselves. EGI is a nested
      axis-aligned grid and a shot's partition square contains its
      coordinates by construction, so polygon intersection is exact; the
      ROI is densified before reprojection (EPSG:6933 bends straight
      lon/lat edges — an un-densified 40-degree edge deviates ~150 km from
      its chord) and the 1 m buffer absorbs the float-boundary rounding
      documented in ``egi.to_hash``. Degenerate edge-of-grid squares are
      kept rather than decided against.
    * Anything else (hive trees, hand-named files, datasets whose sidecar
      cannot prove the names bound their files — see
      :func:`_dataset_prune_is_safe`) returns the full list.

    Row-level clipping downstream is unchanged — this only removes files
    that provably cannot contribute.
    """
    if region is None or not data_files:
        return data_files

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    by_id = {}
    for f in data_files:
        by_id.setdefault(os.path.splitext(os.path.basename(f))[0], []).append(f)

    kind = _dataset_partition_kind(list(by_id))
    if kind is None:
        if logger:
            logger.debug("Dataset filenames are not partition IDs; reading all files")
        return data_files

    if not _dataset_prune_is_safe(dataset_path, kind, list(by_id)):
        if logger:
            logger.debug("Dataset sidecar cannot prove filenames bound their "
                         "files (missing/stale sidecar or partition level not "
                         "coarser than index level); reading all files")
        return data_files

    if kind == 'h3':
        keep = set(intersect_h3_geometries(region, h3_ids=list(by_id)))
    else:
        import numpy as _np
        import shapely
        from . import egi
        from .utils import region_to_geometry

        ids = list(by_id)
        tiles = egi.to_geodataframe(_np.array(ids, dtype=_np.uint64), return_polygons=True)
        # Densify before reprojecting: to_crs moves vertices only, and in
        # EPSG:6933 y = A*sin(lat), so a straight lon/lat segment bows away
        # from its chord (~19 km over 20 deg of latitude, ~480 km over 60).
        # 0.1 deg keeps the residual under the 1 m rounding buffer.
        roi_geom = shapely.segmentize(region_to_geometry(region), 0.1)
        roi = gpd.GeoSeries([roi_geom], crs=4326)
        roi = roi.to_crs(egi.EGI_CRS_STRING).iloc[0].buffer(1.0)
        # Undecidable (degenerate / invalid) squares are kept, not dropped.
        undecidable = ~(tiles.geometry.is_valid & (tiles.geometry.area > 0))
        hit_pos = tiles.geometry.sindex.query(roi, predicate='intersects')
        keep = {str(h) for h in tiles.index[hit_pos]}
        keep |= {str(h) for h in tiles.index[undecidable.to_numpy()]}

    selected = [f for pid in keep if pid in by_id for f in by_id[pid]]

    if not selected:
        if not keep_schema_file:
            return []  # pure selection query — an empty answer is the answer
        # Nothing intersects: keep one file so the schema / _meta reads
        # downstream still have a source and the dask graph is well-formed.
        # The row-level clip decides what actually comes back.
        if logger:
            logger.info("No dataset partition intersects the region; keeping "
                        "one file for the schema read")
        return data_files[:1]

    if logger and len(selected) < len(data_files):
        logger.info(f"  Region selects {len(selected)}/{len(data_files)} partitions "
                    f"({kind.upper()} index)")
    return sorted(selected)


def _check_filters_supported(fmt, filters):
    """Reject ``filters=`` on formats with no predicate-pushdown reader.

    Only parquet carries row-group statistics. Silently ignoring the
    predicate on feather / gpkg would hand back unfiltered rows that the
    caller believes are filtered — the one failure mode worth a hard error.
    """
    if filters is not None and fmt != 'parquet':
        raise GediValidationError(
            f"filters= requires a parquet dataset (got '{fmt}'); predicate "
            f"pushdown has no equivalent in this format. Use query= instead.")


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
    region : str | list | GeoDataFrame | GeoSeries | shapely geometry, optional
        Spatial filter for clipping.
    lazy : bool
        If True, return Dask DataFrame. If False, return computed DataFrame.
    filters : list or pyarrow.compute.Expression, optional
        PyArrow predicate pushdown filters, applied per file at read time in
        both eager and lazy mode. Parquet only — see ``_check_filters_supported``.

    Returns
    -------
    dask GeoDataFrame or GeoDataFrame
    """
    from .cliutils import (detect_dataset_format, read_dataset_schema,
                           make_dataset_reader, _add_query_columns)

    # --- Eager mode ---
    if not lazy:
        # Single file
        if smart_isfile(path):
            ext = os.path.splitext(path)[1].lstrip('.').lower()
            fmt = ext if ext in ('parquet', 'feather', 'gpkg') else 'parquet'
            _check_filters_supported(fmt, filters)
            _, has_geo = read_dataset_schema(path, fmt)
            reader = make_dataset_reader(fmt, columns=columns, geo=has_geo,
                                         filters=filters)
            return reader(path)

        fmt = detect_dataset_format(path)
        _check_filters_supported(fmt, filters)
        data_files, fmt = _find_dataset_files(path, fmt)
        data_files = _select_dataset_files(data_files, region, dataset_path=path)
        _, has_geo = read_dataset_schema(data_files[0], fmt)

        if fmt == 'parquet':
            kwargs = {}
            if columns:
                kwargs['columns'] = columns
            if filters:
                kwargs['filters'] = filters
            result = _read_parquet_files(data_files, geo=has_geo, **kwargs)
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

        # Partition selection is coarse — clip to the exact region, as the
        # lazy branch does. Eager loads used to skip this entirely and hand
        # back unfiltered data.
        if region is not None:
            if isinstance(result, gpd.GeoDataFrame):
                from .utils import region_to_geometry
                result = result.clip(region_to_geometry(region))
            else:
                warnings.warn(
                    "region ignored: dataset has no geometry column to clip against",
                    stacklevel=2,
                )
        return result

    # --- Lazy mode ---
    fmt = detect_dataset_format(path)
    _check_filters_supported(fmt, filters)

    # Handle query-column expansion
    load_columns = columns
    query_only_cols = set()
    if query and columns:
        load_columns, query_only_cols = _add_query_columns(columns, query, path, fmt)

    data_files, fmt = _find_dataset_files(path, fmt)

    # Drop partitions the region cannot touch before building the graph —
    # one task per file, so this is the difference between reading a handful
    # of partitions and reading the whole dataset.
    data_files = _select_dataset_files(data_files, region, dataset_path=path)

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

    # Build reader and metadata. `filters` rides along in the reader closure,
    # so every per-file task in the from_map graph pushes the predicate down
    # to the parquet row-group stats — the rows never enter worker memory.
    reader = make_dataset_reader(fmt, columns=load_cols, geo=has_geometry,
                                 filters=filters)
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
        if 'geometry' in ddf.columns:
            from .utils import region_to_geometry
            ddf = ddf.clip(region_to_geometry(region))
        else:
            # Without a geometry column, dask's DataFrame.clip(lower=region)
            # would numerically clamp every value — never call it here.
            warnings.warn(
                "region ignored: dataset has no geometry column to clip against",
                stacklevel=2,
            )

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
    we use fsspec (via smart_open_columnar) to open files as file-like
    objects, and let pyarrow coalesce the column-chunk ranges it needs
    (``pre_buffer=True``) instead of paying fsspec's block read-ahead
    around every one of them — 358 MB vs 18 MB of server-side reads for a
    one-column projection of a 1.8 GB partition.
    """
    reader = gpd.read_parquet if geo else pd.read_parquet

    if isinstance(files, str):
        files = [files]

    remote = len(files) > 0 and is_remote_path(files[0])

    # Single file
    if len(files) == 1:
        if remote:
            with smart_open_columnar(files[0]) as fobj:
                return read_parquet_coalesced(fobj, geo=geo, **kwargs)
        return reader(files[0], **kwargs)

    # Multiple local files: pass list directly (PyArrow handles this)
    if not remote:
        return reader(files, **kwargs)

    # Multiple remote files: read each via fsspec, concat
    dfs = []
    for f in files:
        with smart_open_columnar(f) as fobj:
            dfs.append(read_parquet_coalesced(fobj, geo=geo, **kwargs))
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


def _combine_filters(base, extra):
    """AND two pyarrow predicate specs into one the parquet readers accept.

    Either side may be a conjunctive ``[(col, op, val), ...]`` list, a DNF
    ``[[...], [...]]`` list, or a ``pyarrow.compute.Expression``. Plain
    concatenation is only correct for two conjunctive lists — it silently
    produces a malformed spec when either side is DNF — so both are lifted
    to Expressions and combined with ``&``. ``pq.read_table`` and
    ``gpd.read_parquet`` both take an Expression wherever they take a list.
    """
    if extra is None or (isinstance(extra, (list, tuple)) and not extra):
        return base
    if base is None or (isinstance(base, (list, tuple)) and not base):
        return extra
    import pyarrow.parquet as pq
    return pq.filters_to_expression(base) & pq.filters_to_expression(extra)


def _read_parquet_bbox(path, *, bbox_4326, clip_box, columns, geo, strategy, lat_col, lon_col,
                       extra_filters=None):
    """Single-file bbox-filtered parquet read, routed by `strategy`.

    All three paths return a DataFrame whose rows satisfy the bbox predicate
    EXACTLY (for Point geometries). The first two prune row groups at the
    parquet-stats layer so the peak working set is bounded by the
    bbox-clipped result; the fallback materializes the full column-projected
    file before clipping in memory.

    ``extra_filters`` is a caller-supplied pyarrow predicate spec ANDed with
    whatever the strategy builds, so it is pushed down to the same row-group
    stats layer on every path (including the fallback, where only the bbox
    part degrades to an in-memory clip). Predicate columns need NOT appear in
    ``columns``: pyarrow reads them for the filter and drops them from the
    output. A file whose schema lacks a predicate column raises rather than
    silently returning unfiltered rows.
    """
    from contextlib import nullcontext

    # Remote files go through fsspec with the read-ahead cache off, and
    # pyarrow coalesces the ranges it actually needs (smart_open_columnar).
    remote = is_remote_path(path)
    source_ctx = smart_open_columnar(path) if remote else nullcontext(path)

    # Passed through to every reader below. Kept out of the call when absent
    # so the no-filters call shape is byte-for-byte the legacy one.
    xf = {'filters': extra_filters} if extra_filters is not None else {}

    with source_ctx as src:
        if strategy == 'point':
            # geopandas splices `bbox` and `filters` into one expression
            # (_splice_bbox_and_filters), so both are honored.
            if remote:
                return read_parquet_coalesced(src, columns=columns, geo=True,
                                              bbox=bbox_4326, **xf)
            return gpd.read_parquet(src, bbox=bbox_4326, columns=columns, **xf)

        if strategy == 'coord_filter':
            x0, y0, x1, y1 = bbox_4326
            filt = _combine_filters(
                [(lon_col, '>=', x0), (lon_col, '<=', x1),
                 (lat_col, '>=', y0), (lat_col, '<=', y1)],
                extra_filters)
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
            try:
                if remote:
                    df = read_parquet_coalesced(src, columns=cols, geo=geo, filters=filt)
                else:
                    reader = gpd.read_parquet if geo else pd.read_parquet
                    df = reader(src, columns=cols, filters=filt)
                if extras:
                    # unconditionally: a 0-row result still carries the helper
                    # columns, and leaking them desyncs the dask meta
                    df = df.drop(columns=extras, errors='ignore')
                return df
            except Exception as exc:
                # A schema-drifted file may lack the DB-wide predicate
                # columns (pyarrow raises ArrowInvalid: "No match for
                # FieldRef"). The bbox scanner degrades to a NULL envelope
                # for such files; the read path must degrade the same way —
                # fall through to the geometric full-read + clip below
                # instead of turning a region query into a hard crash.
                import pyarrow as pa
                if not isinstance(exc, (KeyError, pa.lib.ArrowInvalid)):
                    raise

        # 'fallback' — full read + in-memory geometric clip. Last resort.
        # `extra_filters` still pushes down here: only the bbox half of the
        # predicate degrades to an in-memory clip.
        df = (read_parquet_coalesced(src, columns=columns, geo=True, **xf) if remote
              else gpd.read_parquet(src, columns=columns, **xf))
        if len(df) > 0:
            df = df[df.geometry.intersects(clip_box)]
        return df


# =============================================================================
# Data-bbox index — root sidecar `_bbox_index.parquet`
# =============================================================================
# The GeoParquet bbox each partition file carries is the *padded partition
# polygon* bbox (h3_partition_bbox — cell math, no data scan), which is a
# geometric worst case, not the data envelope: measured 3.8x taller than the
# actual shots on a production file. The database therefore stores no
# information about where the data really sits, and every spatial query pays
# for it — 64% of the (tile x year-file) reads a representative EGI query
# schedules contain zero rows in the target tile.
#
# The true envelope is already on disk for free: parquet row-group statistics
# of the lat/lon columns. This index materializes them once (footer-only
# scan, minutes for a continental DB) into one compact root sidecar, and the
# query paths use it to skip files a-priori — pillar 4: never do work the
# structure of the data already answers.
#
# Consumers are strictly fail-safe: a file missing from the index, a NULL
# bbox (incomplete stats), or a missing/unreadable sidecar all mean "keep
# the file / fall back to the unindexed path". Staleness is impossible by
# construction — a stale index would silently under-select (the one
# unacceptable failure), absence only costs speed — via three layers:
# `_merge_and_finalize` deletes the index at merge ENTRY (before the first
# partition write, so a killed build cannot leave one behind),
# `gh3_doctor --fix` drops it after any applied remedy, and
# `_load_bbox_index` ignores any index older than the build log (every
# producer saves the log — the O(1) guard subsumes forgotten hooks).

def _bbox_cols_from_meta(gh3_dir):
    """lat/lon predicate columns from the cached schema (a-priori, no I/O)."""
    names = gh3_read_meta('h3_columns', gh3_root_dir=gh3_dir) or []
    for lat, lon in (('lat_lowestmode_l2a', 'lon_lowestmode_l2a'),
                     ('lat_lowestmode', 'lon_lowestmode')):
        if lat in names and lon in names:
            return lat, lon
    return None, None


def _bbox_index_key(path):
    """Normalize a partition parquet path to its index key: the last three
    segments (``h3_XX=<cell>/year=<Y>/<file>.parquet``). Stable across local
    roots, URLs and OS separators."""
    return '/'.join(str(path).replace(os.sep, '/').rstrip('/').split('/')[-3:])


def _bbox_disjoint(b, bbox):
    """True when data bbox ``b`` cannot overlap query bbox ``bbox`` (both
    (minx, miny, maxx, maxy), inclusive edges — touching is NOT disjoint)."""
    return b[0] > bbox[2] or b[2] < bbox[0] or b[1] > bbox[3] or b[3] < bbox[1]


def _scan_file_bbox(item):
    """Worker: one partition file -> bbox-index record (footer-only read).

    Aggregates row-group min/max statistics of the lat/lon columns. Any row
    group without complete stats yields a NULL bbox — consumers treat NULL
    as "unknown, keep the file". NaN coordinates are excluded from parquet
    stats by the writer, matching the existing `coord_filter` pushdown
    semantics (a NaN-coordinate row never satisfies the predicates today).
    """
    path, rel, lat_col, lon_col = item
    import pyarrow.parquet as pq
    rec = {'file': rel, 'lon_min': None, 'lat_min': None,
           'lon_max': None, 'lat_max': None, 'n_rows': 0}
    try:
        md = pq.ParquetFile(path).metadata
        rec['n_rows'] = md.num_rows
        names = {md.schema.column(i).name: i for i in range(md.num_columns)}
        if lat_col not in names or lon_col not in names:
            return rec
        li, oi = names[lat_col], names[lon_col]
        lat_lo = lat_hi = lon_lo = lon_hi = None
        for rg in range(md.num_row_groups):
            s_lat = md.row_group(rg).column(li).statistics
            s_lon = md.row_group(rg).column(oi).statistics
            if not (s_lat and s_lon and s_lat.has_min_max and s_lon.has_min_max):
                return rec
            lat_lo = s_lat.min if lat_lo is None else min(lat_lo, s_lat.min)
            lat_hi = s_lat.max if lat_hi is None else max(lat_hi, s_lat.max)
            lon_lo = s_lon.min if lon_lo is None else min(lon_lo, s_lon.min)
            lon_hi = s_lon.max if lon_hi is None else max(lon_hi, s_lon.max)
        if lat_lo is not None:
            rec.update(lon_min=float(lon_lo), lat_min=float(lat_lo),
                       lon_max=float(lon_hi), lat_max=float(lat_hi))
    except Exception:
        pass  # unreadable footer -> unknown bbox (fail-safe: keep the file)
    return rec


def gh3_build_bbox_index(source=None):
    """Build the ``_bbox_index.parquet`` root sidecar for an H3 database.

    One row per partition year-file with the true data envelope derived
    from existing parquet row-group statistics — footer-only reads, no data
    scan (~minutes for a continental database; seconds with a dask client).
    Query paths (`gh3_load` with ``region=``, `egi_load`) then skip files
    whose envelope cannot intersect the query a-priori.

    Uses the registered dask client when one exists (`parallel_map`);
    otherwise a local thread pool — footer reads are tiny and I/O bound.
    Local databases only: the index is written at the database root.

    Returns the index path.
    """
    from .config import BBOX_INDEX_FILENAME
    from .utils import atomic_parquet_write

    gh3_dir = source or GH3_DEFAULT_H3_DIR
    if is_remote_path(gh3_dir):
        raise GediValidationError(
            "The bbox index is written at the database root - build it where "
            "the database lives (local path), then read it from anywhere.")

    h3_part = gh3_read_meta('h3_partition_level', gh3_root_dir=gh3_dir)
    part_col = f"h3_{int(h3_part):02d}"
    lat_col, lon_col = _bbox_cols_from_meta(gh3_dir)
    if not lat_col:
        raise GediValidationError(
            "Database schema has no lat/lon predicate columns "
            "(lat_lowestmode[_l2a]); cannot build a bbox index.")

    files = smart_glob(smart_join(gh3_dir, f'{part_col}=*/year=*/*.parquet'))
    if not files:
        files = smart_glob(smart_join(gh3_dir, f'{part_col}=*/**/*.parquet'),
                           recursive=True)
    if not files:
        raise GediDatabaseNotFoundError(f"No partition parquet files in {gh3_dir}")

    items = [(f, _bbox_index_key(f), lat_col, lon_col) for f in files]

    from .utils import get_dask_client
    client = get_dask_client()

    records = []
    if client is not None:
        from .parallel import parallel_map
        # batch: one task per file would swamp the scheduler at 10^5+ files
        for _item, res in parallel_map(items, _scan_file_bbox,
                                       desc='bbox index', unit='file',
                                       batch_size=256):
            if isinstance(res, Exception):
                raise res
            records.append(res)
    else:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(32, os.cpu_count() or 4)) as ex:
            records = list(ex.map(_scan_file_bbox, items))

    index_df = pd.DataFrame.from_records(records).sort_values('file')
    index_df = index_df.astype({'file': 'string', 'n_rows': 'int64'})
    opath = os.path.join(gh3_dir, BBOX_INDEX_FILENAME)
    atomic_parquet_write(index_df.reset_index(drop=True), opath)
    _BBOX_INDEX_CACHE.pop(opath, None)
    return opath


def refresh_bbox_index_after_build(gh3_dir, enabled=True, logger=None):
    """Best-effort bbox-index (re)build for a just-completed build.

    MUST run after the final build-log save: ``_load_bbox_index`` treats
    any index older than the log as stale, so an index written before the
    COMPLETED save would be silently ignored. The index is a derived
    artifact — failure here never fails the build (absence only costs
    speed); the warning points at the manual ``gh3_bbox_index`` recovery.
    Returns the index path, or ``None`` when disabled or failed.
    """
    if not enabled:
        return None
    try:
        opath = gh3_build_bbox_index(gh3_dir)
        if logger:
            logger.info(f"BBox index written to {opath}")
        return opath
    except Exception as exc:
        if logger:
            logger.warning(
                f"BBox index build skipped ({type(exc).__name__}: {exc}) — "
                f"queries fall back to the unindexed path; run gh3_bbox_index "
                f"manually to restore the speedup.")
        return None


def invalidate_bbox_index(gh3_dir):
    """Delete the bbox-index sidecar when partition data may have changed.

    The producer-side half of the index contract: a stale index silently
    under-selects (the one unacceptable failure), an absent one only costs
    speed. Called by ``_merge_and_finalize`` (before its manifest write —
    unlinking a root sidecar bumps the root dir mtime) and by
    ``gh3_doctor --fix`` after any applied remedy. Idempotent; returns
    True when a sidecar was actually removed.
    """
    from .config import BBOX_INDEX_FILENAME
    path = os.path.join(gh3_dir, BBOX_INDEX_FILENAME)
    _BBOX_INDEX_CACHE.pop(path, None)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


_BBOX_INDEX_CACHE = {}  # index path -> (stamp, mapping-or-None)


def _load_bbox_index(gh3_dir):
    """Read the bbox-index sidecar -> ``{rel_key: (lon0, lat0, lon1, lat1)}``.

    Rows with a NULL bbox are omitted (unknown -> caller keeps the file).
    Returns ``None`` when the sidecar is absent or unreadable — callers fall
    back to the unindexed path — or when the build log is NEWER than the
    index (local roots): every producer that changes the database saves the
    log, so this O(1) stat self-guards against any invalidation hook a
    future writer forgets. Rebuild with ``gh3_bbox_index`` after any build
    or update. Cached per root: local entries revalidate on
    ``(mtime_ns, size, inode)``; remote entries (including absence) are held
    for the process, same doctrine as ``json_read_cached``.
    """
    from .config import BBOX_INDEX_FILENAME, BUILD_LOG_FILENAME
    path = smart_join(gh3_dir, BBOX_INDEX_FILENAME)
    remote = is_remote_path(path)
    stamp = None
    if not remote:
        try:
            st = os.stat(path)
            stamp = (st.st_mtime_ns, st.st_size, st.st_ino)
        except OSError:
            _BBOX_INDEX_CACHE.pop(path, None)
            return None
        # Self-guard (O(1)): every producer that changes the database saves
        # the build log, so an index older than the log may describe stale
        # envelopes — treat it as absent rather than risk under-selection.
        # This subsumes any invalidation hook a future writer forgets.
        try:
            log_mtime = os.stat(os.path.join(gh3_dir, BUILD_LOG_FILENAME)).st_mtime_ns
            if log_mtime > st.st_mtime_ns:
                return None
        except OSError:
            pass  # no build log locally — nothing to compare against
    cached = _BBOX_INDEX_CACHE.get(path)
    if cached is not None and cached[0] == stamp:
        return cached[1]

    mapping = None
    try:
        if remote:
            from .utils import smart_open_columnar
            with smart_open_columnar(path) as f:
                idx = pd.read_parquet(f)
        else:
            idx = pd.read_parquet(path)
        idx = idx.dropna(subset=['lon_min', 'lat_min', 'lon_max', 'lat_max'])
        mapping = {r.file: (r.lon_min, r.lat_min, r.lon_max, r.lat_max)
                   for r in idx.itertuples()}
    except Exception:
        mapping = None  # absent/unreadable -> unindexed path
    _BBOX_INDEX_CACHE[path] = (stamp, mapping)
    return mapping


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


def gh3_load_hex(d, _bbox_index=None, part_col=None, _storage_cfg=None, **kwargs):
    _restore_storage_on_worker(_storage_cfg)

    # Region pushdown plumbing (driver-set, see _load_h3_database):
    # `_bbox_4326` switches the per-file read to _read_parquet_bbox so the
    # region prefilter happens at the parquet-stats layer instead of after a
    # full read; `_bbox_index` (data-envelope per year file, from the
    # `_bbox_index.parquet` sidecar) skips files that provably cannot
    # intersect the region without opening them at all. Both are strict
    # supersets of the exact clip applied downstream — results unchanged.
    # `_bbox_index` is the SECOND from_map iterable (one small per-partition
    # dict per task), never a broadcast kwarg — from_map re-serializes
    # kwargs into every task, which would ship the whole index N times on
    # continental queries (pillar 1: no driver-side fan-out cost).
    bbox4326 = kwargs.pop('_bbox_4326', None)
    bbox_strategy = kwargs.pop('_bbox_strategy', None) or ('fallback', None, None)
    bbox_index = _bbox_index if _bbox_index is not None else kwargs.pop('_bbox_index', None)

    files = smart_glob(smart_join(d, '**/*.parquet'), recursive=True)
    cols = kwargs.get('columns')
    use_geo = cols is None or 'geometry' in cols

    clip_geom = None
    if bbox4326 is not None:
        from shapely.geometry import box as _box
        clip_geom = _box(*bbox4326)

    def _read_one(f):
        if bbox4326 is not None:
            strategy, lat_col, lon_col = bbox_strategy
            # The caller's pyarrow predicate (set by _load_h3_database from
            # `gh3_load(filters=...)`) is ANDed with the bbox predicate
            # inside the reader — both are pushed down, neither is dropped.
            return _read_parquet_bbox(
                f, bbox_4326=bbox4326, clip_box=clip_geom,
                columns=cols, geo=use_geo,
                strategy=strategy, lat_col=lat_col, lon_col=lon_col,
                extra_filters=kwargs.get('filters'))
        return _read_parquet_files([f], geo=use_geo, **kwargs)

    # Per-file read so we can attach the `year` hive partition column from
    # each file's path. pd.read_parquet on a LIST of files does NOT
    # reconstruct hive partition columns (only a directory read or
    # pyarrow.dataset would), so a list read would return data without
    # `year` while the synthetic Dask meta (built from h3_columns_dtypes)
    # always includes it — producing a "Missing: ['year']" mismatch on
    # every .compute(). Reading per file is the same I/O the list-read
    # would do internally; the only overhead is N small open() calls.
    parts = []
    skipped = []
    for f in files:
        if bbox4326 is not None and bbox_index:
            b = bbox_index.get(_bbox_index_key(f))
            if b is not None and _bbox_disjoint(b, bbox4326):
                skipped.append(f)
                continue
        sub = _read_one(f)
        if 'year' not in sub.columns and sub.index.name != 'year':
            m = _YEAR_HIVE_RE.search(str(f))
            if m:
                sub['year'] = np.int32(m.group(1))
        parts.append(sub)

    if len(parts) == 0 and skipped:
        # Every file was index-skipped. Produce the correct EMPTY frame by
        # reading one skipped file through the same bbox path — its row
        # groups all prune, so this is a footer-only read with the right
        # schema (columns, dtypes, index).
        sub = _read_one(skipped[0])
        m = _YEAR_HIVE_RE.search(str(skipped[0]))
        if 'year' not in sub.columns and sub.index.name != 'year' and m:
            # explicit dtype: scalar assignment on a 0-row frame does not
            # reliably preserve int32, and the dask meta expects it
            sub['year'] = np.full(len(sub), np.int32(m.group(1)), dtype='int32')
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
        if not h3_ids:
            # Region touches no partition. Keep one so the schema read and
            # the dask graph stay well-formed (from_map rejects an empty
            # list); the bbox pushdown + exact clip below then produce the
            # correct empty result — same doctrine as _select_dataset_files.
            _all_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir) or []
            if not _all_ids:
                raise GediDatabaseNotFoundError(
                    f"No partitions listed in the build log of {gh3_dir}")
            h3_ids = sorted(_all_ids)[:1]
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

        # Region bbox pushdown: filter rows at the parquet-stats layer inside
        # each partition read instead of full-read + clip-later, and skip
        # whole year files a-priori via the `_bbox_index.parquet` data
        # envelopes when the sidecar exists. Both are supersets of the exact
        # `ddf.clip(region)` applied below, so results are unchanged.
        # Applies unconditionally, including alongside a caller-supplied
        # `filters`: `_read_parquet_bbox` ANDs the two predicates together
        # (`_combine_filters`) on every strategy, so neither is dropped.
        # Before that existed, region + filters silently fell back to a full
        # read + in-memory clip — the one combination that lost the pushdown.
        _per_dir_bbox = None
        if region is not None:
            from .utils import region_to_geometry
            _bbox = tuple(region_to_geometry(region).bounds)
            _lat, _lon = _bbox_cols_from_meta(gh3_dir)
            fm_filter['_bbox_4326'] = _bbox
            fm_filter['_bbox_strategy'] = (
                ('coord_filter', _lat, _lon) if _lat else ('fallback', None, None))
            _idx = _load_bbox_index(gh3_dir)
            if _idx:
                # Group by partition dir and align one small dict per task
                # (second from_map iterable). A kwarg would be re-serialized
                # into EVERY task — the whole index x N on continental
                # queries, the exact driver fan-out cost pillar 1 forbids.
                _by_dir = {}
                for _k, _v in _idx.items():
                    _by_dir.setdefault(_k.split('/', 1)[0], {})[_k] = _v
                _per_dir_bbox = [
                    _by_dir.get(os.path.basename(_d.rstrip('/'))) or None
                    for _d in h3_dirs]
                if not any(_per_dir_bbox):
                    _per_dir_bbox = None

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
            _meta = gh3_load_hex(h3_dirs[0],
                                 _per_dir_bbox[0] if _per_dir_bbox else None,
                                 part_col=h3_part_col, **fm_filter)
        if _per_dir_bbox is not None:
            ddf = dask.dataframe.from_map(gh3_load_hex, h3_dirs, _per_dir_bbox,
                                          part_col=h3_part_col, **fm_filter,
                                          meta=_meta)
        else:
            ddf = dask.dataframe.from_map(gh3_load_hex, h3_dirs,
                                          part_col=h3_part_col, **fm_filter,
                                          meta=_meta)
        if 'geometry' in ddf.columns:
            ddf = dask_geopandas.from_dask_dataframe(ddf, geometry='geometry')
    else:
        # DEPRECATED — slated for removal, along with the `from_map` argument
        # itself. This is the original dask_geopandas.read_parquet path; the
        # from_map branch above superseded it and is the default, so this one
        # never received the work the primary path did. It reads `_metadata`
        # (the cost from_map exists to avoid on databases with thousands of
        # partitions), it has no region bbox pushdown and no `_bbox_index`
        # file skipping, and `filters` here still drive dask's own hive
        # partition pruning — which is why the region + user predicate
        # combination is left as plain list concatenation instead of the
        # `_combine_filters` expression the from_map path uses (an Expression
        # may not prune h3_part_col the same way). Do not extend it; when
        # `from_map=False` goes, this whole branch goes with it.
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
        # Same normalizer as the partition selection above — a region that
        # selects partitions must also be clippable (string/bbox/geo forms).
        from .utils import region_to_geometry
        mask = gpd.GeoDataFrame(geometry=[region_to_geometry(region)], crs=4326)
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
        If None, falls back to default H3 directory. Self-hosted S3 sources
        may carry their endpoint in the URL (``s3://host:port/bucket/...``);
        an endpoint already configured via ``configure_storage`` wins.
    columns : list, optional
        Columns to load.
    region : str | list | GeoDataFrame | GeoSeries | shapely geometry, optional
        Spatial filter.
    query : str, optional
        Pandas query string for filtering.
    from_map : bool
        Use from_map loading for H3 databases (default True). DEPRECATED —
        ``from_map=False`` selects the original ``dask_geopandas.read_parquet``
        path, which is unmaintained and slower on every axis (reads
        ``_metadata``, no region bbox pushdown, no ``_bbox_index`` file
        skipping). The argument and that branch are slated for removal; leave
        it at the default.
    lazy : bool
        If True (default), return Dask DataFrame. If False, return computed
        pandas DataFrame.
    filters : list or pyarrow.compute.Expression, optional
        PyArrow predicate pushdown filters (conjunctive list of
        ``(column, op, value)`` tuples, a DNF list-of-lists, or an Expression),
        applied as per-file row-group pushdown during the read. Works for H3
        databases and simplified parquet datasets, and combines (AND) with
        ``region`` when both are given — the region bbox prefilter stays on,
        both predicates are pushed to the same row-group stats layer.
        Predicate columns do not have to be listed in ``columns``.

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
        # Determine output filename from partition ID.
        #
        # INVARIANT the naming below relies on: this dask partition holds
        # rows of exactly ONE spatial partition, so the first row's cell
        # names them all. That holds for the gedih3 pipeline (from_map
        # gives one dask partition per H3 partition dir) but NOT for a
        # repartitioned/shuffled frame — there the name describes only
        # some rows, and `_select_dataset_files` refuses to prune such
        # datasets (see `_dataset_prune_is_safe`: it requires the sidecar
        # partition level to be strictly coarser than the index level).
        # Use group_by_partition=True for frames that may mix partitions.
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

    # Raster formats go through the rasterization pipeline rather than the
    # per-format file writers. There is one rasterization implementation, so
    # both index types behave the same: EGI used to have its own raster branch
    # inside _write_egi_file (bypassing the outer-tile invariant and the VRT)
    # while H3 had none at all and raised "Unsupported export format: tif".
    from .raster import RASTER_FORMATS
    if fmt in RASTER_FORMATS:
        result = gh3_rasterize(
            ddf, output, merge=merge, fmt=fmt, index_type=index_type,
            partition_level=(h3_partition_level or naming_partition_level
                             if index_type == 'h3' else None),
            show_progress=show_progress,
        )
        ofiles = [result] if merge else list(result)
        if not ofiles:
            raise GediProcessingError("No output files were created.")
        if write_metadata and not merge:
            if h3_partition_level is not None:
                metadata_kwargs.setdefault('h3_partition_level', h3_partition_level)
            gh3_write_dataset_meta(
                opath=output, index_type=index_type or 'unknown',
                index_level=index_level, columns=list(ddf.columns),
                source_database=source_database, tool=tool, file_format=fmt,
                **metadata_kwargs
            )
        return ofiles

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

    # Get H3 partition info
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir)

    # Normalize the region through the shared normalizer so the EGI path
    # accepts exactly what gh3_load accepts (vector path, "W,S,E,N", bbox
    # list, GeoDataFrame/GeoSeries, shapely) and is always EPSG:4326 —
    # keeping the two index types from drifting apart on region handling.
    region_gdf = None
    if region is not None:
        from .utils import region_to_geometry
        try:
            region_gdf = gpd.GeoDataFrame(
                geometry=[region_to_geometry(region)], crs=4326
            )
        except TypeError as exc:
            raise GediValidationError(str(exc))

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

    # Restrict the H3 geometry build to partitions the selected tiles can
    # possibly touch (a-priori cell math instead of constructing 10k+
    # polygons and a global sindex). EPSG:6933 is per-axis monotonic in
    # lon/lat, so the tiles' axis-aligned total_bounds maps to an EXACT
    # axis-aligned 4326 box; intersect_h3_geometries' ring-1 then yields a
    # provable superset of everything egi_h3_intersection can select —
    # tile-intersecting cells are box-intersecting cells, their ring-1
    # neighbors are within ring-1 of the box set, and both are restricted
    # to the same DB partition list. Result identical, cost proportional
    # to the region instead of the database.
    if region_gdf is not None and h3_ids and len(egi_tiles):
        try:
            from shapely.geometry import box as _box
            from pyproj import Transformer as _Transformer
            _t = _Transformer.from_crs('EPSG:6933', 'EPSG:4326', always_xy=True)
            _x0, _y0, _x1, _y1 = egi_tiles.total_bounds
            _lon0, _lat0 = _t.transform(_x0, _y0)
            _lon1, _lat1 = _t.transform(_x1, _y1)
            _cand = intersect_h3_geometries(_box(_lon0, _lat0, _lon1, _lat1),
                                            h3_ids=h3_ids, expand_ring=1)
            if _cand:
                h3_ids = _cand
        except Exception:
            # Pure optimization: degenerate tile sets (all-clamped at the
            # grid edge -> NaN bounds) must not turn into a GEOSException;
            # fall back to the full partition list.
            pass

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
                            bbox_strategy='fallback', bbox_lat_col=None, bbox_lon_col=None,
                            file_bboxes=None, filters=None):
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
    file_bboxes : dict, optional
        ``{rel_key: (lon0, lat0, lon1, lat1)}`` data envelopes from the
        ``_bbox_index.parquet`` sidecar, restricted to this task's
        partitions. Year files whose envelope cannot intersect the tile are
        skipped without being opened. Files absent from the dict (or the
        dict being ``None``) are always read — fail-safe.
    filters : list or pyarrow.compute.Expression, optional
        Pyarrow predicate pushed into each year-file read (ANDed with the
        tile's bbox predicate), so non-matching row groups are never
        decompressed. Applied BEFORE ``query``, EGI indexing and the
        spillover filter.

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
            # A-priori skip: the bbox-index data envelope proves this year
            # file holds no shot inside the tile — don't even open it.
            # (64% of the reads a representative EGI query schedules are
            # such dead reads; the ring-1 partitions they mostly belong to
            # rescue only ~0.03% of rows.)
            if file_bboxes:
                b = file_bboxes.get(_bbox_index_key(pf))
                if b is not None and _bbox_disjoint(b, wgs84_bbox):
                    continue
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
                extra_filters=filters,
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
        # Footer-only read: skip fsspec's block read-ahead entirely
        with smart_open_columnar(parquet_file) as fobj:
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
                               index_level=1, partition_level=12, filters=None):
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

    # When no explicit column list is requested, resolve it to the concrete
    # data-column list from the DB schema. Passing an explicit list into the
    # per-file read below suppresses geopandas' hive-partition inference —
    # ``gpd.read_parquet(file)`` with ``columns=None`` injects the ``h3_03``
    # and ``year`` partition columns from the directory path, which then
    # appear in every computed partition but are absent from the Dask
    # ``_meta`` (built via ``pq.read_schema`` on a single file, which never
    # sees them). That schema mismatch either leaks H3 partition artifacts
    # into EGI output or trips a meta-mismatch error in a downstream op.
    # Resolving to the explicit data columns routes columns=None through the
    # same always-correct machinery as an explicit request. (See
    # tests/test_egi_load_meta.py.)
    if columns is None:
        _schema_data_cols = _get_schema_columns(None, gh3_dir, exclude_geometry=True)[1]
        columns = filter_data_columns(_schema_data_cols, exclude_geometry=True)

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

    # Per-task data envelopes from the bbox-index sidecar (None when absent).
    # Group once by partition dir, then hand each task ONLY its partitions'
    # entries — tiny inlined dicts, no client.scatter (established doctrine:
    # see the streaming-build driver and _build_add_variables).
    _bbox_idx = _load_bbox_index(gh3_dir)
    _idx_by_dir = {}
    for _k, _v in (_bbox_idx or {}).items():
        _idx_by_dir.setdefault(_k.split('/', 1)[0], {})[_k] = _v

    def _task_bboxes(h3_list):
        if not _bbox_idx:
            return None
        out = {}
        for _h in h3_list:
            out.update(_idx_by_dir.get(f"{h3_part_col}={_h}", {}))
        return out or None

    # Build list of (egi_id, h3_list, egi_bbox, file_bboxes) tuples for from_map
    tile_args = [
        (egi_id, h3_list, egi_tiles.loc[egi_id, 'geometry'].bounds,
         _task_bboxes(h3_list))
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
        egi_id, h3_list, egi_bbox, file_bboxes = args
        return _load_egi_tile_from_h3(
            egi_bbox, h3_list, gh3_dir, h3_part_col, load_cols,
            query, index_level, partition_level, set_index=True,
            tile_egi_id=egi_id,
            bbox_strategy=bbox_strategy,
            bbox_lat_col=bbox_lat_col,
            bbox_lon_col=bbox_lon_col,
            file_bboxes=file_bboxes,
            filters=filters,
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
             index_level=1, partition_level=12, lazy=True, filters=None):
    """Load EGI-indexed GEDI data from any source.

    Auto-detects whether the source is an H3 database (direct EGI loading)
    or a simplified EGI dataset and loads accordingly.

    Parameters
    ----------
    source : str, optional
        Path to data source (H3 database or EGI dataset).
        If None, falls back to default H3 directory. Self-hosted S3 sources
        may carry their endpoint in the URL (``s3://host:port/bucket/...``);
        an endpoint already configured via ``configure_storage`` wins.
    columns : list, optional
        Columns to load.
    region : str | list | GeoDataFrame | GeoSeries | shapely geometry, optional
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
    filters : list or pyarrow.compute.Expression, optional
        PyArrow predicate pushdown filters (conjunctive list of
        ``(column, op, value)`` tuples, a DNF list-of-lists, or an
        Expression) — same contract as ``gh3_load(filters=...)``. Pushed
        straight into each per-year parquet read, so row groups that cannot
        match are never decompressed, and ANDed with the tile bbox predicate
        when both apply. Unlike ``query``, which filters in pandas after the
        read, the rows never enter worker memory. Predicate columns do not
        have to be listed in ``columns``. A file whose schema lacks a
        predicate column raises rather than silently returning unfiltered
        rows.

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

    >>> # Predicate pushdown: quality-flag rows are dropped at the parquet
    >>> # row-group layer, before anything reaches worker memory.
    >>> ddf = gh3.egi_load(
    ...     source='/path/to/h3_database',
    ...     columns=['agbd_l4a'],
    ...     region='region.shp',
    ...     filters=[('l2_quality_flag_l4a', '==', 1), ('agbd_l4a', '>', 0)],
    ... )
    """
    path, info = _detect_source(source)
    columns = _resolve_columns(columns, path, info)

    if info['source_type'] == 'h3_database':
        # Direct EGI loading from H3 database (no shuffle)
        ddf = _load_egi_from_h3_database(
            columns=columns, region=region, query=query,
            gh3_dir=path, index_level=index_level, partition_level=partition_level,
            filters=filters
        )
    elif info.get('index_type') == 'egi':
        # Simplified EGI dataset
        ddf = _load_dataset(path, columns=columns, query=query, region=region,
                            lazy=True, filters=filters)
    elif info.get('index_type') == 'h3':
        raise GediValidationError(
            f"Source '{path}' is an H3 dataset. Use gh3_load() for H3 data, "
            f"or load from an H3 database with egi_load() for direct EGI conversion."
        )
    else:
        # Parquet directory with unknown index — try loading as dataset
        ddf = _load_dataset(path, columns=columns, query=query, region=region,
                            lazy=True, filters=filters)

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
        output targets the right tile without inference. When None and a
        raster format is requested, ``geodf_to_raster`` requires the data to
        resolve to a single outer tile and raises ``GediRasterizationError``
        on genuine multi-tile input (single-file merge mode) — split by tile
        or pass ``outer_tile`` rather than silently dropping pixels.

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
    Convert a single spatially-indexed GeoDataFrame to raster.

    Dispatches on the frame's spatial index: H3 frames go through
    ``raster.h3_to_raster``, EGI frames through ``egi.geodf_to_raster``.

    This handles ONE in-memory frame. To rasterize every partition of a Dask
    DataFrame — or a dataset directory — in a single call, use
    :func:`gh3_rasterize`.

    Parameters
    ----------
    gdf : GeoDataFrame
        H3- or EGI-indexed GeoDataFrame.
    columns : list of str, optional
        Columns to rasterize. If None, all numeric columns.
    output_path : str, optional
        If provided, save raster to this path
    compress : str
        Compression method for GeoTIFF

    Returns
    -------
    xr.Dataset
        Raster dataset. The CRS follows the index type: EPSG:4326 for H3,
        EPSG:6933 (EASE-Grid 2.0) for EGI. EGI rasters are never reprojected
        — that alignment is the reason EGI exists.

    Raises
    ------
    GediValidationError
        If the spatial index type cannot be determined from the frame.
    GediRasterizationError
        Propagated from the rasterizer. Notably, an EGI frame spanning more
        than one level-12 outer tile is refused rather than silently reduced
        — use ``gh3_rasterize(..., merge=True)`` for that.

    Examples
    --------
    >>> # Rasterize aggregated data
    >>> raster = gh3_to_raster(agg_gdf)
    >>> raster.rio.to_raster("output.tif")
    >>>
    >>> # Or save directly
    >>> raster = gh3_to_raster(agg_gdf, output_path="output.tif")

    See Also
    --------
    gh3_rasterize : Dask DataFrame or dataset directory to rasters, one call.
    """
    index_type = get_spatial_index_type(gdf)

    if index_type == 'egi':
        from . import egi
        xras = egi.geodf_to_raster(gdf, columns=columns)
    elif index_type == 'h3':
        from .raster import h3_to_raster
        xras = h3_to_raster(gdf, columns=columns)
    else:
        raise GediValidationError(
            "Cannot determine the spatial index type of the input frame: no 'h3_*' or "
            "'egi*' index name or column found. Pass an H3- or EGI-indexed GeoDataFrame."
        )

    if output_path:
        from .raster import export_raster
        export_raster(xras, output_path, compress=compress)

    return xras


def _h3_partition_level_from_dataset(path, info):
    """Resolve the H3 partition level of a simplified dataset directory.

    ``cliutils.get_dataset_index_info`` already covers the sidecar
    ``h3_partition_level`` and the ``partition_ids[0]`` fallback; this adds the
    two steps it does not: deriving the level from the parquet filenames, and
    cross-checking a sidecar value against them.

    Filenames are ground truth — a sidecar can go stale when a dataset is
    regenerated in place at another level, and a wrong partition level produces
    silently wrong tiling rather than an error. One local basename is free to
    check; remote paths keep the sidecar fast path.

    Parameters
    ----------
    path : str
        Dataset directory.
    info : dict
        The ``get_dataset_index_info`` result for ``path``.

    Returns
    -------
    tuple
        ``(level, source_label)``. ``level`` is None when undeterminable, in
        which case the rasterizer falls back to detecting it from the frame's
        ``h3_NN`` columns.
    """
    import logging
    logger = logging.getLogger(__name__)

    partition_level = info.get('partition_level')
    source = 'metadata'

    if partition_level is None:
        candidates = [
            os.path.splitext(os.path.basename(f))[0]
            for f in smart_glob(smart_join(path, '*.parquet'))
        ]
        source = 'filenames'
        for cell in candidates:
            try:
                partition_level = h3.get_resolution(cell)
                break
            except (ValueError, TypeError):
                continue
    elif not is_remote_path(path):
        for f in smart_glob(smart_join(path, '*.parquet'))[:1]:
            cell = os.path.splitext(os.path.basename(f))[0]
            try:
                actual = h3.get_resolution(cell)
            except (ValueError, TypeError):
                break
            if actual != partition_level:
                logger.warning(
                    f"Sidecar h3_partition_level={partition_level} disagrees with "
                    f"filenames (H3 {actual}); using the filenames"
                )
                partition_level = actual
                source = 'filenames (sidecar stale)'

    return partition_level, source


def _egi_partition_level(ddf, info):
    """Resolve the EGI level a frame or dataset is partitioned at.

    The sidecar knows it for a dataset; for a bare frame the coarsest ``egiNN``
    column is the partition column (see ``_detect_export_params``). Returns
    None when neither is available, which leaves the rasterizer on its
    level-12 default.

    Parameters
    ----------
    ddf : DataFrame
        Dask or in-memory EGI-indexed frame.
    info : dict
        ``get_dataset_index_info`` result, or ``{}`` for frame input.

    Returns
    -------
    int or None
    """
    level = info.get('egi_partition_level') or info.get('partition_level')
    if level is not None:
        return int(level)

    _, part_col, _, _ = _detect_export_params(ddf, index_type='egi')
    if part_col:
        return int(str(part_col).replace('egi', ''))
    return None


def gh3_rasterize(data, output, columns=None, merge=False, query=None,
                  index_type=None, partition_level=None, fmt='tif',
                  compress='LZW', show_progress=True):
    """
    Rasterize a dataset or Dask DataFrame to GeoTIFF in a single call.

    Works for both spatial index types: EGI partitions are rasterized with
    ``egi.rasterize_partition`` (EASE-Grid 2.0 aligned, EPSG:6933) and H3
    partitions with ``raster.rasterize_h3_partition`` (EPSG:4326). The index
    type is detected, so callers do not pick a rasterizer.

    This is the library form of the ``gh3_rasterize`` CLI, which delegates to
    it — the two cannot diverge.

    Parameters
    ----------
    data : str or DataFrame
        Either a dataset directory written by ``gh3_extract`` / ``gh3_aggregate``
        (i.e. holding a ``gedih3_dataset.json`` sidecar), or an already-loaded
        Dask / in-memory (Geo)DataFrame. A raw H3 *database* is not accepted —
        aggregate or extract from it first.
    output : str
        Output directory (``merge=False``) or output file path (``merge=True``,
        a missing ``.tif`` suffix is appended).
    columns : list of str, optional
        Columns to rasterize; None means every numeric column. fnmatch
        wildcards (e.g. ``'agbd_*'``) are expanded for dataset-path input; for
        a frame the caller already knows its columns.
    merge : bool
        False (default) writes one GeoTIFF per spatial tile plus a
        ``mosaic.vrt``. True merges every tile into a single GeoTIFF (no VRT);
        note this gathers all tiles on the driver.
    query : str, optional
        Pandas query applied at load time. Dataset-path input only — for a
        frame, call ``.query()`` before passing it in.
    index_type : {'h3', 'egi'}, optional
        Override the detected index type.
    partition_level : int, optional
        H3 tiling level. Ignored for EGI, which always tiles at level 12. When
        None it is resolved from the dataset (path input) or from the frame's
        ``h3_NN`` columns.
    fmt : str
        Tile format ('tif', 'nc'). Ignored when ``merge=True``, which always
        writes GeoTIFF.
    compress : str
        GeoTIFF compression.
    show_progress : bool
        Show the Dask progress bar.

    Returns
    -------
    str or list of str
        The written file path when ``merge=True``; otherwise a flat list of
        the written tile paths (``mosaic.vrt`` is not included).

    Raises
    ------
    GediValidationError
        If ``data`` is an H3 database, if the index type cannot be determined,
        or if ``query`` is given with an already-loaded frame.
    GediRasterizationError
        Propagated from the rasterizers (e.g. nothing valid to merge).

    Examples
    --------
    >>> ddf = egi_load(source=db, region='region.shp', level=6)
    >>> paths = gh3_rasterize(ddf, 'tiles/')
    >>>
    >>> # From a dataset directory, merged into one file
    >>> gh3_rasterize('/data/agg_egi', 'agbd.tif', merge=True)

    See Also
    --------
    gh3_to_raster : single in-memory frame to an ``xr.Dataset``.
    gh3_rasterize_partitions : H3-only variant returning per-partition results.
    """
    import logging
    from . import raster

    logger = logging.getLogger(__name__)

    if isinstance(data, str):
        path, info = _detect_source(data)
        if info.get('source_type') == 'h3_database':
            raise GediValidationError(
                f"'{path}' is an H3 database, not a rasterizable dataset. Rasterization "
                f"needs a dataset produced by gh3_aggregate or gh3_extract — run one of "
                f"those on the database first."
            )
        if index_type is None:
            index_type = info.get('index_type')
        columns = _resolve_columns(columns, path, info)
        # _load_dataset, not gh3_load: the latter refuses EGI-indexed datasets.
        ddf = _load_dataset(path, columns=columns, query=query)
    else:
        if query is not None:
            raise GediValidationError(
                "query= applies to dataset-path input only. Call .query() on the frame "
                "before passing it to gh3_rasterize()."
            )
        path, info = None, {}
        ddf = data
        if index_type is None:
            index_type = get_spatial_index_type(ddf._meta if hasattr(ddf, '_meta') else ddf)

    if index_type not in ('h3', 'egi'):
        raise GediValidationError(
            "Cannot determine the spatial index type to rasterize. Pass index_type='h3' "
            "or index_type='egi', or supply data carrying an 'h3_*' / 'egi*' index."
        )

    rasterize_kwargs = {}
    if index_type == 'egi':
        from . import egi
        rasterize_func = egi.rasterize_partition
        # Output files are named from the partition id. Below level 12 several
        # partitions share an outer tile, so the rasterizer needs the level to
        # name them apart — otherwise they all collide on the tile id.
        if partition_level is None:
            partition_level = _egi_partition_level(ddf, info)
        if partition_level is not None:
            rasterize_kwargs['partition_level'] = partition_level
        logger.info(f"Rasterizing EGI-indexed data ({ddf.npartitions} partitions)"
                    if hasattr(ddf, 'npartitions') else "Rasterizing EGI-indexed data")
    else:
        rasterize_func = raster.rasterize_h3_partition
        if partition_level is None and path is not None:
            partition_level, source = _h3_partition_level_from_dataset(path, info)
            if partition_level is not None:
                logger.info(f"  Partition level: H3 {partition_level} (from {source})")
        if partition_level is not None:
            rasterize_kwargs['partition_level'] = partition_level

    if merge:
        if fmt not in ('tif', 'tiff', 'geotiff'):
            raise GediValidationError(
                f"merge=True writes GeoTIFF only; fmt='{fmt}' is not supported there. "
                f"Use merge=False for tiled {fmt} output, or fmt='tif' to merge."
            )
        merged_output = output if output.endswith('.tif') else f"{output}.tif"
        os.makedirs(os.path.dirname(os.path.abspath(merged_output)), exist_ok=True)
        return raster.merge_and_export_rasters(
            ddf, merged_output, rasterize_func,
            columns=columns, compress=compress, show_progress=show_progress,
            **rasterize_kwargs
        )

    os.makedirs(output, exist_ok=True)
    result = raster.rasterize_and_export_partitions(
        ddf, output, rasterize_func,
        columns=columns, fmt=fmt, compress=compress, show_progress=show_progress,
        **rasterize_kwargs
    )
    # export_raster_partition comma-joins when a partition yields several tiles.
    # EGI never does (one partition = one tile); H3 can when a partition holds
    # cells from several parents. Flatten so the return is always file paths.
    paths = [p for entry in result if entry for p in entry.split(',') if p]

    # Tiles are named from the partition id carried in the raster attributes.
    # If two partitions resolve to the same id they write to the same path and
    # the last one silently replaces the rest — never let that pass quietly.
    # The usual cause is EGI partitions finer than level 12 on a frame that
    # carries no egiNN partition column, so the level cannot be recovered.
    duplicates = {p for p in paths if paths.count(p) > 1}
    if duplicates:
        logger.error(
            f"{len(paths) - len(set(paths))} raster tile(s) were overwritten: "
            f"{sorted(duplicates)[:3]}{'...' if len(duplicates) > 3 else ''}. Several "
            f"partitions resolved to the same output name, so only the last write "
            f"survives. Pass partition_level= explicitly, or rasterize data that "
            f"still carries its partition column."
        )

    return paths


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
        One entry per Dask partition, comma-joined when a partition produced
        several tiles. Use :func:`gh3_rasterize` for a flat list of paths.

    See Also
    --------
    gh3_rasterize : index-aware equivalent; also handles EGI, dataset
        directories and merged output.
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
    region : str | list | GeoDataFrame | GeoSeries | shapely geometry, optional
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