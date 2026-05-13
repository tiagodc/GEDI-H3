# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

import os, re, glob, h5py, h3
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
import h3pandas
import dask
import dask.dataframe
import dask_geopandas
import dask.bag as dbg
import pyarrow.parquet as pq
from typing import Union, List, Dict, Optional, Tuple, Any, Callable
from earthaccess.store import EarthAccessFile
from dask.distributed import progress

from .config import GEDI_BEAMS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_PRODUCTS, GEDI_START_DATE, BUILD_LOG_FILENAME, PARTITION_META_FILENAME, _get_versioned, _GEDI_L2A_ESSENTIALS, _PRODUCT_QUALITY_FLAGS
from .utils import now, json_read, json_write, to_geojson, parquet_append_columns, parquet_merge_files, read_parquet_schema, h5_is_valid, get_dask_client, generate_manifest, check_nan_only_columns, h3_partition_bbox, parse_h3_partition_dirname, AtomicFileWriter
from .h3utils import intersect_h3_geometries, h3_index_df, fix_h3_geometry
from .gedidriver import GEDIFile, add_special_columns, soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5, gedi_vars_static, gedi_subset, validate_soc_files, load_h5, load_h5_merged, expand_var_wildcards
from .daac import gedi_download
from .logging_config import get_logger
from .validation import validate_h3_params, validate_product_vars, validate_directory_exists
from .exceptions import H3ValidationError, GediValidationError, GediFileError, GediMergeError, GediError

logger = get_logger(__name__)


def _init_earthaccess_worker():
    """Pre-authenticate earthaccess in Dask worker processes."""
    import earthaccess
    if not earthaccess.__auth__.authenticated:
        earthaccess.login(strategy='netrc', persist=False)


def download_soc(product_vars: Dict, spatial=None, temporal=None, direct_access=False, update=False, version=None, odir=GH3_DEFAULT_SOC_DIR, n_jobs=5, on_granule_complete=None, ensure_l2a=True):
    """
    Download GEDI HDF5 files in SOC (Science Operation Center) format.

    Expands variable specifications, ensures L2A essentials and shot_number
    are included, then delegates to ``gedi_download`` for retrieval.

    Parameters
    ----------
    product_vars : dict
        Mapping of GEDI product codes (e.g., 'L2A', 'L4A') to variable
        lists. Accepts 'default', 'minimal', or explicit variable names.
    spatial : GeoDataFrame, list, or str, optional
        Spatial filter (vector file, bbox as [W,S,E,N], or ISO3 code).
    temporal : tuple of str, optional
        Temporal range as (start_date, end_date) in 'YYYY-MM-DD' format.
    direct_access : bool, default False
        If True, use S3 streaming instead of downloading to disk.
    update : bool, default False
        If True, resume a previous download (skip already-downloaded files).
    version : int or str, optional
        GEDI data version (e.g., 2). If None, uses latest available.
    odir : str
        Output directory for downloaded HDF5 files.
    n_jobs : int, default 5
        Number of parallel download workers.
    on_granule_complete : callable, optional
        Callback ``(granule_info_dict, status_str) -> None`` for per-granule
        progress tracking. Passed through to ``gedi_download()``; see its
        docstring for the ``granule_info_dict`` contract (includes a
        ``path`` key).
    ensure_l2a : bool, default True
        If True, automatically add L2A essentials when L2A is not in
        product_vars. Set to False for variable-only updates where L2A
        data already exists in the target database.

    Returns
    -------
    list
        List of downloaded SOC file paths or EarthAccessFile objects.
    """
    product_vars = gedi_vars_expand(product_vars, version=version)

    if ensure_l2a and 'L2A' not in product_vars:
        essentials = _get_versioned(_GEDI_L2A_ESSENTIALS, version)
        product_vars.update({'L2A': essentials})

    for k, val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    # Ensure quality flag variables are included for every product with an explicit var list.
    for prod, val in product_vars.items():
        flag_map = _PRODUCT_QUALITY_FLAGS.get(prod)
        if flag_map and val is not None:
            flags = _get_versioned(flag_map, version)
            for flag_name, _condition in flags:
                if flag_name not in val:
                    val.append(flag_name)

    soc_files = gedi_download(
        product_vars=product_vars,
        odir=None if direct_access else odir,
        spatial=spatial, temporal=temporal,
        version=version,
        resume=update, n_jobs=n_jobs,
        to_list=direct_access,
        on_granule_complete=on_granule_complete,
    )

    return soc_files


def _subset_s3_file(granule, prod, product_vars, odir, file_idx, n_files):
    """Subset a single S3 file to a compact local HDF5. Process-safe via granule serialization.

    Accepts a serializable DataGranule object (not an EarthAccessFile handle).
    Each worker re-authenticates and opens its own S3 handle, enabling true
    multiprocessing via Dask workers instead of GIL-constrained threads.
    """
    import earthaccess
    from .logging_config import get_logger

    logger = get_logger(__name__)

    # Parse filename from granule data link
    gf = GEDIFile(granule.data_links()[0])
    local_dir = os.path.join(odir, str(gf.date.year), gf.date.strftime('%j'))
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, gf.full_name)

    # Track whether we computed a union of vars from an existing file
    union_vars = None
    vars_for_prod = product_vars.get(prod)

    if os.path.exists(local_path):
        try:
            existing_vars = set(gedi_vars_from_h5(local_path))
            needed_vars = set(vars_for_prod) if vars_for_prod else set()
            if needed_vars.issubset(existing_vars):
                logger.debug(f"[{file_idx}/{n_files}] Skipping {gf.full_name} (already exists with required variables)")
                return local_path
            # Re-download with union of existing + requested
            union_vars = sorted(existing_vars | needed_vars)
            logger.debug(f"[{file_idx}/{n_files}] Re-downloading {gf.full_name} (missing {len(needed_vars - existing_vars)} variables)")
            os.unlink(local_path)
        except Exception:
            logger.warning(f"[{file_idx}/{n_files}] Could not read existing {gf.full_name}, re-downloading")
            os.unlink(local_path)

    # Re-authenticate in worker process (same pattern as daac.py:379)
    if not earthaccess.__auth__.authenticated:
        earthaccess.login(strategy='netrc', persist=False)

    # Open S3 handle with retry (transient failures common during worker startup)
    from .exceptions import RETRY_DEFAULTS
    max_attempts = RETRY_DEFAULTS['max_attempts']
    initial_wait = RETRY_DEFAULTS['initial_wait']
    max_wait = RETRY_DEFAULTS['max_wait']

    s3_files = None
    for attempt in range(1, max_attempts + 1):
        try:
            s3_files = earthaccess.open([granule], show_progress=False,
                                        open_kwargs={'block_size': 16 * 1024 * 1024})
            if s3_files:
                break
        except Exception as e:
            if attempt == max_attempts:
                logger.warning(f"[{file_idx}/{n_files}] Failed to open S3 handle for "
                               f"{gf.full_name} after {max_attempts} attempts: {e}")
                return None
            wait_time = min(initial_wait * (2 ** (attempt - 1)), max_wait)
            logger.debug(f"[{file_idx}/{n_files}] S3 open attempt {attempt} failed for "
                         f"{gf.full_name}, retrying in {wait_time:.1f}s: {e}")
            import time
            time.sleep(wait_time)

    if not s3_files:
        logger.warning(f"[{file_idx}/{n_files}] No S3 handle returned for "
                       f"{gf.full_name} after {max_attempts} attempts")
        return None

    s3_file = s3_files[0]

    # Use union vars if we merged with existing file, otherwise resolve from product_vars
    if union_vars is not None:
        vars_for_prod = union_vars
    else:
        vars_for_prod = product_vars.get(prod)
        if vars_for_prod is None:
            # NASA release file on S3 → static manifest is canonical and free.
            # Falls back to remote H5 enumeration if no manifest ships for
            # this (product, version).
            vars_for_prod = gedi_vars_static(prod, version=gf.version)
            if vars_for_prod is None:
                vars_for_prod = gedi_vars_from_h5(s3_file)

    logger.info(f"[{file_idx}/{n_files}] Subsetting {gf.full_name} ({len(vars_for_prod)} vars)")
    try:
        result = gedi_subset(s3_file, local_path, vars_for_prod)
        if result is None:
            logger.warning(f"Subsetting produced no output for {gf.full_name}")
            return None
        return local_path
    except Exception as e:
        logger.warning(f"Failed to subset {gf.full_name}: {e}")
        if os.path.exists(local_path):
            os.unlink(local_path)
        return None


def s3_etl_subset(product_vars, spatial=None, temporal=None, version=None, odir=None, ensure_l2a=True):
    """
    ETL-style S3 build: search for GEDI granules, then dispatch Dask workers
    to open remote HDF5 files, extract only selected variables via range
    requests, and write compact local HDF5 files.

    This transfers and stores significantly less data than full downloads
    (10-50x smaller depending on variable selection). Workers run as true
    separate processes via Dask (GIL-free), matching the parallelism of
    download mode.

    Parameters
    ----------
    product_vars : dict
        Mapping of GEDI product codes (e.g., 'L2A', 'L4A') to variable
        lists. Accepts 'default', 'minimal', or explicit variable names.
    spatial : GeoDataFrame, list, or str, optional
        Spatial filter (vector file, bbox as [W,S,E,N], or ISO3 code).
    temporal : tuple of str, optional
        Temporal range as (start_date, end_date) in 'YYYY-MM-DD' format.
    version : int or str, optional
        GEDI data version. If None, uses latest available.
    odir : str
        Output directory for compact local HDF5 files in SOC structure.
    ensure_l2a : bool, default True
        If True, automatically add L2A essentials when L2A is not in
        product_vars. Set to False for variable-only updates where L2A
        data already exists in the target database.

    Returns
    -------
    str
        Path to the output directory containing compact HDF5 files.

    Raises
    ------
    GediFileError
        If no GEDI files are found on S3 for the given parameters.
    """
    from .daac import GEDIAccessor

    # Expand variable specifications and ensure L2A essentials + shot_number
    product_vars = gedi_vars_expand(product_vars, version=version)

    if ensure_l2a and 'L2A' not in product_vars:
        essentials = _get_versioned(_GEDI_L2A_ESSENTIALS, version)
        product_vars.update({'L2A': essentials})

    for k, val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    # Search for granules per product (no S3 handles opened yet)
    logger.info("Searching NASA DAAC for GEDI granules")
    gass = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)

    for prod in product_vars:
        prod_version = version if version is not None else GEDI_PRODUCTS.get(prod.upper(), {}).get('version')
        gass.search_data(product=prod, version=prod_version)

    # Resolve None variables (dump all) from the static per-product manifest;
    # NASA release files on S3 share the canonical schema, so no remote
    # HDF5 metadata round-trip is needed. Falls back to opening one S3
    # handle if no manifest ships for this (product, version).
    for prod, prod_vars in product_vars.items():
        if prod_vars is not None:
            continue
        granules = gass.product_files.get(prod, [])
        if not granules:
            continue
        static_vars = gedi_vars_static(prod, version=version)
        if static_vars is not None:
            product_vars[prod] = static_vars
            logger.info(f"Discovered {len(product_vars[prod])} variables for {prod} (static manifest)")
            continue
        s3_files = gass.link_s3(product=prod)
        if s3_files:
            product_vars[prod] = gedi_vars_from_h5(s3_files[0])
            logger.info(f"Discovered {len(product_vars[prod])} variables for {prod} (HDF5 introspection)")

    # Build flat task list of (granule, product) from search results
    os.makedirs(odir, exist_ok=True)
    tasks = []
    for prod, granules in gass.product_files.items():
        for granule in granules:
            tasks.append((granule, prod))

    n_files = len(tasks)
    if n_files == 0:
        raise GediFileError("No GEDI files found on S3 for the given parameters")

    # Always-parallel dispatch to Dask workers (true multiprocessing,
    # GIL-free). gedih3 CLIs (gh3_build, gh3_doctor, …) all create a
    # Client at startup, so this assertion fires only for direct
    # library/notebook callers — which can wrap in ``with Client(...)``.
    client = get_dask_client()
    if client is None:
        raise GediError(
            "s3_etl_subset requires a registered dask.distributed Client. "
            "CLI tools create one at startup; library callers must wrap "
            "in `with dask.distributed.Client(...) as client: ...`."
        )

    completed_count = 0
    failed = 0
    n_workers = sum(client.nthreads().values())
    logger.info(f"Subsetting {n_files} files from S3 with Dask ({n_workers} workers)")

    client.run(_init_earthaccess_worker)

    futures = [
        client.submit(
            _subset_s3_file, granule, prod, product_vars,
            odir, idx + 1, n_files, pure=False,
        )
        for idx, (granule, prod) in enumerate(tasks)
    ]

    from distributed import as_completed as dask_as_completed
    for future, result in dask_as_completed(futures, with_results=True):
        completed_count += 1
        if result is None:
            failed += 1
        if completed_count % 10 == 0 or completed_count == n_files:
            logger.info(f"S3 ETL progress: {completed_count}/{n_files} files ({failed} failed)")

    if failed > 0:
        logger.warning(f"S3 ETL completed with {failed}/{n_files} failures")

    # R2 producer-driven refresh: s3_etl_subset writes HDF5s to the SOC
    # tree, so it must persist the manifest before returning. Without
    # this, the next consumer (gh3_build, gh3_doctor) re-pays the
    # multi-million-file recursive glob on cold GPFS.
    from .gedidriver import write_soc_manifest
    n_manifest = write_soc_manifest(odir)
    if n_manifest:
        logger.info(f"SOC manifest refreshed ({n_manifest} files)")

    return odir


def h3_part_files(df, dir_path, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode', roi_tiles=[]):
    """
    Write a DataFrame to H3-partitioned parquet files.

    Indexes the DataFrame by H3 cell, groups rows by partition cell, and
    writes each group as a separate parquet file. If a file already exists
    for a partition, new rows are appended.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with coordinate columns and ``root_beam``,
        ``root_file`` metadata columns.
    dir_path : str
        Base directory for output partition subdirectories.
    res : int, default 12
        H3 resolution for shot-level indexing.
    part : int, default 3
        H3 resolution for file partitioning.
    lat_col : str, default 'lat_lowestmode'
        Column name for latitude values.
    lon_col : str, default 'lon_lowestmode'
        Column name for longitude values.
    roi_tiles : list of str, optional
        If non-empty, only write partitions whose H3 cell ID is in this list.

    Returns
    -------
    list of str or None
        List of written parquet file paths, or None if the input is empty.
    """
    if df.empty:
        return
    
    df = h3_index_df(df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)
    df = df.reset_index().set_index(f'h3_{part:02d}')
    
    files = []
    for i in df.index.unique():
        if len(roi_tiles) > 0 and i not in roi_tiles:
            continue
        
        hex_path = os.path.join(dir_path,i)        
        hex_df = df.loc[[i]]
        gedi_name = re.sub('\\.h5$',f'.{hex_df.root_beam.iloc[0]}.parquet', hex_df.root_file.iloc[0])
        f = os.path.join(hex_path, gedi_name)
        
        if f.endswith('.parquet'):
            os.makedirs(hex_path, exist_ok=True)
            if os.path.exists(f):
                parquet_append_columns(hex_df, f)
            else:
                hex_df.to_parquet(f, engine='pyarrow', index=True, compression='zstd')
            
        files.append(f)
        del hex_df
    
    del df    
    return files

def h3_write_metadata(h3_file, stats=None):
    """
    Write a sidecar metadata JSON file for an H3 partition parquet file.

    Parameters
    ----------
    h3_file : str
        Path to the H3 partition parquet file.
    stats : dict, optional
        Pre-computed stats from ``parquet_merge_files``'s streaming pass:
        ``{'shot_count', 'shot_min', 'shot_max', 'dt_min', 'dt_max',
        'root_files'}``. When provided, skips the ~1.5-2 GB ``pd.read_parquet``
        re-read of the just-written merged file. When ``None`` or any field is
        ``None``, falls back to reading the columns from disk (slower, more
        memory).

    Returns
    -------
    str
        Path to the written metadata JSON file (``*PARTITION_META_FILENAME``).
    """
    meta_file = h3_file.replace('.parquet', PARTITION_META_FILENAME)
    h3_part, year = os.path.basename(h3_file).split('.')[:2]

    cols = read_parquet_schema(h3_file)

    # Fast path: caller provided streaming stats — no data re-read.
    can_skip_read = (
        stats is not None
        and stats.get('root_files')
        and stats.get('shot_min') is not None
        and stats.get('dt_min') is not None
    )

    if can_skip_read:
        gedi_files = [GEDIFile(f) for f in stats['root_files']]
        shot_range = (int(stats['shot_min']), int(stats['shot_max']))
        # dt_min / dt_max may be pandas/datetime/numpy types; pandas-parse
        # to ensure consistent strftime formatting.
        dt_min = pd.Timestamp(stats['dt_min'])
        dt_max = pd.Timestamp(stats['dt_max'])
        date_range = (dt_min.strftime('%Y-%m-%d'), dt_max.strftime('%Y-%m-%d'))
        shot_count = stats['shot_count']
    else:
        df = pd.read_parquet(h3_file, engine='pyarrow',
                             columns=['shot_number', 'root_file_l2a', 'datetime'])
        gedi_files = [GEDIFile(f) for f in df['root_file_l2a'].unique()]
        shot_range = (int(df['shot_number'].min()), int(df['shot_number'].max()))
        date_range = (df['datetime'].min().strftime('%Y-%m-%d'),
                      df['datetime'].max().strftime('%Y-%m-%d'))
        shot_count = len(df)

    granule_identifiers = [{'orbit': gf.orbit, 'granule': gf.orbit_granule,
                            'track': gf.track} for gf in gedi_files]
    l2a_version = gedi_files[0].version

    h3_polygon = gpd.GeoDataFrame(geometry=[fix_h3_geometry(h3_part)],
                                  crs=4326, index=[h3_part])

    meta = {
        'last_modified': now(),
        'l2a_version': l2a_version,
        'h3_partition': h3_part,
        'h3_geometry': to_geojson(h3_polygon),
        'year': int(year),
        'shot_count': shot_count,
        'shot_range': shot_range,
        'date_range': date_range,
        'granules': granule_identifiers,
        'columns': cols['column'].tolist(),
        # Per-column pyarrow dtype string (e.g. 'int64', 'double',
        # 'binary', 'timestamp[ns]'). Aggregated upstream into the
        # build log's h3_columns_dtypes field so the query path can
        # build a Dask _meta without sampling a parquet file.
        'column_dtypes': dict(zip(cols['column'].tolist(), cols['dtype'].tolist())),
    }

    json_write(meta, meta_file, rewrite=True)
    return meta_file

def h3_read_metadata(h3_file):
    """
    Read the sidecar metadata JSON for an H3 partition parquet file.

    Parameters
    ----------
    h3_file : str
        Path to the H3 partition parquet file. The metadata file is
        expected at the same path with a ``PARTITION_META_FILENAME`` extension.

    Returns
    -------
    dict or None
        Parsed metadata dictionary, or None if the metadata file does not
        exist.
    """
    meta_file = h3_file.replace('.parquet', PARTITION_META_FILENAME)
    if os.path.exists(meta_file):
        return json_read(meta_file)
    return None

def h3_merge_metadata(h3_subdir):
    """
    Merge per-year metadata files into a single summary for an H3 partition.

    Aggregates shot counts, expands shot and date ranges, and deduplicates
    granule identifiers across all year subdirectories within one H3 cell.

    Parameters
    ----------
    h3_subdir : str
        Path to an H3 partition directory (e.g., ``h3_03=<cell_id>/``)
        containing year subdirectories with parquet and metadata files.

    Returns
    -------
    str or None
        Path to the merged metadata JSON file, or None if no metadata
        files were found.
    """
    files = glob.glob(os.path.join(h3_subdir,'*','*.parquet'))
    year_metadata = [h3_read_metadata(f) for f in files]
    year_metadata = [m for m in year_metadata if m is not None]
    
    if len(year_metadata) == 0:
        return None
    
    mmeta = year_metadata[0].copy()
    
    del mmeta['year']
    mmeta['years'] = set()
    
    mmeta['last_modified'] = now()
    mmeta['shot_range'] = list(mmeta['shot_range'])
    mmeta['date_range'] = list(mmeta['date_range'])
    
    for ym in year_metadata[1:]:
        mmeta['shot_count'] += ym['shot_count']
        mmeta['shot_range'][0] = min(mmeta['shot_range'][0], ym['shot_range'][0])
        mmeta['shot_range'][1] = max(mmeta['shot_range'][1], ym['shot_range'][1])
        mmeta['date_range'][0] = min(mmeta['date_range'][0], ym['date_range'][0])
        mmeta['date_range'][1] = max(mmeta['date_range'][1], ym['date_range'][1])
        mmeta['years'].add(ym['year'])
        
        for g in ym['granules']:
            if g not in mmeta['granules']:
                mmeta['granules'].append(g)
    
    mmeta['years'] = sorted(mmeta['years'])
    ofile = os.path.join(h3_subdir, f"{mmeta['h3_partition']}{PARTITION_META_FILENAME}")
    json_write(mmeta, ofile, rewrite=True)
    return ofile

def h3_skip_part(h3_dir, h3_part, gedi_file, cols=None):
    """
    Check whether an H3 partition already contains data from a GEDI granule.

    Reads the partition's merged metadata to determine if the granule has
    already been indexed and, optionally, if the requested columns are
    already present.

    Parameters
    ----------
    h3_dir : str
        Root directory of the H3 database.
    h3_part : str
        H3 cell ID of the partition to check.
    gedi_file : str
        Path or filename of the GEDI HDF5 file to test against.
    cols : list of str, optional
        If provided, also verify that these columns already exist in the
        partition. The partition is only skipped when both the granule
        is present and all requested columns are available.

    Returns
    -------
    bool
        True if the partition should be skipped (granule already indexed
        and columns present), False otherwise.
    """
    res = h3.get_resolution(h3_part)
    meta_file = os.path.join(h3_dir, f"h3_{res:02d}={h3_part}", f"{h3_part}{PARTITION_META_FILENAME}")    
    
    if not os.path.exists(meta_file):
        return False
    
    metadata = json_read(meta_file)

    skip_cols = True
    if cols:
        existing_cols = set(metadata['columns'])
        cols = set(cols) - {'year', f'h3_{res:02d}'}
        skip_cols = cols.issubset(existing_cols)
    
    gf = GEDIFile(gedi_file)
    granule_id = {'orbit':gf.orbit, 'granule':gf.orbit_granule, 'track':gf.track}
    
    return skip_cols and granule_id in metadata['granules']

def h3_add_skip_column(df, h3_dir):
    """
    Add a ``_skip`` boolean column indicating partitions to skip.

    Designed for use with ``dask.dataframe.map_partitions``. Checks each
    unique H3 partition cell individually, since a single beam can span
    multiple H3 cells. Delegates to ``h3_skip_part`` per cell.

    Parameters
    ----------
    df : pandas.DataFrame
        A single Dask partition containing H3 partition column(s)
        (``h3_XX``) and ``root_file_l2a``.
    h3_dir : str
        Root directory of the H3 database.

    Returns
    -------
    pandas.DataFrame
        Input DataFrame with an added ``_skip`` column (True if the
        cell's granule data already exists in the database).
    """
    if df.empty:
        df['_skip'] = True
        return df
    h3_col = sorted([c for c in df.columns if re.match(r'h3_\d{2}', c)])[0]
    gedi_file = df['root_file_l2a'].iloc[0]
    cols = df.columns.tolist()

    # Check each unique H3 partition cell individually
    skip_map = {}
    for h3_part in df[h3_col].unique():
        skip_map[h3_part] = h3_skip_part(h3_dir=h3_dir, h3_part=h3_part, gedi_file=gedi_file, cols=cols)

    df = df.assign(_skip=df[h3_col].map(skip_map))
    return df

@dask.delayed
def dh3_merge_metadata(h3_subdir):
    return h3_merge_metadata(h3_subdir)

def h3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    """
    Merge multiple parquet files for an H3 partition into a single file.

    Reads all parquet files in ``in_dir``, merges them (deduplicating shots
    when appending to an existing file), writes the result to ``out_dir``,
    and generates a sidecar metadata JSON file.

    Parameters
    ----------
    in_dir : str
        Input directory containing parquet fragment files for one
        H3 cell/year combination.
    out_dir : str
        Output directory where the merged file will be written,
        preserving the ``<h3_cell>/<year>/`` structure.
    rm_src : bool, default True
        If True, remove the source directory after a successful merge.
    replace : bool, default False
        If True, overwrite existing output files. If False, merge new
        data into any existing output file.

    Returns
    -------
    str or None
        Path to the merged parquet file, or None if ``in_dir`` contained
        no parquet files.
    """
    files = glob.glob(os.path.join(in_dir,'*.parquet'))

    if len(files) == 0:
        return

    year_dir =  os.path.dirname(in_dir)
    year = os.path.basename(year_dir.rstrip('/'))

    h3_dir = os.path.dirname(year_dir)
    h3part = os.path.basename(h3_dir.rstrip('/'))

    odir = os.path.join(out_dir, h3part, year)
    os.makedirs(odir, exist_ok=True)

    # Per-partition stale tmp cleanup: a prior crash between .merge.tmp write
    # and os.replace leaves orphaned <out_file>.tmp / .merge.tmp here. Scoping
    # this to odir keeps the cost O(1) per merge (one listdir on the partition's
    # own dir) rather than O(N_partitions) globbed on the driver.
    try:
        for _name in os.listdir(odir):
            if _name.endswith('.merge.tmp') or _name.endswith('.parquet.tmp'):
                try:
                    os.unlink(os.path.join(odir, _name))
                except OSError:
                    pass
    except OSError:
        pass

    oname = f'{h3part.split('=')[-1]}.{year.split('=')[-1]}.0.parquet'
    out_file = os.path.join(odir, oname)
    h3_file = out_file

    # Disk-canonical skip: if the final parquet exists and is newer than every
    # source fragment, this merge already completed in a prior run and only
    # the source-cleanup step was interrupted. Just clean up and return — no
    # need to re-merge identical data through the dedup path. Validate that
    # the dest is actually a readable parquet first; a corrupt newer-than-
    # source dest must NOT short-circuit (we'd return a broken partition).
    if not replace and os.path.exists(out_file):
        try:
            out_mtime = os.path.getmtime(out_file)
            if all(os.path.getmtime(f) <= out_mtime for f in files):
                try:
                    pq.ParquetFile(out_file).metadata  # readability check
                    if rm_src:
                        shutil.rmtree(in_dir, ignore_errors=True)
                    return h3_file
                except Exception:
                    # Corrupt dest — fall through to the merge path, which
                    # detects the same corruption below and overwrites it.
                    pass
        except OSError:
            pass

    if is_temp := (os.path.exists(out_file) and not replace):
        # Validate the existing dest is readable before adding it to flist.
        # A prior crash mid-write can leave a corrupt parquet here; trying to
        # merge through it would raise and abort the whole merge phase.
        # Treat corruption as "no usable dest" — merge only the fragments and
        # overwrite the bad dest atomically via .merge.tmp + os.replace.
        try:
            pq.ParquetFile(out_file).metadata  # cheap header check
            files.insert(0, out_file)
            files = list(set(files))
            out_file += '.tmp'
        except Exception as e:
            logger.warning(
                f"Existing partition {h3_file} is unreadable ({e!r}); "
                f"discarding it and merging tmp fragments fresh"
            )
            is_temp = False  # treat as no dest, full overwrite

    # Derive bbox from the H3 partition geometry directly (no data scan).
    # Buffered enough to safely contain all level-N children at any depth
    # via the empirical icosahedral-distortion factor in h3_partition_bbox.
    cell_id, parent_res = parse_h3_partition_dirname(h3part)
    bbox = h3_partition_bbox(cell_id, parent_res) if cell_id is not None else None

    # Capture streaming stats from the merge so h3_write_metadata doesn't
    # have to re-read the just-written file (saves ~1.5-2 GB peak memory
    # per dense partition + the GPFS read-back I/O).
    stats = parquet_merge_files(
        out_file, files, check_shots=is_temp, rm_src=rm_src, bbox=bbox,
    )

    if is_temp:
        os.replace(out_file, h3_file)
    if rm_src:
        shutil.rmtree(in_dir, ignore_errors=True)

    meta_file = h3_write_metadata(h3_file, stats=stats)
    return h3_file

@dask.delayed
def dh3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    return h3_merge_files(in_dir=in_dir, out_dir=out_dir, rm_src=rm_src, replace=replace)


def _expand_product_vars(
    product_vars: Dict[str, List[str]],
    soc_files: List[Dict[str, str]],
    version: Optional[int] = None
) -> Dict[str, List[str]]:
    """
    Expand product variable specifications and ensure L2A essentials are included.

    Parameters
    ----------
    product_vars : dict
        Raw product variable specifications (may contain 'default', 'minimal', etc.)
    soc_files : list of dict
        List of SOC file dictionaries to sample for 'all' variable expansion
    version : int or None
        GEDI data version. If None, auto-detected from the first SOC file.

    Returns
    -------
    dict
        Expanded product variables with L2A essentials included
    """
    # Auto-detect version from first SOC file if not specified
    if version is None and soc_files:
        for soc_dict in soc_files:
            for prod, path in soc_dict.items():
                try:
                    version = GEDIFile(path).version
                    break
                except Exception:
                    continue
            if version is not None:
                break

    product_vars = gedi_vars_expand(product_vars, version=version)

    def _first_valid_sample(product):
        # Pick the first file for this product that opens cleanly and has at
        # least one BEAM group, so wildcard / "all variables" introspection
        # never trips on a corrupt head-of-list granule.
        for sf in soc_files:
            f = sf.get(product)
            if f and h5_is_valid(f):
                return f
        return None

    # Expand wildcard patterns (e.g. 'rh_*', 'geolocation/sensitivity_a?')
    # against available HDF5 variables before further processing.
    for k, val in product_vars.items():
        if val is not None and any(any(c in v for c in ('*', '?', '[', ']')) for v in val):
            file = _first_valid_sample(k)
            if file:
                available = gedi_vars_from_h5(file)
                product_vars[k] = expand_var_wildcards(val, available)

    essentials = _get_versioned(_GEDI_L2A_ESSENTIALS, version)
    if 'L2A' in product_vars:
        if product_vars['L2A'] is not None:
            product_vars['L2A'] = list(set(product_vars['L2A'] + essentials))
        # None means "all variables" — essentials already included
    else:
        product_vars['L2A'] = essentials

    # Ensure quality flag variables are included for every product with an explicit var list.
    # (None means "all variables" — quality flags already included.)
    from .config import _PRODUCT_QUALITY_FLAGS
    for prod, val in product_vars.items():
        flag_map = _PRODUCT_QUALITY_FLAGS.get(prod)
        if flag_map and val is not None:
            flags = _get_versioned(flag_map, version)
            for flag_name, _condition in flags:
                if flag_name not in val:
                    val.append(flag_name)

    for k, val in product_vars.items():
        if val is None:
            file = _first_valid_sample(k)
            if file is None:
                raise GediFileError(
                    f"No valid SOC file found for product {k} to introspect variables"
                )
            product_vars[k] = gedi_vars_from_h5(file)

    return product_vars


def _granule_id_from_l2a_path(path: str) -> Optional[Tuple[int, int, int]]:
    """Parse (orbit, granule, track) from a GEDI L2A HDF5 filename.

    Cheap path-only parse that mirrors GEDIFile but skips the os.path.getsize
    check, which is expensive on shared filesystems. Returns None if the
    basename doesn't match the GEDI naming convention.
    """
    try:
        fl = os.path.basename(str(path)).split('_')
        return (int(fl[3][1:]), int(fl[4]), int(fl[5][1:]))
    except (IndexError, ValueError, AttributeError):
        return None


def _granule_ids_in_fragment(parquet_file: str) -> set:
    """Read root_file_l2a from a single tmp parquet fragment, return granule IDs.

    Tries column statistics first (no data read needed when min==max, which
    holds for the common case of one granule per fragment under by_beam=True).
    Falls back to reading the column. Returns an empty set on read failure.
    """
    try:
        pf = pq.ParquetFile(parquet_file)
        if 'root_file_l2a' in pf.schema_arrow.names:
            col_idx = pf.schema_arrow.get_field_index('root_file_l2a')
            try:
                stats = pf.metadata.row_group(0).column(col_idx).statistics
                if stats is not None and stats.has_min_max and stats.min == stats.max:
                    gid = _granule_id_from_l2a_path(stats.min)
                    return {gid} if gid is not None else set()
            except Exception:
                pass
            tbl = pq.read_table(parquet_file, columns=['root_file_l2a'])
            out = set()
            for path in tbl['root_file_l2a'].to_pylist():
                if path is None:
                    continue
                gid = _granule_id_from_l2a_path(path)
                if gid is not None:
                    out.add(gid)
            return out
    except Exception:
        pass
    return set()


# Match fragment basenames written by stage 1 (v0.8.0+):
#   ``O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}.parquet``
# (see _create_h3_dataframe). When the filename matches, BOTH the granule
# ID and the beam are recoverable without any parquet I/O — just string
# parsing on the worker, microseconds per file. The beam capture is
# load-bearing for the reconcile's partial-granule detection: without it
# a granule with a single beam fragment on disk would be marked INDEXED
# and the remaining 7 beams' shots would be silently dropped on resume.
_FRAGMENT_BASENAME_RE = re.compile(r'^O(\d+)_G(\d+)_T(\d+)\.([A-Za-z0-9_]+)\.parquet$')

# Sentinel used in the per-granule beam set returned by
# ``_process_h3_partition`` for legacy fragments whose filename does not
# encode the beam (pre-v0.8.0 ``part.NNN.parquet``). The reconcile treats
# the sentinel as "trust this granule is complete" — legacy builds rarely
# resume and reaching into parquet contents to recover the beam is more
# I/O than the rare case warrants.
_LEGACY_BEAM_SENTINEL = '*'


# ---------------------------------------------------------------------------
# Streaming partition-write shared helpers
# ---------------------------------------------------------------------------
#
# The streaming writer (_write_partitioned_streaming) emits one of these
# completion sentinels per successful (granule × beam) task, after every
# leaf parquet for that task has been atomically committed via
# AtomicFileWriter. The reconcile then trusts the sentinel as proof that
# the (granule × beam) is fully on disk — eliminating the legacy
# "any-beam-fragment-equals-complete-granule" data-loss path (Agent 3
# adversarial review #E.1).
_COMPLETE_SENTINEL_DIRNAME = '_complete'


def _granule_beam_frag_name(soc_dict: Dict[str, str], beam: str) -> Optional[str]:
    """Stable basename (without ``.parquet``) for one (granule, beam) tuple.

    Matches ``_FRAGMENT_BASENAME_RE`` exactly so the reconcile, merge, and
    legacy ``to_parquet(name_function=...)`` paths all see identical
    fragment paths. Returns ``None`` when the source HDF5 filename cannot
    be parsed (matches the legacy fallback at the prior inline builder
    site — caller falls back to dask-default naming, which in the
    streaming path means we just skip the (granule × beam) since opaque
    names would not round-trip through the reconcile cleanly).
    """
    try:
        path = next(iter(soc_dict.values()))
        fl = os.path.basename(str(path)).split('_')
        orbit = int(fl[3][1:])
        granule = int(fl[4])
        track = int(fl[5][1:])
        return f"O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}"
    except Exception:
        return None


def _complete_sentinel_path(tmp_dir: str, frag_name: str) -> str:
    """Path of the per-(granule × beam) completion sentinel.

    Lives under ``tmp_dir/_complete/`` (one directory, all sentinels) so
    the reconcile can enumerate completions via a single ``os.scandir``
    on the sentinel dir instead of a recursive walk of the partition
    tree. ``frag_name`` matches ``_FRAGMENT_BASENAME_RE`` (no ``.parquet``
    suffix) so the sentinel basename uniquely identifies the task.
    """
    return os.path.join(tmp_dir, _COMPLETE_SENTINEL_DIRNAME, f'{frag_name}.done')


def _emit_complete_sentinel(tmp_dir: str, frag_name: str) -> None:
    """Touch the completion sentinel for one (granule × beam). Idempotent.

    Atomic via ``open(... 'x')`` semantics — concurrent emitters on shared
    GPFS race-create the same file; only one wins, the others observe
    ``FileExistsError`` and treat it as "already done". No need for
    AtomicFileWriter here: the file is zero-byte (its existence is the
    signal); a partial write cannot leave a half-emitted sentinel.
    """
    path = _complete_sentinel_path(tmp_dir, frag_name)
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    try:
        with open(path, 'x'):
            pass
    except FileExistsError:
        pass


def _scan_complete_sentinels(tmp_dir: str) -> set:
    """Return the set of frag_names with an emitted completion sentinel.

    One ``os.scandir`` over ``tmp_dir/_complete/``; O(n_completed_tasks)
    rather than O(n_fragments). Empty set if the sentinel dir doesn't
    exist yet (fresh build or pre-migration legacy tmp tree).
    """
    sentinel_dir = os.path.join(tmp_dir, _COMPLETE_SENTINEL_DIRNAME)
    out: set = set()
    try:
        with os.scandir(sentinel_dir) as it:
            for e in it:
                if e.is_file(follow_symlinks=False) and e.name.endswith('.done'):
                    out.add(e.name[:-len('.done')])
    except FileNotFoundError:
        pass
    except OSError:
        pass
    return out


def _canonical_write_schema(meta_df, part: int) -> Any:
    """Build the canonical pyarrow schema for streaming per-leaf writes.

    Mirrors dask's ``_meta_nonempty → pyarrow_schema_dispatch`` chain used
    by ``dd.to_parquet``. Building schema once on the driver and passing
    it to every per-leaf write enforces column-order and nullable-dtype
    parity across all fragments — without this guard, per-leaf
    schema-inference from data would produce subtle divergences (e.g.
    ``Int64`` nullable vs ``int64``, datetime tz) that break
    parquet_merge_files's schema union at the merge phase.

    Parameters
    ----------
    meta_df : pandas / geopandas DataFrame
        Empty frame with the canonical column set, dtypes, and index name
        that every per-leaf write should match. Should already include
        ``geometry`` + ``datetime`` + ``year`` (post-add_special_columns,
        post year-assign) and the partition columns (we drop them here).
    part : int
        Partition resolution; drives the ``h3_{part:02d}`` partition
        column name to drop from the body schema.

    Returns
    -------
    pyarrow.Schema
        Schema with partition columns removed. For GeoDataFrame input the
        schema carries the GeoParquet ``geo`` metadata so per-leaf cast
        preserves the geometry encoding contract.
    """
    drop_cols = [f'h3_{part:02d}', 'year']
    body = meta_df.drop(columns=drop_cols, errors='ignore').head(0)
    if isinstance(body, gpd.GeoDataFrame):
        from geopandas.io.arrow import _geopandas_to_arrow
        return _geopandas_to_arrow(body, index=True).schema
    import pyarrow as pa
    return pa.Schema.from_pandas(body, preserve_index=True)


def _list_year_subdirs(h3_dir: str) -> List[str]:
    """Return ``<h3_dir>/year=*/`` paths (with trailing separator).

    Worker-side body of the merge-phase tmp partition listing. Called once
    per ``h3_*`` dir via ``client.map`` so the cumulative readdir latency
    on shared GPFS is parallelized across all workers instead of being
    paid serially on the driver. ``os.scandir`` is preferred over
    ``glob``/``listdir`` because its DirEntry caches the directory bit
    from the parent readdir — no extra ``stat()`` round-trip per child.
    """
    out: List[str] = []
    try:
        with os.scandir(h3_dir) as it:
            for e in it:
                if e.name.startswith('year=') and e.is_dir(follow_symlinks=False):
                    out.append(e.path + os.sep)
    except OSError:
        pass
    return out


def _process_h3_partition(h3_dir: str) -> Dict[Tuple[int, int, int], set]:
    """Return ``{(orbit, granule, track): set(beam_str, ...)}`` for granules
    represented under ``h3_dir/year=*/*.parquet``.

    The beam set is what makes this partial-resume safe. A granule with
    only some of its beams on disk (e.g. one (granule × beam) task finished
    writing, the others were still pending when the build was killed)
    would otherwise be indistinguishable from a fully-extracted granule.
    The reconcile aggregates beam sets across all h3_* partitions and
    only flips INDEXED when every expected beam is present — without that,
    the missing beams' shots are silently dropped on the next stage-1 run.

    Worker-side body of the resume reconcile, run as one Dask task per
    ``h3_*`` tmp partition. All cluster parallelism comes from
    ``client.map`` over h3 partitions — there is intentionally no
    thread/process pool inside the task itself: every worker spawning N
    threads multiplies cluster-wide concurrency by N×nworkers, and on a
    shared GPFS that overwhelms the metadata server long before per-task
    throughput catches up.

    Fast path: if the fragment basename matches the v0.8.0+ naming
    convention ``O{orbit}_G{granule}_T{track}.{beam}.parquet``, both
    granule ID and beam are parsed from the filename — no parquet I/O,
    microseconds per file.

    Fallback: legacy ``part.NNN.parquet`` names lack beam info, so we
    read parquet column statistics (``_granule_ids_in_fragment``) — ~90
    ms cold per file on GPFS — and tag the granule with
    ``_LEGACY_BEAM_SENTINEL`` so the reconcile treats it as complete.
    """
    out: Dict[Tuple[int, int, int], set] = {}
    fallback_paths: list = []
    try:
        for ye in os.scandir(h3_dir):
            if not (ye.is_dir(follow_symlinks=False) and ye.name.startswith('year=')):
                continue
            try:
                for fe in os.scandir(ye.path):
                    if not (fe.is_file(follow_symlinks=False) and fe.name.endswith('.parquet')):
                        continue
                    m = _FRAGMENT_BASENAME_RE.match(fe.name)
                    if m is not None:
                        key = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
                        out.setdefault(key, set()).add(m.group(4))
                    else:
                        fallback_paths.append(fe.path)
            except OSError:
                continue
    except OSError:
        return out
    for p in fallback_paths:
        for gid in _granule_ids_in_fragment(p):
            out.setdefault(gid, set()).add(_LEGACY_BEAM_SENTINEL)
    return out


def _reconcile_granules_from_disk(h3_dir: str, h3_logger, tmp_dir: Optional[str] = None) -> int:
    """Mark granules INDEXED based on what's already on disk.

    Scans the finalized partition metadata under ``h3_dir`` AND the tmp fragment
    tree under ``tmp_dir`` to identify granules whose data is already present.
    Flips matching ``H3BuildLogger.granule_info`` entries to ``status='INDEXED'``
    so the next stage-1 invocation skips them via ``get_finished_granules()``.

    Idempotent. Safe to call before every build. Caller is responsible for
    persisting the change with ``h3_logger.save_log(...)``.

    Parameters
    ----------
    h3_dir : str
        Final H3 database directory (contains ``h3_*/<year>/*.parquet`` and
        per-partition ``*.metadata.json`` sidecars).
    h3_logger : H3BuildLogger
        Live build-log object whose ``granule_info`` will be mutated in place.
    tmp_dir : str, optional
        Temporary partitions directory (typically ``<build_tmp>/partitions``).
        Skipped if None or non-existent.

    Returns
    -------
    int
        Number of granule entries flipped from non-INDEXED to INDEXED.
    """
    if not hasattr(h3_logger, 'granule_info') or not h3_logger.granule_info:
        return 0
    # Short-circuit: nothing to flip → no need to touch disk.
    if not any(g.get('status') != 'INDEXED' for g in h3_logger.granule_info):
        logger.info(
            "Resume reconciliation: build log shows no pending granules, "
            "skipping disk scan"
        )
        return 0

    # ``indexed_ids`` holds granules confirmed fully on disk and safe to skip
    # on the next stage 1 run.
    indexed_ids: set = set()
    # ``granule_beams`` aggregates beam sets across every h3_* tmp partition
    # so we can distinguish fully-extracted granules from partial ones that
    # were killed mid-write. Without this completeness check a granule with
    # only one beam fragment would be flipped INDEXED and the remaining 7
    # beams' shots would be silently dropped on resume.
    granule_beams: Dict[Tuple[int, int, int], set] = {}

    # Pass A — finalized partition metadata JSONs (cheap, always run).
    # Granules named here are inside an h3 partition that has already been
    # merged and finalized → all their beams' rows are already consolidated
    # into the final parquet, so they're complete by construction.
    meta_files = glob.glob(os.path.join(h3_dir, 'h3_*', f'*{PARTITION_META_FILENAME}'))
    meta_files += glob.glob(os.path.join(h3_dir, 'h3_*', '*', f'*{PARTITION_META_FILENAME}'))
    for mf in meta_files:
        try:
            for g in (json_read(mf) or {}).get('granules', []):
                indexed_ids.add((g['orbit'], g['granule'], g['track']))
        except Exception:
            continue

    # Pass B — tmp fragments. One Dask task per h3_* partition, with the
    # parquet metadata reads inside each task parallelized via a thread
    # pool (see _process_h3_partition).
    if tmp_dir and os.path.isdir(tmp_dir):
        try:
            h3_dirs = sorted(
                e.path for e in os.scandir(tmp_dir)
                if e.is_dir(follow_symlinks=False) and e.name.startswith('h3_')
            )
        except OSError:
            h3_dirs = []
        if h3_dirs:
            logger.info(
                f"Reconciling granule status from {len(h3_dirs)} h3_* tmp partitions"
            )
            client = None
            try:
                client = get_dask_client()
            except Exception:
                pass

            def _merge_partition_data(data: Dict[Tuple[int, int, int], set]) -> None:
                for gid, beams in data.items():
                    granule_beams.setdefault(gid, set()).update(beams)

            from tqdm import tqdm as tqdm_bar
            if client is not None:
                from dask.distributed import as_completed as dask_as_completed
                futures = client.map(_process_h3_partition, h3_dirs, pure=False)
                pbar = tqdm_bar(
                    total=len(futures),
                    desc="Reconcile partitions", unit="dir",
                )
                try:
                    for fut in dask_as_completed(futures):
                        try:
                            _merge_partition_data(fut.result())
                        except Exception:
                            pass
                        finally:
                            fut.release()
                        pbar.update(1)
                finally:
                    pbar.close()
            else:
                # In-process fallback for tiny scenarios (no client).
                for d in tqdm_bar(h3_dirs, desc="Reconcile partitions", unit="dir"):
                    try:
                        _merge_partition_data(_process_h3_partition(d))
                    except Exception:
                        continue

    # Pass C — scan streaming completion sentinels under tmp_dir/_complete/.
    # Each sentinel proves one (granule × beam) task ran to completion under
    # the streaming writer (all leaves committed atomically + sentinel
    # emitted as the final step). Sentinels are the authoritative
    # completeness signal going forward.
    sentinel_beams: Dict[Tuple[int, int, int], set] = {}
    sentinel_dir_exists = False
    if tmp_dir and os.path.isdir(tmp_dir):
        sentinel_dir_exists = os.path.isdir(
            os.path.join(tmp_dir, _COMPLETE_SENTINEL_DIRNAME)
        )
        if sentinel_dir_exists:
            for frag_name in _scan_complete_sentinels(tmp_dir):
                m = _FRAGMENT_BASENAME_RE.match(f'{frag_name}.parquet')
                if m is None:
                    continue
                gid = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
                sentinel_beams.setdefault(gid, set()).add(m.group(4))

    # Decide reconcile mode.
    #
    # sentinel_mode=True  → the streaming writer has run at least once on
    # this tmp tree, so sentinels are the authoritative completeness signal.
    # Fragment-presence alone (Pass B) is NO LONGER sufficient because the
    # streaming writer may have written some leaves of a (granule × beam)
    # before being killed mid-task — only the sentinel proves all leaves
    # were committed.
    #
    # sentinel_mode=False → tmp tree predates the streaming writer (built
    # by the legacy ddf.to_parquet path). Fall back to the pre-existing
    # fragment-presence heuristic and EMIT sentinels for any granule we
    # flip INDEXED — that bridges the legacy tree into the sentinel model
    # so subsequent resumes are sentinel-aware.
    sentinel_mode = sentinel_dir_exists

    expected_beams = set(GEDI_BEAMS)
    n_partial = 0
    n_migrated_sentinels = 0
    migration_emit_pairs: List[Tuple[str, str]] = []  # (frag_name, beam) — for clarity in logs

    if sentinel_mode:
        # AUTHORITATIVE PATH: granules complete only when every expected
        # beam has its sentinel emitted. Fragment-presence in granule_beams
        # is ignored for completeness; we still report partials based on it
        # as a diagnostic.
        for gid, beams in sentinel_beams.items():
            if gid in indexed_ids:
                continue
            if expected_beams.issubset(beams):
                indexed_ids.add(gid)
            else:
                n_partial += 1
        # Also surface fragment-on-disk-but-no-sentinel granules as partial
        # in the diagnostic count (they will be re-extracted on next run).
        for gid in granule_beams:
            if gid in indexed_ids:
                continue
            if gid not in sentinel_beams:
                n_partial += 1
    else:
        # LEGACY MIGRATION PATH: use the pre-streaming fragment-presence
        # heuristic. For every granule flipped INDEXED here, emit sentinels
        # for each of its beams so subsequent reconciles are sentinel-aware.
        for gid, beams in granule_beams.items():
            if gid in indexed_ids:
                continue
            if _LEGACY_BEAM_SENTINEL in beams or expected_beams.issubset(beams):
                indexed_ids.add(gid)
                # Migration: synthesise a frag_name per beam in `beams` and
                # emit its sentinel. For the legacy-sentinel case we don't
                # know the actual beam, so emit for the full GEDI_BEAMS set
                # (consistent with the legacy "trust this granule" semantics).
                emit_beams = set(GEDI_BEAMS) if _LEGACY_BEAM_SENTINEL in beams else beams
                orbit, granule, track = gid
                for beam in emit_beams:
                    frag_name = f"O{orbit:05d}_G{granule:02d}_T{track:05d}.{beam}"
                    if tmp_dir:
                        _emit_complete_sentinel(tmp_dir, frag_name)
                    migration_emit_pairs.append((frag_name, beam))
                    n_migrated_sentinels += 1
            else:
                n_partial += 1

    if n_migrated_sentinels:
        logger.info(
            f"Resume reconciliation (migration): emitted {n_migrated_sentinels} "
            f"completion sentinels for legacy tmp fragments; subsequent reconciles "
            f"will use sentinel-authoritative mode"
        )
    if n_partial:
        logger.info(
            f"Resume reconciliation: {n_partial} granule(s) found with partial "
            f"beam coverage on disk; leaving non-INDEXED for re-extraction"
        )

    if not indexed_ids:
        return 0

    n_flipped = 0
    for g in h3_logger.granule_info:
        key = (g['orbit'], g['granule'], g['track'])
        if key in indexed_ids and g.get('status') != 'INDEXED':
            g['status'] = 'INDEXED'
            n_flipped += 1
    if n_flipped:
        logger.info(
            f"Resume reconciliation: {n_flipped} granules flipped to INDEXED "
            f"({len(indexed_ids)} unique granules found on disk)"
        )
    else:
        logger.info(
            f"Resume reconciliation: no new granules to flip "
            f"({len(indexed_ids)} unique granules already accounted for)"
        )
    return n_flipped


def _filter_granules(
    prod_soc_files: List[Dict[str, str]],
    product_vars: Dict[str, List[str]],
    skip_granules: Optional[List[Dict]] = None
) -> List[Dict[str, str]]:
    """
    Filter SOC files to exclude incomplete, corrupted, or already-processed granules.

    Parameters
    ----------
    prod_soc_files : list of dict
        List of product file dictionaries
    product_vars : dict
        Required products and their variables
    skip_granules : list of dict, optional
        Granule identifiers to skip

    Returns
    -------
    list of dict
        Filtered list of valid SOC file dictionaries
    """
    def _filter_soc_file(prod):
        if not set(product_vars.keys()).issubset(set(prod.keys())):
            return None

        if skip_granules is not None:
            gedifile = GEDIFile(list(prod.values())[0])
            gran = {'orbit': gedifile.orbit, 'granule': gedifile.orbit_granule, 'track': gedifile.track}
            if gran in skip_granules:
                return None

        if any(isinstance(f, EarthAccessFile) for f in prod.values()):
            return prod
        for f in prod.values():
            if not h5_is_valid(f):
                return None

        return prod

    logger.info("Checking for incomplete, corrupted, or existing granules to skip")

    bag_task = (
        dbg.from_sequence(prod_soc_files, partition_size=100)
          .map(_filter_soc_file)
          .filter(lambda x: x is not None)
          .persist()
    )

    # Per-batch tqdm bar over the bag's futures — renders reliably in TTY,
    # SSH, and log-redirected contexts where distributed.progress() can be
    # invisible. Mirrors the partition-merge pattern below.
    from dask.distributed import futures_of, as_completed as dask_as_completed
    from tqdm import tqdm as tqdm_bar
    bag_futures = futures_of(bag_task)
    if bag_futures:
        pbar = tqdm_bar(dask_as_completed(bag_futures), total=len(bag_futures),
                        desc="Checking SOC files", unit="batch")
        for _ in pbar:
            pass
        pbar.close()
    soc_files = list(bag_task.compute())
    del bag_task

    n_skipped = len(prod_soc_files) - len(soc_files)
    if n_skipped > 0:
        logger.info(f"Skipped {n_skipped}/{len(prod_soc_files)} granules (already indexed, incomplete, or corrupted)")

    return soc_files


def _create_h3_dataframe(
    soc_files: List[Dict[str, str]],
    product_vars: Dict[str, List[str]],
    res: int,
    part: int
) -> Tuple[dask_geopandas.GeoDataFrame, str, str, str, List[str]]:
    """
    Create a Dask GeoDataFrame with H3 indexing from SOC files.

    Parameters
    ----------
    soc_files : list of dict
        List of valid SOC file dictionaries
    product_vars : dict
        Products and variables to extract
    res : int
        H3 resolution for indexing
    part : int
        H3 resolution for partitioning

    Returns
    -------
    tuple
        (dask_geopandas.GeoDataFrame, lat_col, lon_col, dat_col, frag_names)
        ``frag_names[i]`` is a stable, content-derived basename for Dask
        partition ``i``, used as ``name_function`` in ``to_parquet`` so re-runs
        write to the same path and overwrite in place (no shot duplication).
    """
    logger.info(f"Found {len(soc_files)} new GEDI granules with requested products")

    ddf = dask_h5_merged(soc_files, product_vars, shots=None, dropna=True, by_beam=True, suffix_all=True)

    # Build per-partition stable names matching dask_h5_merged's by_beam=True
    # enumeration: itertools.product(soc_files, GEDI_BEAMS). Each Dask partition
    # i corresponds to one (granule, beam) tuple. Delegated to the shared
    # _granule_beam_frag_name helper so the streaming writer
    # (_write_partitioned_streaming) and the legacy to_parquet path agree on
    # the same filename convention by construction.
    import itertools as _it
    frag_names: List[str] = []
    for _soc, _beam in _it.product(soc_files, GEDI_BEAMS):
        name = _granule_beam_frag_name(_soc, _beam)
        if name is None:
            # Fallback: opaque but deterministic per-partition name. Only
            # fires when the source HDF5 path can't be parsed (never happens
            # for NASA-formatted granules). Legacy behaviour preserved.
            name = f"part.{len(frag_names):08d}"
        frag_names.append(name)

    lat_col = 'lat_lowestmode'
    lon_col = 'lon_lowestmode'
    dat_col = 'delta_time'

    if 'lat_lowestmode_l2a' in ddf.columns:
        lat_col += '_l2a'
    if 'lon_lowestmode_l2a' in ddf.columns:
        lon_col += '_l2a'
    if 'delta_time_l2a' in ddf.columns:
        dat_col += '_l2a'

    logger.info(f"Indexing H3 at resolution {res}, partitioning at {part}")

    ddf = ddf.map_partitions(h3_index_df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)

    return ddf, lat_col, lon_col, dat_col, frag_names


def _apply_spatial_filter(
    ddf: dask.dataframe.DataFrame,
    spatial,
    part: int,
    h3_dir: str
) -> dask.dataframe.DataFrame:
    """
    Apply spatial and existing-data filters to the Dask DataFrame.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame with H3 index
    spatial : GeoDataFrame, list, or str
        Spatial filter geometry
    part : int
        H3 partition resolution
    h3_dir : str
        Path to existing H3 database (for skip detection)

    Returns
    -------
    dask.dataframe.DataFrame
        Filtered Dask DataFrame
    """
    h3_tiles = []
    if spatial is not None:
        h3_tiles = intersect_h3_geometries(spatial, res=part)

    if len(h3_tiles) > 0:
        logger.info("Removing H3 partitions outside spatial filter")
        ddf = ddf[ddf[f'h3_{part:02d}'].isin(h3_tiles)]

    build_log = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    if os.path.exists(build_log):
        logger.info("Checking for existing indexed GEDI data to skip")
        _meta = ddf._meta.copy()
        _meta['_skip'] = False
        ddf = ddf.map_partitions(h3_add_skip_column, h3_dir=h3_dir, meta=_meta)
        ddf = ddf[~ddf['_skip']]
        ddf = ddf.drop(columns=['_skip'])

    return ddf


def _write_one_granule_beam(
    task: Tuple[Dict[str, str], str, str],
    *,
    product_vars: Dict[str, List[str]],
    res: int,
    part: int,
    tmp_dir: str,
    h3_dir: str,
    lat_col: str,
    lon_col: str,
    dat_col: str,
    spatial_h3_tiles: Optional[List[str]] = None,
    skip_check_enabled: bool = False,
    schema: Any = None,
) -> Dict[str, Any]:
    """Worker-side body of the streaming partition write.

    Loads ONE (granule × beam) HDF5, applies the same transformation chain
    the legacy dask graph used (h3_index_df → optional spatial filter →
    h3_add_skip_column → add_special_columns → year synthesis → groupby on
    [h3_{part:02d}, year]), writes one parquet leaf per (h3 cell × year)
    group via AtomicFileWriter + GeoDataFrame.to_parquet, then emits a
    per-(granule × beam) completion sentinel only AFTER every leaf is
    committed. The sentinel is what the reconcile trusts as proof that the
    (granule × beam) is fully on disk — eliminating the legacy
    "any-beam-fragment-equals-complete-granule" data-loss path on
    kill-mid-write resume.

    Parameters
    ----------
    task : (soc_dict, beam, frag_name)
        ``soc_dict`` maps product code → HDF5 path for one granule.
        ``beam`` is one of ``GEDI_BEAMS``. ``frag_name`` matches
        ``_FRAGMENT_BASENAME_RE`` and is the per-task basename used both
        for the leaf parquet files and the completion sentinel.
    product_vars, res, part, lat_col, lon_col, dat_col
        Identical to the legacy chain's kwargs.
    tmp_dir, h3_dir
        Output tmp tree root and existing-h3-db root.
    spatial_h3_tiles
        Driver-broadcast list of H3 cell IDs (at resolution ``part``) to
        keep. ``None`` disables spatial filtering. Replaces the legacy
        ``ddf[ddf[h3_part_col].isin(h3_tiles)]`` at _apply_spatial_filter.
    skip_check_enabled
        When True, runs ``h3_add_skip_column`` to drop rows whose target
        h3 partition already has finalized data in ``h3_dir``.
    schema
        Driver-built pyarrow Schema used for every per-leaf write to
        force column-order + dtype parity across fragments. Without
        this, per-leaf schema inference would drift between fragments
        and break the merge phase's schema union. See
        ``_canonical_write_schema``.

    Returns
    -------
    dict
        ``{'frag_name': str, 'leaves': int, 'rows': int, 'skipped': bool,
        'error': Optional[str]}``. ``skipped=True`` covers
        empty-after-load, empty-after-spatial-filter, and
        empty-after-skip-check (no sentinel emitted in any of these
        cases — the (granule × beam) genuinely produced no data).
    """
    soc_dict, beam, frag_name = task
    stats = {'frag_name': frag_name, 'leaves': 0, 'rows': 0, 'skipped': False, 'error': None}

    # 1) Load HDF5 for one (granule, beam) — identical contract to
    #    dask_h5_merged(by_beam=True)'s inner load_by_beam closure.
    try:
        df = load_h5_merged(
            soc_dict, product_vars=product_vars,
            which_beams=[beam], shots=None,
            dropna=True, suffix_all=True,
        )
    except Exception as e:
        # Mirrors the legacy graph's _load_h5_merged exception swallow —
        # corrupt h5 returns an empty meta upstream rather than failing the
        # whole job. Streaming surfaces the error in stats for visibility.
        stats['skipped'] = True
        stats['error'] = f"load_h5_merged: {type(e).__name__}: {e}"
        return stats
    if df is None or df.empty:
        stats['skipped'] = True
        return stats

    # 2) H3 index — same call as legacy ddf.map_partitions(h3_index_df, ...).
    df = h3_index_df(df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)
    if df.empty:
        stats['skipped'] = True
        return stats

    h3_part_col = f'h3_{part:02d}'

    # 3) Spatial filter — replaces _apply_spatial_filter's isin branch.
    #    Tile set is precomputed driver-side and scattered (constant per build).
    if spatial_h3_tiles is not None:
        df = df[df[h3_part_col].isin(spatial_h3_tiles)]
        if df.empty:
            stats['skipped'] = True
            return stats

    # 4) Skip-existing-data filter — replaces _apply_spatial_filter's
    #    h3_add_skip_column branch. h3_add_skip_column reads from h3_dir
    #    to detect cells whose finalized data already covers this granule.
    if skip_check_enabled and 'root_file_l2a' in df.columns:
        df = h3_add_skip_column(df, h3_dir=h3_dir)
        df = df[~df['_skip']].drop(columns=['_skip'])
        if df.empty:
            stats['skipped'] = True
            return stats

    # 5) Special columns + year — same calls as legacy.
    df = add_special_columns(df, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    df = df.assign(year=df['datetime'].dt.year)

    # 6) Groupby + per-leaf atomic write. observed=True + sort=False mirror
    #    the legacy partition_on=[h3_part, year] semantics — partition
    #    columns are stored in directory names, dropped from file body.
    leaves_written = 0
    rows_written = 0
    # Lazy import — only the streaming write path uses this private hook
    # into geopandas's arrow conversion. Kept identical to what dask's
    # GeoArrowEngine._pandas_to_arrow_table calls under the hood, so the
    # streaming output matches the legacy chain byte-for-byte (modulo
    # row order within identical input).
    from geopandas.io.arrow import _geopandas_to_arrow
    for (h3_cell, year), leaf_df in df.groupby([h3_part_col, 'year'], sort=False, observed=True):
        if leaf_df.empty:
            continue
        leaf_dir = os.path.join(tmp_dir,
                                f'{h3_part_col}={h3_cell}',
                                f'year={int(year)}')
        out_path = os.path.join(leaf_dir, f'{frag_name}.parquet')
        body = leaf_df.drop(columns=[h3_part_col, 'year'])
        # Convert to pyarrow Table via the geopandas hook (carries the
        # GeoParquet ``geo`` schema metadata + WKB geometry encoding),
        # then cast to the canonical driver-built schema to lock down
        # column order and nullable-dtype tagging across all fragments.
        table = _geopandas_to_arrow(body, index=True)
        if schema is not None:
            table = table.cast(schema)
        with AtomicFileWriter(out_path) as tmp_path:
            pq.write_table(table, tmp_path, compression='zstd')
        leaves_written += 1
        rows_written += len(body)

    # 7) Completion sentinel — the load-bearing data-loss guard. Emitted
    #    only AFTER every leaf is committed (AtomicFileWriter.__exit__
    #    succeeded). If the worker dies between leaves, no sentinel is
    #    emitted → reconcile leaves the granule non-INDEXED → next resume
    #    re-extracts the (granule × beam) idempotently.
    if leaves_written > 0:
        _emit_complete_sentinel(tmp_dir, frag_name)
    else:
        stats['skipped'] = True

    stats['leaves'] = leaves_written
    stats['rows'] = rows_written
    return stats


def _streaming_enabled() -> bool:
    """Whether the partition-write phase should use the streaming path.

    Streaming is the default since the v0.9.5 cutover. Set
    ``GH3_WRITE_STREAMING={0,false,off,no}`` to opt back into the legacy
    ``ddf.to_parquet`` path (kept for one release cycle as a diagnostic
    fallback; will be removed in v0.10.0).
    """
    val = os.environ.get('GH3_WRITE_STREAMING', '').strip().lower()
    if val in ('0', 'false', 'off', 'no'):
        return False
    return True


def _streaming_batch_size() -> int:
    """Rolling-window inflight target for the streaming driver.

    Tunable via ``GH3_WRITE_STREAMING_BATCH`` env. The driver maintains
    roughly this many in-flight (granule × beam) tasks at any moment via
    ``as_completed.add`` top-up. Default ``2000`` ≈ 7 tasks per worker
    on a 274-worker cluster — enough I/O overlap without scheduler-side
    memory pile-up.
    """
    try:
        return max(1, int(os.environ.get('GH3_WRITE_STREAMING_BATCH', '2000')))
    except ValueError:
        return 2000


def _write_partitioned_streaming(
    ddf: dask.dataframe.DataFrame,
    soc_files: List[Dict[str, str]],
    product_vars: Dict[str, List[str]],
    res: int,
    part: int,
    tmp_dir: str,
    h3_dir: str,
    spatial,
    lat_col: str,
    lon_col: str,
    dat_col: str,
    inflight_target: Optional[int] = None,
) -> bool:
    """Streaming replacement for the legacy ``ddf.to_parquet().persist()``.

    Emits one per-(granule × beam) ``client.submit`` task and drains via
    ``as_completed`` with a rolling inflight window of
    ``inflight_target`` futures. Each completed future is released
    immediately, so worker memory plateaus at the per-task working set
    instead of accumulating across the whole graph the way the to-parquet-
    barrier-bound legacy path does (see dask/dask#4894, #5159, #8377).

    Correctness contract vs. the legacy path:
      * Fragment basenames identical (``_granule_beam_frag_name``).
      * Output schema identical (driver-built canonical pyarrow schema,
        cast per leaf — matches dask's GeoArrowEngine pipeline).
      * Hive layout identical (``h3_{part:02d}={cell}/year={year}/``).
      * Resume-safe: only granules whose worker emitted a ``.done``
        completion sentinel are recognized as finished by the reconcile.

    Parameters
    ----------
    ddf
        Lazy ddf returned by ``_create_h3_dataframe``. Used here only to
        derive the canonical write schema from its ``_meta`` (no
        ``.persist()``; the lazy graph is discarded after the schema
        probe).
    soc_files, product_vars, res, part, lat_col, lon_col, dat_col
        Same inputs as the legacy chain.
    tmp_dir, h3_dir
        Output tree root + existing-h3-db root.
    spatial
        Same spatial filter input as ``_apply_spatial_filter`` —
        intersected to a tile list ONCE driver-side and scattered.
    inflight_target
        Max in-flight futures at any moment. ``None`` → env-driven
        default (``GH3_WRITE_STREAMING_BATCH``).
    """
    import itertools as _it
    from dask.distributed import as_completed as dask_as_completed
    from tqdm import tqdm as tqdm_bar

    if inflight_target is None:
        inflight_target = _streaming_batch_size()

    client = get_dask_client()
    if client is None:
        raise GediError(
            "_write_partitioned_streaming requires a registered dask "
            "Client (wrap your call in `with Client(...): ...`)."
        )

    logger.info(f"Writing partitioned H3 data (streaming) to: {tmp_dir}")
    os.makedirs(tmp_dir, exist_ok=True)

    # ── Driver-side preflight: build everything that should be broadcast ──
    # The merge phase's _merge_and_finalize uses the same pattern at
    # gh3builder.py:_merge_and_finalize for its h3-dir scatter list.

    # 1) Canonical schema — apply the streaming-side transforms to the
    #    ddf._meta and let _canonical_write_schema produce the pyarrow
    #    schema we'll cast each leaf to. Locks column order + dtypes.
    meta = ddf._meta.copy()
    meta = add_special_columns(meta, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    if 'datetime' in meta.columns:
        meta = meta.assign(year=meta['datetime'].dt.year.astype('int32'))
    canonical_schema = _canonical_write_schema(meta, part=part)

    # 2) Spatial tile set — replaces _apply_spatial_filter's isin branch.
    spatial_h3_tiles: Optional[List[str]] = None
    if spatial is not None:
        tiles = intersect_h3_geometries(spatial, res=part)
        if len(tiles) > 0:
            logger.info("Pre-computing spatial H3 tile list (driver-side)")
            spatial_h3_tiles = list(tiles)

    # 3) Skip-check enablement gate — only fire h3_add_skip_column when
    #    the destination h3_dir already has an indexed build log AND a
    #    persisted partition list. Without the persisted list, every
    #    h3_skip_part call would just stat-storm GPFS for no benefit
    #    (Agent 3 adversarial review #J.2).
    build_log_path = os.path.join(h3_dir, BUILD_LOG_FILENAME)
    skip_check_enabled = False
    if os.path.exists(build_log_path):
        try:
            log = json_read(build_log_path) or {}
            skip_check_enabled = bool(log.get('h3_partition_ids'))
        except Exception:
            skip_check_enabled = False
    if skip_check_enabled:
        logger.info("Skip-check enabled (existing finalized partitions detected)")

    # 4) Skip already-completed granule×beam tasks via sentinel scan. A
    #    resume picks up exactly where the previous run stopped — completed
    #    tasks are not re-submitted, partial tasks (no sentinel) are.
    completed_frags = _scan_complete_sentinels(tmp_dir)
    if completed_frags:
        logger.info(
            f"Streaming resume: {len(completed_frags)} (granule × beam) sentinel(s) "
            f"found on disk; matching tasks will be skipped at submission time."
        )

    # ── Scatter broadcast values ───────────────────────────────────────
    # client.map / submit normally inline kwargs into the task graph,
    # which for spatial_h3_tiles × 584k tasks would be hundreds of GB of
    # scheduler-side serialization. client.scatter with broadcast=True
    # ships each value once per worker.
    #
    # CRITICAL: ``client.scatter(value, ...)`` on an iterable value (list,
    # dict, set, pyarrow.Schema — anything with __iter__) scatters EACH
    # ELEMENT as its own Future and returns an iterable of Futures, not a
    # single Future. For a 41k-element spatial_h3_tiles list this creates
    # 41k scheduler-side string tasks and deadlocks the driver while it
    # broadcasts each to every worker. The wrap-in-singleton-list +
    # ``[0]`` indexing pattern forces dask to treat the value as a single
    # opaque object → one Future, broadcast once.
    schema_fut = client.scatter([canonical_schema], broadcast=True)[0]
    product_vars_fut = client.scatter([product_vars], broadcast=True)[0]
    spatial_fut = (client.scatter([spatial_h3_tiles], broadcast=True)[0]
                   if spatial_h3_tiles is not None else None)

    submit_kwargs = dict(
        product_vars=product_vars_fut,
        res=res, part=part,
        tmp_dir=tmp_dir, h3_dir=h3_dir,
        lat_col=lat_col, lon_col=lon_col, dat_col=dat_col,
        spatial_h3_tiles=spatial_fut,
        skip_check_enabled=skip_check_enabled,
        schema=schema_fut,
    )

    # ── Build task stream ─────────────────────────────────────────────
    # Generator over (soc_dict, beam, frag_name). Skips tasks whose
    # sentinel is already on disk (resume fast-path).
    def _task_stream():
        for soc, beam in _it.product(soc_files, GEDI_BEAMS):
            frag_name = _granule_beam_frag_name(soc, beam)
            if frag_name is None:
                continue  # opaque soc filename — never happens for NASA granules
            if frag_name in completed_frags:
                continue
            yield (soc, beam, frag_name)

    tasks = list(_task_stream())  # materialize so we know the total
    total = len(tasks)
    if total == 0:
        logger.info("Streaming write: no remaining tasks (all granules already complete)")
        return any(
            entry.is_dir() and entry.name.startswith('h3_')
            for entry in os.scandir(tmp_dir)
        ) if os.path.isdir(tmp_dir) else False

    logger.info(
        f"Streaming write: {len(soc_files)} granules × {len(GEDI_BEAMS)} beams = "
        f"{total} tasks (inflight_target={inflight_target}, "
        f"skipped_by_resume={len(completed_frags)})"
    )

    # ── Rolling-window submission ─────────────────────────────────────
    # Maintain ~inflight_target futures in flight at any moment via
    # as_completed.add on each completion. Eliminates the strict-batch
    # straggler-idle problem (Agent 3 review #F.3) — workers stay busy
    # until the task stream is exhausted.
    tasks_iter = iter(tasks)

    def _submit_next() -> Optional[Any]:
        try:
            t = next(tasks_iter)
        except StopIteration:
            return None
        return client.submit(_write_one_granule_beam, t, pure=False, **submit_kwargs)

    # Prime the inflight window.
    initial_futures = []
    for _ in range(inflight_target):
        fut = _submit_next()
        if fut is None:
            break
        initial_futures.append(fut)
    if not initial_futures:
        return False

    ac = dask_as_completed(initial_futures)

    pbar = tqdm_bar(total=total, desc="Stage1 partition writes", unit="task")
    n_ok = n_fail = n_leaves = 0
    try:
        for fut in ac:
            try:
                result = fut.result()
                if result.get('error'):
                    n_fail += 1
                    logger.warning(
                        f"Stage1 task {result.get('frag_name')}: {result['error']}"
                    )
                else:
                    n_ok += 1
                    n_leaves += result.get('leaves', 0)
            except Exception as e:
                n_fail += 1
                logger.warning(f"Stage1 task raised: {type(e).__name__}: {e}")
            finally:
                fut.release()
            pbar.update(1)
            pbar.set_postfix(ok=n_ok, fail=n_fail, leaves=n_leaves)
            # Top up the rolling window with one new task.
            new_fut = _submit_next()
            if new_fut is not None:
                ac.add(new_fut)
    finally:
        pbar.close()

    if n_fail:
        logger.error(
            f"Streaming write: {n_fail}/{total} tasks failed. Their granules "
            f"remain non-INDEXED and will be retried on the next resume."
        )

    # O(1) emptiness check — identical to the legacy _write_partitioned tail.
    try:
        return any(
            entry.is_dir() and entry.name.startswith('h3_')
            for entry in os.scandir(tmp_dir)
        )
    except FileNotFoundError:
        return False


def _write_partitioned(
    ddf: dask.dataframe.DataFrame,
    tmp_dir: str,
    part: int,
    lat_col: str,
    lon_col: str,
    dat_col: str,
    frag_names: Optional[List[str]] = None,
) -> bool:
    """
    Write H3-indexed data to partitioned parquet files.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame with H3 index
    tmp_dir : str
        Temporary directory for output
    part : int
        H3 partition resolution
    lat_col, lon_col, dat_col : str
        Column names for coordinates and datetime
    frag_names : list of str, optional
        Stable per-Dask-partition basenames (without ``.parquet`` extension)
        used as ``name_function`` in ``to_parquet``. When provided, re-runs of
        the same granule overwrite the same file in place (idempotent resume,
        no shot duplication). Falls back to dask defaults when None.

    Returns
    -------
    bool
        True if any partition file was written, False if the tmp tree is
        empty (e.g. all granules filtered out). The caller only needs an
        emptiness check; building a full list of written paths via a recursive
        glob scales as O(n_files) and is unbounded for global builds (millions
        of fragments × shared filesystem stat latency).
    """
    logger.info("Adding date and geometry columns to H3 database")

    ddf = ddf.map_partitions(add_special_columns, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    ddf = ddf.assign(year=ddf.datetime.dt.year)
    ddf = dask_geopandas.from_dask_dataframe(ddf)

    logger.info(f"Writing partitioned H3 data to temporary directory: {tmp_dir}")

    # overwrite=False preserves any tmp fragments from prior killed runs;
    # name_function with granule-derived names ensures re-extracted granules
    # overwrite their own files in place rather than appending duplicates.
    _to_parquet_kwargs = dict(
        write_index=True,
        overwrite=False,
        compression='zstd',
        partition_on=[f'h3_{part:02d}', 'year'],
        # write_metadata_file=False: gh3 maintains its own gedih3_build_log.json
        # sidecar. Forcing False prevents the global _metadata aggregation task,
        # which holds every per-partition write output in scheduler memory until
        # all millions of writes complete (the global-build memory plateau).
        write_metadata_file=False,
        compute=False,
    )
    if frag_names is not None:
        # Closure captures frag_names; dask passes the partition index i.
        _to_parquet_kwargs['name_function'] = lambda i: f"{frag_names[i]}.parquet"

    write_task = ddf.to_parquet(tmp_dir, **_to_parquet_kwargs)
    # optimize_graph=True (default) fuses read_h5 → h3_index → add_skip →
    # add_special_columns → from_dask_dataframe → to_parquet into one task per
    # input partition. Without fusion the scheduler eagerly produces reads
    # ahead of writes and tens of thousands of intermediate dataframes pile up
    # on workers. Fusion does NOT shuffle or reduce partition cardinality; with
    # partition_on=[...] every input partition still emits its own file in each
    # leaf hive directory (see dask/dask#8445 + #8487).
    write_task = write_task.persist()

    try:
        progress(write_task)

        # Check for worker errors before cancelling
        from distributed import futures_of
        futures = futures_of(write_task)
        errors = [f for f in futures if f.status == 'error']
        if errors:
            errors[0].result()  # Re-raises the original worker exception
    finally:
        logger.debug("Clearing dask workers")
        client = get_dask_client()
        client.cancel(write_task, force=True)
        del write_task, ddf

    # O(1) emptiness check: at least one h3_* leaf dir means stage 1 wrote
    # something. Avoid recursive glob over the full tmp tree (would walk
    # millions of fragments on a global build).
    try:
        return any(
            entry.is_dir() and entry.name.startswith('h3_')
            for entry in os.scandir(tmp_dir)
        )
    except FileNotFoundError:
        return False


def _merge_and_finalize(
    tmp_dir: str,
    h3_dir: str
) -> List[str]:
    """
    Merge temporary partitioned files into the final H3 database.

    Tracks progress via an append-only ``_merge_progress.txt`` file in
    ``tmp_dir``. On resume, already-merged partitions are skipped.

    Parameters
    ----------
    tmp_dir : str
        Temporary directory containing partitioned files
    h3_dir : str
        Final output directory for H3 database

    Returns
    -------
    list of str
        List of merged H3 parquet file paths
    """
    logger.info(f"Merging H3 partitions into final database path: {h3_dir}")

    # List candidate tmp partitions. Two-level walk:
    #   1. Driver-side os.scandir of <tmp_dir> for h3_* — one readdir, fast.
    #   2. Per-h3 year=*/ listing fanned out across all workers via
    #      client.map(_list_year_subdirs); cumulative readdir latency on
    #      shared GPFS is parallelized instead of paid serially on the driver.
    # Empty year dirs are NOT filtered here — h3_merge_files short-circuits
    # on no-input via `if len(files) == 0: return`.
    client = get_dask_client()
    try:
        h3_part_dirs = sorted(
            e.path for e in os.scandir(tmp_dir)
            if e.is_dir(follow_symlinks=False) and e.name.startswith('h3_')
        )
    except OSError:
        h3_part_dirs = []
    if h3_part_dirs and client is not None:
        year_lists = client.gather(client.map(_list_year_subdirs, h3_part_dirs, pure=False))
        tmp_h3_dirs = [p for sub in year_lists for p in sub]
    else:
        tmp_h3_dirs = [p for d in h3_part_dirs for p in _list_year_subdirs(d)]
    os.makedirs(h3_dir, exist_ok=True)

    # Stale .merge.tmp cleanup is delegated to h3_merge_files (worker-side,
    # scoped to the single partition's odir). The previous global sweep
    # `glob(h3_dir/h3_*/*/*.merge.tmp)` walked the entire final database from
    # the driver — another serial GPFS pass that scaled with DB size for no
    # benefit (parquet_merge_files already overwrites its own stale tmp).

    # Resume support: skip already-merged partitions
    merge_progress_file = os.path.join(tmp_dir, '_merge_progress.txt')
    merged_parts = set()
    if os.path.exists(merge_progress_file):
        with open(merge_progress_file, 'r') as f:
            merged_parts = {line.strip() for line in f if line.strip()}
        if merged_parts:
            logger.info(f"Resuming merge: {len(merged_parts)} partitions already merged")

    remaining_dirs = [d for d in tmp_h3_dirs if d.rstrip('/') not in merged_parts]

    if not remaining_dirs:
        logger.info("All partitions already merged (resume)")
    else:
        from dask.distributed import as_completed as dask_as_completed
        from tqdm import tqdm as tqdm_bar

        # Submit every remaining merge to the scheduler at once and let dask
        # distribute work across all available workers. No driver-side
        # throttling: scaling decisions belong to the scheduler.
        futures_list = client.map(
            h3_merge_files,
            remaining_dirs,
            out_dir=h3_dir,
            rm_src=True,
            replace=False,
        )
        futures: Dict[Any, str] = dict(zip(futures_list, remaining_dirs))

        merged_count = 0
        failed_count = 0
        pbar = tqdm_bar(total=len(remaining_dirs), desc="Merging partitions", unit="part")

        # Do NOT call future.release() per completion — keeping the futures
        # alive (via futures_list) makes the scheduler retain finished task
        # records, so the dashboard progress bar fills monotonically (X done
        # / 47k total) instead of draining as tasks are released. Memory cost
        # is ~tens of MB on the scheduler for the entire merge phase.
        for future in dask_as_completed(futures_list):
            d = futures.pop(future)
            try:
                future.result()
                with open(merge_progress_file, 'a') as f:
                    f.write(d.rstrip('/') + '\n')
                merged_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning(f"Merge failed for {os.path.basename(d.rstrip('/'))}: {e}")
            pbar.update(1)
            pbar.set_postfix(ok=merged_count, fail=failed_count)

        pbar.close()
        del futures, futures_list

        if failed_count > 0:
            logger.error(f"{failed_count} partition merges failed. Re-run to retry.")

    h3_files = glob.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet'), recursive=False)

    # Check a sample of merged files for NaN-only columns
    if h3_files:
        import geopandas as _gpd
        for f in h3_files[:5]:
            try:
                _sample = _gpd.read_parquet(f)
                check_nan_only_columns(_sample, context=f"Build partition {os.path.basename(f)}: ", logger=logger)
            except Exception:
                pass

    logger.info("Compiling H3 metadata files")

    h3_subdirs = glob.glob(os.path.join(h3_dir, 'h3_*/'))
    meta_tasks = [dh3_merge_metadata(i) for i in h3_subdirs]
    meta_tasks = dask.persist(*meta_tasks, optimize_graph=False)
    progress(meta_tasks)
    meta_files = list(dask.compute(*meta_tasks))
    del meta_tasks

    # Generate manifest for accelerated file listing
    logger.info("Generating file manifest")
    generate_manifest(h3_dir, tree_shape='h3db')

    return h3_files


def _add_variables_to_partition(h3_partition_dir, new_product_vars, soc_source, version=None):
    """Add new variables to a single H3 partition via shot_number join.

    Reads only shot_number + new variables from source HDF5 files,
    joins them into the existing partition parquet. Memory-efficient
    via ``parquet_join_columns()`` which processes row-groups one at a time.

    Parameters
    ----------
    h3_partition_dir : str
        Path to an H3 partition directory (e.g., ``h3_03=abc123/``)
    new_product_vars : dict
        Product code → list of new variable names to add
    soc_source : str, list, or None
        Source for HDF5 files (directory, file list, or None for S3)
    version : int or None
        GEDI data version for S3 queries and local file filtering

    Returns
    -------
    str or None
        Path to the updated parquet file, or None if no update was needed
    """
    from .utils import parquet_join_columns
    import tempfile

    # Find the merged metadata file
    meta_files = glob.glob(os.path.join(h3_partition_dir, f'*{PARTITION_META_FILENAME}'))
    if not meta_files:
        return None

    # Use the partition-level metadata (not year-level)
    meta = json_read(meta_files[0])
    granules = meta.get('granules', [])
    if not granules:
        return None

    # Get existing parquet files in this partition
    parquet_files = glob.glob(os.path.join(h3_partition_dir, '*', '*.parquet'))
    if not parquet_files:
        return None

    # Early exit: check if new columns already exist in partition
    first_schema = read_parquet_schema(parquet_files[0])
    existing_cols = set(first_schema['column'].tolist())
    new_cols = set()
    for prod, var_list in new_product_vars.items():
        if var_list:
            suffix = f"_{prod.lower()}"
            new_cols.update(
                v if v.endswith(suffix) else f"{v}{suffix}"
                for v in var_list
                if v != 'shot_number'
            )
    if new_cols and new_cols.issubset(existing_cols):
        logger.debug(f"Partition already has new variables, skipping: {os.path.basename(h3_partition_dir)}")
        return None

    # Build the SOC file tree for locating source HDF5 files
    if isinstance(soc_source, str):
        glob_kwargs = {'version': version} if version is not None else None
        all_soc = soc_file_tree(soc_source, to_list=False, glob_kwargs=glob_kwargs)
    elif isinstance(soc_source, list):
        all_soc = soc_file_tree(soc_source, to_list=False)
    elif soc_source is None:
        # For S3 mode, we need to download the specific granules
        # This is handled by the caller providing a pre-built soc tree
        return None
    else:
        return None

    # Collect new variable data from source HDF5 files for each granule
    new_vars_list = []
    for gran in granules:
        orb_track = f"O{gran['orbit']:05d}_{gran['granule']:02d}_T{gran['track']:05d}"
        if orb_track not in all_soc:
            logger.debug(f"Granule {orb_track} not found in SOC source, skipping")
            continue

        soc_files = all_soc[orb_track]

        for prod, var_list in new_product_vars.items():
            if prod not in soc_files or var_list is None:
                continue

            try:
                cols_to_read = ['shot_number'] + var_list
                df = load_h5(soc_files[prod], columns=cols_to_read, include_source=False)
                if df is not None and not df.empty:
                    # Add product suffix to match normal build naming convention
                    suffix = f"_{prod.lower()}"
                    df = df.rename(columns=lambda x: x if x.endswith(suffix) else f"{x}{suffix}")
                    new_vars_list.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {prod} vars from {orb_track}: {e}")

    if not new_vars_list:
        return None

    import pandas as pd
    # load_h5 returns DataFrames indexed by shot_number — preserve that index
    new_vars_df = pd.concat(new_vars_list)
    new_vars_df = new_vars_df[~new_vars_df.index.duplicated(keep='first')]
    new_vars_df = new_vars_df.reset_index()  # shot_number becomes a regular column

    # Write new vars to temp parquet
    tmp_file = None
    updated_files = []
    try:
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_file = tmp.name
        new_vars_df.to_parquet(tmp_file, engine='pyarrow', index=False)
        del new_vars_df

        # Join into each year-level parquet file
        for pf in parquet_files:
            try:
                parquet_join_columns([pf, tmp_file], pf, key_col='shot_number')
                updated_files.append(pf)
            except Exception as e:
                logger.warning(f"Failed to join columns into {pf}: {e}")
    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

    # Update partition metadata with new columns
    if updated_files:
        for pf in updated_files:
            h3_write_metadata(pf)
        h3_merge_metadata(h3_partition_dir)

    return updated_files[0] if updated_files else None


def _build_add_variables(h3_dir, new_product_vars, soc_source=None, version=None):
    """Add new variables to existing H3 database partitions via shot_number join.

    Reads only shot_number + new variables from source HDF5 files,
    then joins into existing partition parquet files. No re-indexing needed.

    Parameters
    ----------
    h3_dir : str
        Root directory of the H3 database
    new_product_vars : dict
        Product code → list of new variable names to add
    soc_source : str, list, or None
        Source for HDF5 files
    version : int or None
        GEDI data version for S3 queries and local file filtering

    Returns
    -------
    list of str or None
        List of updated parquet file paths
    """
    logger.info("Variable-only expansion detected: adding new columns via shot_number join")

    h3_subdirs = glob.glob(os.path.join(h3_dir, 'h3_*/'))
    if not h3_subdirs:
        logger.info("No existing partitions to update")
        return None

    logger.info(f"Updating {len(h3_subdirs)} H3 partitions with new variables")

    from dask.distributed import as_completed as dask_as_completed

    client = get_dask_client()
    futures = {
        client.submit(_add_variables_to_partition, d, new_product_vars, soc_source, version): d
        for d in h3_subdirs
    }

    updated_files = []
    skipped_count = 0
    failed_count = 0
    for future in dask_as_completed(futures.keys()):
        d = futures[future]
        try:
            result = future.result()
            if result is not None:
                updated_files.append(result)
            else:
                skipped_count += 1
        except Exception as e:
            failed_count += 1
            logger.warning(f"Variable update failed for {os.path.basename(d.rstrip('/'))}: {e}")

        total = len(updated_files) + skipped_count + failed_count
        if total % 20 == 0 or total == len(h3_subdirs):
            logger.info(f"Variable update progress: {len(updated_files)} updated, {skipped_count} skipped / {len(h3_subdirs)} total")

    del futures

    if failed_count > 0:
        logger.error(f"{failed_count} partition variable updates failed. Re-run to retry.")

    logger.info(f"Updated {len(updated_files)}/{len(h3_subdirs)} partitions with new variables")

    # Regenerate manifest
    generate_manifest(h3_dir, tree_shape='h3db')

    return updated_files if updated_files else None


def build_h3db(
    product_vars: Dict[str, List[str]],
    res: int = 12,
    part: int = 3,
    spatial=None,
    temporal=None,
    soc_source: Union[str, List, None] = None,
    version: Optional[int] = None,
    tmp_dir: str = os.path.join(GH3_DEFAULT_TMP_DIR, 'gh3_build'),
    h3_dir: str = GH3_DEFAULT_H3_DIR,
    skip_granules: Optional[List[Dict]] = None,
    status_callback: Optional[Callable[[str], None]] = None,
    variable_only_update: bool = False,
    exclude: Optional[List[str]] = None,
) -> Optional[List[str]]:
    """
    Build an H3-indexed GEDI database from local SOC files or S3 download.

    Parameters
    ----------
    product_vars : dict
        Dictionary mapping GEDI product codes (e.g., 'L2A', 'L4A') to lists of
        variable names to extract. Use 'default', 'minimal', or explicit lists.
    res : int, default 12
        H3 resolution level for indexing individual shots (0-15).
    part : int, default 3
        H3 resolution level for file partitioning (0-15, must be <= res).
    spatial : GeoDataFrame, list, or str, optional
        Spatial filter for the region of interest.
    temporal : tuple, optional
        Temporal filter as (start_date, end_date) in 'YYYY-MM-DD' format.
    soc_source : str, list, or None
        - ``None``: download from NASA S3 to a temp directory (cleaned up after build)
        - ``str``: path to local directory containing GEDI SOC HDF5 files
        - ``list``: pre-acquired list of file paths or EarthAccessFile objects
    version : int or None
        GEDI data version. If None, uses latest available.
        Also used to filter local files by version when soc_source is a directory.
    tmp_dir : str
        Path to temporary directory for intermediate files.
    h3_dir : str
        Output path for the H3-indexed parquet database.
    skip_granules : list of dict, optional
        List of granule identifiers to skip (from previous builds).
    status_callback : callable, optional
        Called with status string at pipeline stage transitions.
    variable_only_update : bool, default False
        If True, only add new variable columns to existing partition files
        via shot_number join (``_build_add_variables``). Skips full pipeline.
        The caller (CLI) determines this from the build logger state.

    Returns
    -------
    list of str or None
        List of output parquet file paths, or None if no new data processed.

    Raises
    ------
    H3ValidationError
        If H3 resolution or partition parameters are invalid
    GediFileError
        If local source directory doesn't exist or S3 returns no files
    """
    # Validate parameters
    logger.debug("Validating build parameters")
    res, part = validate_h3_params(res, part)

    # Track temp directory for S3 download cleanup
    _s3_tmp_dir = None

    # Determine source mode and acquire SOC files
    if soc_source is None:
        # S3 ETL mode: open remote files via earthaccess, extract only selected
        # variables via h5py range requests, write compact local HDF5 files.
        # Transfers 10-50x less data than full download depending on variable selection.
        _s3_tmp_dir = os.path.join(tmp_dir, '_s3_download')
        os.makedirs(_s3_tmp_dir, exist_ok=True)

        if variable_only_update:
            # Download only new products (no L2A essentials), then join to existing partitions
            logger.info(f"S3 variable-only update: adding {set(product_vars.keys())} to existing database")
            try:
                if status_callback:
                    status_callback('PROCESSING')
                s3_etl_subset(
                    product_vars=product_vars.copy(),
                    spatial=spatial,
                    temporal=temporal,
                    version=version,
                    odir=_s3_tmp_dir,
                    ensure_l2a=False,
                )
                result = _build_add_variables(
                    h3_dir, product_vars,
                    soc_source=_s3_tmp_dir, version=version,
                )
                if not result:
                    logger.warning(
                        "Variable update completed but no partition files were modified. "
                        "New columns may not have been added to the database."
                    )
                return result
            finally:
                if os.path.exists(_s3_tmp_dir):
                    shutil.rmtree(_s3_tmp_dir, ignore_errors=True)

        # Fresh build or mixed update: full pipeline with L2A
        logger.info(f"ETL subset from S3 to temp directory: {_s3_tmp_dir}")
        s3_etl_subset(
            product_vars=product_vars.copy(),
            spatial=spatial,
            temporal=temporal,
            version=version,
            odir=_s3_tmp_dir,
        )
        soc_source = _s3_tmp_dir
        logger.info("S3 ETL subset complete, building from local files")

    if isinstance(soc_source, list):
        # Pre-acquired file list (local or EarthAccessFile)
        logger.info(f"Using {len(soc_source)} pre-acquired source files")
        if version is not None:
            before = len(soc_source)
            def _matches_version(p):
                try:
                    pp = p.path if isinstance(p, EarthAccessFile) else p
                    return GEDIFile(pp).version == version
                except Exception:
                    return False
            soc_source = [p for p in soc_source if _matches_version(p)]
            dropped = before - len(soc_source)
            if dropped:
                logger.warning(f"Dropped {dropped} pre-acquired file(s) not matching version V{version:03d}")
        all_soc_files = soc_file_tree(soc_source, to_list=True, exclude=exclude)
    elif isinstance(soc_source, str):
        # Local directory mode (also used after S3 download to temp dir)
        if not os.path.exists(soc_source):
            raise GediFileError(f"SOC source directory not found: {soc_source}")
        # Resolve version BEFORE listing so the glob filter excludes other
        # versions and soc_file_tree's pivot key (orb_track, no version) cannot
        # collide same-orbit granules across versions.
        if version is None:
            sample = next(iter(glob.glob(os.path.join(soc_source, '**', 'GEDI*.h5'), recursive=True)), None)
            if sample is not None:
                try:
                    version = GEDIFile(sample).version
                    logger.info(f"Auto-detected GEDI version V{version:03d} from {os.path.basename(sample)}")
                except Exception:
                    logger.warning("Could not auto-detect GEDI version from sample file; listing without version filter")
        logger.info("Listing source SOC files")
        glob_kwargs = {'version': version} if version is not None else None
        all_soc_files = soc_file_tree(soc_source, to_list=True, glob_kwargs=glob_kwargs, exclude=exclude)
    else:
        raise GediValidationError(f"Invalid soc_source type: {type(soc_source)}")

    try:
        if status_callback:
            status_callback('PROCESSING')

        if variable_only_update:
            logger.info(f"Variable-only update: adding {set(product_vars.keys())} to existing database")
            result = _build_add_variables(h3_dir, product_vars, soc_source=soc_source, version=version)
            if not result:
                logger.warning(
                    "Variable update completed but no partition files were modified. "
                    "New columns may not have been added to the database."
                )
            return result

        # Expand variable specifications and ensure L2A essentials
        product_vars = _expand_product_vars(product_vars, all_soc_files, version=version)

        # Filter to only files with required products
        prod_soc_files = [{k: val for k, val in i.items() if k in product_vars} for i in all_soc_files]

        # Filter out incomplete, corrupted, or already-processed granules
        soc_files = _filter_granules(prod_soc_files, product_vars, skip_granules)

        if len(soc_files) == 0:
            logger.info("No new granules to process")
            return None

        # Create H3-indexed Dask DataFrame
        ddf, lat_col, lon_col, dat_col, frag_names = _create_h3_dataframe(soc_files, product_vars, res, part)

        # Apply spatial and existing-data filters
        os.makedirs(tmp_dir, exist_ok=True)
        ddf = _apply_spatial_filter(ddf, spatial, part, h3_dir)

        if status_callback:
            status_callback('PARTITIONING')

        # Write partitioned parquet files to a subdirectory of tmp_dir to avoid
        # conflicting with Dask worker scratch space (dask-worker-space/dirlock)
        # which causes PermissionError on Windows when overwrite=True deletes tmp_dir
        parquet_dir = os.path.join(tmp_dir, 'partitions')
        if _streaming_enabled():
            # Streaming writer: client.map + as_completed with per-task atomic
            # leaves and per-(granule × beam) completion sentinels. Bounded
            # worker memory; resume-safe; identical fragment layout to the
            # legacy path (see _write_partitioned_streaming docstring).
            wrote_any = _write_partitioned_streaming(
                ddf, soc_files, product_vars, res, part,
                parquet_dir, h3_dir, spatial,
                lat_col, lon_col, dat_col,
            )
        else:
            wrote_any = _write_partitioned(
                ddf, parquet_dir, part, lat_col, lon_col, dat_col,
                frag_names=frag_names,
            )

        if not wrote_any:
            logger.info("No new data to process")
            return None

        if status_callback:
            status_callback('MERGING')

        # Merge and finalize database
        h3_files = _merge_and_finalize(parquet_dir, h3_dir)

        return h3_files
    finally:
        # Clean up S3 temp download directory
        if _s3_tmp_dir is not None and os.path.exists(_s3_tmp_dir):
            logger.debug(f"Cleaning up S3 temp directory: {_s3_tmp_dir}")
            shutil.rmtree(_s3_tmp_dir, ignore_errors=True)

def merge_build_logs(log_file_1: str, log_file_2: str, output_log_file: str) -> dict:
    """
    Merge two build log files from separate databases.

    Combines granules, columns, partition IDs, and date ranges while validating
    configuration consistency (gedi_version, h3_resolution_level, h3_partition_level).

    Parameters
    ----------
    log_file_1 : str
        Path to first build log (base log)
    log_file_2 : str
        Path to second build log (log to merge in)
    output_log_file : str
        Path to write merged build log

    Returns
    -------
    dict
        Merged log dictionary

    Raises
    ------
    FileNotFoundError
        If either log file does not exist
    ValueError
        If log configurations are incompatible (different gedi_version, h3_resolution_level, or h3_partition_level)

    Examples
    --------
    >>> merged_log = merge_build_logs(
    ...     f'/path/to/database_world/{BUILD_LOG_FILENAME}',
    ...     f'/path/to/database_world_a10/{BUILD_LOG_FILENAME}',
    ...     f'/path/to/database_world_merged/{BUILD_LOG_FILENAME}'
    ... )
    """
    
    # Load both log files
    if not os.path.exists(log_file_1):
        raise GediFileError(f"Log file not found: {log_file_1}")
    if not os.path.exists(log_file_2):
        raise GediFileError(f"Log file not found: {log_file_2}")
    
    log1 = json_read(log_file_1)
    log2 = json_read(log_file_2)
    
    # Validate configuration compatibility
    for key in ['gedi_version', 'h3_resolution_level', 'h3_partition_level']:
        if log1.get(key) != log2.get(key):
            raise GediMergeError(
                f"Incompatible {key}: log1={log1.get(key)}, log2={log2.get(key)}. "
                f"Cannot merge logs with different configurations."
            )
    
    # Start with log1 as base
    merged_log = log1.copy()
    merged_log['last_modified'] = now()
    
    # Merge products and variables
    products_1 = log1.get('products', {})
    products_2 = log2.get('products', {})
    
    merged_products = products_1.copy()
    for prod_key, prod_data in products_2.items():
        if prod_key in merged_products:
            # Product exists in both logs - merge variables
            existing_vars = set(merged_products[prod_key].get('variables', []))
            new_vars = set(prod_data.get('variables', []))
            merged_vars = sorted(list(existing_vars | new_vars))
            merged_products[prod_key]['variables'] = merged_vars
            # Use COMPLETED status if both are complete
            if merged_products[prod_key].get('status') == 'COMPLETED' and prod_data.get('status') == 'COMPLETED':
                merged_products[prod_key]['status'] = 'COMPLETED'
            # Update timestamp to most recent
            ts1 = merged_products[prod_key].get('last_modified', '')
            ts2 = prod_data.get('last_modified', '')
            merged_products[prod_key]['last_modified'] = max(ts1, ts2) if ts1 and ts2 else (ts1 or ts2)
        else:
            # Product only in log2 - add it
            merged_products[prod_key] = prod_data.copy()
    
    merged_log['products'] = merged_products
    
    # Merge granules (deduplicate)
    granules_1 = log1.get('granules', [])
    granules_2 = log2.get('granules', [])
    merged_granules = granules_1.copy()
    for g in granules_2:
        if g not in merged_granules:
            merged_granules.append(g)
    if merged_granules:
        merged_log['granules'] = merged_granules
    
    # Merge h3_columns (deduplicate and sort)
    cols_1 = set(log1.get('h3_columns', []))
    cols_2 = set(log2.get('h3_columns', []))
    merged_cols = sorted(list(cols_1 | cols_2))
    if merged_cols:
        merged_log['h3_columns'] = merged_cols

    # Merge h3_columns_dtypes (post-merge invariant: identical schema
    # across partitions, so the two logs should agree on overlapping
    # columns; later log wins on conflict, matching merge_build_logs'
    # general "log2 augments log1" semantics).
    dtypes_1 = log1.get('h3_columns_dtypes') or {}
    dtypes_2 = log2.get('h3_columns_dtypes') or {}
    if dtypes_1 or dtypes_2:
        merged_log['h3_columns_dtypes'] = {**dtypes_1, **dtypes_2}
    
    # Merge h3_partition_ids (deduplicate and sort)
    parts_1 = set(log1.get('h3_partition_ids', []))
    parts_2 = set(log2.get('h3_partition_ids', []))
    merged_parts = sorted(list(parts_1 | parts_2))
    if merged_parts:
        merged_log['h3_partition_ids'] = merged_parts
    
    # Merge date_range (min start date, max end date)
    dr1 = log1.get('date_range')
    dr2 = log2.get('date_range')
    if dr1 and dr2:
        merged_log['date_range'] = [min(dr1[0], dr2[0]), max(dr1[1], dr2[1])]
    elif dr2:
        merged_log['date_range'] = dr2
    
    # Merge spatial_filter (use union of geometries if both exist)
    spatial_1 = log1.get('spatial_filter')
    spatial_2 = log2.get('spatial_filter')
    if spatial_1 and spatial_2:
        # Combine feature collections
        features_1 = spatial_1.get('features', [])
        features_2 = spatial_2.get('features', [])
        merged_features = features_1 + features_2
        merged_log['spatial_filter'] = {
            'type': 'FeatureCollection',
            'features': merged_features
        }
        # Update bbox if available
        if 'bbox' in spatial_1 and 'bbox' in spatial_2:
            bbox1 = spatial_1['bbox']
            bbox2 = spatial_2['bbox']
            merged_log['spatial_filter']['bbox'] = [
                min(bbox1[0], bbox2[0]),  # min lon
                min(bbox1[1], bbox2[1]),  # min lat
                max(bbox1[2], bbox2[2]),  # max lon
                max(bbox1[3], bbox2[3])   # max lat
            ]
    elif spatial_2:
        merged_log['spatial_filter'] = spatial_2
    
    # Write merged log
    os.makedirs(os.path.dirname(output_log_file), exist_ok=True)
    json_write(merged_log, output_log_file, rewrite=True)
    
    logger.info(f"Merged build logs: {log_file_1} + {log_file_2} -> {output_log_file}")
    
    return merged_log