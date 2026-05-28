# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

import os, re, glob, h5py, h3, json
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
    # Install HTTP timeouts on the cloned-per-thread sessions used by
    # earthaccess._download_file. Without this, a dead CloudFront edge
    # connection blocks the worker indefinitely (CLOSE-WAIT recv hang).
    from .daac import _install_request_timeouts
    _install_request_timeouts()


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

    logger.debug(f"[{file_idx}/{n_files}] Subsetting {gf.full_name} ({len(vars_for_prod)} vars)")
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
    from tqdm import tqdm as tqdm_bar
    pbar = tqdm_bar(total=n_files, desc="S3 ETL subset", unit="file")
    try:
        for future, result in dask_as_completed(futures, with_results=True):
            completed_count += 1
            if result is None:
                failed += 1
            pbar.set_postfix(failed=failed, refresh=False)
            pbar.update(1)
    finally:
        pbar.close()

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

    # Preventative: drop 0-byte source fragments before they reach
    # parquet_merge_files (which fails the whole merge on the first bad
    # file). 0-byte parquets are the dominant SIGKILL-leftover class —
    # ``AtomicFileWriter`` writes to ``.tmp`` then ``os.replace``s, but a
    # worker killed between the empty-file creation and any actual data
    # write leaves a final-named 0-byte parquet. One ``stat`` per source
    # fragment, effectively free since the file open hits the same
    # metadata anyway. The worker logs each unlinked fragment so the
    # operator can correlate with which (granule × beam) is being lost.
    _filtered = []
    for f in files:
        try:
            if os.path.getsize(f) == 0:
                logger.warning(
                    f"h3_merge_files: dropping 0-byte source fragment {f}"
                )
                try:
                    os.unlink(f)
                except OSError:
                    pass
                continue
        except OSError:
            # File vanished between glob and stat — race with another
            # worker's rm_src cleanup. Drop it silently; merge proceeds
            # with the rest.
            continue
        _filtered.append(f)
    files = _filtered
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


# Per-failed-merge sentinel dir, parallel to ``_COMPLETE_SENTINEL_DIRNAME``.
# Each failed (h3_partition × year) merge writes one file naming the
# partition and the error so L1 resume + L2 doctor can recover without
# scanning the tmp tree. Atomic per-failure (no append-to-shared-file
# torn-line risk that plain ``open('a')`` would have on SIGKILL).
_MERGE_FAILURES_DIRNAME = '_merge_failures'


def _merge_failure_sentinel_name(tmp_dir: str, partition_dir: str) -> str:
    """Stable filename derived from the partition's path relative to tmp_dir.

    ``tmp/partitions/h3_03=8366c1fffffffff/year=2019`` →
    ``h3_03=8366c1fffffffff__year=2019.fail`` — readable and unique under
    the canonical tree layout (no slashes left to escape).
    """
    rel = os.path.relpath(partition_dir.rstrip('/'), tmp_dir)
    return rel.replace('/', '__') + '.fail'


def _merge_failure_sentinel_path(tmp_dir: str, partition_dir: str) -> str:
    return os.path.join(
        tmp_dir, _MERGE_FAILURES_DIRNAME,
        _merge_failure_sentinel_name(tmp_dir, partition_dir),
    )


def _emit_merge_failure_sentinel(tmp_dir: str, partition_dir: str, error: BaseException) -> None:
    """Persist a one-shot record of a failed merge.

    Contents: two lines — the absolute ``partition_dir`` and the formatted
    exception. ``AtomicFileWriter`` is overkill here since the file is
    small + single-write + per-failure-unique; ``open(path, 'w')`` is
    sufficient and the worst case (write interrupted by SIGKILL) just
    leaves a truncated record that L1 resume re-derives by re-attempting
    the merge.
    """
    path = _merge_failure_sentinel_path(tmp_dir, partition_dir)
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    body = f"{partition_dir}\n{type(error).__name__}: {error}\n"
    try:
        with open(path, 'w') as f:
            f.write(body)
    except OSError:
        # The failure sentinel is a safety net, not a correctness contract;
        # losing one is non-fatal (next resume re-discovers via re-merge).
        pass


_GRANULE_FAILURES_FILENAME = '_granule_failures.jsonl'

# Per-build sidecar enumerating granules whose status must be flipped back
# from INDEXED → MERGE_FAILED because their merged partition broke on a
# recoverable, known-bad fragment class. Written by ``_merge_and_finalize``
# during the failure handler; folded into the build-log JSON by the CLI
# (``apply_merge_failures_to_logger``) after merge returns. Decouples the
# merge driver from the H3BuildLogger so ``_merge_and_finalize`` keeps a
# narrow signature ``(tmp_dir, h3_dir)``.
_MERGE_FAILED_GRANULES_FILENAME = '_merge_failed_granules.jsonl'

# Marker tokens we use to recognize the fragment-corruption classes the
# resume layer can recover from (delete fragment + re-extract granule).
# Other exception kinds (disk full on dest, missing schema field, etc.)
# leave the granule status alone — those are infrastructure issues, not
# data issues, and flipping them would force pointless re-extraction.
_RECOVERABLE_FRAGMENT_ERROR_MARKERS = (
    'Parquet file size is 0 bytes',
    'Parquet magic bytes not found in footer',
    'Couldn\'t deserialize thrift',  # truncated footer mid-Thrift
)


def _is_recoverable_fragment_error(exc: BaseException) -> bool:
    """True when the exception text matches a known partial-write artifact
    class. See ``_RECOVERABLE_FRAGMENT_ERROR_MARKERS`` for the curated list.
    """
    msg = str(exc)
    return any(marker in msg for marker in _RECOVERABLE_FRAGMENT_ERROR_MARKERS)


def _granules_in_partition_dir(partition_dir: str) -> List[Tuple[int, int, int]]:
    """Parse (orbit, granule, track) tuples from fragment basenames in one
    h3_*/year=* partition directory. Returns a deduplicated list.

    O(N_fragments_in_dir). Never iterates other partitions.
    """
    out: set = set()
    try:
        entries = list(os.scandir(partition_dir))
    except OSError:
        return []
    for e in entries:
        if not e.is_file() or not e.name.endswith('.parquet'):
            continue
        m = _FRAGMENT_BASENAME_RE.match(e.name)
        if m:
            out.add((int(m.group(1)), int(m.group(2)), int(m.group(3))))
    return sorted(out)


def _emit_merge_failed_granules(tmp_dir: str, partition_dir: str,
                                 granules: List[Tuple[int, int, int]],
                                 error: BaseException) -> None:
    """Append granule-flip-back records to the merge-failed-granules JSONL.

    One record per granule (not per partition) so the CLI fold can drive
    status updates directly. The whole list for a single failed partition
    is written in one ``open + write`` to keep the per-failure cost O(1)
    on the hot path.
    """
    if not granules:
        return
    path = os.path.join(tmp_dir, _MERGE_FAILED_GRANULES_FILENAME)
    err_str = f"{type(error).__name__}: {error}"
    lines = [
        json.dumps({
            'orbit': g[0], 'granule': g[1], 'track': g[2],
            'partition_dir': partition_dir,
            'error': err_str,
        })
        for g in granules
    ]
    try:
        with open(path, 'a') as f:
            f.write('\n'.join(lines) + '\n')
    except OSError:
        # Safety net, not a correctness contract — the next resume's merge
        # will still re-discover via re-failing the same merge.
        pass


def _read_merge_failed_granules(tmp_dir: str) -> List[Dict[str, Any]]:
    """Read every recorded granule-flip-back record. O(N_failures)."""
    path = os.path.join(tmp_dir, _MERGE_FAILED_GRANULES_FILENAME)
    out: List[Dict[str, Any]] = []
    if not os.path.isfile(path):
        return out
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return out


def preclean_merge_failures(tmp_dir: str) -> Dict[str, int]:
    """L1 resume pre-clean: act on every recorded ``_merge_failures`` sentinel.

    For each partition the prior run's merge marked failed:

    1. Stat each ``*.parquet`` in the partition — unlink if 0-byte (the
       canonical class B artifact). Truncated-but-nonzero parquets (class C)
       are unlinked only when ``pq.ParquetFile`` cannot open them — checked
       cheaply by opening the footer once. The cost is bounded by the
       failure list, never the full healthy tree.
    2. Unlink any co-located ``*.tmp`` / ``*.merge.tmp`` siblings. These
       survive SIGKILL when ``AtomicFileWriter.__exit__`` never runs and
       would otherwise pollute future merges.
    3. Delete the failure sentinel itself so re-running the pre-clean is
       idempotent (next run only re-acts on freshly-failed merges).

    Returns ``{'partitions_cleaned': N, 'parquets_removed': N, 'tmps_removed': N}``.
    Companion to ``apply_merge_failures_to_logger`` — calling this without
    the granule flip-back would unlink fragments and leave their granules
    marked INDEXED, permanently dropping rows. The CLI runs both together.
    """
    out = {'partitions_cleaned': 0, 'parquets_removed': 0, 'tmps_removed': 0}
    failures = _scan_merge_failure_sentinels(tmp_dir)
    if not failures:
        return out
    for partition_dir, _err in failures.items():
        if not os.path.isdir(partition_dir):
            # Partition was deleted between runs (cleanup, manual rm); just
            # drop the sentinel so it doesn't fire again.
            try:
                os.unlink(_merge_failure_sentinel_path(tmp_dir, partition_dir))
            except OSError:
                pass
            continue
        try:
            entries = list(os.scandir(partition_dir))
        except OSError:
            continue
        for e in entries:
            if not e.is_file():
                continue
            name = e.name
            try:
                size = e.stat().st_size
            except OSError:
                continue
            if name.endswith('.tmp') or name.endswith('.merge.tmp'):
                # AtomicFileWriter orphan from SIGKILL — always safe to remove.
                try:
                    os.unlink(e.path)
                    out['tmps_removed'] += 1
                except OSError:
                    pass
                continue
            if not name.endswith('.parquet'):
                continue
            should_remove = False
            if size == 0:
                should_remove = True
            else:
                # Cheap header probe — only opens the footer, not the body.
                try:
                    pq.ParquetFile(e.path).metadata
                except Exception:
                    should_remove = True
            if should_remove:
                try:
                    os.unlink(e.path)
                    out['parquets_removed'] += 1
                except OSError:
                    pass
        # Drop the sentinel — the cleanup acted; next merge will re-emit if
        # it fails again. Keeping it would loop the pre-clean forever.
        try:
            os.unlink(_merge_failure_sentinel_path(tmp_dir, partition_dir))
        except OSError:
            pass
        out['partitions_cleaned'] += 1
    return out


def apply_merge_failures_to_logger(h3_logger, tmp_dir: str) -> int:
    """Fold the merge-failed-granules sidecar into the build-log's
    granule status (INDEXED → MERGE_FAILED). Returns the count flipped.

    Idempotent: re-applying after a successful resume is a no-op because
    re-extracted granules have already been flipped back to INDEXED by
    ``_reconcile_granules_from_disk``. Truncates the sidecar after fold to
    keep it bounded across resumes.

    Callable from the CLI right after ``_merge_and_finalize`` returns,
    regardless of which code path invoked merge. Does NOT call
    ``h3_logger.save_log`` — the caller controls save cadence.
    """
    records = _read_merge_failed_granules(tmp_dir)
    if not records:
        return 0
    flipped = 0
    for rec in records:
        try:
            key = {'orbit': int(rec['orbit']),
                   'granule': int(rec['granule']),
                   'track': int(rec['track'])}
        except (KeyError, ValueError, TypeError):
            continue
        # Only flip if currently INDEXED. PENDING / other statuses are
        # already non-skip on resume so no flip is needed; preserves
        # idempotency across repeated folds.
        if not hasattr(h3_logger, 'granule_info'):
            return flipped
        for g in h3_logger.granule_info:
            if (g.get('orbit'), g.get('granule'), g.get('track')) == \
               (key['orbit'], key['granule'], key['track']):
                if g.get('status') == 'INDEXED':
                    g['status'] = 'MERGE_FAILED'
                    flipped += 1
                break
    # Truncate the sidecar — next resume re-derives only if a new merge
    # fails. Leaving stale records would cause an old MERGE_FAILED flip
    # to keep firing every resume forever.
    try:
        os.unlink(os.path.join(tmp_dir, _MERGE_FAILED_GRANULES_FILENAME))
    except OSError:
        pass
    return flipped

# Pattern: HDF5 "object 'X' doesn't exist" / "Unable to synchronously open
# object (object 'X' doesn't exist)" — used by ``_classify_load_h5_failure``
# to recognize the missing-variable case so downstream tooling
# (``gh3_update --recover-missing-vars``) can offer a precise recipe.
_MISSING_VAR_RE = re.compile(r"object\s+['\"]([^'\"]+)['\"]\s+doesn'?t\s+exist", re.IGNORECASE)


def _classify_load_h5_failure(exc: BaseException, soc_dict: Dict[str, str]) -> Dict[str, Any]:
    """Structured classification of a Stage1 ``load_h5_merged`` failure.

    Today we only specialize the ``missing_var`` case (NASA-side L2A/L2B/L4A
    schema variance across orbit clusters — orbits O20752–O20767 of L2A,
    which lack ``l2a_quality_flag_rel3_a10`` despite the shipped manifest
    claiming they have it). Other failures get a generic ``other`` kind
    with the raw exception text — still queryable, just not auto-recoverable.

    Returns a JSON-serializable dict so the driver can append it directly
    to the per-build ``_granule_failures.jsonl`` sidecar.
    """
    msg = str(exc)
    kind: str = 'other'
    var: Optional[str] = None
    product: Optional[str] = None
    if isinstance(exc, KeyError) or 'KeyError' in type(exc).__name__:
        m = _MISSING_VAR_RE.search(msg)
        if m:
            kind = 'missing_var'
            var = m.group(1)
    # Best-effort product inference: which product file does the source path
    # belong to? soc_dict keys are the product codes (L2A/L2B/L4A); pick the
    # first one whose path appears in the message, else None.
    if msg:
        for prod_code, h5_path in (soc_dict or {}).items():
            try:
                if h5_path and os.path.basename(str(h5_path)) in msg:
                    product = prod_code
                    break
            except Exception:
                continue
    return {
        'kind': kind,
        'var': var,
        'product': product,
        'error_type': type(exc).__name__,
        'error_message': msg,
    }


def _append_granule_failure(tmp_dir: str, frag_name: str, failure: Dict[str, Any]) -> None:
    """Append one failure record to ``tmp_dir/_granule_failures.jsonl``.

    Single-writer (driver thread) so no concurrency guard needed. Append-only
    + line-buffered for crash-safety — a SIGKILL between batches loses only
    the in-flight line. The whole file is folded into the build-log JSON at
    finalize so post-build consumers (``gh3_update --recover-missing-vars``)
    can resolve {orbit,granule,track} → failure cause with no log-grep.

    Why JSONL, not full JSON rewrite: rewriting the 97k-granule build log on
    every failure would be the exact O(N) driver-side I/O Pillar 1 bans.
    Append-only delta + finalize-time fold gives O(1) per failure on the
    hot path and O(N_failures) at end-of-build instead of O(N_granules)
    per failure.
    """
    path = os.path.join(tmp_dir, _GRANULE_FAILURES_FILENAME)
    record = {'frag_name': frag_name, **failure}
    try:
        with open(path, 'a') as f:
            f.write(json.dumps(record) + '\n')
    except OSError:
        # Same logic as merge-failure sentinel: this is a safety net, not a
        # correctness contract — the in-memory ``n_fail`` counter is still
        # accurate, and the WARN log line still surfaces the error.
        pass


def _read_granule_failures(tmp_dir: str) -> List[Dict[str, Any]]:
    """Read all recorded granule-failure records. O(N_failures); never
    iterates partitions. Used by the finalize fold and by gh3_update."""
    path = os.path.join(tmp_dir, _GRANULE_FAILURES_FILENAME)
    out: List[Dict[str, Any]] = []
    if not os.path.isfile(path):
        return out
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    # Tolerate the rare torn last line from a SIGKILL during
                    # the writer's flush — same principle as parquet_merge
                    # tolerating corrupt fragments.
                    continue
    except OSError:
        pass
    return out


def _scan_merge_failure_sentinels(tmp_dir: str) -> Dict[str, str]:
    """Return ``{partition_dir: error_string}`` for every recorded failure.

    O(N_failures) — never iterates healthy partitions. Used by L1 resume
    and L2 doctor (``tmp_partitions_health``).
    """
    out: Dict[str, str] = {}
    sentinel_dir = os.path.join(tmp_dir, _MERGE_FAILURES_DIRNAME)
    if not os.path.isdir(sentinel_dir):
        return out
    try:
        entries = list(os.scandir(sentinel_dir))
    except OSError:
        return out
    for entry in entries:
        if not entry.is_file() or not entry.name.endswith('.fail'):
            continue
        try:
            with open(entry.path, 'r') as f:
                lines = f.read().splitlines()
        except OSError:
            continue
        if not lines:
            continue
        partition_dir = lines[0].strip()
        err = lines[1].strip() if len(lines) >= 2 else ''
        if partition_dir:
            out[partition_dir] = err
    return out


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


def _derive_merged_output_paths(merge_progress_file: str, h3_dir: str) -> List[str]:
    """Derive absolute output parquet paths from the merge-progress file.

    Pure in-memory transform of the merge-progress lines (one tmp partition
    dir per line) into the deterministic final paths via the
    ``h3_merge_files`` naming contract:
    ``<tmp>/h3_<p>=X/year=Y`` → ``<h3_dir>/h3_<p>=X/year=Y/X.Y.0.parquet``.

    Used by ``_merge_and_finalize`` to (a) write the final database
    manifest without walking the tree at end-of-merge, and (b) refresh
    the manifest incrementally during long merge phases so consumers
    reading mid-build see partial-but-fresh state. Zero GPFS metadata ops.
    """
    out: List[str] = []
    if not os.path.exists(merge_progress_file):
        return out
    try:
        with open(merge_progress_file, 'r') as f:
            for line in f:
                tmp_p = line.strip()
                if not tmp_p:
                    continue
                year_bn = os.path.basename(tmp_p.rstrip('/'))
                ydir = os.path.dirname(tmp_p.rstrip('/'))
                h3part = os.path.basename(ydir.rstrip('/'))
                if not h3part.startswith('h3_') or '=' not in h3part:
                    continue
                h3val = h3part.split('=', 1)[-1]
                yval = year_bn.split('=', 1)[-1] if '=' in year_bn else year_bn
                out.append(os.path.join(
                    h3_dir, h3part, year_bn, f'{h3val}.{yval}.0.parquet'
                ))
    except OSError:
        return []
    return out


def _scan_partition_meta_granules(
    partition_dir: str,
    *,
    meta_filename: str = PARTITION_META_FILENAME,
) -> set:
    """Worker: parse granule IDs from every PARTITION_META JSON under one
    h3_* partition. Module-level so it pickles for dask.

    Looks in two locations:
    1. ``partition_dir/*<meta>`` — partition-level meta (no year subdir)
    2. ``partition_dir/*/*<meta>`` — year-level meta (h3_*/year=*/...)

    Returns a set of ``(orbit, granule, track)`` tuples; empty on any read
    failure (the caller treats per-partition errors as non-fatal, matching
    the existing reconcile semantics).
    """
    out: set = set()
    for pat in (f'*{meta_filename}', os.path.join('*', f'*{meta_filename}')):
        for mf in glob.glob(os.path.join(partition_dir, pat)):
            try:
                for g in (json_read(mf) or {}).get('granules', []):
                    try:
                        out.add((g['orbit'], g['granule'], g['track']))
                    except (KeyError, TypeError):
                        continue
            except Exception:
                continue
    return out


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

    # Pass A — finalized partition metadata JSONs.
    # Granules named here are inside an h3 partition that has already been
    # merged and finalized → all their beams' rows are already consolidated
    # into the final parquet, so they're complete by construction.
    #
    # Sourcing the partition list:
    #   * Prefer the manifest sentinel (one cached read) — at continental
    #     scale this is the only way to keep Pass A O(N_partitions) instead
    #     of O(N_finalized_files) (two recursive globs over the entire DB
    #     tree, which was the dominant resume cost on million-partition
    #     databases).
    #   * Fall back to a single ``os.scandir`` on h3_dir for legacy DBs
    #     without a manifest sentinel — still avoids the recursive globs.
    #
    # Dispatch the per-partition meta read across workers when a dask
    # client is registered; serial loop otherwise (small DB or library /
    # notebook context with no cluster).
    partition_dirs_set: set = set()
    try:
        from .utils import _read_manifest as _read_db_manifest
        _mp = _read_db_manifest(h3_dir)
    except Exception:
        _mp = None
    if _mp:
        for _rel in _mp:
            _head = _rel.split('/', 1)[0]
            if _head.startswith('h3_'):
                partition_dirs_set.add(os.path.join(h3_dir, _head))
    if not partition_dirs_set:
        try:
            for e in os.scandir(h3_dir):
                if e.is_dir(follow_symlinks=False) and e.name.startswith('h3_'):
                    partition_dirs_set.add(e.path)
        except OSError:
            pass
    partition_dirs_list = sorted(partition_dirs_set)
    if partition_dirs_list:
        client = None
        try:
            client = get_dask_client()
        except Exception:
            pass
        if client is not None and len(partition_dirs_list) > 100:
            from .parallel import parallel_map
            for _, _gids in parallel_map(
                partition_dirs_list,
                _scan_partition_meta_granules,
                desc='Reconcile Pass A',
                unit='part',
                meta_filename=PARTITION_META_FILENAME,
            ):
                if isinstance(_gids, Exception):
                    continue
                indexed_ids.update(_gids)
        else:
            for _pd in partition_dirs_list:
                indexed_ids.update(
                    _scan_partition_meta_granules(
                        _pd, meta_filename=PARTITION_META_FILENAME,
                    )
                )

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
    stats = {'frag_name': frag_name, 'leaves': 0, 'rows': 0, 'skipped': False,
             'error': None, 'failure': None}

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
        # whole job. Streaming surfaces the error in stats for visibility,
        # plus a structured ``failure`` record so the driver can persist it
        # for downstream recovery (gh3_update --recover-missing-vars) without
        # needing to grep the WARN log lines later.
        stats['skipped'] = True
        stats['error'] = f"load_h5_merged: {type(e).__name__}: {e}"
        stats['failure'] = _classify_load_h5_failure(e, soc_dict)
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


def _streaming_batch_size(n_workers: Optional[int] = None) -> int:
    """Rolling-window inflight target for the streaming driver.

    Defaults to ``max(n_workers * 2, 100)`` when ``n_workers`` is known —
    just enough buffering above the 1-task-per-worker minimum to absorb
    task-time variance without hiding stragglers. A 2000-inflight default
    (an earlier design) was 8× overkill on this cluster: most queued tasks
    would sit invisible behind the rolling-window's release-as-completed
    loop, defeating the visibility goal.

    Override via ``GH3_WRITE_STREAMING_BATCH`` env. ``n_workers=None``
    falls back to a static ``500`` (~2× the gsapp cluster's typical
    capacity) when the driver hasn't connected to the scheduler yet.
    """
    val = os.environ.get('GH3_WRITE_STREAMING_BATCH')
    if val is not None:
        try:
            return max(1, int(val))
        except ValueError:
            pass
    if n_workers and n_workers > 0:
        return max(n_workers * 2, 100)
    return 500


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
    import time as _time
    from dask.distributed import as_completed as dask_as_completed
    from tqdm import tqdm as tqdm_bar

    client = get_dask_client()
    if client is None:
        raise GediError(
            "_write_partitioned_streaming requires a registered dask "
            "Client (wrap your call in `with Client(...): ...`)."
        )

    # Inflight-target sizing: tied to the live worker count so the rolling
    # window stays just-large-enough to keep workers busy without hiding
    # stragglers in a deep queue. Caller can override (e.g. for the
    # LocalCluster-based regression test which runs with 2 workers).
    if inflight_target is None:
        try:
            n_workers_live = len(client.scheduler_info().get('workers', {})) or None
        except Exception:
            n_workers_live = None
        inflight_target = _streaming_batch_size(n_workers=n_workers_live)

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

    # ── Bake broadcast values into a functools.partial ──────────────────
    # Iteration history on this driver's broadcast-kwarg handling:
    #   1. scatter(value, broadcast=True) — scattered iterables element-wise,
    #      created thousands of stray futures.
    #   2. scatter([value], broadcast=True)[0] — fixed cardinality but
    #      broadcast=True hung waiting for every SSH-tunneled worker ACK.
    #   3. client.map(fn, tasks, **kwargs) inlining — dask treats certain
    #      kwarg names as scheduler TaskState keys (a kwarg named
    #      "spatial_h3_tiles" became <TaskState 'spatial_h3_tiles'
    #      processing> stuck-in-processing); workers then crashed on
    #      AssertionError when trying to resolve the dependency.
    #
    # Final approach: bake all broadcast kwargs into a ``functools.partial``
    # closure around the worker function. ``client.map(partial_fn, tasks)``
    # then sees a single callable + the per-task iterable — dask cannot
    # introspect or split the partial's captured kwargs, so there is no
    # opportunity to misinterpret them as scheduler tasks or scatter them
    # element-wise.
    #
    # Memory accounting:
    #   * The partial is pickled ONCE into the Blockwise layer (one ~210 KB
    #     blob covering spatial_h3_tiles + product_vars + the pyarrow
    #     schema), and per-task entries are tiny refs into that layer.
    #   * Total scheduler-side graph footprint for a 532k-task build:
    #     ~530 MB — negligible on a 1 TB hub. Drains as fut.release()
    #     runs per completion below.
    import functools as _functools
    worker_fn = _functools.partial(
        _write_one_granule_beam,
        product_vars=product_vars,
        res=res, part=part,
        tmp_dir=tmp_dir, h3_dir=h3_dir,
        lat_col=lat_col, lon_col=lon_col, dat_col=dat_col,
        spatial_h3_tiles=spatial_h3_tiles,
        skip_check_enabled=skip_check_enabled,
        schema=canonical_schema,
    )
    logger.info(
        f"Driver: kwargs baked into partial (no scatter, no separate TaskStates). "
        f"spatial_h3_tiles={len(spatial_h3_tiles) if spatial_h3_tiles else 0} cells, "
        f"product_vars={len(product_vars)} products, "
        f"schema cols={len(canonical_schema.names) if canonical_schema is not None else 0}."
    )

    # ── Build task stream ─────────────────────────────────────────────
    # Generator over (soc_dict, beam, frag_name). Skips tasks whose
    # sentinel is already on disk (resume fast-path).
    logger.info("Driver: building task list...")
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
        f"{total} tasks (skipped_by_resume={len(completed_frags)})"
    )

    # ── Submit the entire batch via client.map ─────────────────────────
    # client.map(fn, iterable, **kwargs) builds a Blockwise graph layer
    # where the function + kwargs live ONCE in the scheduler's graph
    # (one ~210 KB blob covering spatial_h3_tiles + product_vars + the
    # canonical pyarrow schema), and per-task entries are tiny refs into
    # that layer. Total scheduler-side footprint for the full 534k-task
    # build: ~535 MB — negligible on a 1 TB hub.
    #
    # Why this beats the prior rolling-window submit-then-as_completed-add
    # pattern:
    #   * Dashboard sees the full 534k-task scope from the start →
    #     real progress visualization instead of a fake 100-task window.
    #   * Worker saturation is dask-managed (via the scheduler's queue),
    #     so every worker is busy as long as tasks remain — no manual
    #     inflight knob to mis-tune.
    #   * The "scheduler-side serialization explosion" concern from
    #     Agent 3 #D.2 was specific to ``for t in tasks: client.submit(
    #     fn, t, **kwargs)`` which serializes kwargs once per submit
    #     call. ``client.map`` shares kwargs across the batch (single
    #     entry in the graph) so the explosion does not apply.
    #
    # Memory drains as ``fut.release()`` runs per completion below.
    logger.info(f"Driver: submitting {total} tasks via client.map (partial-wrapped fn)...")
    all_futures = client.map(worker_fn, tasks, pure=False)
    logger.info(
        f"Driver: submission complete — {len(all_futures)} futures in graph. "
        f"Scheduler will distribute across all live workers."
    )

    ac = dask_as_completed(all_futures)

    pbar = tqdm_bar(total=total, desc="Stage1 partition writes", unit="task")
    n_ok = n_fail = n_leaves = 0
    # Periodic-progress INFO is opt-in only — tqdm.set_postfix already shows
    # ok/fail/leaves on the terminal, so emitting the same data to the log
    # every 60s was operator clutter (~840 lines per 14h continental build).
    # Set ``GH3_LOG_PROGRESS=1`` to re-enable for detached / tail-followed
    # log workflows. WARN/ERROR lines (per-failure + end-of-phase summary)
    # remain unconditional — those are actionable, not progress noise.
    log_progress = os.environ.get('GH3_LOG_PROGRESS', '').strip().lower() in ('1', 'true', 'yes', 'on')
    log_every_seconds = 60
    next_log_t = _time.monotonic() + log_every_seconds if log_progress else float('inf')
    try:
        for fut in ac:
            try:
                result = fut.result()
                if result.get('error'):
                    n_fail += 1
                    logger.warning(
                        f"Stage1 task {result.get('frag_name')}: {result['error']}"
                    )
                    # Persist a structured failure record (driver-side,
                    # single-writer, append-only) so the build log's
                    # per-granule status can be enriched with a precise
                    # cause at finalize without log-grep. See
                    # _classify_load_h5_failure + _append_granule_failure.
                    failure = result.get('failure')
                    if failure:
                        _append_granule_failure(tmp_dir, result.get('frag_name'), failure)
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
            if log_progress:
                now = _time.monotonic()
                if now >= next_log_t:
                    done = n_ok + n_fail
                    logger.info(
                        f"Streaming write: {done}/{total} done "
                        f"({100*done/total:.1f}%) — ok={n_ok} fail={n_fail} "
                        f"leaves={n_leaves}"
                    )
                    next_log_t = now + log_every_seconds
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

    # Pre-clean: act on any _merge_failures sentinels left by a prior run.
    # Removes the 0-byte / truncated parquets that would otherwise re-fail
    # this merge for the same partitions. Idempotent + bounded by the
    # failure-list size (never scans the healthy tree). Paired with the
    # CLI-side ``apply_merge_failures_to_logger`` fold so the granules in
    # those partitions are PENDING for Stage 1 to re-extract.
    _pc = preclean_merge_failures(tmp_dir)
    if any(_pc.values()):
        logger.info(
            f"Merge pre-clean: removed {_pc['parquets_removed']} bad "
            f"parquets + {_pc['tmps_removed']} .tmp orphans across "
            f"{_pc['partitions_cleaned']} partition(s)."
        )

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

        # Incremental manifest refresh: at continental scale the merge
        # phase runs for hours, during which any consumer reading the
        # database (gh3_load, gh3_extract) would see a stale manifest
        # sentinel from the last full refresh — including the previous
        # build, if any. Periodic in-merge refreshes give those consumers
        # a fresh view of the partially-built DB. Trigger: every
        # ``manifest_refresh_every`` successful merges. The refresh itself
        # is O(N_merged_so_far) — pure in-memory derivation from
        # ``_merge_progress.txt`` + one atomic file write; no tree walk.
        # Skip in tests / library callers that disable via env var.
        manifest_refresh_every = max(
            1, int(os.environ.get('GH3_MANIFEST_REFRESH_EVERY', '1000'))
        )
        _next_refresh_at = manifest_refresh_every

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
                # Incremental manifest refresh — see comment block above
                # the loop. Bounded by ``manifest_refresh_every`` so the
                # total cost across a 50k-merge run is ~50 refreshes,
                # each <1s — negligible against the multi-minute merge.
                if merged_count >= _next_refresh_at:
                    try:
                        _paths = _derive_merged_output_paths(merge_progress_file, h3_dir)
                        if _paths:
                            generate_manifest(h3_dir, files=_paths, tree_shape='h3db')
                    except Exception as _re:
                        # Manifest refresh is best-effort — never let a
                        # write failure abort an in-flight merge.
                        logger.debug(f"Incremental manifest refresh skipped: {_re}")
                    _next_refresh_at = merged_count + manifest_refresh_every
            except Exception as e:
                failed_count += 1
                # Preserve the h3_cell + year in the relative path so the
                # operator can locate the failing partition without scanning
                # the tmp tree. Combined with the [file=<path>] suffix that
                # parquet_merge_files now attaches inside ``e``, a single
                # grep on the WARN line gives both the partition and the
                # exact bad fragment.
                logger.warning(
                    f"Merge failed for {os.path.relpath(d, tmp_dir)}: "
                    f"{type(e).__name__}: {e}"
                )
                # Persist an atomic per-failure sentinel so L1 resume +
                # L2 doctor can recover without re-running the full merge
                # to rediscover what broke. See _emit_merge_failure_sentinel.
                _emit_merge_failure_sentinel(tmp_dir, d, e)
                # If the failure is a known-bad-fragment class (0-byte /
                # truncated parquet), parse the granules whose fragments
                # live in this partition and record them for the CLI to
                # flip back from INDEXED → MERGE_FAILED. Without this,
                # L1 resume pre-clean (Phase 2.2) would unlink the bad
                # fragment but leave the granule INDEXED, permanently
                # dropping its rows. Only fires for the curated marker
                # set; infrastructure errors (disk full, etc.) skip this
                # and the granule stays INDEXED.
                if _is_recoverable_fragment_error(e):
                    grans = _granules_in_partition_dir(d)
                    _emit_merge_failed_granules(tmp_dir, d, grans, e)
            pbar.update(1)
            pbar.set_postfix(ok=merged_count, fail=failed_count)

        pbar.close()
        del futures, futures_list

        if failed_count > 0:
            logger.error(f"{failed_count} partition merges failed. Re-run to retry.")

    # Derive h3_files + h3_subdirs from the authoritative _merge_progress.txt
    # instead of two driver-side GPFS scans (``glob('h3_*/*/*.parquet')`` +
    # ``glob('h3_*/')``). Zero GPFS metadata ops — pure in-memory derivation
    # via the deterministic ``h3_merge_files`` naming contract. See
    # ``_derive_merged_output_paths``.
    h3_files: List[str] = sorted(
        _derive_merged_output_paths(merge_progress_file, h3_dir)
    )
    h3_subdirs_set: set = {
        os.path.dirname(os.path.dirname(p)) + '/' for p in h3_files
    }

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

    h3_subdirs = sorted(h3_subdirs_set)
    meta_tasks = [dh3_merge_metadata(i) for i in h3_subdirs]
    meta_tasks = dask.persist(*meta_tasks, optimize_graph=False)
    progress(meta_tasks)
    meta_files = list(dask.compute(*meta_tasks))
    del meta_tasks

    # Generate manifest for accelerated file listing
    logger.info("Generating file manifest")
    generate_manifest(h3_dir, tree_shape='h3db')

    return h3_files


def _add_variables_to_year_file(year_pf, new_product_vars, all_soc, version=None):
    """Worker: add new variables to a single ``(cell, year)`` parquet file.

    Per-(cell, year) granularity matches the fresh-build merge phase
    (``h3_merge_files`` over ``<tmp>/h3_<p>=X/year=Y`` dirs) and avoids
    the per-cell granularity's two pitfalls: (a) loading every year's
    granules into a single in-memory ``new_vars_df``, and (b) the
    per-cell skip check having to inspect every year file just to know
    whether the whole partition can be short-circuited.

    The granule list for this exact year file is read from the
    per-year sidecar (``<cell>.<year>.0.metadata.json``) — already
    pre-filtered to granules that contributed shots to this year, so
    no date/filename parsing is needed in the worker.

    ``all_soc`` is built ONCE on the driver and broadcast via
    ``client.scatter`` — workers must never call ``soc_file_tree``
    themselves, since that fans a ``walk_soc_parallel`` back to the
    cluster and deadlocks when there are no free threads.

    Parameters
    ----------
    year_pf : str
        Path to one ``<cell>/year=YYYY/<cell>.YYYY.0.parquet``.
    new_product_vars : dict
        Product code → list of new variable names to add.
    all_soc : dict
        Pre-built ``orb_track → {prod: hdf5_path}`` mapping. Driver
        builds and broadcasts; workers consume read-only.
    version : int or None
        GEDI data version (only used for downstream consistency; the
        SOC tree was already version-filtered on the driver).

    Returns
    -------
    str or None
        ``year_pf`` on a successful update; ``None`` when the file was
        skipped (already has the new columns / no matching granules).
    """
    from .utils import parquet_join_columns
    import tempfile

    # Compute target column set (with product suffix). Matches the
    # naming convention enforced everywhere else in the build path.
    new_cols = set()
    for prod, var_list in new_product_vars.items():
        if var_list:
            suffix = f"_{prod.lower()}"
            new_cols.update(
                v if v.endswith(suffix) else f"{v}{suffix}"
                for v in var_list
                if v != 'shot_number'
            )

    # Per-file resume check (cheap footer-only read). If the file
    # already carries every requested column we're done — a prior
    # run completed this exact (cell, year).
    if new_cols:
        existing_cols = set(read_parquet_schema(year_pf)['column'].tolist())
        if new_cols.issubset(existing_cols):
            return None

    # Per-year sidecar lists the granules that contributed to this year
    # (h3_write_metadata writes it after every merge / variable join).
    year_meta_path = year_pf.replace('.parquet', PARTITION_META_FILENAME)
    if not os.path.exists(year_meta_path):
        return None
    meta = json_read(year_meta_path)
    granules = meta.get('granules', [])
    if not granules:
        return None

    # Pull (shot_number + new vars) from each granule's source h5.
    new_vars_list = []
    for gran in granules:
        orb_track = f"O{gran['orbit']:05d}_{gran['granule']:02d}_T{gran['track']:05d}"
        if orb_track not in all_soc:
            continue
        soc_files = all_soc[orb_track]
        for prod, var_list in new_product_vars.items():
            if prod not in soc_files or var_list is None:
                continue
            try:
                cols_to_read = ['shot_number'] + var_list
                df = load_h5(soc_files[prod], columns=cols_to_read, include_source=False)
                if df is not None and not df.empty:
                    suffix = f"_{prod.lower()}"
                    df = df.rename(columns=lambda x: x if x.endswith(suffix) else f"{x}{suffix}")
                    new_vars_list.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {prod} vars from {orb_track}: {e}")

    if not new_vars_list:
        return None

    # load_h5 returns DataFrames indexed by shot_number — preserve that index
    new_vars_df = pd.concat(new_vars_list)
    new_vars_df = new_vars_df[~new_vars_df.index.duplicated(keep='first')]
    new_vars_df = new_vars_df.reset_index()

    tmp_file = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_file = tmp.name
        new_vars_df.to_parquet(tmp_file, engine='pyarrow', index=False)
        del new_vars_df
        # parquet_join_columns is atomic via .join.tmp + os.replace and
        # filters out columns already present in year_pf, so re-running
        # against a partially-updated year file never produces duplicate
        # columns.
        parquet_join_columns([year_pf, tmp_file], year_pf, key_col='shot_number')
    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

    # Per-year-file sidecar refresh (column list / dtypes / shot range).
    # Per-cell aggregation runs as a separate Phase 2 on the driver.
    h3_write_metadata(year_pf)
    return year_pf


def _build_add_variables(h3_dir, new_product_vars, soc_source=None, version=None):
    """Add new variables to an existing H3 database via shot_number join.

    Per-(cell, year) task granularity (mirrors the fresh-build merge
    phase). SOC tree is built ONCE on the driver and broadcast to
    workers via ``client.scatter`` — earlier per-task ``soc_file_tree``
    calls would have to ``walk_soc_parallel`` from inside a worker,
    fanning work back to the same cluster and deadlocking under any
    workers-equal-threads cluster configuration.

    Phase 1: per-year-file join (parallel)
    Phase 2: per-cell metadata re-merge for cells whose year files
             changed (parallel; aggregates the per-year sidecars
             ``h3_write_metadata`` wrote into ``<cell>.metadata.json``)

    Parameters
    ----------
    h3_dir : str
        Root directory of the H3 database.
    new_product_vars : dict
        Product code → list of new variable names to add.
    soc_source : str, list, or None
        Source for HDF5 files. ``None`` (S3 ETL mode) is not
        supported on this path.
    version : int or None
        GEDI data version for the SOC tree's filename filter.

    Returns
    -------
    list of str or None
        Year-file paths updated this run, or None when nothing changed.
    """
    logger.info("Variable-only expansion detected: adding new columns via shot_number join")

    client = get_dask_client()

    # Phase 0: enumerate (cell, year) units. Driver scandirs cell dirs;
    # year-subdir listing is fanned across workers via client.map
    # (same shape as the fresh-build merge enumeration).
    try:
        h3_part_dirs = sorted(
            e.path for e in os.scandir(h3_dir)
            if e.is_dir(follow_symlinks=False) and e.name.startswith('h3_')
        )
    except OSError:
        h3_part_dirs = []
    if not h3_part_dirs:
        logger.info("No existing partitions to update")
        return None

    year_lists = client.gather(
        client.map(_list_year_subdirs, h3_part_dirs, pure=False)
    )

    year_files = []
    year_file_to_cell = {}
    for cell_dir, ydirs in zip(h3_part_dirs, year_lists):
        for ydir in ydirs:
            for pf in glob.glob(os.path.join(ydir, '*.parquet')):
                year_files.append(pf)
                year_file_to_cell[pf] = cell_dir
    if not year_files:
        logger.info("No (cell, year) parquet files found to update")
        return None

    logger.info(f"Updating {len(year_files)} (cell, year) parquet files across {len(h3_part_dirs)} cells")

    # Build the SOC tree ONCE on the driver. soc_file_tree fans
    # walk_soc_parallel across the cluster on its own (a single fan-out
    # the workers don't compete with), then pivots into the
    # orb_track → {prod: path} dict every worker will need.
    if isinstance(soc_source, str):
        glob_kwargs = {'version': version} if version is not None else None
        all_soc = soc_file_tree(soc_source, to_list=False, glob_kwargs=glob_kwargs)
    elif isinstance(soc_source, list):
        all_soc = soc_file_tree(soc_source, to_list=False)
    elif soc_source is None:
        logger.warning("Variable-only update requires a local SOC source (no S3 ETL support on this path)")
        return None
    else:
        return None

    if not all_soc:
        logger.warning("SOC tree is empty — nothing to join")
        return None

    logger.info(f"SOC tree built ({len(all_soc)} orb_tracks); broadcasting to workers")
    all_soc_future = client.scatter(all_soc, broadcast=True)

    # Phase 1: per-year-file parallel join
    from dask.distributed import as_completed as dask_as_completed
    from tqdm import tqdm as tqdm_bar

    futures = client.map(
        _add_variables_to_year_file,
        year_files,
        new_product_vars=new_product_vars,
        all_soc=all_soc_future,
        version=version,
        pure=False,
    )
    fut_to_pf = dict(zip(futures, year_files))

    updated_files = []
    touched_cells = set()
    skipped_count = 0
    failed_count = 0
    pbar = tqdm_bar(total=len(year_files), desc="Updating year files", unit="file")
    try:
        for future in dask_as_completed(futures):
            pf = fut_to_pf[future]
            try:
                result = future.result()
                if result is not None:
                    updated_files.append(result)
                    touched_cells.add(year_file_to_cell[pf])
                else:
                    skipped_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning(f"Variable update failed for {os.path.relpath(pf, h3_dir)}: {e}")
            pbar.update(1)
            pbar.set_postfix(
                updated=len(updated_files),
                skipped=skipped_count,
                failed=failed_count,
            )
    finally:
        pbar.close()

    if failed_count > 0:
        logger.error(f"{failed_count} year-file updates failed. Re-run to retry.")

    logger.info(
        f"Updated {len(updated_files)}/{len(year_files)} year files "
        f"(skipped={skipped_count}, failed={failed_count})"
    )

    # Phase 2: per-cell metadata aggregation for cells that changed.
    # h3_merge_metadata reads each year's sidecar and rewrites the
    # cell-level <cell>.metadata.json. Independent across cells so we
    # fan it out the same way the year-file join was fanned out.
    if touched_cells:
        logger.info(f"Merging per-cell metadata for {len(touched_cells)} updated cells")
        merge_futures = client.map(h3_merge_metadata, sorted(touched_cells), pure=False)
        merge_pbar = tqdm_bar(total=len(touched_cells), desc="Merging cell metadata", unit="cell")
        try:
            for f in dask_as_completed(merge_futures):
                try:
                    f.result()
                except Exception as e:
                    logger.warning(f"Cell metadata merge failed: {e}")
                merge_pbar.update(1)
        finally:
            merge_pbar.close()

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