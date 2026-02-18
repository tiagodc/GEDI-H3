import os, re, glob, json, h5py, h3
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

from .config import GEDI_BEAMS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS, GEDI_PRODUCTS, GEDI_START_DATE, BUILD_LOG_FILENAME, PARTITION_META_FILENAME
from .utils import now, json_read, json_write, to_geojson, parquet_append_columns, parquet_merge_files, read_parquet_schema, h5_is_valid, get_dask_client, parquet_schema_add_bbox, generate_manifest
from .h3utils import intersect_h3_geometries, h3_index_df, fix_h3_geometry
from .gedidriver import GEDIFile, add_special_columns, soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5, validate_soc_files
from .daac import gedi_download
from .logging_config import get_logger
from .validation import validate_h3_params, validate_product_vars, validate_directory_exists
from .exceptions import H3ValidationError, GediValidationError, GediFileError, GediMergeError

logger = get_logger(__name__)


def download_soc(product_vars: Dict, spatial = None, temporal = None, direct_access = False, update=False, odir=GH3_DEFAULT_SOC_DIR, n_jobs=5):
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
    odir : str
        Output directory for downloaded HDF5 files.
    n_jobs : int, default 5
        Number of parallel download workers.

    Returns
    -------
    list
        List of downloaded SOC file paths or EarthAccessFile objects.
    """
    product_vars = gedi_vars_expand(product_vars)
    
    if 'L2A' not in product_vars:
        product_vars.update({'L2A': GEDI_L2A_ESSENTIALS})

    for k,val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    soc_files = gedi_download(product_vars=product_vars, odir=None if direct_access else odir, spatial=spatial, temporal=temporal, resume=update, n_jobs=n_jobs, to_list=direct_access)

    return soc_files

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

def h3_write_metadata(h3_file):
    """
    Write a sidecar metadata JSON file for an H3 partition parquet file.

    Reads shot_number, root_file_l2a, and datetime from the parquet file to
    compute summary statistics (shot count, shot range, date range, granule
    identifiers) and writes them alongside the parquet file.

    Parameters
    ----------
    h3_file : str
        Path to the H3 partition parquet file.

    Returns
    -------
    str
        Path to the written metadata JSON file (``*PARTITION_META_FILENAME``).
    """
    meta_file = h3_file.replace('.parquet', PARTITION_META_FILENAME)
    h3_part, year = os.path.basename(h3_file).split('.')[:2]
    
    df = pd.read_parquet(h3_file, engine='pyarrow', columns=['shot_number','root_file_l2a','datetime'])

    cols = read_parquet_schema(h3_file)
    gedi_files = [GEDIFile(f) for f in df['root_file_l2a'].unique()]
    shot_range = (int(df['shot_number'].min()), int(df['shot_number'].max()))
    date_range = (df['datetime'].min().strftime('%Y-%m-%d'), df['datetime'].max().strftime('%Y-%m-%d'))

    granule_identifiers = [{'orbit':gf.orbit, 'granule':gf.orbit_granule, 'track':gf.track} for gf in gedi_files]
    l2a_version = gedi_files[0].version

    h3_polygon = gpd.GeoDataFrame(geometry=[fix_h3_geometry(h3_part)], crs=4326, index=[h3_part])

    meta = {
        'last_modified': now(),
        'l2a_version': l2a_version,
        'h3_partition': h3_part,
        'h3_geometry': to_geojson(h3_polygon),
        'year': int(year),
        'shot_count': len(df),
        'shot_range': shot_range,
        'date_range': date_range,
        'granules': granule_identifiers,
        'columns': cols['column'].tolist()
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

    Designed for use with ``dask.dataframe.map_partitions``. Inspects the
    first row of each partition to determine the H3 cell and GEDI granule,
    then delegates to ``h3_skip_part`` to check if the data already exists.

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
        partition's granule data already exists in the database).
    """
    if df.empty:
        df['_skip'] = True
        return df    
    h3_col = sorted([c for c in df.columns if re.match(r'h3_\d{2}', c)])[0]
    h3_part = df[h3_col].iloc[0]
    gedi_file = df['root_file_l2a'].iloc[0]
    df = df.assign(_skip=h3_skip_part(h3_dir=h3_dir, h3_part=h3_part, gedi_file=gedi_file, cols=df.columns.tolist()))
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
    
    oname = f'{h3part.split('=')[-1]}.{year.split('=')[-1]}.0.parquet'
    out_file = os.path.join(odir, oname)
    h3_file = out_file

    if is_temp := (os.path.exists(out_file) and not replace):
        files.insert(0,out_file)
        files = list(set(files))
        out_file += '.tmp'
    
    parquet_merge_files(out_file, files, check_shots=is_temp, rm_src=rm_src)
    
    if is_temp:
        os.replace(out_file, h3_file)
    if rm_src:
        shutil.rmtree(in_dir, ignore_errors=True)

    meta_file = h3_write_metadata(h3_file)
    return h3_file

@dask.delayed
def dh3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    return h3_merge_files(in_dir=in_dir, out_dir=out_dir, rm_src=rm_src, replace=replace)


def _expand_product_vars(
    product_vars: Dict[str, List[str]],
    soc_files: List[Dict[str, str]]
) -> Dict[str, List[str]]:
    """
    Expand product variable specifications and ensure L2A essentials are included.

    Parameters
    ----------
    product_vars : dict
        Raw product variable specifications (may contain 'default', 'minimal', etc.)
    soc_files : list of dict
        List of SOC file dictionaries to sample for 'all' variable expansion

    Returns
    -------
    dict
        Expanded product variables with L2A essentials included
    """
    product_vars = gedi_vars_expand(product_vars)

    if 'L2A' in product_vars:
        product_vars['L2A'] = list(set(product_vars['L2A'] + GEDI_L2A_ESSENTIALS))
    else:
        product_vars['L2A'] = GEDI_L2A_ESSENTIALS

    for k, val in product_vars.items():
        if val is None:
            file = soc_files[0].get(k)
            product_vars[k] = gedi_vars_from_h5(file)

    return product_vars


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

        for f in prod.values():
            if isinstance(f, EarthAccessFile):
                break
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

    progress(bag_task)
    soc_files = list(bag_task.compute())
    del bag_task

    return soc_files


def _create_h3_dataframe(
    soc_files: List[Dict[str, str]],
    product_vars: Dict[str, List[str]],
    res: int,
    part: int
) -> Tuple[dask_geopandas.GeoDataFrame, str, str, str]:
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
        (dask_geopandas.GeoDataFrame, lat_col, lon_col, dat_col)
    """
    logger.info(f"Found {len(soc_files)} new GEDI granules with requested products")

    ddf = dask_h5_merged(soc_files, product_vars, shots=None, dropna=True, by_beam=True, suffix_all=True)

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

    return ddf, lat_col, lon_col, dat_col


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


def _write_partitioned(
    ddf: dask.dataframe.DataFrame,
    tmp_dir: str,
    part: int,
    lat_col: str,
    lon_col: str,
    dat_col: str
) -> List[str]:
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

    Returns
    -------
    list of str
        List of written parquet file paths, or empty list if no data
    """
    logger.info("Adding date and geometry columns to H3 database")

    ddf = ddf.map_partitions(add_special_columns, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    ddf = ddf.assign(year=ddf.datetime.dt.year)
    ddf = dask_geopandas.from_dask_dataframe(ddf)

    logger.info(f"Writing partitioned H3 data to temporary directory: {tmp_dir}")

    write_task = ddf.to_parquet(tmp_dir, write_index=True, overwrite=True, compression='zstd', partition_on=[f'h3_{part:02d}', 'year'], compute=False)
    write_task = write_task.persist(optimize_graph=False)
    progress(write_task)

    logger.debug("Clearing dask workers")

    client = get_dask_client()
    client.cancel(write_task, force=True)

    del write_task, ddf

    tmp_files = glob.glob(os.path.join(tmp_dir, '**', '*.parquet'), recursive=True)
    return tmp_files


def _merge_and_finalize(
    tmp_dir: str,
    h3_dir: str
) -> List[str]:
    """
    Merge temporary partitioned files into the final H3 database.

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

    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/*/'))
    os.makedirs(h3_dir, exist_ok=True)

    h3_tasks = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_tasks = dask.persist(*h3_tasks, traverse=False)
    progress(h3_tasks)
    h3_file_meta = list(dask.compute(*h3_tasks))
    del h3_tasks

    h3_files = [i for i in h3_file_meta if i is not None]

    logger.info("Compiling H3 metadata files")

    h3_subdirs = glob.glob(os.path.join(h3_dir, 'h3_*/'))
    meta_tasks = [dh3_merge_metadata(i) for i in h3_subdirs]
    meta_tasks = dask.persist(*meta_tasks, optimize_graph=False)
    progress(meta_tasks)
    meta_files = list(dask.compute(*meta_tasks))
    del meta_tasks

    # Generate manifest for accelerated file listing
    logger.info("Generating file manifest")
    generate_manifest(h3_dir)

    return h3_files


def build_h3db(
    product_vars: Dict[str, List[str]],
    res: int = 12,
    part: int = 3,
    spatial = None,
    soc_source: str = GH3_DEFAULT_SOC_DIR,
    version_kwargs: Optional[Dict] = None,
    tmp_dir: str = os.path.join(GH3_DEFAULT_TMP_DIR, 'gh3_build'),
    h3_dir: str = GH3_DEFAULT_H3_DIR,
    skip_granules: Optional[List[Dict]] = None
) -> Optional[List[str]]:
    """
    Build an H3-indexed GEDI database from SOC HDF5 files.

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
    soc_source : str
        Path to directory containing GEDI SOC HDF5 files.
    version_kwargs : dict, optional
        Keyword arguments for filtering by GEDI version.
    tmp_dir : str
        Path to temporary directory for intermediate files.
    h3_dir : str
        Output path for the H3-indexed parquet database.
    skip_granules : list of dict, optional
        List of granule identifiers to skip (from previous builds).

    Returns
    -------
    list of str or None
        List of output parquet file paths, or None if no new data processed.

    Raises
    ------
    H3ValidationError
        If H3 resolution or partition parameters are invalid
    GediFileError
        If source directory doesn't exist
    """
    # Validate parameters
    logger.debug("Validating build parameters")
    res, part = validate_h3_params(res, part)

    if not os.path.exists(soc_source):
        raise GediFileError(f"SOC source directory not found: {soc_source}")

    # List and organize SOC files
    logger.info("Listing source SOC files")
    all_soc_files = soc_file_tree(soc_source, to_list=True, glob_kwargs=version_kwargs)

    # Expand variable specifications and ensure L2A essentials
    product_vars = _expand_product_vars(product_vars, all_soc_files)

    # Filter to only files with required products
    prod_soc_files = [{k: val for k, val in i.items() if k in product_vars} for i in all_soc_files]

    # Filter out incomplete, corrupted, or already-processed granules
    soc_files = _filter_granules(prod_soc_files, product_vars, skip_granules)

    if len(soc_files) == 0:
        logger.info("No new granules to process")
        return None

    # Create H3-indexed Dask DataFrame
    ddf, lat_col, lon_col, dat_col = _create_h3_dataframe(soc_files, product_vars, res, part)

    # Apply spatial and existing-data filters
    os.makedirs(tmp_dir, exist_ok=True)
    ddf = _apply_spatial_filter(ddf, spatial, part, h3_dir)

    # Write partitioned parquet files
    tmp_files = _write_partitioned(ddf, tmp_dir, part, lat_col, lon_col, dat_col)

    if len(tmp_files) == 0:
        logger.info("No new data to process")
        return None

    # Merge and finalize database
    h3_files = _merge_and_finalize(tmp_dir, h3_dir)

    return h3_files

def build_parquet_metadata(gh3_dir):
    """
    Build ``_metadata`` and ``_common_metadata`` files for an H3 database.

    Scans all parquet files in the database, collects their row-group
    metadata, computes a merged bounding box from per-file geo metadata,
    and writes the consolidated PyArrow metadata files used by Dask and
    other tools for efficient partition discovery.

    Parameters
    ----------
    gh3_dir : str
        Root directory of the H3 parquet database.

    Returns
    -------
    None
        Writes ``_metadata`` and ``_common_metadata`` files to ``gh3_dir``.
        Returns early with no output if the directory contains no parquet
        files.
    """
    h3_files = glob.glob(os.path.join(gh3_dir,'**','*.parquet'), recursive=True)
    
    if len(h3_files) == 0:
        return    
    
    def _pq_meta(h3_file):
        pq_metadata = pq.read_metadata(h3_file)
        rel_path = os.path.relpath(h3_file, gh3_dir)
        pq_metadata.set_file_path(rel_path)
        return pq_metadata
    
    h3_metas = dbg.from_sequence(h3_files, partition_size=10).map(_pq_meta).compute()
    base_schema = pq.read_schema(h3_files[0])
    
    merged_bbox = None
    if b'geo' in base_schema.metadata:
        def _get_box(pq_metadata):
            if b'geo' in pq_metadata.metadata:
                return json.loads(pq_metadata.metadata[b'geo'])['columns']['geometry']['bbox']
            return None
        
        meta_boxes = dbg.from_sequence(h3_metas, partition_size=100).map(_get_box).filter(lambda x: x is not None).compute()
        meta_boxes = np.array(meta_boxes)
        merged_bbox = meta_boxes[:,:2].min(axis=0).tolist() + meta_boxes[:,2:].max(axis=0).tolist()
        del meta_boxes
    
    base_schema = parquet_schema_add_bbox(base_schema, bbox=merged_bbox)
    pq.write_metadata(schema=base_schema, where=os.path.join(gh3_dir, '_metadata'))
    
    cmeta = pq.ParquetDataset(h3_files)
    cmeta_schema = parquet_schema_add_bbox(cmeta.schema, bbox=merged_bbox)
    pq.write_metadata(schema=cmeta_schema, where=os.path.join(gh3_dir, '_common_metadata'))


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