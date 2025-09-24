import os, re, glob, h5py
import shutil
import pandas as pd
import geopandas as gpd
import h3pandas
import dask
import dask.dataframe
from typing import Union, List, Dict, Optional, Tuple, Any
from dask.distributed import progress

from .config import GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS, GEDI_PRODUCTS
from .utils import parquet_append_columns, parquet_merge_files, read_vector_file
from .h3utils import intersect_h3_geometries
from .gedidriver import GEDIFile, soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5, validate_soc_files
from .daac import gedi_download
from .logger import H3BuildLogger


def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import h3pandas
    return df.reset_index().h3.geo_to_h3(res, lat_col=lat_col, lng_col=lon_col).h3.h3_to_parent(part).reset_index().set_index(df.index.name)

def h3_part_files(df, dir_path, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode', roi_tiles=[]):
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
                hex_df.to_parquet(f, engine='pyarrow')
            
        files.append(f)
        del hex_df
    
    del df    
    return files

def h3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    files = glob.glob(os.path.join(in_dir,'*.parquet'))
    
    if len(files) == 0:
        return    
    
    out_file = os.path.join(out_dir, os.path.basename(in_dir.rstrip('/'))+'.parquet')
    
    if is_temp := (os.path.exists(out_file) and not replace):
        files.insert(0,out_file)
        files = list(set(files))
        in_file = out_file
        out_file += '.tmp'
    
    parquet_merge_files(out_file, files, check_shots=True, rm_src=rm_src)
    
    if is_temp:
        os.replace(out_file, in_file)
    if rm_src:
        shutil.rmtree(in_dir, ignore_errors=True)
    return out_file

@dask.delayed
def dh3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    return h3_merge_files(in_dir=in_dir, out_dir=out_dir, rm_src=rm_src, replace=replace)

def download_soc(product_vars: Dict, spatial = None, temporal = None, direct_access = False, resume=False, update=False, n_jobs=5, dask_client=None, build_logger=None):
    product_vars = gedi_vars_expand(product_vars)
    
    if build_logger is not None and (resume or update):
        product_vars = build_logger.prod_vars
        spatial = build_logger.spatial
        temporal = build_logger.temporal    
    
    if 'L2A' not in product_vars:
        product_vars.update({'L2A': GEDI_L2A_ESSENTIALS})

    for k,val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    if build_logger is not None:
        build_logger.set_status('DOWNLOADING', db_target='soc')
        build_logger.save_log()

    soc_files = gedi_download(product_vars=product_vars, odir=None if direct_access else GH3_DEFAULT_SOC_DIR, spatial=spatial, temporal=temporal, resume=resume, n_jobs=n_jobs, to_list=direct_access, dask_client=dask_client)

    if not direct_access and build_logger is not None and soc_files:
        gedi_files = soc_file_tree(GH3_DEFAULT_SOC_DIR, to_list=True)
        for prod_level, vars_list in product_vars.items():
            prod_files = [f[prod_level] for f in gedi_files]
            gedi_prods = [GEDIFile(f) for f in prod_files if f and os.path.exists(f)]
            gedi_dates = [f.doy_date_str for f in gedi_prods]
            gedi_orbits = [f.orbit for f in gedi_prods]
                        
            build_logger.update_product_info(prod_level, {
                'variables': vars_list,
                'file_count': len(prod_files),
                'size_gb': sum(f.file_size for f in gedi_prods),
                'date_range': (min(gedi_dates), max(gedi_dates)),
                'orbit_range': (min(gedi_orbits), max(gedi_orbits))
            }, db_target='soc')

        build_logger.set_status('COMPLETED', db_target='soc')
        build_logger.save_log()

    return soc_files

def build_h3db_from_soc(gedi_prod_level='l4c', h3_vars=['wsci'], res=12, part=3, spatial=None, soc_source=None, build_logger=None):
    pl = gedi_prod_level.upper()
    soc_input = soc_source if soc_source is not None else GH3_DEFAULT_SOC_DIR
    all_soc_files = soc_file_tree(soc_input, to_list=True)
    
    if h3_vars is None:
        pl_file = all_soc_files[0][pl]
        h3_vars = gedi_vars_from_h5(pl_file)
    
    build_vars = {}
    if pl == 'L2A':
        build_vars[pl] = list(set(h3_vars + GEDI_L2A_ESSENTIALS))
    else:
        build_vars[pl] = h3_vars
        build_vars['L2A'] = GEDI_L2A_ESSENTIALS

    soc_files = [{k:val for k,val in i.items() if k in build_vars} for i in all_soc_files]
        
    ddf = dask_h5_merged(soc_files, build_vars, shots=None, dropna=True, by_beam=True)
    
    lat_col='lat_lowestmode'
    lon_col='lon_lowestmode'
    
    if 'lat_lowestmode_l2a' in ddf.columns:
        lat_col+='_l2a'
    if 'lon_lowestmode_l2a' in ddf.columns:
        lon_col+='_l2a'
    
    h3_tiles = []
    if spatial is not None:
        h3_tiles = intersect_h3_geometries(spatial, res=part)

    tmp_dir = os.path.join(GH3_DEFAULT_TMP_DIR, gedi_prod_level.lower())
    os.makedirs(tmp_dir, exist_ok=True)
    
    if build_logger is not None:
        build_logger.set_status('PARTITIONING', db_target='h3')
        build_logger.save_log()

    tmp_files = ddf.map_partitions(h3_part_files, res=res, part=part, lat_col=lat_col, lon_col=lon_col, dir_path=tmp_dir, roi_tiles=h3_tiles, meta=pd.Series([], dtype=str))
    tmp_files = tmp_files.to_delayed()
    tmp_files = dask.persist(*tmp_files, optimize_graph=False)
    progress(tmp_files)

    if build_logger is not None:
        build_logger.set_status('MERGING', db_target='h3')
        build_logger.save_log()

    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/'))
    h3_dir = os.path.join(GH3_DEFAULT_H3_DIR, gedi_prod_level.lower())
    os.makedirs(h3_dir, exist_ok=True)

    h3_files = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_files = dask.persist(*h3_files, optimize_graph=False)
    progress(h3_files)

    h3_result = list(dask.compute(*h3_files))

    # Update logger with H3 product information
    if build_logger is not None:
        # Get H3 tiles from directories
        h3_tiles_created = [os.path.basename(d.rstrip('/')) for d in tmp_h3_dirs]

        build_logger.update_product_info(pl, {
            'variables': h3_vars if h3_vars else [],
            'file_count': len(h3_result),
            'h3_tiles': h3_tiles_created,
            'parquet_files': [f for f in h3_result if f and f.endswith('.parquet')],
            'indexed_shots': 0  # Could be calculated from the dataframes if needed
        }, db_target='h3')

        build_logger.set_status('COMPLETED', db_target='h3')
        build_logger.save_log()

    return h3_result

def gh3_build_main(product_vars, spatial=None, temporal=None, res=12, part=3, direct_access=False, dask_client=None, skip_download=False, resume=False, update=False, db_type='both'):
    product_vars = gedi_vars_expand(product_vars)

    if isinstance(spatial, str):
        spatial = read_vector_file(spatial)

    build_logger = H3BuildLogger(GH3_DEFAULT_DOWNLOAD_DIR, product_vars, res=res, part=part, spatial=spatial, temporal=temporal, resume=resume, update=update, db_type=db_type)
    build_logger.save_log()

    soc_source = None
    if skip_download:
        validation_report = validate_soc_files(build_logger.prod_vars, soc_dir=GH3_DEFAULT_SOC_DIR)
        if not validation_report["can_skip"]:
            build_logger.set_status('FAILED')
            build_logger.save_log()
            raise ValueError(validation_report.get('error_msg', "SOC files validation failed."))
    else:
        soc_files = download_soc(build_logger.prod_vars, spatial=build_logger.spatial, temporal=build_logger.temporal, direct_access=direct_access, resume=resume, update=update, dask_client=dask_client, build_logger=build_logger)
        if direct_access:
            soc_source = soc_files

    # Only build H3 database if needed
    h3_products = {}
    if build_logger.db_type in ('h3', 'both'):
        try:
            for k,val in build_logger.prod_vars.items():
                build_logger.set_current_level(k)
                build_logger.save_log()

                print(f"Building H3 database for GEDI {k.upper()}")
                h3_files = build_h3db_from_soc(gedi_prod_level=k, h3_vars=val, res=build_logger.res, part=build_logger.part, spatial=build_logger.spatial, build_logger=build_logger, soc_source=soc_source)
                h3_products[k] = h3_files
        except Exception as e:
            build_logger.set_status('FAILED', db_target='h3')
            build_logger.save_log()
            raise e

        build_logger.set_current_level(None)

    # Set final completion status
    build_logger.set_status('COMPLETED')
    build_logger.save_log()
    return h3_products

def _testit(odir=None):
    from datetime import datetime
    from dask.distributed import Client
    import psutil

    t0 = datetime.now()
    print("process started at", t0)

    # Track network I/O
    net_io_start = psutil.net_io_counters()
    
    product_vars = {'L1B': ['minimal'], 'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['*']}
    # product_vars = {'L2A': ['minimal'], 'L4A': ['shot_number', 'agbd_se'], 'L4C': ['*']}
    spatial = [-50.5,0.5,-50,1]
    temporal = ('2020-01-01','2020-07-01')
    # client = Client() 
    with Client(n_workers=20, threads_per_worker=1, processes=True) as client:
        print(client.dashboard_link)
        gh3_build_main(product_vars, spatial=spatial, temporal=temporal, res=12, part=3, dask_client=client, direct_access=False, skip_download=True)        

    t1 = datetime.now()
    print("process finished at", t1)
    print(t1 - t0)

    # Calculate network I/O used during the process
    net_io_end = psutil.net_io_counters()
    bytes_sent = net_io_end.bytes_sent - net_io_start.bytes_sent
    bytes_recv = net_io_end.bytes_recv - net_io_start.bytes_recv

    print(f"\nNetwork I/O Summary:")
    print(f"Downloads: {bytes_recv / (1024**3):.3f} GB")
    print(f"Uploads: {bytes_sent / (1024**3):.3f} GB")
    print(f"Total: {(bytes_sent + bytes_recv) / (1024**3):.3f} GB")