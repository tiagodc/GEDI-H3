import os, re, glob
import shutil
import pandas as pd
import h3pandas
import dask
import dask.dataframe
from typing import Union, List, Dict, Optional, Tuple, Any
from dask.distributed import progress

from config import GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS
from utils import parquet_append_columns, parquet_append_rows, parquet_merge_files
from h3utils import intersect_h3_geometries
from gedidriver import soc_file_tree, dask_h5_merged
from daac import gedi_download

def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import h3pandas
    return df.reset_index().h3.geo_to_h3(res, lat_col=lat_col, lng_col=lon_col).h3.h3_to_parent(part).reset_index().set_index(df.index.name)

def h3_tmp_files(df, dir_path, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode', roi_tiles=[]):
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
        gedi_name = re.sub('\\.h5$','.parquet', hex_df.root_file.iloc[0])        
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

def download_soc(product_vars: Dict, spatial = None, temporal = None, n_jobs=5):
    if 'l2a' not in product_vars:
        product_vars = product_vars.update({'l2a': GEDI_L2A_ESSENTIALS})
        
    for k,val in product_vars.items():
        if 'shot_number' not in val:
            val.append('shot_number')
    
    return gedi_download(product_vars=product_vars, odir=GH3_DEFAULT_SOC_DIR, spatial=spatial, temporal=temporal, n_jobs=n_jobs, to_list=True)

def build_h3db_from_soc(gedi_prod_level='l4c', h3_vars=['wsci'], res=12, part=3, spatial=[-50.5,0.5,-50,1]):
    pl = gedi_prod_level.upper()
    build_vars = {}
    if pl == 'L2A':
        build_vars[pl] = list(set(h3_vars + GEDI_L2A_ESSENTIALS))
    else:
        build_vars[pl] = h3_vars
        build_vars['L2A'] = GEDI_L2A_ESSENTIALS
    
    all_soc_files = soc_file_tree(GH3_DEFAULT_SOC_DIR, to_list=True)
    soc_files = [{k:val for k,val in i.items() if k in build_vars} for i in all_soc_files]
        
    ddf = dask_h5_merged(soc_files, build_vars, shots=None, dropna=True)
    
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
    
    tmp_files = ddf.map_partitions(h3_tmp_files, res=res, part=part, lat_col=lat_col, lon_col=lon_col, dir_path=tmp_dir, roi_tiles=h3_tiles, meta=pd.Series([], dtype=str))
    tmp_files = tmp_files.to_delayed()
    tmp_files = dask.persist(*tmp_files, optimize_graph=False)
    progress(tmp_files)
    
    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/'))
    h3_dir = os.path.join(GH3_DEFAULT_H3_DIR, gedi_prod_level.lower())
    os.makedirs(h3_dir, exist_ok=True)
        
    h3_files = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_files = dask.persist(*h3_files, optimize_graph=False)
    progress(h3_files)
    
    return list(dask.compute(*h3_files))
        

def _testit(odir=None):
    from datetime import datetime
    from dask.distributed import Client, progress
    import psutil
    print("building from S3")
    t0 = datetime.now()
    print("process started at", t0)

    # Track network I/O
    net_io_start = psutil.net_io_counters()
    print(f"Initial network stats - Sent: {net_io_start.bytes_sent / (1024**3):.3f} GB, Recv: {net_io_start.bytes_recv / (1024**3):.3f} GB")

    n_jobs=10
    
    product_vars = {'L1B': ['minimal'], 'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['*']}
    spatial = [-50.5,0.5,-50,1]
    temporal = ('2020-01-01','2020-07-01')
    
    print('... downloading')
    d = gedi_download(product_vars, odir, spatial=spatial, temporal=temporal, n_jobs=n_jobs, to_list=True)
    
    with Client(n_workers=n_jobs, threads_per_worker=1) as client:
        print(client.dashboard_link)
        
        # prod_vars = {'L1B':['rxwaveform'], 'L2A': ['shot_number', 'rh'], 'L4A':['agbd'], 'L4C': ['wsci']}        
        prod_vars = {'L2A': ['shot_number','lon_lowestmode','lat_lowestmode','elev_lowestmode','rh']}
        all_files = soc_file_tree(d, to_list=True)
        ddf = dask_h5_merged(all_files, prod_vars)
        
        print("... generating tmp files")    
        tmp_files = ddf.map_partitions(h3_tmp_files)
        tmp_files = tmp_files.persist(optimize_graph=False)
        progress(tmp_files)

        print("... generating h3 files")    
        tmp_h3_dirs = glob.glob(os.path.join(GH3_DEFAULT_TMP_DIR, '*/'))
        h3_files = dask.dataframe.from_map(h3_merge_files, tmp_h3_dirs, rm_src=True)
        h3_files = h3_files.persist(optimize_graph=False)
        progress(h3_files)

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

if __name__ == '__main__':
    # _testit(GH3_DEFAULT_SOC_DIR)  # ~12.5 min
    _testit() # ~12 min