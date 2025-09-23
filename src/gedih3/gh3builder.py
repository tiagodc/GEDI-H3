import os, re, glob, h5py
import shutil
import pandas as pd
import geopandas as gpd
import h3pandas
import dask
import dask.dataframe
from typing import Union, List, Dict, Optional, Tuple, Any
from dask.distributed import progress

from config import GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS, GEDI_PRODUCTS
from utils import parquet_append_columns, parquet_merge_files, json_read, json_write, read_vector_file, to_geojson, read_as_geojson
from h3utils import intersect_h3_geometries
from gedidriver import soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5
from daac import gedi_download

class H3BuildLogger:
    _VALID_STATUSES = ('INITIALIZED', 'DOWNLOADING', 'PARTITIONING', 'MERGING', 'COMPLETED', 'FAILED')

    def __init__(self, odir, prod_vars, res=12, part=3, spatial=None, temporal=None):
        self.odir = odir
        
        self.log_file = os.path.join(self.odir, 'build_log.json')
        log_data = {}            
        if os.path.exists(self.log_file):
            log_data = json_read(self.log_file)
        
        self.prod_vars = log_data.get('product_variables', prod_vars)
        self.res = log_data.get('h3_resolution', res)
        self.part = log_data.get('h3_partition', part)
        self.temporal = log_data.get('temporal_filter', temporal)
        self.spatial = log_data.get('spatial_filter', self._process_spatial(spatial))
        self.status = log_data.get('status', 'INITIALIZED')
        
        if 'orbit_limits' in log_data:
            self.orbit_limits = log_data['orbit_limits']
        if 'date_range' in log_data:
            self.date_range = log_data['date_range']
        if 'building_product_level' in log_data:
            self.building_product_level = log_data['building_product_level']
    
    def _process_spatial(self, spatial):
        if spatial is None:
            return None
        
        if isinstance(spatial, list) and len(spatial) == 4:
            spatial = tuple(spatial)
        elif isinstance(spatial, str):
            spatial = read_as_geojson(spatial)
        elif isinstance(spatial, gpd.GeoDataFrame):
            spatial = to_geojson(spatial.to_crs(4326))
        else:
            raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
        return spatial    
    
    def set_status(self, new_status: str):
        if new_status not in self._VALID_STATUSES:
            raise ValueError(f"Invalid status '{new_status}'. Must be one of {self._VALID_STATUSES}")
        self.status = new_status
    
    def set_current_level(self, gedi_prod_level: str = None):
        if gedi_prod_level is None and hasattr(self, 'building_product_level'):
            del self.building_product_level
            return
        self.building_product_level = gedi_prod_level.upper()
    
    def to_dict(self):
        log_dict = {
            'product_variables': self.prod_vars,
            'h3_resolution': self.res,
            'h3_partition': self.part,
            'spatial_filter': self.spatial,
            'temporal_filter': self.temporal,
            'status': self.status,
            'product_versions': {k: {'doi':v['doi'], 'version':v['version']} for k,v in GEDI_PRODUCTS.items() if k in self.prod_vars}
        }
        
        if hasattr(self, 'orbit_limits'):
            log_dict['orbit_limits'] = self.orbit_limits
        if hasattr(self, 'date_range'):
            log_dict['date_range'] = self.date_range
        if hasattr(self, 'building_product_level'):
            log_dict['building_product_level'] = self.building_product_level
        
        return log_dict

    def save_log(self):
        json_write(self.to_dict(), self.log_file, mode='w', rewrite=True)

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

def download_soc(product_vars: Dict, spatial = None, temporal = None, direct_access = False, n_jobs=5, dask_client=None, build_logger=None):
    if 'L2A' not in product_vars:
        product_vars.update({'L2A': GEDI_L2A_ESSENTIALS})
        
    for k,val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')
    
    if build_logger is not None:
        build_logger.set_status('DOWNLOADING')
        build_logger.save_log()
                    
    return gedi_download(product_vars=product_vars, odir=None if direct_access else GH3_DEFAULT_SOC_DIR, spatial=spatial, temporal=temporal, n_jobs=n_jobs, to_list=True, dask_client=dask_client)

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
        build_logger.set_status('PARTITIONING')
        build_logger.save_log()
        
    tmp_files = ddf.map_partitions(h3_part_files, res=res, part=part, lat_col=lat_col, lon_col=lon_col, dir_path=tmp_dir, roi_tiles=h3_tiles, meta=pd.Series([], dtype=str))
    tmp_files = tmp_files.to_delayed()
    tmp_files = dask.persist(*tmp_files, optimize_graph=False)
    progress(tmp_files)
    
    if build_logger is not None:
        build_logger.set_status('MERGING')
        build_logger.save_log()
    
    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/'))
    h3_dir = os.path.join(GH3_DEFAULT_H3_DIR, gedi_prod_level.lower())
    os.makedirs(h3_dir, exist_ok=True)
        
    h3_files = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_files = dask.persist(*h3_files, optimize_graph=False)
    progress(h3_files)
    
    if build_logger is not None:
        build_logger.set_status('COMPLETED')
        build_logger.save_log()
               
    return list(dask.compute(*h3_files))
        
def gh3_build_main(product_vars, spatial=None, temporal=None, res=12, part=3, direct_access=False, dask_client=None):
    gedi_vars_expand(product_vars)

    if isinstance(spatial, str):
        spatial = read_vector_file(spatial)

    build_logger = H3BuildLogger(GH3_DEFAULT_DOWNLOAD_DIR, product_vars, res=res, part=part, spatial=spatial, temporal=temporal)
    build_logger.save_log()

    soc_files = download_soc(product_vars, spatial=spatial, temporal=temporal, direct_access=direct_access, dask_client=dask_client, build_logger=build_logger)
    soc_source = soc_files if direct_access else None
    
    try:
        h3_products = {}
        for k,val in product_vars.items():
            build_logger.set_current_level(k)
            build_logger.save_log()

            print(f"Building H3 database for GEDI {k.upper()}")
            h3_files = build_h3db_from_soc(gedi_prod_level=k, h3_vars=val, res=res, part=part, spatial=spatial, build_logger=build_logger, soc_source=soc_source)
            h3_products[k] = h3_files
    except Exception as e:
        build_logger.set_status('FAILED')
        build_logger.save_log()
        raise e

    build_logger.set_current_level(None)
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
    # product_vars = {'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['*']}
    spatial = [-50.5,0.5,-50,1]
    temporal = ('2020-01-01','2020-07-01')
    # client = Client() 
    with Client(n_workers=20, threads_per_worker=1, processes=True) as client:
        print(client.dashboard_link)
        gh3_build_main(product_vars, spatial=spatial, temporal=temporal, res=12, part=3, dask_client=client, direct_access=False)        

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
    _testit()