import os, re, glob, h5py
import shutil
import pandas as pd
import geopandas as gpd
import h3pandas
import dask
import dask.dataframe
import dask_geopandas
import itertools
from typing import Union, List, Dict, Optional, Tuple, Any
from dask.distributed import progress

from .config import GEDI_BEAMS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS, GEDI_PRODUCTS, GEDI_START_DATE
from .utils import parquet_append_columns, parquet_merge_files, read_vector_file
from .h3utils import intersect_h3_geometries
from .gedidriver import GEDIFile, add_special_columns, soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5, validate_soc_files
from .daac import gedi_download

def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import h3pandas
    return df.reset_index().h3.geo_to_h3(res, lat_col=lat_col, lng_col=lon_col).h3.h3_to_parent(part).reset_index().set_index(f"h3_{res:02d}")

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
    
    year_dir =  os.path.dirname(in_dir)
    year = os.path.basename(year_dir.rstrip('/'))
    
    h3_dir = os.path.dirname(year_dir)
    h3part = os.path.basename(h3_dir.rstrip('/'))
    
    odir = os.path.join(out_dir, h3part, year)
    os.makedirs(odir, exist_ok=True)
    
    oname = f'{h3part.split('=')[-1]}.{year.split('=')[-1]}.0.parquet'
    out_file = os.path.join(odir, oname)

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

def download_soc(product_vars: Dict, spatial = None, temporal = None, direct_access = False, resume=False, update=False, n_jobs=5, dask_client=None):
    product_vars = gedi_vars_expand(product_vars)    
    
    if 'L2A' not in product_vars:
        product_vars.update({'L2A': GEDI_L2A_ESSENTIALS})

    for k,val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    soc_files = gedi_download(product_vars=product_vars, odir=None if direct_access else GH3_DEFAULT_SOC_DIR, spatial=spatial, temporal=temporal, resume = resume or update, n_jobs=n_jobs, to_list=direct_access, dask_client=dask_client)

    return soc_files

def build_h3db_from_soc(product_vars, res=12, part=3, spatial=None, soc_source=GH3_DEFAULT_SOC_DIR, version_kwargs=None, tmp_dir=GH3_DEFAULT_TMP_DIR, h3_dir=GH3_DEFAULT_H3_DIR):
    # add resume/update logic
    all_soc_files = soc_file_tree(soc_source, to_list=True, glob_kwargs=version_kwargs)

    if 'L2A' in product_vars:
        product_vars['L2A'] = list(set(product_vars['L2A'] + GEDI_L2A_ESSENTIALS))
    else:
        product_vars['L2A'] = GEDI_L2A_ESSENTIALS
        
    for k,val in product_vars.items():
        if val is None:
            file = all_soc_files[0].get(k)
            product_vars[k] = gedi_vars_from_h5(file)

    soc_files = [{k:val for k,val in i.items() if k in product_vars} for i in all_soc_files]
    ddf = dask_h5_merged(soc_files, product_vars, shots=None, dropna=True, by_beam=True, suffix_all=True)

    lat_col='lat_lowestmode'
    lon_col='lon_lowestmode'
    dat_col = 'delta_time'
    
    if 'lat_lowestmode_l2a' in ddf.columns:
        lat_col+='_l2a'
    if 'lon_lowestmode_l2a' in ddf.columns:
        lon_col+='_l2a'
    if 'delta_time_l2a' in ddf.columns:
        dat_col+='_l2a'
    
    os.makedirs(tmp_dir, exist_ok=True)
    ddf = ddf.map_partitions(h3_index_df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)
    
    h3_tiles = []
    if spatial is not None:
        h3_tiles = intersect_h3_geometries(spatial, res=part)

    if len(h3_tiles) > 0:
        ddf = ddf[ddf[f'h3_{part:02d}'].isin(h3_tiles)]

    ddf = ddf.map_partitions(add_special_columns, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    ddf['year'] = ddf.datetime.dt.year
    ddf = dask_geopandas.from_dask_dataframe(ddf)
    
    tmp_files = ddf.to_parquet(tmp_dir, write_index=True, overwrite=True, compression='zstd', partition_on=[f'h3_{part:02d}', 'year'], compute=False).persist()
    progress(tmp_files)
    tmp_files = tmp_files.compute()
    
    # tmp_files = ddf.map_partitions(h3_part_files, res=res, part=part, lat_col=lat_col, lon_col=lon_col, dir_path=tmp_dir, roi_tiles=h3_tiles, meta=pd.Series([], dtype=str))
    # tmp_files = tmp_files.to_delayed()
    # tmp_files = dask.persist(*tmp_files, optimize_graph=False)
    # progress(tmp_files)

    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/*/'))
    os.makedirs(h3_dir, exist_ok=True)
    
    h3_files = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_files = dask.persist(*h3_files, optimize_graph=False)
    progress(h3_files)

    h3_result = list(dask.compute(*h3_files))

    return h3_result

def gh3_build_all(product_vars, spatial=None, temporal=None, res=12, part=3, direct_access=False, dask_client=None, skip_download=False, resume=False, update=False):
    product_vars = gedi_vars_expand(product_vars)

    if isinstance(spatial, str):
        spatial = read_vector_file(spatial)

    soc_source = None
    if skip_download:
        validation_report = validate_soc_files(product_vars, soc_dir=GH3_DEFAULT_SOC_DIR)
        if not validation_report["can_skip"]:
            raise ValueError(validation_report.get('error_msg', "SOC files validation failed."))

        soc_files = download_soc(product_vars, spatial=spatial, temporal=temporal, direct_access=direct_access, resume=resume, update=update, dask_client=dask_client)

        if direct_access:
            soc_source = soc_files
    
    h3_products = {}
    try:
        for k,val in product_vars.items():
            print(f"Building H3 database for GEDI {k.upper()}")
            # add resume/update logic
            h3_files = build_h3db_from_soc(gedi_prod_level=k, h3_vars=val, res=res, part=part, spatial=spatial, soc_source=soc_source)
            h3_products[k] = h3_files
    except Exception as e:
        raise e
    
    return h3_products