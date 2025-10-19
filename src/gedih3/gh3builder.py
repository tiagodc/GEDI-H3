import os, re, glob, json, h5py, h3
import warnings
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
import h3pandas
import dask
import dask.dataframe
import dask_geopandas
import dask.bag as dbg
import itertools
from typing import Union, List, Dict, Optional, Tuple, Any
from dask.distributed import progress

from .config import GEDI_BEAMS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, GEDI_L2A_ESSENTIALS, GEDI_PRODUCTS, GEDI_START_DATE
from .utils import now, json_read, json_write, to_geojson, parquet_append_columns, parquet_merge_files, read_parquet_schema, h5_is_valid
from .h3utils import intersect_h3_geometries, h3_index_df, fix_h3_geometry
from .gedidriver import GEDIFile, add_special_columns, soc_file_tree, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5, validate_soc_files
from .daac import gedi_download


def download_soc(product_vars: Dict, spatial = None, temporal = None, direct_access = False, update=False, odir=GH3_DEFAULT_SOC_DIR, n_jobs=5):
    product_vars = gedi_vars_expand(product_vars)    
    
    if 'L2A' not in product_vars:
        product_vars.update({'L2A': GEDI_L2A_ESSENTIALS})

    for k,val in product_vars.items():
        if val is None:
            continue
        if 'shot_number' not in val:
            val.append('shot_number')

    soc_files = gedi_download(product_vars=product_vars, odir=odir, spatial=spatial, temporal=temporal, resume=update, n_jobs=n_jobs, to_list=direct_access)

    return soc_files

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
                hex_df.to_parquet(f, engine='pyarrow', index=True, compression='zstd')
            
        files.append(f)
        del hex_df
    
    del df    
    return files

def h3_write_metadata(h3_file):
    meta_file = h3_file.replace('.parquet','.metadata.json')
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
    meta_file = h3_file.replace('.parquet','.metadata.json')
    if os.path.exists(meta_file):
        return json_read(meta_file)
    return None

def h3_merge_metadata(h3_subdir):
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
    ofile = os.path.join(h3_subdir, f"{mmeta['h3_partition']}.metadata.json")
    json_write(mmeta, ofile, rewrite=True)
    return ofile

def h3_skip_part(h3_dir, h3_part, gedi_file, cols=None):
    res = h3.get_resolution(h3_part)
    meta_file = os.path.join(h3_dir, f"h3_{res:02d}={h3_part}", f"{h3_part}.metadata.json")    
    
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
    
    parquet_merge_files(out_file, files, check_shots=True, rm_src=rm_src)
    
    if is_temp:
        os.replace(out_file, h3_file)
    if rm_src:
        shutil.rmtree(in_dir, ignore_errors=True)

    meta_file = h3_write_metadata(h3_file)
    return h3_file

@dask.delayed
def dh3_merge_files(in_dir, out_dir, rm_src=True, replace=False):
    return h3_merge_files(in_dir=in_dir, out_dir=out_dir, rm_src=rm_src, replace=replace)

def build_h3db(product_vars, res=12, part=3, spatial=None, soc_source=GH3_DEFAULT_SOC_DIR, version_kwargs=None, tmp_dir=GH3_DEFAULT_TMP_DIR, h3_dir=GH3_DEFAULT_H3_DIR, skip_granules=None, verbose=True):
    
    if verbose:
        print("Listing source SOC files.")
    all_soc_files = soc_file_tree(soc_source, to_list=True, glob_kwargs=version_kwargs)

    if 'L2A' in product_vars:
        product_vars['L2A'] = list(set(product_vars['L2A'] + GEDI_L2A_ESSENTIALS))
    else:
        product_vars['L2A'] = GEDI_L2A_ESSENTIALS
        
    for k,val in product_vars.items():
        if val is None:
            file = all_soc_files[0].get(k)
            product_vars[k] = gedi_vars_from_h5(file)

    prod_soc_files = [{k:val for k,val in i.items() if k in product_vars} for i in all_soc_files]
    
    def _filter_soc_file(prod):
        # Check if all required products are present
        if not np.isin(list(product_vars.keys()), list(prod.keys())).all():
            return None
        
        # Check skip_granules if provided
        if skip_granules is not None:
            gedifile = GEDIFile(list(prod.values())[0])
            gran = {'orbit': gedifile.orbit, 'granule': gedifile.orbit_granule, 'track': gedifile.track}
            if gran in skip_granules:
                return None
            
        for f in prod.values():
            if not h5_is_valid(f):
                return None
        
        return prod
    
    if verbose:
        print(f"Checking for incomplete, corrupted, or existing granules to skip.")
    
    bag_result = (
        dbg.from_sequence(prod_soc_files, partition_size=100)
          .map(_filter_soc_file)
          .filter(lambda x: x is not None)
          .persist()
    )
    progress(bag_result)
    soc_files = bag_result.compute()
    
    if len(soc_files) == 0:
        if verbose:
            print("No new granules to process. Finishing.")
        return
    
    if verbose:
        print(f"Found {len(soc_files)} new GEDI granules with requested products.")
    
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
    
    if verbose:
        print(f"Indexing H3 at resolution {res}, partitioning at {part}.")

    os.makedirs(tmp_dir, exist_ok=True)
    ddf = ddf.map_partitions(h3_index_df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)
    
    h3_tiles = []
    if spatial is not None:
        h3_tiles = intersect_h3_geometries(spatial, res=part)

    if len(h3_tiles) > 0:
        if verbose:
            print(f"Removing H3 partitions outside spatial filter.")
        ddf = ddf[ddf[f'h3_{part:02d}'].isin(h3_tiles)]
    
    build_log = os.path.join(h3_dir, 'gedih3_build_log.json')
    if os.path.exists(build_log):
        if verbose:
            print(f"Checking for existing indexed GEDI data to skip.")            
        _meta = ddf._meta.copy()
        _meta['_skip'] = False
        ddf = ddf.map_partitions(h3_add_skip_column, h3_dir=h3_dir, meta=_meta)
        ddf = ddf[~ddf['_skip']]
        ddf = ddf.drop(columns=['_skip'])

    if verbose:
        print(f"Adding date and geometry columns to H3 database.")

    ddf = ddf.map_partitions(add_special_columns, lon_col=lon_col, lat_col=lat_col, dat_col=dat_col)
    ddf['year'] = ddf.datetime.dt.year
    ddf = dask_geopandas.from_dask_dataframe(ddf)

    if verbose:
        print(f"Writing partitioned H3 data to temporary directory: {tmp_dir}")
        
    tmp_files = ddf.to_parquet(tmp_dir, write_index=True, overwrite=True, compression='zstd', partition_on=[f'h3_{part:02d}', 'year'], compute=False).persist()
    progress(tmp_files)
    tmp_files = tmp_files.compute()
    
    if not bool(tmp_files):
        if verbose:
            print("No new data to process. Finishing.")
        return
    
    if verbose:
        print(f"Merging H3 partitions into final database path: {h3_dir}")

    tmp_h3_dirs = glob.glob(os.path.join(tmp_dir, '*/*/'))
    os.makedirs(h3_dir, exist_ok=True)
    
    h3_files = [dh3_merge_files(in_dir=i, out_dir=h3_dir, rm_src=True, replace=False) for i in tmp_h3_dirs]
    h3_files = dask.persist(*h3_files, optimize_graph=False)
    progress(h3_files)
    
    if verbose:
        print("Compiling H3 metadata files.")

    h3_subdirs = glob.glob(os.path.join(h3_dir,'h3_*/'))
    h3_meta_files = [dh3_merge_metadata(i) for i in h3_subdirs]
    h3_meta_files = dask.persist(*h3_meta_files, optimize_graph=False)
    progress(h3_meta_files)

    h3_result = list(dask.compute(*h3_files))
    return h3_result