import os, glob
import pandas as pd
import dask.dataframe

from.config import GH3_DEFAULT_H3_DIR
from .h3utils import intersect_h3_geometries

def gh3_list_files(gh3_root_dir=GH3_DEFAULT_H3_DIR, product=None):
    if recursive := product is None:
        product = '**'
    return glob.glob(os.path.join(gh3_root_dir, product.lower(), '*.parquet'), recursive=recursive)

def gh3_list_parts(gh3_root_dir=GH3_DEFAULT_H3_DIR, product=None):
    files = gh3_list_files(gh3_root_dir=gh3_root_dir, product=product)
    h3_ids = list({os.path.basename(i).replace('.parquet', '') for i in files})
    return h3_ids    

def gh3_load_part(h3_part_id, h3_product_vars, query=None, gh3_dir=GH3_DEFAULT_H3_DIR):
    df = None
    for prod,cols in h3_product_vars.items():
        if 'shot_number' not in cols:
            cols.append('shot_number')
        ipath = os.path.join(gh3_dir, prod.lower(), f"{h3_part_id}.parquet")
        idf = pd.read_parquet(ipath, engine='pyarrow', columns=cols)
        if df is None:
            idx_name = idf.index.name
            df = idf.reset_index()
        else:
            df = df.merge(idf, how='inner', on='shot_number', suffixes=(f'', f'_{prod}'))
    df = df.set_index(idx_name)
    
    if query is not None:
        df = df.query(query)
    
    return df

def gh3_load_all(h3_product_vars, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR): 
    h3_ids = gh3_list_parts(gh3_root_dir=gh3_dir)
    
    if region is not None:
        h3_ids = intersect_h3_geometries(region, h3_ids=h3_ids)
        
    return dask.dataframe.from_map(gh3_load_part, h3_ids, h3_product_vars=h3_product_vars, query=query, gh3_dir=gh3_dir)