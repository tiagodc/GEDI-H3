import os, glob
import pandas as pd
import geopandas as gpd
import dask.dataframe
import dask_geopandas

from.config import GH3_DEFAULT_H3_DIR, configure_environment
from .utils import json_read
from .h3utils import intersect_h3_geometries, fix_h3_geometry

def gh3_set_db_path(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    os.environ['GH3_DEFAULT_H3_DIR'] = gh3_root_dir
    configure_environment()

def gh3_list_files(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    return glob.glob(os.path.join(gh3_root_dir, '**', '*.parquet'), recursive=True)

def gh3_list_parts(gh3_root_dir=GH3_DEFAULT_H3_DIR):
    files = glob.glob(os.path.join(gh3_root_dir, 'h3_*/'))
    h3_ids = [i.split('=')[-1].rstrip('/') for i in files]
    return h3_ids

def gh3_read_meta(var, gh3_root_dir=GH3_DEFAULT_H3_DIR):
    meta_path = os.path.join(gh3_root_dir, "gedih3_build_log.json")
    meta = json_read(meta_path)
    return meta.get(var)

def gh3_part_from_df(df):
    h3_cols = [col for col in df.columns if col.startswith('h3_')]
    return sorted(h3_cols)[0]

def gh3_aggregate_func(df, res, agg='mean', cols=None, **kwargs):
    import h3pandas
    h3col = f"h3_{res:02d}"
    g = df.h3.h3_to_parent(resolution=res).groupby(h3col, observed=True)
    if cols is not None:
        g = g[cols]
    out = g.apply(agg, include_groups=False, **kwargs) if callable(agg) else g.agg(agg)

    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ['_'.join(map(str, col)).strip() for col in out.columns.values]

    return out.reset_index()

def gh3_add_geometry(df):
    geo = [fix_h3_geometry(i) for i in df.index]
    gdf = gpd.GeoDataFrame(df, geometry=geo, crs=4326)
    return gdf

def gh3_load(columns=None, region=None, query=None, gh3_dir=GH3_DEFAULT_H3_DIR): 
    h3_part = gh3_read_meta("h3_partition_level", gh3_root_dir=gh3_dir)
    h3_part_col = f"h3_{h3_part:02d}"
    h3_ids = gh3_read_meta("h3_partition_ids", gh3_root_dir=gh3_dir)
    
    h3_filter = {}
    out_cols = None
    if columns is not None:
        if h3_part_col not in columns:
            columns.append(h3_part_col)

        out_cols = columns.copy()
        
        if query is not None:
            available_cols = gh3_read_meta("h3_columns", gh3_root_dir=gh3_dir)
            q_cols = [col for col in available_cols if col in query]
            columns = list(set(columns + q_cols))
        
        h3_filter['columns'] = columns

    if region is not None:
        h3_ids = intersect_h3_geometries(region, h3_ids=h3_ids)
        h3_filter['filters'] = [(h3_part_col,'in',h3_ids)]
    
    ddf = dask_geopandas.read_parquet(gh3_dir, 
                                      calculate_divisions=False, 
                                      split_row_groups=False, 
                                      aggregate_files=False, 
                                      gather_spatial_partitions=False, 
                                      ignore_metadata_file=False, 
                                      **h3_filter)

    if query is not None:
        ddf = ddf.query(query)
        if out_cols is not None:
            ddf = ddf[out_cols]

    return ddf

def gh3_aggregate(gh3_df, target_res=5, agg='mean', columns=None, query=None, add_geometry=True, **kwargs):
    _meta = gh3_aggregate_func(df=gh3_df.head(), res=target_res, agg=agg, cols=columns, **kwargs)

    if query is not None:
        gh3_df = gh3_df.query(query)

    h3part = gh3_part_from_df(gh3_df)
    agg_df = gh3_df.groupby(h3part, observed=True).apply(gh3_aggregate_func, res=target_res, agg=agg, include_groups=False, meta=_meta, **kwargs)
    agg_df = agg_df.set_index(f"h3_{target_res:02d}", sort=False)
    
    if add_geometry:
        _gmeta = gpd.GeoDataFrame(columns=agg_df._meta.columns.tolist() + ['geometry'], geometry='geometry', crs=4326)
        agg_df = agg_df.map_partitions(gh3_add_geometry, meta=_gmeta)
        if isinstance(agg_df, dask.dataframe.DataFrame):
            agg_df = dask_geopandas.from_dask_dataframe(agg_df)

    return agg_df