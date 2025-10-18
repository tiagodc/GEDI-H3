from datetime import datetime
from itertools import chain
import os
import pyarrow
import fiona
import json
import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from dask.distributed import get_client
from shapely.ops import orient
from shapely.geometry.base import BaseGeometry
from typing import Union, List, Dict, Optional, Tuple, Any
import glob

from .config import GEDI_PRODUCTS

def now():
    return datetime.now().isoformat()

def _glob(x): 
    return glob.glob(x, recursive=True)

def parallel_glob(parent_dir, pattern, tree_level=2, n_jobs = 1 + os.cpu_count() // 2, engine='threads', show_progress=False):
    sdirs = glob.glob(os.path.join(parent_dir, *['*/']*tree_level))
    
    if len(sdirs) < 2:
        return glob.glob(os.path.join(parent_dir, '**', pattern), recursive=True)
    
    if engine == 'processes':
        from pqdm.processes import pqdm
    elif engine == 'threads':
        from pqdm.threads import pqdm    

    patterns = [os.path.join(i, '**', pattern) for i in sdirs]
    n_jobs = min(n_jobs, os.cpu_count(), 32)
    batches = pqdm(patterns, _glob, n_jobs=n_jobs, disable=not show_progress)
    
    files = list(chain.from_iterable(batches))
    return files

def json_write(obj, path, mode='w', rewrite=False):
    if os.path.isfile(path) and not rewrite:
        obj = json_read(path) | obj
    with open(path, mode) as file:
        json.dump(obj, file)

def json_read(path, mode='r'):
    with open(path, mode) as f:
        obj = json.load(f)
        return obj

def is_parquet(file: str) -> bool:
    return file.lower().endswith(('.parquet','.parq','.pq'))

def read_parquet_schema(path):
    """
    path: parquet file path
    
    returns a pandas.DataFrame with the parquet column structure
    """
    schema = pyarrow.parquet.read_schema(path, memory_map=True)
    schema = pd.DataFrame(({"column": name, "dtype": str(pa_dtype)} for name, pa_dtype in zip(schema.names, schema.types)))
    return schema

def read_geopackage_schema(path):
    """
    path: gpkg file path
    
    returns a pandas.DataFrame with the gpkg column structure
    """
    gpkg_file = fiona.open(path, driver='GPKG')
    schema = pd.DataFrame([{'column':i, 'dtype':j} for i,j in gpkg_file.schema.get('properties').items()])
    return schema

def h5_is_valid(file):
    try:
        with h5py.File(file, mode='r', locking=False, swmr=True) as f:
            _ = list(f.keys())
    except Exception as e:
        return False
    return True

def h5_traverse(h5_file, root=None):
    def h5py_dataset_iterator(g, prefix=''):
        for key in g.keys():
            item = g[key]
            path = f'{prefix}/{key}'            
            if root is not None and not path.startswith(f"/{root}"):
                continue
            if isinstance(item, h5py.Dataset):
                yield (path, item)
            elif isinstance(item, h5py.Group):
                yield from h5py_dataset_iterator(item, path)

    for path, _ in h5py_dataset_iterator(h5_file):
        yield path    

def h5_info(hdf_file, root=None): 
    info_map = {'path':[], 'rows':[], 'cols':[], 'dtype': []}
    with h5py.File(hdf_file, 'r') as f:
        for dset in h5_traverse(f, root):
            info_map['path'].append(dset)           
            info_map['dtype'].append(f[dset].dtype)

            xy = f[dset].shape
            x = xy[0]
            y = 1 if len(xy) == 1 else xy[1]            
            info_map['rows'].append(x)
            info_map['cols'].append(y)            
    return pd.DataFrame(info_map)

def h5_var(file, var, col:int=None):
    with h5py.File(file, 'r') as f:
        return f.get(var)[:] if col is None  else f.get(var)[:,col]

def h5_meta(file, var='METADATA/DatasetIdentification'):
    with h5py.File(file, 'r') as f:
        return dict(f[var].attrs.items())
    
def h5_copy_subset(source_file, dest_file, variables):
    with h5py.File(source_file, 'r') as src, h5py.File(dest_file, 'w') as dst:
        def copy_item(name):
            if name in variables:
                src.copy(name, dst, name=f"/{name}", expand_soft=True, expand_refs=True)
        src.visit_links(copy_item)

def read_vector_file(filepath: str, crs: Union[str, int] = 4326) -> gpd.GeoDataFrame:
    geodf = gpd.read_parquet(filepath) if is_parquet(filepath) else gpd.read_file(filepath)
    geodf = gpd.GeoDataFrame(geometry=[geodf.union_all()], crs=geodf.crs)
    
    if crs is not None:
        geodf = geodf.to_crs(crs)
    
    return geodf

def geo_to_umm(obj):
    """
    Converts a GeoDataFrame, shapely Polygon, or GeoJSON dictionary to a UMM-style list of coordinates.
    """   
    geodf = None
    if isinstance(obj, dict):
        geodf = from_geojson(obj)
    elif isinstance(obj, gpd.GeoDataFrame):
        geodf = obj.geometry.apply(orient, args=(1,))
    
    if geodf is not None:        
        geodf = geodf.explode(index_parts=False).reset_index()
        if len(geodf) > 1:
            geo_umm = [list(zip(*polygon.exterior.coords.xy)) for polygon in geodf.geometry]
        else:
            xy = geodf.geometry.get_coordinates()
            geo_umm = list(zip(xy.x, xy.y))    
    
    elif isinstance(obj, BaseGeometry):
        oriented_geom = orient(obj, 1)
        if oriented_geom.geom_type == 'MultiPolygon':
            geo_umm = [list(zip(*polygon.exterior.coords.xy)) for polygon in oriented_geom.geoms]
        else:
            coords = oriented_geom.exterior.coords
            geo_umm = list(coords)
    
    else:
        raise TypeError(f"Unsupported type: {type(obj)}")
        
    return geo_umm

def to_geojson(geodf: gpd.GeoDataFrame) -> Dict:
    geodf.geometry = geodf.geometry.apply(orient, args=(1,))
    # geojson = {"shapefile": ("roi.geojson", geodf.geometry.to_json(), "application/geo+json")}
    return geodf.geometry.to_json()

def from_geojson(geojson) -> gpd.GeoDataFrame:
    # geojson_features = geojson['shapefile'][1]
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    return gpd.GeoDataFrame.from_features(geojson, crs=4326)

def read_as_geojson(geofile: str, box_only: bool = False) -> Dict:
    roi = read_vector_file(geofile, crs=4326) 
    if box_only:
        from shapely.geometry import box
        roi = gpd.GeoDataFrame(geometry=[box(*roi.total_bounds)], columns=['geometry'], crs=roi.crs)
    geojson = to_geojson(roi)
    return geojson

def parquet_append_rows(df: pd.DataFrame, f: str, id_col: str = 'shot_number', tmp_suffix: str = '.row.tmp'):    
    parquet_file = pq.ParquetFile(f)
    
    if id_col:
        idx = parquet_file.read([id_col]).to_pandas().values.flatten()
        df = df[~df[id_col].isin(idx)]
    
    if df.empty:
        return
    
    new_table = pa.Table.from_pandas(df)
    
    temp_f = f + tmp_suffix
    with pq.ParquetWriter(temp_f, parquet_file.schema.to_arrow_schema(), compression='zstd') as writer:
        for batch in parquet_file.iter_batches():
            writer.write_batch(batch)        
        writer.write_table(new_table)
    
    os.replace(temp_f, f)

def parquet_append_columns(df: pd.DataFrame, f: str, tmp_suffix:str = '.col.tmp'):
    parquet_file = pq.ParquetFile(f)
    new_table = pa.Table.from_pandas(df)
    
    existing_schema = parquet_file.schema.to_arrow_schema()
    existing_fields = list(existing_schema)
    new_fields = [field for field in new_table.schema if field.name not in existing_schema.names]
    combined_schema = pa.schema(existing_fields + new_fields)

    temp_f = f + tmp_suffix
    with pq.ParquetWriter(temp_f, combined_schema, compression='zstd') as writer:
        for batch in parquet_file.iter_batches():
            batch_dict = batch.to_pydict()
            for field in new_table.schema:
                if field.name not in batch.schema.names:
                    batch_dict[field.name] = [None] * len(batch)
            writer.write_batch(pa.RecordBatch.from_pydict(batch_dict, combined_schema))

        new_batch_dict = new_table.to_pydict()
        for field in existing_schema:
            if field.name not in new_table.schema.names:
                new_batch_dict[field.name] = [None] * len(new_table)
        writer.write_batch(pa.RecordBatch.from_pydict(new_batch_dict, combined_schema))
    
    os.replace(temp_f, f)

def parquet_merge_files(ofile, flist, check_shots=True, rm_src=False):
    shots = np.array([], dtype=np.uint64)
    pqwriter = None
    schema = None
    
    try:
        for f in flist:
            if not os.path.exists(f):
                continue
                
            parquet_file = pq.ParquetFile(f)            
            if schema is None:
                schema = parquet_file.schema.to_arrow_schema()
                pqwriter = pq.ParquetWriter(ofile, schema, compression='zstd')
            
            for batch in parquet_file.iter_batches():
                df = batch.to_pandas()
                idx_name = df.index.name
                
                if check_shots and 'shot_number' in df.columns:
                    new_shots = df['shot_number'].values.astype(np.uint64)
                    mask = ~np.isin(new_shots, shots)
                    df = df[mask]
                    shots = np.concatenate([shots, new_shots[mask]])
                
                if len(df) > 0:
                    df = df.reset_index()[schema.names].set_index(idx_name)
                    table = pa.Table.from_pandas(df, schema=schema)
                    pqwriter.write_table(table)
            
            if rm_src:
                os.unlink(f)
        
    finally:
        if pqwriter is not None:
            pqwriter.close()

def get_dask_client():
    try:
        client = get_client()
        return client
    except (ValueError, RuntimeError):
        return None

def parse_gedi_args(args):
    prod_vars = {}
    for k in GEDI_PRODUCTS.keys():
        if hasattr(args, k.lower()):
            if (vars := getattr(args, k.lower())) is not None:
                prod_vars[k] = vars
    return prod_vars
    
def parse_dask_args(args):
    dask_args = {}
    if args.dask_scheduler:
        dask_args['address'] = args.dask_scheduler
    else:
        dask_args['n_workers'] = args.n_cpus
        dask_args['threads_per_worker'] = args.threads
        dask_args['memory_limit'] = f"{args.ram}GB" if args.ram else None
        dask_args['dashboard_address'] = f":{args.port}" if args.port else None
    return dask_args    
