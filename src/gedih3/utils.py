import os
import logging
import getpass
import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely.ops import orient
from typing import Union, List, Dict, Optional, Tuple, Any

def set_logger(filename:str=None, level=logging.INFO):
    username = getpass.getuser()
    
    logging.getLogger().handlers.clear()
    handlers = [logging.StreamHandler()]
    
    if filename:
        handlers.append(logging.FileHandler(filename))
    
    logging.basicConfig(
        level=level,
        format=f'%(asctime)s - {username} - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %Z',
        handlers=handlers
    )

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
    gdf = gpd.read_parquet(filepath) if is_parquet(filepath) else gpd.read_file(filepath) 
    if crs is not None:
        gdf = gdf.to_crs(crs)
    return gdf
    
def to_geojson(geodf: gpd.GeoDataFrame) -> Dict:
    geodf.geometry = geodf.geometry.apply(orient, args=(1,))
    geojson = {"shapefile": ("roi.geojson", geodf.geometry.to_json(), "application/geo+json")}
    return geojson

def read_as_geojson(geofile: str, box_only: bool = False) -> Dict:
    roi = read_vector_file(geofile, crs=4326) 
    if box_only:
        from shapely.geometry import box
        roi = gpd.GeoDataFrame(geometry=[box(*roi.total_bounds)], columns=['geometry'], crs=roi.crs)
    geojson = to_geojson(roi)
    return geojson

def is_parquet(file: str) -> bool:
    return file.lower().endswith(('.parquet','.parq','.pq'))

def parquet_append_rows(df: pd.DataFrame, f: str, id_col: str = 'shot_number', tmp_suffix: str = '.row.tmp'):    
    parquet_file = pq.ParquetFile(f)
    
    if id_col:
        idx = parquet_file.read([id_col]).to_pandas().values.flatten()
        df = df[~df[id_col].isin(idx)]
    
    if df.empty:
        return
    
    new_table = pa.Table.from_pandas(df)
    
    temp_f = f + tmp_suffix
    with pq.ParquetWriter(temp_f, parquet_file.schema.to_arrow_schema()) as writer:
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
    with pq.ParquetWriter(temp_f, combined_schema) as writer:
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
                pqwriter = pq.ParquetWriter(ofile, schema)
            
            for batch in parquet_file.iter_batches():
                df = batch.to_pandas()
                
                if check_shots and 'shot_number' in df.columns:
                    new_shots = df['shot_number'].values.astype(np.uint64)
                    mask = ~np.isin(new_shots, shots)
                    df = df[mask]
                    shots = np.concatenate([shots, new_shots[mask]])
                
                if len(df) > 0:
                    table = pa.Table.from_pandas(df)
                    table = table.cast(schema)
                    pqwriter.write_table(table)
            
            if rm_src:
                os.unlink(f)
        
    finally:
        if pqwriter is not None:
            pqwriter.close()
