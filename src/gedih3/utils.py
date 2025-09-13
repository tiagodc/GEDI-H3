import logging
import getpass
import h5py
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

def is_parquet(file: str) -> bool:
    return file.lower().endswith(('.parquet','.parq','.pq'))

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

