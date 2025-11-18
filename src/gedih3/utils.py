# Standard library imports (fast)
from datetime import datetime
import os
import json
from typing import Union, List, Dict, Optional, Tuple, Any

# Heavy imports are moved to lazy loading inside functions:
# - psutil: used in get_system_resources
# - pyarrow/pandas: used in parquet and schema functions
# - h5py: used in h5_* functions
# - geopandas/shapely/rioxarray/fiona: used in geo functions
# - numpy: used in parquet_merge_files
# - dask.distributed: used in get_dask_client

def get_package_version():
    """Get the current package version"""
    try:
        from importlib.metadata import version
        return version('gedih3')
    except ImportError:
        try:
            from . import __version__
            return __version__
        except:
            return "unknown"

def now():
    return datetime.now().isoformat()

def get_system_resources(disk_path:str=None):
    import psutil
    ram = psutil.virtual_memory().total / (1024**3)
    storage = psutil.disk_usage(os.getcwd() if disk_path is None else disk_path).free / (1024**3)
    cpus = os.cpu_count()
    return cpus, ram, storage

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

def is_hive_directory(dir_path: str, match_str=r'.+=.+') -> bool:
    if not os.path.isdir(dir_path):
        return False
    subdirs = os.listdir(dir_path)    
    subdirs = [d for d in subdirs if os.path.isdir(os.path.join(dir_path, d))]
    if match_str is not None:
        import re
        pattern = re.compile(match_str)
        subdirs = [d for d in subdirs if pattern.match(d)]
    return len(subdirs) > 0    

def read_parquet_schema(path):
    """
    path: parquet file path

    returns a pandas.DataFrame with the parquet column structure
    """
    import pyarrow.parquet as pq
    import pandas as pd
    schema = pq.read_schema(path, memory_map=True)
    schema = pd.DataFrame(({"column": name, "dtype": str(pa_dtype)} for name, pa_dtype in zip(schema.names, schema.types)))
    return schema

def read_geopackage_schema(path):
    """
    path: gpkg file path

    returns a pandas.DataFrame with the gpkg column structure
    """
    import fiona
    import pandas as pd
    gpkg_file = fiona.open(path, driver='GPKG')
    schema = pd.DataFrame([{'column':i, 'dtype':j} for i,j in gpkg_file.schema.get('properties').items()])
    return schema

def h5_is_valid(file):
    import h5py
    try:
        with h5py.File(file, mode='r') as f:
            _ = list(f.keys())
    except Exception as e:
        return False
    return True

def h5_traverse(h5_file, root=None):
    import h5py
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
    import h5py
    import pandas as pd
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
    import h5py
    with h5py.File(file, 'r') as f:
        return f.get(var)[:] if col is None  else f.get(var)[:,col]

def h5_meta(file, var='METADATA/DatasetIdentification'):
    import h5py
    with h5py.File(file, 'r') as f:
        return dict(f[var].attrs.items())

def h5_copy_subset(source_file, dest_file, variables):
    import h5py
    with h5py.File(source_file, 'r') as src, h5py.File(dest_file, 'w') as dst:
        def copy_item(name):
            if name in variables:
                src.copy(name, dst, name=f"/{name}", expand_soft=True, expand_refs=True)
        src.visit_links(copy_item)

def read_vector_file(filepath: str, crs: Union[str, int] = 4326):
    import geopandas as gpd
    geodf = gpd.read_parquet(filepath) if is_parquet(filepath) else gpd.read_file(filepath)
    geodf = gpd.GeoDataFrame(geometry=[geodf.union_all()], crs=geodf.crs)

    if crs is not None:
        geodf = geodf.to_crs(crs)

    return geodf

def read_img_bounds(filepath: str, crs=4326):
    import rioxarray
    import geopandas as gpd
    from shapely.geometry import box
    img = rioxarray.open_rasterio(filepath)
    bounds = list(img.rio.bounds())
    geobox = gpd.GeoDataFrame(geometry=[box(*bounds)], crs=img.rio.crs, index=[0])
    return geobox.to_crs(crs)

def geo_to_umm(obj):
    """
    Converts a GeoDataFrame, shapely Polygon, or GeoJSON dictionary to a UMM-style list of coordinates.
    """
    import geopandas as gpd
    from shapely.ops import orient
    from shapely.geometry.base import BaseGeometry
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

def to_geojson(geodf) -> Dict:
    import geopandas as gpd
    from shapely.ops import orient
    geodf['geometry'] = geodf.geometry.apply(orient, args=(1,))
    return geodf.geometry.to_json()

def from_geojson(geojson):
    import geopandas as gpd
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    return gpd.GeoDataFrame.from_features(geojson, crs=4326)

def read_as_geojson(geofile: str, box_only: bool = False) -> Dict:
    import geopandas as gpd
    from shapely.geometry import box
    roi = read_vector_file(geofile, crs=4326)
    if box_only:
        roi = gpd.GeoDataFrame(geometry=[box(*roi.total_bounds)], columns=['geometry'], crs=roi.crs)
    geojson = to_geojson(roi)
    return geojson

def parquet_append_rows(df, f: str, id_col: str = 'shot_number', tmp_suffix: str = '.row.tmp'):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
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

def parquet_append_columns(df, f: str, tmp_suffix:str = '.col.tmp'):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
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

def parquet_schema_add_bbox(schema, bbox):
    if bbox is None:
        return schema    
    geo_meta = json.loads(schema.metadata[b'geo'])
    geo_meta['columns']['geometry']['bbox'] = bbox
    new_metadata = {**schema.metadata, b'geo': json.dumps(geo_meta).encode('utf-8')}
    return schema.with_metadata(new_metadata)
    
def parquet_merge_files(ofile, flist, check_shots=True, rm_src=False, rows_per_group=100_000):
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    import pyarrow as pa
    import pyarrow.parquet as pq
    # import geoarrow.pyarrow as ga

    shots = set()
    pqwriter = None
    schema = None
    accumulated_tables = []
    accumulated_rows = 0
    merged_bbox = None
    
    try:
        gds = pq.ParquetDataset(flist)
        if 'geometry' in gds.schema.names:
            geodf = gpd.read_parquet(flist, columns=['geometry'])
            merged_bbox = list(geodf.total_bounds)
            # table = gds.read(columns=['geometry'])
            # geometry_array = ga.as_geoarrow(table['geometry'])
            # bbox = ga.box_agg(geometry_array)
            # merged_bbox = list(bbox.bounds.values())

        for f in flist:
            if not os.path.exists(f):
                continue

            parquet_file = pq.ParquetFile(f)

            if schema is None:
                schema = parquet_file.schema.to_arrow_schema()                
                schema = parquet_schema_add_bbox(schema, bbox=merged_bbox)
                pqwriter = pq.ParquetWriter(ofile, schema, compression='zstd')

            for batch in parquet_file.iter_batches(batch_size=rows_per_group):
                df = batch.to_pandas()
                idx_name = df.index.name

                if check_shots and 'shot_number' in df.columns:
                    new_shots = df['shot_number'].values.astype(np.uint64)
                    mask = ~np.isin(new_shots, np.array(list(shots)))
                    if not mask.any():
                        continue
                    df = df[mask]
                    shots.update(new_shots[mask])

                df_reset = df.reset_index()
                df_reordered = df_reset.reindex(columns=schema.names, copy=False)
                df_final = df_reordered.set_index(idx_name)
                table = pa.Table.from_pandas(df_final, schema=schema)

                accumulated_tables.append(table)
                accumulated_rows += len(table)

                if accumulated_rows >= rows_per_group:
                    combined_table = pa.concat_tables(accumulated_tables)
                    pqwriter.write_table(combined_table)
                    accumulated_tables = []
                    accumulated_rows = 0

            if rm_src:
                os.unlink(f)

        # Write remaining data
        if accumulated_tables:
            combined_table = pa.concat_tables(accumulated_tables)
            pqwriter.write_table(combined_table)

    finally:
        if pqwriter is not None:
            pqwriter.close()

def parse_temporal(temporal):
    if temporal is None:
        return None
    
    if isinstance(temporal, (list, tuple)) and len(temporal) == 2:
        start, end = temporal
        if isinstance(start, str):
            start = datetime.fromisoformat(start.replace('Z', '+00:00'))
            start = start.strftime('%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.fromisoformat(end.replace('Z', '+00:00'))
            end = end.strftime('%Y-%m-%d')
        return (start, end)
    else:
        raise ValueError("Invalid temporal input. Must be a list or tuple of two dates.")

def parse_spatial(spatial):
    if spatial is None:
        return None

    import geopandas as gpd
    from shapely.geometry import box

    if isinstance(spatial, dict):
        spatial = from_geojson(spatial)
    elif isinstance(spatial, str):
        if os.path.exists(spatial) or spatial.lower().startswith(('http://', 'https://', 's3://')):
            if spatial.lower().endswith(('.tif', '.tiff', '.vrt', '.geotif', '.geotiff', '.img')):
                spatial = read_img_bounds(spatial, crs=4326)
            else:
                spatial = read_vector_file(spatial, crs=4326)        
        else:            
            try:
                spatial = from_geojson(spatial)
            except:
                raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
    elif isinstance(spatial, list) and len(spatial) == 4:
        spatial = gpd.GeoDataFrame(geometry=[box(*spatial)], crs=4326, index=[0])
    elif isinstance(spatial, gpd.GeoDataFrame):
        spatial = spatial.to_crs(epsg=4326)
        spatial = gpd.GeoDataFrame(geometry=[spatial.union_all()], crs=spatial.crs)
    else:
        raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")

    return spatial

def merge_spatial(existing, new):
    if new is None:
        return existing, None

    import geopandas as gpd

    new = parse_spatial(new)

    if existing is None:
        return new, None

    gdf_union = gpd.overlay(existing, new, how='union').union_all()
    gdf_union = gpd.GeoDataFrame(geometry=[gdf_union], crs=existing.crs)

    gdf_sdiff = gpd.overlay(existing, new, how='symmetric_difference').union_all()
    gdf_sdiff = gpd.GeoDataFrame(geometry=[gdf_sdiff], crs=existing.crs)

    if gdf_sdiff.geometry.iloc[0].is_empty:
        gdf_sdiff = None

    return gdf_union, gdf_sdiff

def get_dask_client():
    from dask.distributed import get_client
    try:
        client = get_client()
        return client
    except (ValueError, RuntimeError):
        return None