# Standard library imports (fast)
from datetime import datetime
import os
import json
from typing import Union, List, Dict, Optional, Tuple, Any

from .exceptions import (GediDatabaseNotFoundError, GediFileError, GediValidationError,
                         GediSpatialError, GediTemporalError)

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
    import geopandas as gpd
    import pandas as pd
    gdf = gpd.read_file(path, rows=1)
    return pd.DataFrame({"column": gdf.columns, "dtype": [str(d) for d in gdf.dtypes]})

def read_feather_schema(path):
    """
    Read schema from a feather (Arrow IPC) file.

    Parameters
    ----------
    path : str
        Path to feather file

    Returns
    -------
    pandas.DataFrame
        DataFrame with 'column' and 'dtype' columns
    """
    import pyarrow.feather as feather
    import pandas as pd
    schema = feather.read_table(path, columns=[]).schema
    return pd.DataFrame(({"column": name, "dtype": str(pa_dtype)}
                          for name, pa_dtype in zip(schema.names, schema.types)))


def read_h3_database_schema(db_path):
    """Read parquet schema from an H3 hive-partitioned database.

    Finds the first parquet file inside any H3 partition directory
    and reads its schema via read_parquet_schema().

    Parameters
    ----------
    db_path : str
        Path to H3 database root directory (containing h3_XX=* subdirs)

    Returns
    -------
    pandas.DataFrame
        DataFrame with 'column' and 'dtype' columns

    Raises
    ------
    FileNotFoundError
        If no H3 partition directories or parquet files found
    """
    import glob as globmod
    partition_dirs = sorted(globmod.glob(os.path.join(db_path, 'h3_*=*/')))
    if not partition_dirs:
        raise GediDatabaseNotFoundError(f"No H3 partition directories found in {db_path}")
    for pdir in partition_dirs:
        # Search recursively — partitions may have nested hive dirs (e.g. year=*)
        pq_files = sorted(globmod.glob(os.path.join(pdir, '**', '*.parquet'), recursive=True))
        if pq_files:
            return read_parquet_schema(pq_files[0])
    raise GediDatabaseNotFoundError(f"No parquet files found in any partition of {db_path}")


def read_schema(path, root=None):
    """
    Read schema from a data file or dataset directory, auto-detecting format.

    Supports parquet, feather, gpkg, and HDF5 files. For directories, detects
    the dataset format from metadata or file extensions. Also detects H3
    databases by the presence of the build log file.

    Parameters
    ----------
    path : str
        Path to a file or dataset directory

    Returns
    -------
    pandas.DataFrame
        DataFrame with 'column' and 'dtype' columns (for vector/tabular formats),
        or 'column', 'dtype', and 'shape' columns (for HDF5)

    Raises
    ------
    FileNotFoundError
        If no data files found
    ValueError
        If format cannot be determined
    """
    import glob as globmod

    if os.path.isdir(path):
        # Check for H3 database first (has build log)
        from .config import BUILD_LOG_FILENAME
        build_log = os.path.join(path, BUILD_LOG_FILENAME)
        if os.path.exists(build_log):
            return read_h3_database_schema(path)
        # Fall through to simplified dataset detection
        from .cliutils import detect_dataset_format, list_dataset_files
        fmt = detect_dataset_format(path)
        files = list_dataset_files(path, fmt=fmt)
        path = files[0]
    else:
        ext = os.path.splitext(path)[1].lstrip('.').lower()
        fmt = {
            'parquet': 'parquet', 'parq': 'parquet', 'pq': 'parquet',
            'feather': 'feather',
            'gpkg': 'gpkg', 'geopackage': 'gpkg',
            'h5': 'h5', 'hdf5': 'h5',
        }.get(ext)
        if fmt is None:
            raise GediFileError(f"Cannot determine format from extension: {ext}")

    if fmt == 'parquet':
        return read_parquet_schema(path)
    elif fmt == 'feather':
        return read_feather_schema(path)
    elif fmt == 'gpkg':
        return read_geopackage_schema(path)
    elif fmt == 'h5':
        return h5_info(path, root=root)
    else:
        raise GediFileError(f"Unsupported format: {fmt}")

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
        raise GediValidationError(f"Unsupported type: {type(obj)}")

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
    
def parquet_merge_files(ofile, flist, check_shots=False, rm_src=False, rows_per_group=100_000):
    import numpy as np
    import geopandas as gpd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    shots = None
    merged_bbox = None

    dataset = ds.dataset(flist, format="parquet")
    schema = dataset.schema

    if 'geometry' in schema.names:
        geodf = gpd.read_parquet(flist, columns=['geometry'])
        merged_bbox = list(geodf.total_bounds)
        schema = parquet_schema_add_bbox(schema, bbox=merged_bbox)

    writer = pq.ParquetWriter(ofile, schema, compression="zstd")
    shots = None
    acc = []
    acc_rows = 0

    scanner = dataset.scanner(batch_size=rows_per_group, use_threads=False)
    for batch in scanner.to_batches():
        if check_shots and "shot_number" in batch.schema.names:
            arr = batch["shot_number"].to_numpy().astype(np.uint64)
            if shots is None:
                shots = np.unique(arr)
            else:
                keep = ~np.isin(arr, shots, assume_unique=True)
                if not keep.any():
                    continue
                batch = batch.filter(pa.array(keep))
                shots = np.unique(np.concatenate([shots, arr[keep]]))

        acc.append(pa.Table.from_batches([batch], schema=schema))
        acc_rows += batch.num_rows
        if acc_rows >= rows_per_group:
            writer.write_table(pa.concat_tables(acc))
            acc.clear()
            acc_rows = 0

    if acc:
        writer.write_table(pa.concat_tables(acc))
    writer.close()

    if rm_src:
        for f in flist:
            if os.path.exists(f):
                os.unlink(f)

def parquet_join_columns(flist: List[str], ofile: str, key_col: str = 'shot_number',
                         tmp_suffix: str = '.join.tmp', join_how='left'):
    """
    Memory-efficient column-wise join of parquet files. Equivalent to pd.concat(axis=1)
    but processes in batches to avoid loading entire files into memory.

    Parameters
    ----------
    flist : List[str]
        Parquet files to join. First file determines row order, index, and metadata.
    ofile : str
        Output file path.
    key_col : str, default='shot_number'
        Column for joining (not the index).
    rm_src : bool, default=False
        Remove source files after join.
    rows_per_group : int, optional
        Output row group size. Defaults to first file's row group size.
    tmp_suffix : str, default='.join.tmp'
        Temporary file suffix.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    if len(flist) < 2:
        raise GediFileError("Need at least 2 files to join")

    # Get base file info
    base_file = pq.ParquetFile(flist[0])
    base_schema = base_file.schema_arrow

    # Determine which columns to read from each file (only new ones)
    base_cols = set(base_schema.names)
    other_files = {}
    for f in flist[1:]:
        f_schema = pq.read_schema(f)
        new_cols = [c for c in f_schema.names if c not in base_cols]
        if new_cols:
            other_files[f] = new_cols
            base_cols.update(new_cols)

    # Load other files (only new columns + key_col) indexed by key_col
    other_data = {}
    for f, new_cols in other_files.items():
        df = pd.read_parquet(f, columns=[key_col] + new_cols)
        other_data[f] = df.set_index(key_col)

    # Build output schema
    combined_schema = base_schema
    for f, new_cols in other_files.items():
        f_schema = pq.read_schema(f)
        for col in new_cols:
            combined_schema = combined_schema.append(f_schema.field(col))

    if base_schema.metadata:
        combined_schema = combined_schema.with_metadata(base_schema.metadata)

    pardir = os.path.dirname(ofile)
    if not os.path.exists(pardir):
        os.makedirs(pardir, exist_ok=True)

    # Process in batches
    temp_ofile = ofile + tmp_suffix
    with pq.ParquetWriter(temp_ofile, combined_schema, compression='zstd') as writer:
        for rg_idx in range(base_file.metadata.num_row_groups):
            # Read batch from base file
            batch = base_file.read_row_group(rg_idx).to_pandas()

            # Save original index name if it exists
            idx_name = batch.index.name

            # Reset index first to make it a column, then use key_col for joining
            if idx_name:
                batch = batch.reset_index()

            # Join new columns from other files
            batch_indexed = batch.set_index(key_col)
            for indexed_df in other_data.values():
                batch_indexed = batch_indexed.join(indexed_df, how=join_how)

            # Reset index to get key_col back as column
            batch = batch_indexed.reset_index()

            # Restore original index if it had a name
            if idx_name:
                batch = batch.set_index(idx_name)

            # Reorder columns to match schema (excluding index)
            cols_to_select = [c for c in combined_schema.names if c in batch.columns]
            batch = batch[cols_to_select]

            writer.write_table(pa.Table.from_pandas(batch, schema=combined_schema))
   
    os.replace(temp_ofile, ofile)

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
        raise GediTemporalError("Invalid temporal input. Must be a list or tuple of two dates.")

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
                raise GediSpatialError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
    elif isinstance(spatial, list) and len(spatial) == 4:
        spatial = gpd.GeoDataFrame(geometry=[box(*spatial)], crs=4326, index=[0])
    elif isinstance(spatial, gpd.GeoDataFrame):
        spatial = spatial.to_crs(epsg=4326)
        spatial = gpd.GeoDataFrame(geometry=[spatial.union_all()], crs=spatial.crs)
    else:
        raise GediSpatialError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")

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


# =============================================================================
# Transaction Safety for File Operations
# =============================================================================

class AtomicFileWriter:
    """
    Context manager for atomic file writes with automatic rollback on failure.

    Writes to a temporary file first, then atomically replaces the target
    file only on successful completion. If an error occurs, the temporary
    file is cleaned up and the original file (if any) is preserved.

    Parameters
    ----------
    target_path : str
        The final destination file path
    suffix : str
        Suffix for the temporary file (default: '.tmp')
    backup : bool
        If True, keep a backup of the original file (default: False)
    backup_suffix : str
        Suffix for backup files (default: '.bak')

    Examples
    --------
    >>> with AtomicFileWriter('/path/to/output.parquet') as tmp_path:
    ...     df.to_parquet(tmp_path)
    # File is atomically renamed to /path/to/output.parquet on success

    >>> with AtomicFileWriter('/path/to/output.json', backup=True) as tmp_path:
    ...     with open(tmp_path, 'w') as f:
    ...         json.dump(data, f)
    # Original file backed up to .bak, new file replaces it
    """

    def __init__(
        self,
        target_path: str,
        suffix: str = '.tmp',
        backup: bool = False,
        backup_suffix: str = '.bak'
    ):
        self.target_path = target_path
        self.temp_path = target_path + suffix
        self.backup = backup
        self.backup_path = target_path + backup_suffix
        self._success = False

    def __enter__(self) -> str:
        # Ensure parent directory exists
        parent_dir = os.path.dirname(self.target_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        return self.temp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Success - atomically replace target
            try:
                if self.backup and os.path.exists(self.target_path):
                    # Create backup of original
                    if os.path.exists(self.backup_path):
                        os.unlink(self.backup_path)
                    os.rename(self.target_path, self.backup_path)

                os.replace(self.temp_path, self.target_path)
                self._success = True
            except Exception:
                # Cleanup temp file on rename failure
                if os.path.exists(self.temp_path):
                    os.unlink(self.temp_path)
                raise
        else:
            # Failure - cleanup temp file
            if os.path.exists(self.temp_path):
                os.unlink(self.temp_path)

        return False  # Don't suppress exceptions


def safe_file_replace(src: str, dst: str, backup: bool = False) -> str:
    """
    Atomically replace a file with rollback on failure.

    Parameters
    ----------
    src : str
        Source file path
    dst : str
        Destination file path
    backup : bool
        If True, keep backup of original destination file

    Returns
    -------
    str
        Destination file path on success

    Raises
    ------
    FileNotFoundError
        If source file doesn't exist
    OSError
        If file operation fails
    """
    if not os.path.exists(src):
        raise GediFileError(f"Source file not found: {src}")

    backup_path = dst + '.bak'

    try:
        if backup and os.path.exists(dst):
            if os.path.exists(backup_path):
                os.unlink(backup_path)
            os.rename(dst, backup_path)

        os.replace(src, dst)
        return dst

    except Exception as e:
        # Attempt rollback
        if backup and os.path.exists(backup_path) and not os.path.exists(dst):
            os.rename(backup_path, dst)
        raise


def safe_directory_write(
    write_func,
    target_dir: str,
    suffix: str = '.tmp',
    cleanup_on_failure: bool = True
):
    """
    Safely write to a directory with cleanup on failure.

    Parameters
    ----------
    write_func : callable
        Function that takes a directory path and writes files to it
    target_dir : str
        Target directory path
    suffix : str
        Suffix for temporary directory
    cleanup_on_failure : bool
        If True, remove partial writes on failure

    Returns
    -------
    str
        Target directory path on success

    Examples
    --------
    >>> def write_partitions(dir_path):
    ...     for i, df in enumerate(partitions):
    ...         df.to_parquet(os.path.join(dir_path, f'part_{i}.parquet'))
    ...
    >>> safe_directory_write(write_partitions, '/path/to/output/')
    """
    import shutil

    temp_dir = target_dir.rstrip('/') + suffix
    os.makedirs(temp_dir, exist_ok=True)

    try:
        write_func(temp_dir)

        # Success - replace target directory
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        os.rename(temp_dir, target_dir)
        return target_dir

    except Exception:
        if cleanup_on_failure and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def verify_file_integrity(file_path: str, file_type: str = None) -> bool:
    """
    Verify that a file is readable and not corrupted.

    Parameters
    ----------
    file_path : str
        Path to file to verify
    file_type : str, optional
        File type hint ('h5', 'parquet', 'json'). Auto-detected if not provided.

    Returns
    -------
    bool
        True if file is valid, False otherwise
    """
    if not os.path.exists(file_path):
        return False

    if file_type is None:
        ext = os.path.splitext(file_path)[1].lower()
        file_type = {
            '.h5': 'h5',
            '.hdf5': 'h5',
            '.parquet': 'parquet',
            '.parq': 'parquet',
            '.pq': 'parquet',
            '.json': 'json',
        }.get(ext)

    try:
        if file_type == 'h5':
            return h5_is_valid(file_path)
        elif file_type == 'parquet':
            import pyarrow.parquet as pq
            pq.read_metadata(file_path)
            return True
        elif file_type == 'json':
            json_read(file_path)
            return True
        else:
            # Generic check - just ensure file is non-empty
            return os.path.getsize(file_path) > 0
    except Exception:
        return False