# Standard library imports (fast)
from datetime import datetime
import glob as _glob_mod
import os
import json
import re
from typing import Union, List, Dict, Optional, Tuple, Any

from .exceptions import (GediDatabaseNotFoundError, GediFileError, GediValidationError,
                         GediSpatialError, GediTemporalError)


# =============================================================================
# Remote Filesystem Helpers
# =============================================================================
# Thin abstraction over os/glob that transparently handles S3 and HTTP URLs.
# Local paths use the standard library (zero overhead for the common case).

def is_remote_path(path):
    """Check if path is a remote URL (S3, HTTP, FTP, etc.)."""
    return isinstance(path, str) and path.startswith(
        ('s3://', 'http://', 'https://', 'ftp://', 'sftp://', 'ssh://')
    )


_storage_options = {}  # keyed by protocol → options dict


def configure_storage(protocol='s3', **kwargs):
    """Set storage credentials for a remote protocol.

    Credentials are stored at module level and automatically used by every
    ``smart_*`` function (which all flow through ``_get_filesystem`` /
    ``smart_open``).

    Parameters
    ----------
    protocol : str
        Protocol name: ``'s3'``, ``'http'``, ``'https'``, ``'ftp'``,
        ``'sftp'``, ``'ssh'``.
    **kwargs
        Protocol-specific options passed to ``fsspec.filesystem()``.

        **S3** (s3fs): ``key``, ``secret``, ``endpoint_url``, ``anon``.
        ``endpoint_url`` is automatically wrapped into
        ``client_kwargs={'endpoint_url': ...}`` for s3fs compatibility.

        **HTTP/HTTPS** (aiohttp): ``username``/``password`` (basic auth
        via ``client_kwargs``) or ``headers`` dict (bearer tokens, API
        keys).

        **FTP**: ``username``, ``password``, ``host``, ``port``.

        **SFTP/SSH**: ``username``, ``password`` or ``key_filename``
        (path to SSH private key), ``port``.

    Examples
    --------
    >>> configure_storage('s3', endpoint_url='http://localhost:7000', anon=True)
    >>> configure_storage('http', username='user', password='pass')
    >>> configure_storage('https', headers={'Authorization': 'Bearer tok'})
    >>> configure_storage('sftp', username='user', key_filename='/path/to/id_rsa')
    """
    opts = dict(kwargs)

    # S3: wrap endpoint_url into client_kwargs for s3fs
    if protocol == 's3' and 'endpoint_url' in opts:
        ck = opts.pop('client_kwargs', {})
        ck['endpoint_url'] = opts.pop('endpoint_url')
        opts['client_kwargs'] = ck

    # HTTP/HTTPS: wrap username/password into client_kwargs for aiohttp
    if protocol in ('http', 'https'):
        user = opts.pop('username', None)
        pwd = opts.pop('password', None)
        if user and pwd:
            import aiohttp
            ck = opts.pop('client_kwargs', {})
            ck['auth'] = aiohttp.BasicAuth(user, pwd)
            opts['client_kwargs'] = ck

    _storage_options[protocol] = opts


def get_storage_options(protocol=None):
    """Return the stored options for *protocol*.

    Returns ``{'anon': True}`` for S3 when nothing has been configured
    (public-bucket default). Other protocols return ``{}``.

    Parameters
    ----------
    protocol : str or None
        Protocol name (e.g. ``'s3'``). ``None`` returns ``{}``.

    Returns
    -------
    dict
        A **copy** of the stored options (safe to mutate).
    """
    if protocol is None:
        return {}
    if protocol in _storage_options:
        return dict(_storage_options[protocol])
    # S3 default: anonymous access for public buckets
    if protocol == 's3':
        return {'anon': True}
    return {}


def _get_filesystem(path, storage_options=None):
    """Get fsspec filesystem instance for a path.

    Parameters
    ----------
    path : str
        Remote URL (e.g. 's3://bucket/key', 'http://host/path').
    storage_options : dict, optional
        Per-call overrides merged on top of the global config from
        ``configure_storage()``.
    """
    import fsspec
    protocol = path.split('://')[0]
    opts = get_storage_options(protocol)
    if storage_options:
        opts = {**opts, **storage_options}
    # Connection-based protocols need host/port from the URL
    if protocol in ('ftp', 'sftp', 'ssh') and 'host' not in opts:
        from urllib.parse import urlparse
        parsed = urlparse(path)
        opts['host'] = parsed.hostname
        if parsed.port:
            opts['port'] = parsed.port
        if parsed.username and 'username' not in opts:
            opts['username'] = parsed.username
        if parsed.password and 'password' not in opts:
            opts['password'] = parsed.password
    return fsspec.filesystem(protocol, **opts)


def smart_exists(path):
    """os.path.exists() that works with remote paths."""
    if not is_remote_path(path):
        return os.path.exists(path)
    fs = _get_filesystem(path)
    if fs.exists(path):
        return True
    # HTTP/FTP servers may require trailing slash for directories
    if not path.endswith('/'):
        return fs.exists(path + '/')
    return False


def smart_isdir(path):
    """os.path.isdir() that works with remote paths."""
    if not is_remote_path(path):
        return os.path.isdir(path)
    fs = _get_filesystem(path)
    if fs.isdir(path):
        return True
    # HTTP/FTP servers may require trailing slash for directories
    if not path.endswith('/'):
        return fs.isdir(path + '/')
    return False


# =============================================================================
# Manifest-Accelerated File Listing
# =============================================================================
# A _manifest.txt file (one relative path per line) at the database root
# eliminates expensive directory crawling for smart_glob, especially over HTTP.

_manifest_cache = {}  # keyed by root_path → list of relative paths


def _extract_glob_root(pattern):
    """Extract the directory path before the first glob wildcard.

    Parameters
    ----------
    pattern : str
        Glob pattern, e.g. "/data/db/**/*.parquet" or "http://host/db/h3_*/".

    Returns
    -------
    str
        Root path up to (but not including) the first wildcard component.
        Includes trailing separator.
    """
    # Split on protocol to handle remote paths
    if '://' in pattern:
        protocol, rest = pattern.split('://', 1)
        parts = rest.split('/')
        root_parts = []
        for p in parts:
            if '*' in p or '?' in p or '[' in p:
                break
            root_parts.append(p)
        root = protocol + '://' + '/'.join(root_parts)
    else:
        parts = pattern.replace(os.sep, '/').split('/')
        root_parts = []
        for p in parts:
            if '*' in p or '?' in p or '[' in p:
                break
            root_parts.append(p)
        root = '/'.join(root_parts)

    if not root.endswith('/'):
        root += '/'
    return root


def _glob_to_regex(pattern):
    """Convert a glob pattern to a compiled regex with proper ``**`` support.

    Unlike :func:`fnmatch.fnmatch`, this distinguishes ``*`` (single
    path segment) from ``**`` (zero or more segments), which is essential
    for patterns like ``**/*.parquet``.

    Parameters
    ----------
    pattern : str
        Glob pattern (e.g. ``**/*.parquet``, ``h3_*/data.parquet``).

    Returns
    -------
    re.Pattern
        Compiled regex that matches the full relative path.
    """
    # Trailing slash means "match a directory" — strip it for matching
    # but callers can check pattern.endswith('/') if semantics matter.
    pattern = pattern.rstrip('/')

    parts = pattern.split('/')
    regex_parts = []
    for part in parts:
        if part == '**':
            regex_parts.append('(?:.+/)?')
        else:
            # Escape regex metacharacters, then convert glob wildcards
            segment = re.escape(part)
            segment = segment.replace(r'\*', '[^/]*')
            segment = segment.replace(r'\?', '[^/]')
            regex_parts.append(segment + '/')
    # Join and strip the trailing slash from the last segment
    regex = ''.join(regex_parts).rstrip('/')
    return re.compile('^' + regex + '$')


def _read_manifest(root_path):
    """Read manifest file from a database root, with caching.

    Parameters
    ----------
    root_path : str
        Database root directory (local or remote).

    Returns
    -------
    list of str or None
        List of relative file paths, or None if no manifest exists.
    """
    from .config import MANIFEST_FILENAME

    if root_path in _manifest_cache:
        return _manifest_cache[root_path]

    manifest_path = os.path.join(root_path.rstrip('/'), MANIFEST_FILENAME)

    try:
        with smart_open(manifest_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        _manifest_cache[root_path] = lines
        return lines
    except (FileNotFoundError, OSError):
        _manifest_cache[root_path] = None
        return None


def generate_manifest(root_path, pattern='**/*.parquet'):
    """Generate a _manifest.txt file listing all data files under root_path.

    Parameters
    ----------
    root_path : str
        Database root directory (must be local).
    pattern : str
        Glob pattern relative to root_path (default: '**/*.parquet').

    Returns
    -------
    str
        Path to the written manifest file.
    """
    from .config import MANIFEST_FILENAME

    if is_remote_path(root_path):
        raise ValueError("generate_manifest() only works on local paths")

    root = root_path.rstrip('/') + '/'
    files = sorted(_glob_mod.glob(os.path.join(root, pattern), recursive=True))
    rel_paths = [os.path.relpath(f, root) for f in files]

    manifest_path = os.path.join(root, MANIFEST_FILENAME)
    with open(manifest_path, 'w') as f:
        f.write('\n'.join(rel_paths))
        if rel_paths:
            f.write('\n')

    # Invalidate cache for this root
    _manifest_cache.pop(root, None)
    _manifest_cache.pop(root.rstrip('/'), None)

    return manifest_path


def _normalize_remote_path(path):
    """Resolve ``.``/``..`` components and decode URL percent-encoding.

    Works for both ``proto://host/a/./b`` and plain ``/a/../b`` paths.

    Parameters
    ----------
    path : str
        Path to normalize.

    Returns
    -------
    str
        Normalized path with ``%XX`` decoded and ``.``/``..`` resolved.
    """
    from posixpath import normpath
    from urllib.parse import unquote

    if '://' in path:
        proto, rest = path.split('://', 1)
        return proto + '://' + normpath(unquote(rest))
    return normpath(unquote(path))


def _find_under_root(fs, root):
    """Recursively list files strictly under *root*, skipping ``../`` links.

    Python's ``http.server`` directory listings include ``../`` entries.
    fsspec's ``ls()`` faithfully returns these, so a naive ``fs.find()``
    (which calls ``walk()``) follows ``../`` back to the parent and ends
    up crawling every sibling directory.

    This function performs its own BFS, normalizing every entry returned
    by ``ls()`` and only descending into paths that are strict children
    of *root*.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        Filesystem instance.
    root : str
        Root directory (as understood by *fs*, i.e. after
        ``_strip_protocol`` for HTTP).

    Returns
    -------
    list of str
        Normalized file paths strictly under *root*.
    """
    root_norm = _normalize_remote_path(root.rstrip('/'))
    result = []
    queue = [root.rstrip('/')]
    seen = set()

    while queue:
        path = queue.pop(0)
        path_norm = _normalize_remote_path(path)
        if path_norm in seen:
            continue
        seen.add(path_norm)

        try:
            # Trailing slash required by many HTTP servers for dir listings
            ls_path = path if path.endswith('/') else path + '/'
            entries = fs.ls(ls_path, detail=True)
        except Exception:
            continue

        for entry in entries:
            name = entry['name'].rstrip('/')
            name_norm = _normalize_remote_path(name)
            # Only descend into strict children of root
            if not name_norm.startswith(root_norm + '/'):
                continue
            if name_norm in seen:
                continue
            if entry.get('type') == 'directory':
                queue.append(name)   # original form for fs.ls()
            else:
                result.append(name_norm)  # normalized for matching

    return result


def _remote_glob(fs, protocol, pattern, recursive=False):
    """Glob for remote paths with filtered recursive walk.

    For HTTP servers that include ``../`` in directory listings, uses
    :func:`_find_under_root` to avoid crawling parent/sibling directories.
    Paths are normalized (URL-decoded, ``./``/``..`` resolved) before
    matching against the glob pattern.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        Filesystem instance.
    protocol : str
        URL protocol (e.g. 'http', 's3').
    pattern : str
        Full glob pattern including protocol.
    recursive : bool
        Whether to allow '**' patterns.

    Returns
    -------
    list of str
        Sorted matching paths with protocol prefix.
    """
    root = _extract_glob_root(pattern)
    root_stripped = root.rstrip('/')

    # Extract the wildcard portion after the root
    if pattern.startswith(root_stripped):
        rel_pattern = pattern[len(root_stripped):].lstrip('/')
    else:
        return []

    if not rel_pattern:
        return []

    if '**' in rel_pattern and not recursive:
        return []

    # Normalize path for this filesystem (HTTP keeps full URL, S3 strips protocol)
    fs_root = type(fs)._strip_protocol(root_stripped)

    # Compute URL base for path reconstruction.
    # HTTP/S3: _strip_protocol keeps the full URL → url_base is empty.
    # FTP/SFTP: _strip_protocol removes host:port → url_base = 'ftp://host:port'.
    fs_root_norm = _normalize_remote_path(fs_root.rstrip('/'))
    root_norm = _normalize_remote_path(root_stripped)
    if fs_root_norm.startswith(protocol + '://'):
        url_base = ''
    else:
        idx = root_norm.find(fs_root_norm)
        url_base = root_norm[:idx] if idx > 0 else f'{protocol}://'

    # List all files recursively under root, filtering out ../  links
    try:
        all_files = _find_under_root(fs, fs_root)
    except Exception:
        return []

    # Trailing-slash patterns match directories — extract unique parent dirs
    is_dir_pattern = rel_pattern.endswith('/')

    # Compile glob pattern to regex and filter
    rx = _glob_to_regex(rel_pattern)
    # Use normalized root for prefix extraction (matches normalized file paths)
    prefix = fs_root_norm + '/'

    if is_dir_pattern:
        # Extract unique directory paths from file listing
        dirs = set()
        for f in all_files:
            f_clean = f.rstrip('/')
            if f_clean.startswith(prefix):
                rel = f_clean[len(prefix):]
            else:
                rel = f_clean
            # Extract all ancestor directories from the relative path
            parts = rel.split('/')
            for depth in range(1, len(parts)):
                dirs.add('/'.join(parts[:depth]))

        results = []
        for d in dirs:
            if rx.match(d):
                results.append(f"{url_base}{prefix}{d}/")
        return sorted(results)

    results = []
    for f in all_files:
        f_clean = f.rstrip('/')
        if f_clean.startswith(prefix):
            rel = f_clean[len(prefix):]
        else:
            rel = f_clean
        if rx.match(rel):
            results.append(f"{url_base}{f_clean}")

    return sorted(results)


def smart_glob(pattern, recursive=False):
    """glob.glob() that works with remote paths.

    Uses a _manifest.txt file at the glob root when available, filtering
    entries by pattern.  Falls back to filesystem globbing when no
    manifest exists.

    For remote paths, uses fs.find() to list all files under the root,
    then filters with a glob-to-regex matcher.  Results include the full
    protocol prefix.
    """
    # Try manifest-accelerated path first
    root = _extract_glob_root(pattern)
    manifest = _read_manifest(root)
    if manifest is not None:
        # Build relative pattern from root
        root_stripped = root.rstrip('/')
        if pattern.startswith(root_stripped):
            rel_pattern = pattern[len(root_stripped):].lstrip('/')
        else:
            rel_pattern = pattern

        rx = _glob_to_regex(rel_pattern)
        matched = [
            os.path.join(root_stripped, entry)
            for entry in manifest
            if rx.match(entry)
        ]
        return sorted(matched)

    # No manifest — fall back to filesystem globbing
    if not is_remote_path(pattern):
        return sorted(_glob_mod.glob(pattern, recursive=recursive))

    fs = _get_filesystem(pattern)
    protocol = pattern.split('://')[0]
    return _remote_glob(fs, protocol, pattern, recursive=recursive)


def smart_open(path, mode='r', storage_options=None):
    """open() that works with remote paths. Use as context manager.

    Parameters
    ----------
    path : str
        Local or remote file path.
    mode : str
        File mode (default ``'r'``).
    storage_options : dict, optional
        Per-call overrides merged on top of the global config.
    """
    if not is_remote_path(path):
        return open(path, mode)
    import fsspec
    protocol = path.split('://')[0]
    opts = get_storage_options(protocol)
    if storage_options:
        opts = {**opts, **storage_options}
    return fsspec.open(path, mode, **opts)

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
    with smart_open(path, mode) as f:
        obj = json.load(f)
        return obj

def is_parquet(file: str) -> bool:
    return file.lower().endswith(('.parquet','.parq','.pq'))

def is_hive_directory(dir_path: str, match_str=r'.+=.+') -> bool:
    if not smart_isdir(dir_path):
        return False
    if is_remote_path(dir_path):
        fs = _get_filesystem(dir_path)
        entries = fs.ls(dir_path if dir_path.endswith('/') else dir_path + '/', detail=True)
        subdirs = [
            e['name'].rstrip('/').rsplit('/', 1)[-1]
            for e in entries if e.get('type') == 'directory'
        ]
    else:
        subdirs = os.listdir(dir_path)
        subdirs = [d for d in subdirs if os.path.isdir(os.path.join(dir_path, d))]
    if match_str is not None:
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
    if is_remote_path(path):
        with smart_open(path, 'rb') as fobj:
            schema = pq.read_schema(fobj)
    else:
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
    partition_dirs = smart_glob(os.path.join(db_path, 'h3_*=*/'))
    if not partition_dirs:
        raise GediDatabaseNotFoundError(f"No H3 partition directories found in {db_path}")
    for pdir in partition_dirs:
        # Search recursively — partitions may have nested hive dirs (e.g. year=*)
        pq_files = smart_glob(os.path.join(pdir, '**', '*.parquet'), recursive=True)
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
    if smart_isdir(path):
        # Check for H3 database first (has build log)
        from .config import BUILD_LOG_FILENAME
        build_log = os.path.join(path, BUILD_LOG_FILENAME)
        if smart_exists(build_log):
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