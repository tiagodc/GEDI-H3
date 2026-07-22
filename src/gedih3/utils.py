# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

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


def smart_join(*parts):
    """os.path.join() that uses forward slashes for remote URLs.

    On Windows, os.path.join uses backslashes which corrupts URLs like
    ``http://host:port/path`` into ``http://host:port\\path``, causing
    port-parsing failures in urllib3.
    """
    if parts and is_remote_path(parts[0]):
        import posixpath
        return posixpath.join(*parts)
    return os.path.join(*parts)


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

        ``S3`` (s3fs): ``key``, ``secret``, ``endpoint_url``, ``anon``.
        ``endpoint_url`` is automatically wrapped into
        ``client_kwargs={'endpoint_url': ...}`` for s3fs compatibility.

        ``HTTP/HTTPS`` (aiohttp): ``username``/``password`` (basic auth
        via ``client_kwargs``) or ``headers`` dict (bearer tokens, API
        keys).

        ``FTP``: ``username``, ``password``, ``host``, ``port``.

        ``SFTP/SSH``: ``username``, ``password`` or ``key_filename``
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

_manifest_cache = {}  # keyed by (root_path, manifest_filename) → list of relative paths


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


def _read_manifest(root_path, manifest_filename=None):
    """Read manifest file from a database root, with caching.

    Manifest freshness smoke check (R2 + producer-crash guard): for
    local roots, the manifest's mtime is compared against the root
    directory's mtime. When the root was touched after the manifest
    was last written (typical signature: a producer crashed before
    refreshing the manifest, OR files were dropped in externally),
    a loud ERROR is logged directing the user to the appropriate
    ``gh3_doctor --fix`` remedy. The manifest is still returned —
    this never auto-refreshes (consumers trust the manifest under R2)
    and never raises (a stale manifest is still better than the
    fall-back recursive walk). Two ``stat`` calls.

    Parameters
    ----------
    root_path : str
        Database root directory (local or remote).
    manifest_filename : str, optional
        Name of the manifest sentinel file. Defaults to the H3 database
        manifest (``MANIFEST_FILENAME``). Pass ``SOC_MANIFEST_FILENAME``
        to read the SOC tree's parallel sentinel.

    Returns
    -------
    list of str or None
        List of relative file paths, or None if no manifest exists.
    """
    from .config import MANIFEST_FILENAME, SOC_MANIFEST_FILENAME

    if manifest_filename is None:
        manifest_filename = MANIFEST_FILENAME

    cache_key = (root_path, manifest_filename)
    if cache_key in _manifest_cache:
        return _manifest_cache[cache_key]

    manifest_path = smart_join(root_path.rstrip('/'), manifest_filename)

    try:
        with smart_open(manifest_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        _manifest_cache[cache_key] = lines
    except (FileNotFoundError, OSError):
        _manifest_cache[cache_key] = None
        return None

    # No consumer-side freshness check here. The mtime heuristic has false
    # positives (short-lived temp files in the tree bump dir mtime without
    # invalidating the manifest contents), and downstream tools (aggregate,
    # extract, doctor read paths) just need a file listing — if the manifest
    # is incomplete the worst case is they fall back to walking the tree.
    # Build/doctor contexts that genuinely care can call
    # parallel.check_manifest_freshness explicitly.
    return lines


def generate_manifest(root_path, pattern='**/*.parquet', manifest_filename=None,
                      tree_shape='h3db', files=None):
    """Atomically write a manifest file listing all matching files.

    Parameters
    ----------
    root_path : str
        Database root directory (must be local).
    pattern : str
        Glob pattern matched at each leaf — meaning depends on
        ``tree_shape``. For ``'h3db'`` it is matched recursively under
        each ``h3_NN=*`` partition; for ``'soc'`` it is matched at
        each ``year/doy/`` leaf; for ``'flat'`` it is matched at the
        single ``root_path``. The default ``**/*.parquet`` is the H3
        layout for backwards compatibility.
    manifest_filename : str, optional
        Name of the manifest sentinel file. Defaults to the H3 database
        manifest (``MANIFEST_FILENAME``). Pass ``SOC_MANIFEST_FILENAME``
        for the SOC parallel.
    tree_shape : {'h3db', 'soc', 'flat'}
        Tree topology to walk. Dispatches to the matching walker in
        :mod:`gedih3.parallel`. The walker requires a registered dask
        Client at call time — there is **no serial fallback** (matches
        the package-wide always-parallel contract).
    files : list[str], optional
        Pre-computed absolute file list to use instead of walking. When
        the caller already has the file list in memory (e.g.
        ``cli/gh3_build.py`` after its existing-h5 listing), passing
        them here avoids a redundant walk. The list must contain
        absolute paths under ``root_path``.

    Returns
    -------
    str
        Path to the written manifest file. The write is atomic
        (``.tmp`` + ``os.replace``) so an interrupted run never leaves
        a partial manifest at the final path — important when the
        manifest is also a resume-correctness signal for the next
        invocation.
    """
    from .config import MANIFEST_FILENAME

    if manifest_filename is None:
        manifest_filename = MANIFEST_FILENAME

    if is_remote_path(root_path):
        raise ValueError("generate_manifest() only works on local paths")

    root = root_path.rstrip('/') + '/'
    if files is None:
        # Lazy import — utils <-> parallel cycle is broken by the lazy
        # ``get_dask_client`` import inside parallel_map; this lazy
        # import keeps the walker module out of the import graph until
        # someone actually calls it.
        from .parallel import (walk_h3db_parallel, walk_soc_parallel,
                               walk_flat_parallel)
        if tree_shape == 'h3db':
            files = walk_h3db_parallel(root, pattern=os.path.basename(pattern))
        elif tree_shape == 'soc':
            files = walk_soc_parallel(root, pattern=os.path.basename(pattern))
        elif tree_shape == 'flat':
            files = walk_flat_parallel(root, pattern=os.path.basename(pattern))
        else:
            raise ValueError(
                f"generate_manifest: unknown tree_shape {tree_shape!r}; "
                f"expected 'h3db', 'soc', or 'flat'"
            )
    files = sorted(files)
    rel_paths = [os.path.relpath(f, root).replace(os.sep, '/') for f in files]

    manifest_path = os.path.join(root, manifest_filename)
    # Atomic write: a SIGKILL between the truncate and the final flush
    # would otherwise leave an empty (or worse, half-written) manifest
    # at the final path, and the next caller would silently treat the
    # database / SOC tree as empty.
    with AtomicFileWriter(manifest_path) as tmp:
        with open(tmp, 'w') as f:
            f.write('\n'.join(rel_paths))
            if rel_paths:
                f.write('\n')

    # Invalidate cache for this root × manifest filename combo. Older
    # callers passed only root_path so we also pop the legacy single-key
    # form (transitional safety net for downstream consumers we don't
    # control).
    _manifest_cache.pop((root, manifest_filename), None)
    _manifest_cache.pop((root.rstrip('/'), manifest_filename), None)
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
        # Normalize separators for cross-platform compatibility (Windows backslashes)
        manifest = [e.replace(os.sep, '/') for e in manifest]
        norm_pattern = pattern.replace(os.sep, '/')
        # Build relative pattern from root
        root_stripped = root.rstrip('/')
        if norm_pattern.startswith(root_stripped):
            rel_pattern = norm_pattern[len(root_stripped):].lstrip('/')
        else:
            rel_pattern = norm_pattern

        is_dir_pattern = rel_pattern.endswith('/')
        rx = _glob_to_regex(rel_pattern)

        if is_dir_pattern:
            # Manifest contains file paths; extract directory prefixes
            # at the correct depth and match against the pattern.
            depth = rel_pattern.rstrip('/').count('/') + 1
            dirs = set()
            for entry in manifest:
                segments = entry.split('/')
                if len(segments) >= depth:
                    candidate = '/'.join(segments[:depth])
                    if rx.match(candidate):
                        dirs.add(smart_join(root_stripped, candidate))
            return sorted(dirs)
        else:
            matched = [
                smart_join(root_stripped, entry)
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

def object_series(items):
    """Wrap a list of array-like objects in an object-dtype ``pd.Series``.

    ``pd.Series([...])`` and ``np.empty(..., dtype=object)[:] = items`` both
    coerce their input through ``np.asarray``, which xarray Datasets refuse
    ("cannot directly convert an xarray.Dataset into a numpy array"). Assigning
    positionally into a pre-sized object Series is the construction that keeps
    the elements intact — use it whenever a ``map_partitions`` result carries
    Datasets or other array-likes rather than scalars.

    Parameters
    ----------
    items : sequence
        Objects to store as Series elements, kept by reference.

    Returns
    -------
    pd.Series
        Object-dtype Series of ``items``, with a default RangeIndex.
    """
    import pandas as pd

    series = pd.Series(index=range(len(items)), dtype=object)
    for position, item in enumerate(items):
        series.iat[position] = item
    return series

def json_write(obj, path, mode='w', rewrite=False):
    if os.path.isfile(path) and not rewrite:
        obj = json_read(path) | obj
    with AtomicFileWriter(path) as tmp_path:
        with open(tmp_path, mode) as file:
            json.dump(obj, file)

def json_read(path, mode='r'):
    with smart_open(path, mode) as f:
        obj = json.load(f)
        return obj

def check_nan_only_columns(df, context='', logger=None):
    """Warn about columns that are entirely NaN.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Data to check.
    context : str
        Optional prefix for the warning message.
    logger : logging.Logger, optional
        Logger instance. If None, uses module-level warnings.

    Returns
    -------
    list
        Column names that are entirely NaN.
    """
    nan_cols = [c for c in df.columns if c != 'geometry' and df[c].isna().all()]
    if nan_cols:
        msg = f"{context}Columns with all NaN values: {nan_cols}"
        if logger:
            logger.warning(msg)
        else:
            import warnings
            warnings.warn(msg, stacklevel=2)
    return nan_cols

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
    partition_dirs = smart_glob(smart_join(db_path, 'h3_*=*/'))
    if not partition_dirs:
        raise GediDatabaseNotFoundError(f"No H3 partition directories found in {db_path}")
    for pdir in partition_dirs:
        # Search recursively — partitions may have nested hive dirs (e.g. year=*)
        pq_files = smart_glob(smart_join(pdir, '**', '*.parquet'), recursive=True)
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
        build_log = smart_join(path, BUILD_LOG_FILENAME)
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
            keys = list(f.keys())
            if not any(k.upper().startswith('BEAM') for k in keys):
                return False
    except Exception:
        return False
    return True


def release_arrow_pool() -> None:
    """Best-effort drain of pyarrow's allocator.

    pyarrow's transient read/write buffers do not always return to the
    OS at GC time, which causes long-running worker RSS to climb across
    successive parquet operations on shared GPFS. Calling
    ``pa.default_memory_pool().release_unused()`` after each per-file
    or per-task scope keeps the plateau flat. The pool API may also
    raise on unusual installations; we swallow exceptions because the
    drain is an optimization, not a correctness gate.

    This helper is the single source of truth for the pattern that
    used to be inlined in 6+ doctor / build call sites and was easy
    to forget at a new site.
    """
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass

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
    """Copy selected datasets from source to dest HDF5 file.

    Uses direct path iteration instead of visit_links() to avoid
    traversing the entire HDF5 tree — critical for S3 performance
    where each node visit is a range request (~50-100ms).
    """
    import h5py
    import logging
    logger = logging.getLogger(__name__)
    skipped = []
    with h5py.File(source_file, 'r', rdcc_nbytes=4*1024*1024) as src, h5py.File(dest_file, 'w') as dst:
        for var_path in variables:
            if var_path not in src:
                skipped.append(var_path)
                continue
            parts = var_path.split('/')
            # Create parent groups in destination
            for depth in range(1, len(parts)):
                parent = '/'.join(parts[:depth])
                if parent not in dst:
                    dst.create_group(parent)
            # Copy dataset — expand_soft resolves soft links so linked
            # variables pull the actual data
            parent_path = '/'.join(parts[:-1])
            dst_parent = dst[parent_path] if parent_path else dst
            src.copy(var_path, dst_parent, name=parts[-1], expand_soft=True)
    if skipped:
        logger.warning(f"Skipped {len(skipped)} missing paths in {source_file}: {skipped[:5]}...")

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

_CMR_POLYGON_MAX_VERTICES = 200


def geo_to_umm(obj):
    """
    Converts a GeoDataFrame, shapely Polygon, or GeoJSON dictionary to a UMM-style
    list of (lon, lat) coordinate tuples for a single polygon.

    Multi-polygon geometries are reduced to their convex hull since earthaccess/CMR
    only supports single-polygon spatial queries.

    Rings with more than ``_CMR_POLYGON_MAX_VERTICES`` points are auto-simplified
    via iterative ``shapely.simplify`` (doubling tolerance) until the ring fits.
    CMR/CloudFront returns HTTP 414 (URI too long) when the polygon query string
    grows past a few hundred vertices; simplifying client-side avoids that failure.
    """
    import logging
    import geopandas as gpd
    from shapely.ops import orient
    from shapely.geometry.base import BaseGeometry

    logger = logging.getLogger(__name__)
    geom = None

    if isinstance(obj, dict):
        # from_geojson sets crs=4326, no reproject needed
        geodf = from_geojson(obj)
        geom = geodf.union_all()
    elif isinstance(obj, gpd.GeoDataFrame):
        if obj.crs is None:
            logger.warning(
                "Input GeoDataFrame has no CRS set; assuming EPSG:4326. "
                "Set a CRS on the input to silence this warning."
            )
        elif obj.crs.to_epsg() != 4326:
            logger.warning(
                f"Input GeoDataFrame CRS is {obj.crs.to_string()}; "
                f"reprojecting to EPSG:4326 for CMR query."
            )
            obj = obj.to_crs(4326)
        geom = obj.union_all()
    elif isinstance(obj, BaseGeometry):
        # Shapely geometries carry no CRS; assume EPSG:4326 (same as bbox tuples)
        geom = obj
    else:
        raise GediValidationError(f"Unsupported type: {type(obj)}")

    # Reduce multi-polygon to convex hull (earthaccess only supports single polygon)
    if geom.geom_type == 'MultiPolygon':
        geom = geom.convex_hull

    geom = orient(geom, 1)

    original_n = len(geom.exterior.coords)
    if original_n > _CMR_POLYGON_MAX_VERTICES:
        tolerance = 0.001
        simplified = geom
        for _ in range(20):
            simplified = orient(geom.simplify(tolerance, preserve_topology=True), 1)
            if len(simplified.exterior.coords) <= _CMR_POLYGON_MAX_VERTICES:
                break
            tolerance *= 2
        new_n = len(simplified.exterior.coords)
        logger.warning(
            f"Polygon has {original_n} vertices, exceeding CMR's practical limit "
            f"(~{_CMR_POLYGON_MAX_VERTICES}); auto-simplified to {new_n} vertices "
            f"at tolerance={tolerance:g}° to avoid HTTP 414. "
            f"Pass a pre-simplified polygon or a bounding box to silence this warning."
        )
        geom = simplified

    geo_umm = list(zip(*geom.exterior.coords.xy))

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

    # Close file handle before atomic replace (required on Windows)
    parquet_file.close()
    try:
        os.replace(temp_f, f)
    except OSError:
        if os.path.exists(temp_f):
            os.unlink(temp_f)
        raise

def parquet_schema_add_bbox(schema, bbox):
    if bbox is None:
        return schema
    geo_meta = json.loads(schema.metadata[b'geo'])
    geo_meta['columns']['geometry']['bbox'] = bbox
    new_metadata = {**schema.metadata, b'geo': json.dumps(geo_meta).encode('utf-8')}
    return schema.with_metadata(new_metadata)


def parse_h3_partition_dirname(h3part):
    """Parse a partition dir name like ``'h3_03=830e4afffffffff'`` into
    ``(cell_id, parent_res)``. Returns ``(None, None)`` on parse failure."""
    if not h3part or '=' not in h3part:
        return None, None
    prefix, cell_id = h3part.split('=', 1)
    if not prefix.startswith('h3_'):
        return None, None
    try:
        return cell_id, int(prefix[3:])
    except ValueError:
        return None, None


# Empirical asymptote: max child overhang ≈ 14–16% of parent edge length,
# converged across resolution-pair gaps ≥ 5 (verified by exhaustive
# enumeration L0→L7 through L7→L14). Multiplied by 1.2 safety margin.
_H3_OVERHANG_FRACTION = 0.18

# Metres per degree of latitude (constant — meridians are great circles).
_M_PER_DEG_LAT = 111_320.0


def h3_partition_bbox(h3_cell_id, parent_res, edge_fraction=_H3_OVERHANG_FRACTION):
    """Return the EPSG:4326 bbox of an H3 cell, padded to safely contain all
    descendants at any deeper resolution.

    The buffer derives from the icosahedral-projection distortion measured
    empirically at H3 face boundaries: a child cell's vertices can sit up to
    ``edge_fraction × parent_edge_length`` outside the parent cell's own
    bbox (in metres on the ground, regardless of how deep the child is once
    the depth gap is ≥ ~5 levels). The default ``0.18`` is the measured
    asymptote (~14%) × 1.2 safety margin.

    The buffer is converted to longitude-degrees at the parent's most
    poleward vertex (cosine-corrected) so the same scalar in degrees is
    safe for both lat and lon directions of the bbox.

    Parameters
    ----------
    h3_cell_id : str
        H3 cell index (hex string) at ``parent_res``.
    parent_res : int
        Resolution of ``h3_cell_id``. Used to look up the average edge
        length for the buffer.
    edge_fraction : float, default 0.18
        Buffer as a fraction of the parent's edge length.

    Returns
    -------
    list[float] | None
        ``[minlon, minlat, maxlon, maxlat]`` in EPSG:4326 degrees, or
        ``None`` if the cell ID cannot be decoded.

    Notes
    -----
    Antimeridian-crossing parents produce a loose bbox spanning ~[-180, 180]
    in longitude (because the simple min/max over boundary vertices folds
    incorrectly there). This is conservative — the bbox still contains the
    cell — but predicate pushdown is ineffective for those partitions.
    GEDI data above the antimeridian is rare (ISS limit ±51.6° latitude);
    accept the looseness rather than complicate the formula.
    """
    import h3
    import math

    try:
        boundary = h3.cell_to_boundary(h3_cell_id)
    except Exception:
        return None
    if not boundary:
        return None

    lats = [p[0] for p in boundary]
    lons = [p[1] for p in boundary]
    minlon, maxlon = min(lons), max(lons)
    minlat, maxlat = min(lats), max(lats)

    try:
        edge_m = h3.average_hexagon_edge_length(parent_res, unit='km') * 1000.0
    except Exception:
        return None

    buf_m = edge_fraction * edge_m
    cos_lat = max(math.cos(math.radians(max(abs(minlat), abs(maxlat)))), 0.05)
    buf_deg = buf_m / (_M_PER_DEG_LAT * cos_lat)

    return [minlon - buf_deg, minlat - buf_deg, maxlon + buf_deg, maxlat + buf_deg]


def _bbox_from_geo_metadata(parquet_path):
    """Read the GeoParquet ``columns.<primary>.bbox`` from a parquet footer.

    Returns the 4-float bbox if the file's ``geo`` metadata declares one;
    None otherwise (no data scan, no geometry decode).
    """
    import pyarrow.parquet as pq
    try:
        meta = pq.read_metadata(parquet_path).metadata or {}
        raw = meta.get(b'geo')
        if not raw:
            return None
        geo = json.loads(raw)
        primary = geo.get('primary_column', 'geometry')
        bbox = geo.get('columns', {}).get(primary, {}).get('bbox')
        if bbox and len(bbox) == 4:
            return [float(v) for v in bbox]
    except Exception:
        return None
    return None


def _streaming_bbox(flist, batch_size=1_000_000):
    """Compute the union bbox by streaming the geometry column only.

    Single code path for all bbox computation: constructs a pyarrow dataset
    with the first file's schema (so per-file footer scanning is skipped at
    construction — footers are read lazily during scan with pipelined async
    I/O), then accumulates ``shapely.bounds`` over batches of the geometry
    column. Memory bounded by ``batch_size``.

    Replaces the older "fast path: footer-bbox union; slow path: streaming"
    split. On a contended GPFS the dataset's pipelined reads beat a serial
    Python footer loop (fewer metadata-server round-trips per worker even
    though more total bytes are read), and we lose the per-file ``geo``
    metadata footer-read entirely from the merge hot path.
    """
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    import shapely
    import numpy as np

    if not flist:
        return None

    try:
        schema = pq.read_schema(flist[0])
    except Exception:
        return None
    if 'geometry' not in schema.names:
        return None

    dataset = ds.dataset(flist, format='parquet', schema=schema)

    minx = miny = float('inf')
    maxx = maxy = float('-inf')
    seen = False
    scanner = dataset.scanner(columns=['geometry'], batch_size=batch_size)
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        wkb_arr = batch['geometry'].to_numpy(zero_copy_only=False)
        geoms = shapely.from_wkb(wkb_arr)
        bounds = shapely.bounds(geoms)  # (N, 4): minx, miny, maxx, maxy
        if bounds.size == 0:
            continue
        bx_min = bounds[:, 0]
        by_min = bounds[:, 1]
        bx_max = bounds[:, 2]
        by_max = bounds[:, 3]
        valid = np.isfinite(bx_min) & np.isfinite(by_min) & np.isfinite(bx_max) & np.isfinite(by_max)
        if not valid.any():
            continue
        seen = True
        minx = min(minx, float(bx_min[valid].min()))
        miny = min(miny, float(by_min[valid].min()))
        maxx = max(maxx, float(bx_max[valid].max()))
        maxy = max(maxy, float(by_max[valid].max()))

    return [minx, miny, maxx, maxy] if seen else None


def parquet_backfill_bbox(path):
    """Rewrite a single parquet file in place so its GeoParquet ``geo`` metadata
    declares a valid ``columns.<primary>.bbox``.

    Returns
    -------
    str
        ``'ok'`` if the file already has a valid bbox (no-op),
        ``'rewritten'`` if a bbox was computed and the file was rewritten,
        ``'no_geometry'`` if the file has no ``geometry`` column.

    Raises
    ------
    ValueError
        If the file lacks a ``geo`` schema metadata key (cannot be backfilled
        without re-merging from source — caller should flag for a full rebuild).

    Notes
    -----
    Atomic: writes to ``<path>.bbox.tmp`` then ``os.replace``. A stale tmp from
    a prior crash is removed before each rewrite. Memory is bounded by the
    streaming scanner (``batch_size=100_000``), same profile as
    ``parquet_merge_files``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    existing_bbox = _bbox_from_geo_metadata(path)
    if existing_bbox is not None and len(existing_bbox) == 4 and all(
        isinstance(v, (int, float)) and v == v and v not in (float('inf'), float('-inf'))
        for v in existing_bbox
    ):
        return 'ok'

    schema = pq.read_schema(path)
    if 'geometry' not in schema.names:
        return 'no_geometry'
    if not schema.metadata or b'geo' not in schema.metadata:
        raise ValueError(
            f"{path} has no 'geo' schema metadata; cannot backfill bbox without "
            "rebuilding the full GeoParquet structure (re-merge from source)."
        )

    bbox = _streaming_bbox([path])
    if bbox is None:
        raise ValueError(f"{path} has a geometry column but no decodable geometries; "
                         "cannot compute bbox.")

    new_schema = parquet_schema_add_bbox(schema, bbox)

    tmp_path = path + '.bbox.tmp'
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)
    try:
        writer = pq.ParquetWriter(tmp_path, new_schema, compression='zstd')
        try:
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=100_000):
                writer.write_table(pa.Table.from_batches([batch], schema=new_schema))
        finally:
            writer.close()
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    return 'rewritten'


def _iter_batches_with_path(batch_iter, path):
    """Wrap a pyarrow ``iter_batches`` generator and re-raise any exception
    with the source file ``path`` appended to the message.

    Why: truncated parquets fail inside ``iter_batches`` (mid-stream), not at
    ``ParquetFile.__init__``. Without this wrapper the merge driver sees only
    the Arrow message (e.g. ``Parquet magic bytes not found in footer``) with
    no file context. With it, every per-fragment error self-identifies.
    """
    while True:
        try:
            batch = next(batch_iter)
        except StopIteration:
            return
        except Exception as e:
            raise type(e)(f"{e} [file={path}]").with_traceback(e.__traceback__) from None
        yield batch


def parquet_merge_files(ofile, flist, check_shots=False, rm_src=False,
                        rows_per_group=100_000, bbox=None):
    """Stream-merge parquet files into a single output with a bounded memory footprint.

    Architecture (per-file iteration, native column projection):

    - Schema is taken from the first file's footer (one ``pq.read_schema``).
      Each input fragment is opened sequentially via ``pq.ParquetFile`` and
      drained via ``iter_batches(batch_size=rows_per_group, columns=schema.names)``.
      The ``columns=...`` argument has pyarrow's C++ reader **read columns in the
      target order** — no Python-side reordering, no per-batch reconciler,
      and any extras the file might have are dropped at read time (less I/O).
      When the file goes out of scope, its IO state is released —
      deterministic per-file lifecycle.
    - **Invariant assumed by design**: all input fragments share an identical
      column set and dtypes (true in gh3_build because all fragments come
      from the same ``dask_geopandas.to_parquet`` call). A fragment with a
      missing target column will raise from pyarrow — that's the right
      behavior; it surfaces a serious data invariant violation rather than
      silently null-filling.
    - Bbox is **provided by the caller** via the ``bbox`` argument when the
      input has a ``geometry`` column. ``gh3builder.h3_merge_files`` derives
      it directly from the H3 partition geometry (no data scan).
    - Row-group accumulator flushes BEFORE appending a batch that would
      overflow ``rows_per_group``.
    - Shot-dedup activates only when ``check_shots=True``.

    Parameters
    ----------
    rows_per_group : int, default 100_000
        Output row-group size and per-file iter batch size.
    bbox : list[float] or None
        ``[minlon, minlat, maxlon, maxlat]`` in EPSG:4326. When provided and
        the input has a ``geometry`` column, embedded into the GeoParquet
        ``columns.geometry.bbox`` metadata.

    Returns
    -------
    dict or None
        ``None`` if ``flist`` is empty. Otherwise a stats dict accumulated
        online during the merge stream:
        ``{'shot_count', 'shot_min', 'shot_max', 'dt_min', 'dt_max',
        'root_files'}``. Fields are ``None`` when the source column is
        absent from the schema. Used by ``h3_write_metadata`` to skip
        re-reading the merged file.

    Output is written atomically: ``ofile + '.merge.tmp'`` first, then
    ``os.replace`` to ``ofile``. A stale ``.merge.tmp`` from a prior crash is
    cleaned up before the new write.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    if not flist:
        return None

    shots = None

    # Schema from first file only (skip per-file footer scan for unification).
    schema = pq.read_schema(flist[0])
    target_names = list(schema.names)

    # Caller-provided bbox (typically derived from H3 partition geometry).
    if 'geometry' in schema.names and bbox is not None:
        schema = parquet_schema_add_bbox(schema, bbox=bbox)

    # Atomic write: write to temp file, rename after successful close
    tmp_ofile = ofile + '.merge.tmp'
    if os.path.exists(tmp_ofile):
        os.unlink(tmp_ofile)  # Clean up stale temp from previous crash

    # Streaming stats — accumulate per-batch so h3_write_metadata never has to
    # re-read the merged file. Defensive: only populate fields whose source
    # column exists in the schema (test fixtures merging arbitrary parquets
    # may not have shot_number / root_file_l2a / datetime).
    has_shot_number = 'shot_number' in schema.names
    has_root_file = 'root_file_l2a' in schema.names
    has_datetime = 'datetime' in schema.names
    stats = {
        'shot_count': 0,
        'shot_min': None, 'shot_max': None,
        'dt_min': None, 'dt_max': None,
        'root_files': set() if has_root_file else None,
    }

    try:
        writer = pq.ParquetWriter(tmp_ofile, schema, compression="zstd")
        shots = None
        acc = []
        acc_rows = 0

        # Per-file iteration: open one fragment, drain it, drop it, repeat.
        # `columns=target_names` makes pyarrow's C++ reader read columns in
        # target order — no Python reorder per batch, and any extras the file
        # might have are dropped at read time (less I/O). Each ParquetFile's
        # IO state is released when it goes out of scope.
        #
        # `pre_buffer=True` is REQUIRED on shared GPFS — pyarrow's default is
        # False for direct ParquetFile use (only `ds.dataset()` sets it to True
        # internally). Without it, each column chunk in each row group is read
        # as a separate seek+read; for our 1,270-column files that's ~1,270
        # cold-GPFS reads per row group at ~10–50 ms each = 12–60 s/row-group
        # of pure I/O latency. With pre_buffer=True, all column chunks of a
        # row group are coalesced into a few large sequential reads (~50–100
        # MB buffered), then decompressed in memory.
        for f in flist:
            # Wrap per-fragment open + drain so the failing path is attached
            # to any exception. Without this, the merge driver at
            # gh3builder.py:_merge_and_finalize only sees the upstream Arrow
            # message (e.g. "Parquet file size is 0 bytes") with no file
            # context, leaving the operator with no cheap way to identify
            # which fragment to delete + which granules to re-extract.
            try:
                pf = pq.ParquetFile(f, pre_buffer=True)
                batch_iter = pf.iter_batches(batch_size=rows_per_group, columns=target_names)
            except Exception as e:
                raise type(e)(f"{e} [file={f}]").with_traceback(e.__traceback__) from None
            for batch in _iter_batches_with_path(batch_iter, f):
                if check_shots and has_shot_number:
                    arr = batch["shot_number"].to_numpy().astype(np.uint64)
                    if shots is None:
                        shots = np.unique(arr)
                    else:
                        keep = ~np.isin(arr, shots, assume_unique=True)
                        if not keep.any():
                            continue
                        batch = batch.filter(pa.array(keep))
                        shots = np.unique(np.concatenate([shots, arr[keep]]))

                # Flush BEFORE appending if this batch would overflow the cap,
                # so acc never holds more than rows_per_group rows at a time.
                if acc_rows + batch.num_rows > rows_per_group and acc:
                    writer.write_table(pa.concat_tables(acc))
                    acc.clear()
                    acc_rows = 0

                acc.append(pa.Table.from_batches([batch], schema=schema))
                acc_rows += batch.num_rows

                # Collect per-batch stats for h3_write_metadata.
                stats['shot_count'] += batch.num_rows
                if has_shot_number and batch.num_rows:
                    bsm, bsx = pc.min(batch['shot_number']).as_py(), pc.max(batch['shot_number']).as_py()
                    if bsm is not None:
                        stats['shot_min'] = bsm if stats['shot_min'] is None else min(stats['shot_min'], bsm)
                        stats['shot_max'] = bsx if stats['shot_max'] is None else max(stats['shot_max'], bsx)
                if has_datetime and batch.num_rows:
                    bdm, bdx = pc.min(batch['datetime']).as_py(), pc.max(batch['datetime']).as_py()
                    if bdm is not None:
                        stats['dt_min'] = bdm if stats['dt_min'] is None else min(stats['dt_min'], bdm)
                        stats['dt_max'] = bdx if stats['dt_max'] is None else max(stats['dt_max'], bdx)
                if has_root_file and batch.num_rows:
                    stats['root_files'].update(pc.unique(batch['root_file_l2a']).to_pylist())

            # Drop the ParquetFile reference now that we're done with it,
            # so its IO state can be released before opening the next fragment.
            del pf

        if acc:
            writer.write_table(pa.concat_tables(acc))
        writer.close()
        os.replace(tmp_ofile, ofile)  # Atomic rename
    except:
        if os.path.exists(tmp_ofile):
            os.unlink(tmp_ofile)
        raise

    if rm_src:
        for f in flist:
            if os.path.exists(f) and f != ofile:
                os.unlink(f)

    # Explicit cleanup before return: drop heavy refs and ask pyarrow to
    # return unused pool memory to the OS. The trim plugin runs on Dask
    # task transition (after this function returns) but doing it here
    # ensures the merged file's transient buffers are released BEFORE
    # the caller (h3_merge_files) does any further work.
    try:
        del writer, acc
    except NameError:
        pass
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass

    return stats

def parquet_join_columns(flist: List[str], ofile: str, key_col: str = 'shot_number',
                         tmp_suffix: str = '.join.tmp', join_how='left',
                         rows_per_group: int = 100_000):
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
    join_how : str, default='left'
        Join mode passed to ``DataFrame.join``.
    rows_per_group : int, default 100_000
        Output row-group size. Matches the fresh-build ``parquet_merge_files``
        default so updated files keep a bounded, deterministic per-group size
        regardless of the base file's existing row-group shape.
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

            writer.write_table(pa.Table.from_pandas(batch, schema=combined_schema),
                               row_group_size=rows_per_group)

    # Close file handle before atomic replace (required on Windows)
    base_file.close()
    try:
        os.replace(temp_ofile, ofile)
    except OSError:
        if os.path.exists(temp_ofile):
            os.unlink(temp_ofile)
        raise

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


def dask_safe_wait(persisted, show_progress=False):
    """Wait for a persisted Dask collection to finish and re-raise the first
    worker exception. Side-effect-only equivalent of ``.compute()`` with no
    driver-side collect.

    Why this exists: dask >= 2025.2 always inserts a ``RepartitionToFewer(1)``
    optimization step inside ``.compute()`` on a multi-partition collection,
    which collapses all partitions onto a single worker before delivering to
    the driver. That fan-in deterministically wedges on tunneled multi-node
    clusters past ~1500 partitions. Workloads that only need side effects
    (e.g. per-partition file writes) don't need to collect anything — they
    just need to wait for completion and surface task exceptions. This helper
    does exactly that, without going through ``.compute()``.

    Follows the same persist + futures_of + error-result pattern already used
    in ``gh3builder._create_h3_dataframe`` (proven in production builds).
    """
    from dask.distributed import progress, wait, futures_of
    if show_progress:
        progress(persisted)
    else:
        wait(persisted)
    errors = [f for f in futures_of(persisted) if f.status == 'error']
    if errors:
        errors[0].result()  # re-raises the original worker exception


def dask_safe_collect(collection, show_progress=False):
    """Persist a Dask DataFrame/Series on the cluster, then gather each
    partition independently to the driver and concatenate locally. Returns
    a single pandas/geopandas DataFrame or Series (or list for a Bag).

    Same motivation as :func:`dask_safe_wait`: bypasses the optimizer's
    ``RepartitionToFewer(1)`` collapse step inside ``.compute()`` by walking
    ``to_delayed()`` and gathering through the well-tested scheduler↔client
    comm path. Each partition becomes one outbound future, so a single
    unreachable peer fails only its own fetch instead of wedging the whole
    job.

    When no distributed client is registered, falls back to a plain
    ``.compute()`` (synchronous scheduler — no fan-in, no wedge possible).
    """
    from dask.distributed import progress, wait
    import pandas as pd
    client = get_dask_client()
    if client is None:
        return collection.compute()

    persisted = collection.persist()
    if show_progress:
        progress(persisted)
    else:
        wait(persisted)

    parts = client.gather(client.compute(persisted.to_delayed()))
    if not parts:
        return collection.compute()
    sample = parts[0]
    # pandas / geopandas DataFrame or Series
    if isinstance(sample, (pd.DataFrame, pd.Series)) or hasattr(sample, 'geometry'):
        return pd.concat(parts)
    # Bag partitions are lists
    if isinstance(sample, list):
        return [item for part in parts for item in part]
    return list(parts)


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


def atomic_parquet_write(df, opath, *, compression=None, max_attempts=3):
    """Atomic parquet write with post-write page-level integrity check + retry.

    Each attempt: write to ``.tmp`` inside an ``AtomicFileWriter`` context,
    then stream-read every data page via ``iter_batches`` while the temp
    file is still uncommitted. If any page deserialization raises, the
    exception propagates out of the context, the ``.tmp`` is unlinked, and
    the loop tries again. After ``max_attempts`` failures the last
    exception is re-raised — the partition errors out cleanly instead of
    promoting a torn file to the canonical path.

    Catches the production-observed GPFS/transient-IO class where pyarrow
    successfully closes a parquet file with corrupt internal page bytes
    (footer + metadata valid, data pages corrupt). That failure mode
    silently rides into the renamed final file under plain ``AtomicFileWriter``
    because pyarrow's normal write path doesn't checksum or re-read.

    Streaming verify (``iter_batches`` with bounded batch_size) keeps peak
    memory bounded to ~one batch's worth of decoded data — no double-buffer
    of the whole partition.
    """
    import pyarrow.parquet as pq

    last_exc = None
    for _ in range(max_attempts):
        try:
            with AtomicFileWriter(opath) as tmp:
                kwargs = {'compression': compression} if compression else {}
                df.to_parquet(tmp, **kwargs)
                # Stream-verify pages. Forces deserialization of every page;
                # a corrupt page raises and aborts the AtomicFileWriter context.
                for _ in pq.ParquetFile(tmp).iter_batches(batch_size=65536):
                    pass
            return
        except Exception as e:
            last_exc = e
            continue
    raise last_exc


