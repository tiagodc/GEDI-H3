import os
import re
import sys
import logging
import warnings
from typing import Optional, List
from contextlib import contextmanager

from .config import GEDI_PRODUCTS, ISO3_COUNTRIES_URL
from .utils import read_vector_file, parse_spatial
from .exceptions import GediValidationError
# Note: gh3driver imports are done lazily to avoid circular imports

VALID_FORMATS = ['parquet', 'feather', 'shp', 'geojson', 'gpkg', 'txt', 'csv', 'h5', 'hdf5']

# Formats that support downstream processing (column selection at read time, fast schema reading)
PIPELINE_FORMATS = {'parquet', 'feather', 'gpkg'}

FORMAT_EXTENSIONS = {
    'parquet': ('*.parquet',),
    'feather': ('*.feather',),
    'gpkg': ('*.gpkg',),
}

def detect_dataset_format(dataset_path):
    """Detect the file format of a simplified dataset.

    Checks gedih3_dataset.json for 'file_format' field first, then scans
    directory for known extensions. Defaults to 'parquet' for backwards compat.

    Parameters
    ----------
    dataset_path : str
        Path to the dataset directory

    Returns
    -------
    str
        Detected format ('parquet', 'feather', or 'gpkg')

    Raises
    ------
    GediValidationError
        If detected format is not in PIPELINE_FORMATS
    """
    import json
    from glob import glob

    meta_path = os.path.join(dataset_path, 'gedih3_dataset.json')
    if os.path.exists(meta_path):
        with open(meta_path, 'r') as f:
            meta = json.load(f)
        fmt = meta.get('file_format')
        if fmt:
            if fmt not in PIPELINE_FORMATS:
                raise GediValidationError(
                    f"Dataset format '{fmt}' does not support downstream processing. "
                    f"Supported pipeline formats: {', '.join(sorted(PIPELINE_FORMATS))}"
                )
            return fmt

    # Scan directory for known extensions (parquet first for backwards compat)
    for fmt, patterns in FORMAT_EXTENSIONS.items():
        for pattern in patterns:
            if glob(os.path.join(dataset_path, pattern)):
                return fmt

    return 'parquet'


def list_dataset_files(dataset_path, fmt=None):
    """List data files in a simplified dataset directory.

    Parameters
    ----------
    dataset_path : str
        Path to the dataset directory
    fmt : str, optional
        File format. If None, auto-detected via detect_dataset_format().

    Returns
    -------
    list of str
        Sorted list of file paths

    Raises
    ------
    FileNotFoundError
        If no matching files found
    """
    from glob import glob

    if fmt is None:
        fmt = detect_dataset_format(dataset_path)

    patterns = FORMAT_EXTENSIONS.get(fmt)
    if patterns is None:
        raise GediValidationError(
            f"Format '{fmt}' is not a supported pipeline format. "
            f"Supported: {', '.join(sorted(PIPELINE_FORMATS))}"
        )

    files = []
    for pattern in patterns:
        files.extend(glob(os.path.join(dataset_path, pattern)))

    if not files:
        raise FileNotFoundError(
            f"No {fmt} files found in {dataset_path}"
        )

    return sorted(files)


def read_dataset_schema(filepath, fmt):
    """Read column names and geometry flag from a dataset file without loading data.

    Parameters
    ----------
    filepath : str
        Path to a single data file
    fmt : str
        File format ('parquet', 'feather', or 'gpkg')

    Returns
    -------
    tuple
        (column_names: list[str], has_geometry: bool)
    """
    if fmt == 'parquet':
        import pyarrow.parquet as pq
        schema = pq.read_schema(filepath, memory_map=True)
        return schema.names, 'geometry' in schema.names
    elif fmt == 'feather':
        import pyarrow.feather as feather
        schema = feather.read_table(filepath, columns=[]).schema
        return schema.names, 'geometry' in schema.names
    elif fmt == 'gpkg':
        import geopandas as gpd
        gdf = gpd.read_file(filepath, rows=1)
        col_names = gdf.columns.tolist()
        has_geometry = 'geometry' in col_names
        return col_names, has_geometry
    else:
        raise GediValidationError(f"Unsupported format for schema reading: {fmt}")


def make_dataset_reader(fmt, columns=None):
    """Return a callable that reads a single file into a GeoDataFrame.

    The returned callable supports column selection at read time.

    Parameters
    ----------
    fmt : str
        File format ('parquet', 'feather', or 'gpkg')
    columns : list, optional
        Columns to load

    Returns
    -------
    callable
        f(filepath) -> GeoDataFrame
    """
    import geopandas as gpd

    if fmt == 'parquet':
        def reader(f):
            return gpd.read_parquet(f, columns=columns)
        return reader
    elif fmt == 'feather':
        def reader(f):
            return gpd.read_feather(f, columns=columns)
        return reader
    elif fmt == 'gpkg':
        def reader(f):
            kwargs = {}
            if columns:
                kwargs['columns'] = columns
            return gpd.read_file(f, **kwargs)
        return reader
    else:
        raise GediValidationError(f"Unsupported format for dataset reading: {fmt}")


# =============================================================================
# Module-level warning suppression for Dask/distributed
# Applied at import time to catch early warnings during client initialization
# =============================================================================
warnings.filterwarnings('ignore', category=UserWarning, module=r'distributed.*')
warnings.filterwarnings('ignore', category=UserWarning, module=r'dask.*')
warnings.filterwarnings('ignore', message=r'.*Sending large graph.*')
warnings.filterwarnings('ignore', message=r'.*large graph.*')
warnings.filterwarnings('ignore', message=r'.*Consider loading the data.*')


# =============================================================================
# Shared CLI Argument Builders
# =============================================================================

def add_dask_args(parser):
    """Add Dask-related arguments to an argument parser."""
    if '--help' in sys.argv or '-h' in sys.argv:
        n, m = 4, 4  # placeholder defaults for help text
    else:
        from .utils import get_system_resources
        cpus, ram, _ = get_system_resources()
        n = max(1, cpus // 4)
        m = int(max(1, ram / n))

    parser.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", type=str, default=None,
                        help="existing dask scheduler address, e.g. tcp://localhost:8786")
    parser.add_argument("--dask-config", dest="dask_config", type=str, default=None,
                        help="path to Dask YAML config file")
    parser.add_argument("-N", "--cores", dest="cores", type=int, default=n,
                        help=f"number of CPU cores to use [default = {n}]")
    parser.add_argument("-T", "--threads", dest="threads", type=int, default=1,
                        help="number of threads per CPU core [default = 1]")
    parser.add_argument("-M", "--memory", dest="memory", type=int, default=m,
                        help=f"memory limit per worker in GB [default = {m}]")
    parser.add_argument("-P", "--port", dest="port", type=int, default=8787,
                        help="port for Dask dashboard [default = 8787]")
    return parser


def add_verbosity_args(parser):
    """Add verbosity-related arguments to an argument parser."""
    parser.add_argument("-v", "--verbose", dest="verbose", action="count", default=0,
                        help="increase output verbosity (-v for INFO, -vv for DEBUG)")
    parser.add_argument("-Q", "--quiet", dest="quiet", action='store_true',
                        help="suppress all output except errors")
    return parser


def add_product_args(parser):
    """Add GEDI product variable arguments to an argument parser."""
    parser.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None,
                        help="GEDI L1B variables [space-separated list]")
    parser.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None,
                        help="GEDI L2A variables [space-separated list]")
    parser.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None,
                        help="GEDI L2B variables [space-separated list]")
    parser.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None,
                        help="GEDI L4A variables [space-separated list]")
    parser.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None,
                        help="GEDI L4C variables [space-separated list]")
    return parser


def parse_egi_levels(value):
    """
    Parse EGI argument in format 'level' or 'level:partition'.

    This function is used by CLI tools to parse EGI level arguments that
    specify both an index/aggregation level and an optional output partition level.

    EGI levels: 1 = finest (~1m), 12 = coarsest (~160km)
    Note: This is opposite to H3 where higher numbers mean finer resolution.

    Examples:
        '1' -> (1, 12)      # Level 1, partition at level 12 (default)
        '1:12' -> (1, 12)   # Explicit level:partition
        '6:10' -> (6, 10)   # Level 6, partition at level 10

    Parameters
    ----------
    value : str or None
        EGI level specification string

    Returns
    -------
    tuple or None
        (level, partition_level) tuple, or None if value is None

    Raises
    ------
    argparse.ArgumentTypeError
        If the value cannot be parsed or levels are invalid
    """
    import argparse

    if value is None:
        return None

    value = str(value)
    if ':' in value:
        parts = value.split(':')
        if len(parts) != 2:
            raise argparse.ArgumentTypeError(
                f"EGI argument must be 'level' or 'level:partition', got '{value}'"
            )
        try:
            level = int(parts[0])
            partition_level = int(parts[1])
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"EGI levels must be integers, got '{value}'"
            )
    else:
        try:
            level = int(value)
            partition_level = 12  # Default partition level (coarsest, ~160km)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"EGI level must be an integer, got '{value}'"
            )

    # Validate levels
    if not 1 <= level <= 12:
        raise argparse.ArgumentTypeError(
            f"EGI level must be 1-12, got {level}"
        )
    if not 1 <= partition_level <= 12:
        raise argparse.ArgumentTypeError(
            f"EGI partition level must be 1-12, got {partition_level}"
        )
    if partition_level < level:
        raise argparse.ArgumentTypeError(
            f"EGI partition level ({partition_level}) must be >= level ({level})"
        )

    return (level, partition_level)


# =============================================================================
# Shared CLI Setup Functions
# =============================================================================

def setup_logging(args, name=None):
    """Configure logging based on verbosity flags and return a logger.

    Also configures Dask warning suppression for non-DEBUG modes.

    Args:
        args: Parsed arguments with 'quiet' and 'verbose' attributes
        name: Logger name (defaults to calling module's __name__)

    Returns:
        Configured logger instance
    """
    import warnings
    from .logging_config import configure_logging, get_logger

    if args.quiet:
        log_level = logging.ERROR
    elif args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose >= 1:
        log_level = logging.INFO
    else:
        log_level = logging.INFO

    configure_logging(level=log_level, verbose=args.verbose >= 1)

    # Suppress Dask/distributed warnings unless in DEBUG mode
    if log_level > logging.DEBUG:
        # Filter UserWarnings from distributed (large graph warnings, etc.)
        # Use regex pattern for module matching
        warnings.filterwarnings('ignore', category=UserWarning, module=r'distributed.*')
        warnings.filterwarnings('ignore', category=UserWarning, module=r'dask.*')
        warnings.filterwarnings('ignore', message=r'.*Sending large graph.*')
        warnings.filterwarnings('ignore', message=r'.*Consider loading the data.*')
        warnings.filterwarnings('ignore', message=r'.*large graph.*')
        warnings.filterwarnings('ignore', message=r'.*PerformanceWarning.*')

        # Suppress distributed module logging (shuffle, scheduler, worker, memory, etc.)
        for logger_name in [
            'distributed',
            'distributed.shuffle',
            'distributed.shuffle._scheduler_plugin',
            'distributed.worker',
            'distributed.worker.memory',
            'distributed.client',
            'distributed.scheduler',
            'distributed.nanny',
            'distributed.utils_perf',
            'distributed.diskutils',
            'distributed.batched',
            'dask',
            'dask.array',
            'dask.dataframe',
            'tornado',
            'asyncio',
        ]:
            logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    return get_logger(name or __name__)


def print_banner(title, version=None, logger=None):
    """Print a tool banner with centered title.

    Args:
        title: Tool title string
        version: Package version (if None, imports from gedih3)
        logger: Logger to use (if None, uses print)
    """
    if version is None:
        from gedih3 import __version__ as version

    out = logger.info if logger else print
    out("")
    out("=" * 70)
    out(f" {title}".center(70))
    out(f" gedih3 v{version}".center(70))
    out("=" * 70)
    out("")


def print_success(message, logger=None):
    """Print a success message with banner formatting."""
    out = logger.info if logger else print
    out("")
    out("=" * 70)
    out(f" SUCCESS: {message}".center(70))
    out("=" * 70)
    out("")


@contextmanager
def cli_exception_handler(args, logger=None):
    """Standard exception handling context manager for CLI tools.

    Provides consistent error handling across CLI tools:
    - KeyboardInterrupt: Clean exit with message
    - Other exceptions: Print error message, optionally show traceback in verbose mode

    Args:
        args: Parsed arguments with 'verbose' attribute for traceback control
        logger: Optional logger (not currently used, reserved for future use)

    Usage:
        with cli_exception_handler(args):
            # CLI main logic here
            pass

    Example:
        def main():
            args = get_cmd_args()
            with cli_exception_handler(args):
                # Main CLI logic
                client = Client(**dask_kwargs)
                ...
    """
    try:
        yield
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nERROR: {type(e).__name__}: {e}")
        if hasattr(args, 'verbose') and args.verbose >= 2:
            import traceback
            traceback.print_exc()
        sys.exit(1)


# =============================================================================
# Shared Data Loading Functions
# =============================================================================

def configure_database_path(args, logger=None):
    """Configure database path from args or default.

    Args:
        args: Parsed arguments with 'database' attribute
        logger: Optional logger for output

    Returns:
        Database path string
    """
    from .config import GH3_DEFAULT_H3_DIR
    import gedih3.gh3driver as gh3

    if args.database:
        gh3.gh3_set_db_path(args.database)
    else:
        args.database = GH3_DEFAULT_H3_DIR

    if logger:
        logger.info(f"Database: {args.database}")

    return args.database


def get_dataset_index_info(database):
    """
    Get spatial index information from a dataset or database.

    Reads metadata to determine the index type (h3 or egi) and level.

    Parameters
    ----------
    database : str
        Path to H3 database or simplified dataset directory

    Returns
    -------
    dict
        Dictionary with keys:
        - 'source_type': 'h3_database', 'simplified_dataset', or 'parquet_directory'
        - 'index_type': 'h3' or 'egi' (or None if unknown)
        - 'index_level': int (or None if unknown)
        - 'partition_level': int (or None if not applicable)
        - Other metadata fields from the source
    """
    import json

    build_log_path = os.path.join(database, "gedih3_build_log.json")
    dataset_meta_path = os.path.join(database, "gedih3_dataset.json")

    if os.path.exists(build_log_path):
        with open(build_log_path, 'r') as f:
            meta = json.load(f)
        return {
            'source_type': 'h3_database',
            'index_type': 'h3',
            'index_level': meta.get('h3_resolution_level'),
            'partition_level': meta.get('h3_partition_level'),
            **meta
        }
    elif os.path.exists(dataset_meta_path):
        with open(dataset_meta_path, 'r') as f:
            meta = json.load(f)
        return {
            'source_type': 'simplified_dataset',
            'index_type': meta.get('index_type'),
            'index_level': meta.get('index_level'),
            'partition_level': meta.get('egi_partition_level') or meta.get('h3_partition_level'),
            'file_format': meta.get('file_format', 'parquet'),
            **meta
        }
    else:
        return {
            'source_type': 'parquet_directory',
            'index_type': None,
            'index_level': None,
            'partition_level': None
        }


def _add_query_columns(columns, query, dataset_path, fmt):
    """Add query-referenced columns to the load list for simplified datasets.

    When a query references columns not in the user's column selection,
    those columns must be loaded for filtering but excluded from output.

    Parameters
    ----------
    columns : list or None
        User-requested columns to load
    query : str or None
        Query string for filtering
    dataset_path : str
        Path to dataset directory
    fmt : str
        Dataset file format ('parquet', 'feather', 'gpkg')

    Returns
    -------
    tuple
        (load_columns, query_only_cols) where load_columns includes query
        columns and query_only_cols is the set of columns added only for
        the query (to be dropped after filtering). If no query columns
        needed, returns (columns, set()).
    """
    if not query or not columns:
        return columns, set()

    # Get available columns from the dataset schema
    files = list_dataset_files(dataset_path, fmt)
    available_cols, _ = read_dataset_schema(files[0], fmt)

    # Find columns referenced in query that are available but not requested
    q_cols = {col for col in available_cols if col in query and col not in columns}
    if not q_cols:
        return columns, set()

    load_columns = list(columns) + list(q_cols)
    return load_columns, q_cols


def load_data_from_source(database, columns=None, region=None, query=None, logger=None):
    """Load data from H3 database, simplified dataset, or parquet directory.

    Auto-detects the data source type and loads accordingly.

    Args:
        database: Path to database directory
        columns: Columns to load
        region: Spatial filter (GeoDataFrame or bbox)
        query: Query string for filtering
        logger: Optional logger for output

    Returns:
        Dask GeoDataFrame
    """
    import gedih3.gh3driver as gh3

    build_log_path = os.path.join(database, "gedih3_build_log.json")
    dataset_meta_path = os.path.join(database, "gedih3_dataset.json")

    if os.path.exists(build_log_path):
        if logger:
            logger.info("  Source: H3 database")
        ddf = gh3.gh3_load(
            columns=columns,
            region=region,
            query=query,
            gh3_dir=database
        )
    elif os.path.exists(dataset_meta_path):
        fmt = detect_dataset_format(database)
        if logger:
            logger.info(f"  Source: simplified dataset (format: {fmt})")
        load_columns, query_only_cols = _add_query_columns(columns, query, database, fmt)
        ddf = gh3.gh3_load_dataset_lazy(database, columns=load_columns)
        if query:
            ddf = ddf.query(query)
        if query_only_cols:
            keep = [c for c in ddf.columns if c not in query_only_cols]
            ddf = ddf[keep]
        if region is not None:
            ddf = ddf.clip(region)
    else:
        fmt = detect_dataset_format(database)
        if logger:
            logger.info(f"  Source: data directory (format: {fmt})")
        load_columns, query_only_cols = _add_query_columns(columns, query, database, fmt)
        ddf = gh3.gh3_load_dataset_lazy(database, columns=load_columns)
        if query:
            ddf = ddf.query(query)
        if query_only_cols:
            keep = [c for c in ddf.columns if c not in query_only_cols]
            ddf = ddf[keep]
        if region is not None:
            ddf = ddf.clip(region)

    return ddf


# =============================================================================
# Shared Data Processing Functions
# =============================================================================

# Patterns for internal/partition columns that should be excluded from data operations
INTERNAL_COLUMN_PATTERNS = [
    r'^h3_\d{2}$',       # H3 partition columns (h3_03, h3_06, etc.)
    r'^egi\d{2}$',       # EGI index columns (egi06, egi12, etc.)
    r'^_egi_[xy]$',      # Internal EGI coordinate columns
    r'^shot_number',     # Shot identifier (shot_number, shot_number_l2a, etc.)
]


def is_internal_column(col_name):
    """Check if a column name matches internal/partition column patterns.

    Internal columns include H3 partition columns (h3_XX), EGI index columns (egiXX),
    internal EGI coordinates (_egi_x, _egi_y), and shot identifiers.

    Args:
        col_name: Column name to check

    Returns:
        True if column is internal, False otherwise
    """
    return any(re.match(pattern, str(col_name)) for pattern in INTERNAL_COLUMN_PATTERNS)


def filter_data_columns(columns, exclude_geometry=True):
    """Filter out internal/partition columns from a column list.

    Args:
        columns: List of column names
        exclude_geometry: If True, also exclude 'geometry' column

    Returns:
        List of user data columns (excluding internal columns)
    """
    filtered = [col for col in columns if not is_internal_column(col)]
    if exclude_geometry:
        filtered = [col for col in filtered if col != 'geometry']
    return filtered


def get_numeric_columns(ddf, exclude_internal=True):
    """Get list of numeric columns from a Dask DataFrame.

    Args:
        ddf: Dask DataFrame
        exclude_internal: If True (default), exclude internal/partition columns

    Returns:
        List of column names with numeric dtypes
    """
    numeric = [col for col in ddf.columns if ddf[col].dtype.kind in 'biufc']
    if exclude_internal:
        numeric = filter_data_columns(numeric)
    return numeric


def get_rasterizable_columns(ddf):
    """Get columns suitable for rasterization from a Dask DataFrame.

    This is a convenience function that returns numeric columns excluding
    internal columns (h3_XX, egiXX, etc.) and geometry.

    Args:
        ddf: Dask DataFrame

    Returns:
        List of column names suitable for rasterization
    """
    return get_numeric_columns(ddf, exclude_internal=True)


def get_aggregatable_columns(df):
    """Get numeric columns suitable for aggregation from a DataFrame.

    This is a convenience function that returns numeric columns excluding
    internal/partition columns (h3_XX, egiXX, _egi_x, _egi_y, shot_number, etc.).

    This encapsulates the common pattern:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        filtered_cols = filter_data_columns(numeric_cols)

    Args:
        df: DataFrame, GeoDataFrame, or Dask DataFrame

    Returns:
        List of column names suitable for aggregation
    """
    import numpy as np

    # Handle both Dask and pandas DataFrames
    if hasattr(df, '_meta'):
        # Dask DataFrame - use _meta for column type inspection
        numeric_cols = df._meta.select_dtypes(include=[np.number]).columns.tolist()
    else:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    return filter_data_columns(numeric_cols)


def filter_raster_columns(columns, geodf):
    """Filter columns suitable for rasterization, excluding internal columns.

    Internal columns (egi indices, h3 indices, shot_number) should not be
    rasterized as bands - they're metadata, not data values.
    Also excludes the index column since it will become a column after reset_index().

    Args:
        columns: List of column names to filter, or None to auto-detect numeric columns
        geodf: GeoDataFrame to get numeric columns and index name from

    Returns:
        List of filtered column names suitable for rasterization, or None if empty
    """
    import numpy as np

    # Get the index column name to exclude (it becomes a column after reset_index)
    index_col = geodf.index.name

    if columns is not None:
        # Filter provided columns (also exclude index column)
        filtered = [c for c in columns if not is_internal_column(c)
                    and c != 'geometry' and c != index_col]
        return filtered if filtered else None
    else:
        # Auto-detect numeric columns, excluding internal ones and index column
        numeric = geodf.select_dtypes(include=[np.number]).columns.tolist()
        filtered = [c for c in numeric if not is_internal_column(c) and c != index_col]
        return filtered if filtered else None


def h3_col_name(level):
    """Get H3 column name for a given resolution level.

    Args:
        level: H3 resolution level (0-15)

    Returns:
        Column name string, e.g. 'h3_06' for level 6
    """
    return f'h3_{level:02d}'


def find_coordinate_column(columns, base_name):
    """Find a coordinate column by base name, handling product suffixes.

    In the H3 database, coordinate columns may have product suffixes
    (e.g., 'lon_lowestmode_l2a' instead of 'lon_lowestmode').

    Args:
        columns: list-like of available column names
        base_name: Base column name to search for (e.g., 'lon_lowestmode')

    Returns:
        Actual column name if found, None otherwise

    Examples:
        >>> find_coordinate_column(['lon_lowestmode_l2a', 'lat_lowestmode_l2a'], 'lon_lowestmode')
        'lon_lowestmode_l2a'
        >>> find_coordinate_column(['lon', 'lat'], 'lon')
        'lon'
    """
    columns = list(columns)

    # Exact match
    if base_name in columns:
        return base_name

    # Find columns starting with base_name
    matches = [c for c in columns if c.startswith(base_name)]

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        # Prefer _l2a suffix since coordinates typically come from L2A product
        l2a_matches = [c for c in matches if c.endswith('_l2a')]
        return l2a_matches[0] if l2a_matches else matches[0]

    return None

def _make_percentile_func(p):
    """Create a named percentile function for use with pandas .agg().

    Parameters
    ----------
    p : int
        Percentile value (0-100), e.g. 25 for the 25th percentile.

    Returns
    -------
    callable
        A function with __name__ set to 'percentile_XX' (e.g. 'percentile_25')
        so pandas uses it for column naming in MultiIndex flattening.
    """
    import numpy as np
    frac = p / 100

    def percentile_func(x):
        return np.nanquantile(x, frac)

    percentile_func.__name__ = f'p{int(p)}'
    return percentile_func


def _expand_percentile_specs(agg):
    """Replace percentile shorthand (p25, p50, etc.) with callable functions.

    Recognizes patterns like 'p25', 'p50', 'p95' in any position within the
    aggregation spec (string, list, or dict values).
    """
    import re
    pattern = re.compile(r'^p(\d+)$')

    def expand(item):
        if isinstance(item, str):
            m = pattern.match(item)
            if m:
                return _make_percentile_func(int(m.group(1)))
        return item

    if isinstance(agg, str):
        return expand(agg)
    elif isinstance(agg, list):
        return [expand(x) for x in agg]
    elif isinstance(agg, dict):
        result = {}
        for k, v in agg.items():
            if isinstance(v, list):
                result[k] = [expand(x) for x in v]
            else:
                result[k] = expand(v)
        return result
    return agg


def parse_aggregation(agg_str):
    """Parse aggregation spec from CLI string, JSON file, or text file.

    Supports:
        - Single function: 'mean' → 'mean'
        - Percentile shorthand: 'p25', 'p50', 'p95' → named percentile callable
        - List of functions: "['mean', 'std', 'p25', 'p75']" → mixed list
        - Column-specific dict: "{'col':['mean','p50']}" → dict with callables
        - JSON file (.json): parsed as dict or list
        - Text file: one function name per line → list (single line → string)

    Percentile patterns (p25, p90, etc.) are expanded into named callable
    functions that work with pandas .agg() and produce clean column names
    like 'agbd_l4a_percentile_25'.
    """
    import ast
    import json

    agg_str = agg_str.strip()

    # Check if it's a file path
    if os.path.isfile(agg_str):
        if agg_str.endswith('.json'):
            with open(agg_str, 'r') as f:
                result = json.load(f)
            if not isinstance(result, (dict, list)):
                raise GediValidationError(
                    f"JSON aggregation file must contain a dict or list, got {type(result).__name__}"
                )
            return _expand_percentile_specs(result)
        else:
            # Plain text file: one function name per line
            with open(agg_str, 'r') as f:
                funcs = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            if len(funcs) == 0:
                raise GediValidationError(f"Aggregation file is empty: {agg_str}")
            result = funcs if len(funcs) > 1 else funcs[0]
            return _expand_percentile_specs(result)

    # Inline literal (list or dict)
    if '[' in agg_str or '{' in agg_str:
        try:
            result = ast.literal_eval(agg_str)
        except (ValueError, SyntaxError) as e:
            raise GediValidationError(
                f"Invalid aggregation spec: {agg_str}\n"
                f"  Parse error: {e}\n"
                f"  Examples: 'mean', \"['mean','std']\", \"{'col':['mean','count']}\""
            )
        return _expand_percentile_specs(result)

    # Plain function name (or single percentile like 'p50')
    return _expand_percentile_specs(agg_str)


def parse_file_format(args, default='parquet'):
    file_format = args.output.split('.')[-1].lower() if args.output else None

    if file_format in VALID_FORMATS:
        fmt = file_format
    else:
        fmt = args.format.lower() if args.format else default

    if fmt not in VALID_FORMATS:
        raise GediValidationError(f"Invalid file format: {fmt}. Supported formats are: {', '.join(VALID_FORMATS)}")
    return fmt    

def parse_gedi_args(args):
    prod_vars = {}
    for k in GEDI_PRODUCTS.keys():
        if hasattr(args, k.lower()):
            if (vars := getattr(args, k.lower())) is not None:
                prod_vars[k] = vars
    return prod_vars
    
def parse_dask_args(args):
    import dask

    # Load dask config from file if specified
    if hasattr(args, 'dask_config') and args.dask_config:
        if os.path.isfile(args.dask_config):
            dask.config.set(config=dask.config.collect([args.dask_config]))
        else:
            raise GediValidationError(f"Dask config file not found: {args.dask_config}")

    # Configure Dask to suppress performance warnings unless in DEBUG mode
    verbose = getattr(args, 'verbose', 0)
    if verbose < 2:
        # Suppress large graph warnings by raising the threshold
        dask.config.set({'distributed.admin.large-graph-warning-threshold': '500MB'})
        # Suppress other performance-related warnings
        dask.config.set({'distributed.admin.tick.limit': '1h'})

    dask_args = {}
    if args.dask_scheduler:
        dask_args['address'] = args.dask_scheduler
    else:
        dask_args['n_workers'] = args.cores
        dask_args['threads_per_worker'] = args.threads
        dask_args['memory_limit'] = f"{args.memory}GB" if args.memory else None
        dask_args['dashboard_address'] = f":{args.port}" if args.port else None
        if hasattr(args, 'tmpdir') and args.tmpdir:
            os.makedirs(args.tmpdir, exist_ok=True)
            dask_args['local_directory'] = os.path.join(args.tmpdir, 'dask-worker-space')
    return dask_args    

def parse_region(region_str: Optional[str]):
    """Parse region argument into GeoDataFrame or bbox"""
    if region_str is None:
        return None

    # Try as file path
    if os.path.isfile(region_str):
        return parse_spatial(region_str)
    
    # Try as URL
    if region_str.startswith(('http://', 'https://', 's3://')):
        try:
            return read_vector_file(region_str, crs=4326)
        except Exception as e:
            raise GediValidationError(f"Error reading vector file from URL: {e}")

    # Try as bounding box: "W,S,E,N"
    if ',' in region_str:
        from .validation import validate_bbox
        try:
            coords = [float(x.strip()) for x in region_str.split(',')]
            if len(coords) == 4:
                # Validate bbox coordinates
                validate_bbox(coords)
                return parse_spatial(coords)
            else:
                raise GediValidationError(f"Invalid bounding box format: {region_str}")
        except ValueError as e:
            # Re-raise with proper context if it's from validate_bbox
            if 'must be' in str(e):
                raise GediValidationError(f"Invalid bounding box: {e}")
            raise GediValidationError(f"Invalid bounding box format: {region_str}")

    # Try as ISO3 country code
    if len(region_str) == 3 and region_str.isalpha():
        iso3 = region_str.upper()
        try:
            import geopandas as gpd
            world = gpd.read_file(ISO3_COUNTRIES_URL)
            match = world[world['iso3'] == iso3]
            if not match.empty:
                return match.to_crs(4326)
            else:
                raise GediValidationError(f"ISO3 code not found: {iso3}")
        except Exception:
            raise GediValidationError(f"Invalid ISO3 code: {iso3}")

    raise GediValidationError(f"Invalid region specification: {region_str}")

def collect_columns(args, available_columns=None):
    """
    Collect all requested variables from command line arguments and validate against available columns.
    Returns: (column_list, product_map)
    """
    if available_columns is None:
        from .gh3driver import gh3_read_meta
        available_columns = gh3_read_meta('h3_columns', gh3_root_dir=args.database)
    read_cols = []

    if args.list is not None:
        if len(args.list) == 1 and os.path.isfile(args.list[0]):
            with open(args.list[0], 'r') as f:
                read_cols += list({line.strip() for line in f if line.strip() and not line.strip().startswith('#')})
        else:
            read_cols += list({v.strip() for v in args.list if v.strip()})

        missing = [v for v in read_cols if v not in available_columns]
        if missing:
            raise GediValidationError(f"The following variables from --list were not found: {', '.join(missing)}")

    product_map = {i: getattr(args, i.lower()) for i in GEDI_PRODUCTS.keys() if getattr(args, i.lower()) is not None}
    from .gedidriver import gedi_vars_expand
    prod_vars = gedi_vars_expand(product_map)

    for prod, vars in prod_vars.items():
        if vars is None:
            vars = [i for i in available_columns if i.endswith(f"_{prod.lower()}")]
        elif len(vars) == 1 and os.path.isfile(vars[0]):
            with open(vars[0], 'r') as f:
                file_vars = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            vars = file_vars

        for var in vars:
            if '*' in var:
                var = var.replace('*', '.*')
            if not var.endswith(f"_{prod.lower()}"):
                var = f"{var}_{prod.lower()}"

            matched_vars = [col for col in available_columns if re.match(var, col)]

            if len(matched_vars) == 0:
                raise GediValidationError(f"Variable '{var}' from --{prod.lower()} not found in database columns")

            read_cols += matched_vars

    geo_flag = hasattr(args, 'geo') and args.geo    
    if geo_flag or args.region:
        read_cols.append('geometry')
    
    date_flag = hasattr(args, 'add_datetime') and args.add_datetime
    if date_flag or args.time_start or args.time_end:
        read_cols.append('datetime')    

    return list(set(read_cols))

def build_query_string(args, available_columns=None):
    """Build pandas query string from arguments"""
    if available_columns is None:
        from .gh3driver import gh3_read_meta
        available_columns = gh3_read_meta('h3_columns', gh3_root_dir=args.database)
    queries = []

    # Quality filter - use backticks to escape column names with special characters
    if args.quality:
        queries += [f"`{i}` == 1" for i in available_columns if 'quality_flag' in i]

    # Temporal filters
    if args.time_start:
        queries.append(f"datetime >= '{args.time_start}'")
    if args.time_end:
        queries.append(f"datetime <= '{args.time_end}'")
        
    # Custom query
    if args.query:
        queries.append(f"({args.query})")

    return " & ".join(queries) if queries else None


def safe_query(df, query_str):
    """Apply a pandas query, handling column names with '/' that pandas can't parse.

    pandas.DataFrame.query() fails on backtick-quoted names containing '/'
    because it converts the slash to an unresolvable internal token.
    This function works around the limitation by temporarily renaming such columns.
    """
    if not query_str:
        return df

    slash_cols = {c: c.replace('/', '_') for c in df.columns if '/' in c}
    if not slash_cols and '/' not in query_str:
        return df.query(query_str)

    # Sanitize backtick-quoted names with '/' in the query string
    import re as _re
    safe_qstr = _re.sub(
        r'`([^`]*/[^`]*)`',
        lambda m: '`' + m.group(1).replace('/', '_') + '`',
        query_str
    )

    # Also rename any DataFrame columns with '/'
    if slash_cols:
        result = df.rename(columns=slash_cols).query(safe_qstr)
        reverse = {v: k for k, v in slash_cols.items()}
        return result.rename(columns=reverse)

    return df.query(safe_qstr)