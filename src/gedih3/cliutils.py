import os
import re
import logging
import warnings
from typing import Optional, List

from .config import GEDI_PRODUCTS, ISO3_COUNTRIES_URL
from .utils import read_vector_file, parse_spatial
from .gedidriver import gedi_vars_expand
# Note: gh3driver imports are done lazily to avoid circular imports

VALID_FORMATS = ['parquet', 'feather', 'shp', 'geojson', 'gpkg', 'txt', 'csv', 'h5', 'hdf5']

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
            logging.getLogger(logger_name).setLevel(logging.ERROR)

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
            **meta
        }
    else:
        return {
            'source_type': 'parquet_directory',
            'index_type': None,
            'index_level': None,
            'partition_level': None
        }


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
        if logger:
            logger.info("  Source: simplified dataset")
        ddf = gh3.gh3_load_dataset_lazy(database, columns=columns)
        if query:
            ddf = ddf.query(query)
        if region is not None:
            ddf = ddf.clip(region)
    else:
        if logger:
            logger.info("  Source: parquet directory")
        ddf = gh3.gh3_load_dataset_lazy(database, columns=columns)
        if query:
            ddf = ddf.query(query)
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


def h3_col_name(level):
    """Get H3 column name for a given resolution level.

    Args:
        level: H3 resolution level (0-15)

    Returns:
        Column name string, e.g. 'h3_06' for level 6
    """
    return f'h3_{level:02d}'

def parse_file_format(args, default='parquet'):
    file_format = args.output.split('.')[-1].lower() if args.output else None    
    
    if file_format in VALID_FORMATS:
        fmt = file_format
    else:    
        fmt = args.format.lower() if args.format else default
    
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Invalid file format: {fmt}. Supported formats are: {', '.join(VALID_FORMATS)}")
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
            raise ValueError(f"Dask config file not found: {args.dask_config}")

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
            raise ValueError(f"Error reading vector file from URL: {e}")        

    # Try as bounding box: "W,S,E,N"
    if ',' in region_str:
        try:
            coords = [float(x.strip()) for x in region_str.split(',')]
            if len(coords) == 4:
                return parse_spatial(coords)
            else:
                raise ValueError(f"Invalid bounding box format: {region_str}")
        except ValueError:
            raise ValueError(f"Invalid bounding box format: {region_str}")

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
                raise ValueError(f"ISO3 code not found: {iso3}")
        except Exception:
            raise ValueError(f"Invalid ISO3 code: {iso3}")

    raise ValueError(f"Invalid region specification: {region_str}")

def collect_columns(args):
    """
    Collect all requested variables from command line arguments and validate against available columns.
    Returns: (column_list, product_map)
    """
    from .gh3driver import gh3_read_meta
    h3_columns = gh3_read_meta('h3_columns', gh3_root_dir=args.database)        
    read_cols = []
    
    if args.list is not None:
        if len(args.list) == 1 and os.path.isfile(args.list[0]):
            with open(args.list[0], 'r') as f:
                read_cols += list({line.strip() for line in f if line.strip() and not line.strip().startswith('#')})
        else:
            read_cols += list({v.strip() for v in args.list if v.strip()})

        missing = [v for v in read_cols if v not in h3_columns]
        if missing:
            raise ValueError(f"The following variables from --list were not found: {', '.join(missing)}")

    product_map = {i: getattr(args, i.lower()) for i in GEDI_PRODUCTS.keys() if getattr(args, i.lower()) is not None}
    prod_vars = gedi_vars_expand(product_map)

    for prod, vars in prod_vars.items(): 
        if vars is None:
            vars = [i for i in h3_columns if i.endswith(f"_{prod.lower()}")]            
        elif len(vars) == 1 and os.path.isfile(vars[0]):
            with open(vars[0], 'r') as f:
                file_vars = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            vars = file_vars
        
        for var in vars:
            if '*' in var:
                var = var.replace('*', '.*')
            if not var.endswith(f"_{prod.lower()}"):
                var = f"{var}_{prod.lower()}"
            
            h3_vars = [col for col in h3_columns if re.match(var, col)]

            if len(h3_vars) == 0:
                raise ValueError(f"Variable '{var}' from --{prod.lower()} not found in database columns")

            read_cols += h3_vars

    geo_flag = hasattr(args, 'geo') and args.geo    
    if geo_flag or args.region:
        read_cols.append('geometry')
    
    date_flag = hasattr(args, 'add_datetime') and args.add_datetime
    if date_flag or args.time_start or args.time_end:
        read_cols.append('datetime')    

    return list(set(read_cols))

def build_query_string(args):
    """Build pandas query string from arguments"""
    from .gh3driver import gh3_read_meta
    h3_columns = gh3_read_meta('h3_columns', gh3_root_dir=args.database)
    queries = []

    # Quality filter - use backticks to escape column names with special characters
    if args.quality:
        queries += [f"`{i}` == 1" for i in h3_columns if 'quality_flag' in i]

    # Temporal filters
    if args.time_start:
        queries.append(f"datetime >= '{args.time_start}'")
    if args.time_end:
        queries.append(f"datetime <= '{args.time_end}'")
        
    # Custom query
    if args.query:
        queries.append(f"({args.query})")

    return " & ".join(queries) if queries else None