import os
import re
from typing import Optional

from .config import GEDI_PRODUCTS, ISO3_COUNTRIES_URL
from .utils import read_vector_file, parse_spatial
from .gedidriver import gedi_vars_expand
from .gh3driver import gh3_read_meta

VALID_FORMATS = ['parquet', 'shp', 'geojson', 'gpkg', 'txt', 'csv', 'h5', 'hdf5']

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
    dask_args = {}
    if args.dask_scheduler:
        dask_args['address'] = args.dask_scheduler
    else:
        dask_args['n_workers'] = args.cores
        dask_args['threads_per_worker'] = args.threads
        dask_args['memory_limit'] = f"{args.memory}GB" if args.memory else None
        dask_args['dashboard_address'] = f":{args.port}" if args.port else None
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
    
    h3_columns = gh3_read_meta('h3_columns', gh3_root_dir=args.database)
    queries = []

    # Quality filter
    if args.quality:
        queries += [f"{i} == 1" for i in h3_columns if 'quality_flag' in i]

    # Temporal filters
    if args.time_start:
        queries.append(f"datetime >= '{args.time_start}'")
    if args.time_end:
        queries.append(f"datetime <= '{args.time_end}'")
        
    # Custom query
    if args.query:
        queries.append(f"({args.query})")

    return " & ".join(queries) if queries else None