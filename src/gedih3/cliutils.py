import os
from .config import GEDI_PRODUCTS, ISO3_COUNTRIES_URL
from .utils import read_vector_file
from typing import Optional

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

def parse_region(region_str: Optional[str]):
    """Parse region argument into GeoDataFrame or bbox"""
    if region_str is None:
        return None

    import geopandas as gpd
    from shapely.geometry import box
    from gedih3.utils import parse_spatial

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
            world = gpd.read_file(ISO3_COUNTRIES_URL)
            match = world[world['iso3'] == iso3]
            if not match.empty:
                return match.to_crs(4326)
            else:
                raise ValueError(f"ISO3 code not found: {iso3}")
        except Exception:
            raise ValueError(f"Invalid ISO3 code: {iso3}")

    raise ValueError(f"Invalid region specification: {region_str}")
