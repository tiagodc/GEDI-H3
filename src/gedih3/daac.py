import os
from datetime import datetime
from typing import Union, List, Dict, Optional, Tuple, Any
from functools import partial
from itertools import chain
import earthaccess
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import warnings
from pqdm.processes import pqdm
from dask.distributed import progress, get_client

# Import configuration variables
from .config import GH3_DEFAULT_DOWNLOAD_DIR, GEDI_PRODUCTS
from .utils import get_dask_client, read_vector_file, geo_to_umm
from .gedidriver import GEDIFile, soc_file_tree, gedi_subset, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5

class GEDIAccessor:
    """Main class for accessing GEDI data through various methods"""
    
    def __init__(self, authenticate: bool = True, 
                 spatial: Union[List[float], Polygon, str, gpd.GeoDataFrame] = None,
                 temporal: Union[Tuple[str, str], List[str]] = None):
        
        self.product_files = {}
        
        if spatial is not None:
            self.is_bounding_box = False
            self.spatial = self._process_spatial_filter(spatial)
        
        if temporal is not None:
            self.temporal = self._process_temporal_filter(temporal)
        
        self.authenticated = False
        if authenticate:
            self.login()
    
    def login(self, strategy: str = 'all', persist: bool = True):
        try:
            self.auth = earthaccess.login(strategy=strategy, persist=persist)
            self.authenticated = True
            print("Successfully authenticated with Earthdata Login")
        except Exception as e:
            print(f"Authentication failed: {e}")
            self.auth = None
            self.authenticated = False
    
    def search_data(self, product: str, **kwargs) -> List[Any]:
        """
        Search for GEDI granules with spatial and temporal filtering
        
        Parameters:
        -----------
        product : str
            GEDI product level ('L1B', 'L2A', 'L2B', 'L3', 'L4A', 'L4B', 'L4C')
        spatial : various
            Spatial filter - can be:
            - List of 4 floats: [west, south, east, north] bounding box
            - Shapely Polygon
            - Path to shapefile/geojson
            - GeoDataFrame
        temporal : tuple or list
            Temporal filter as (start_date, end_date) strings in 'YYYY-MM-DD' format
        **kwargs : dict
            Additional search parameters
            
        Returns:
        --------
        List of granule objects
        """
        if product.upper() not in GEDI_PRODUCTS:
            raise ValueError(f"Product must be one of: {list(GEDI_PRODUCTS.keys())}")
        
        self.product = GEDI_PRODUCTS[product.upper()]
        
        # Build search parameters
        search_params = {"doi": self.product['doi']}

        # Handle spatial filtering
        if hasattr(self, 'spatial'):
            if self.is_bounding_box:
                search_params["bounding_box"] = self.spatial
            else:
                search_params["polygon"] = self.spatial

        # Handle temporal filtering
        if hasattr(self, 'temporal'):
            search_params["temporal"] = self.temporal

        # Add any additional parameters
        search_params.update(kwargs)
        
        # Search for granules
        self.search_params = search_params
        self.granules = earthaccess.search_data(**search_params)
        self.product_files[product.upper()] = self.granules

        print(f"Found {len(self.granules)} {product} granules")
        return self.granules
    
    def _process_spatial_filter(self, spatial) -> Optional[Tuple[float, float, float, float]]:
        try:
            if (isinstance(spatial, list) or isinstance(spatial, tuple)) and len(spatial) == 4:
                self.is_bounding_box = True
                return tuple(spatial)
            
            elif isinstance(spatial, str) and os.path.exists(spatial):
                spatial = read_vector_file(spatial)
            
            return geo_to_umm(spatial)
            
        except Exception as e:
            warnings.warn(f"Could not process spatial filter: {e}")
            return None
    
    def _process_temporal_filter(self, temporal) -> Tuple[str, str]:
        if isinstance(temporal, (list, tuple)) and len(temporal) == 2:
            start_date, end_date = temporal
            
            # Convert to datetime objects if strings
            if isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                start_date = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            if isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                end_date = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

            return (start_date, end_date)
        
        raise ValueError("Temporal filter must be a tuple/list of 2 date strings")
    
    def download_all(self, download_dir: str = None, product: str = None, **kwargs):
        if not self.authenticated:
            raise RuntimeError("Must authenticate before downloading")
        
        if not hasattr(self, 'granules'):
            raise RuntimeError("No granules found. Please run search_data() first.")
       
        if download_dir is None:
            download_dir = DEFAULT_DOWNLOAD_DIR
        
        granules = self.granules if product is None else self.product_files[product.upper()]
        
        os.makedirs(download_dir, exist_ok=True)        
        downloaded_files = earthaccess.download(granules, download_dir, **kwargs)
        return downloaded_files

    def link_s3(self, product: str = None):
        if not self.authenticated:
            raise RuntimeError("Must authenticate before accessing S3")
        
        if not hasattr(self, 'granules'):
            raise RuntimeError("No granules found. Please run search_data() first.")       
        
        granules = self.granules if product is None else self.product_files[product.upper()]
        
        s3_files = earthaccess.open(granules, pqdm_kwargs={'disable': True})
        return s3_files    
    
    def merge_paths(self, open_s3: bool = False):
        if not hasattr(self, 'product_files'):
            raise RuntimeError("No products found. Please run search_data() first.")
        
        paths = self.product_files        
        if open_s3:
            paths = {prod: self.link_s3(prod) for prod in self.product_files.keys()}
        
        all_files = list(chain(*paths.values()))
        return all_files


def download_granule(granule, odir: str = None, subset_vars: List[str] = None, resume: bool = False):
    gfile = GEDIFile(granule.data_links()[0])
    odir_soc = os.path.join(odir, str(gfile.date.year), gfile.date.strftime('%j'))
    os.makedirs(odir_soc, exist_ok=True)

    expected_filename = gfile.full_name
    expected_path = os.path.join(odir_soc, expected_filename)

    if resume and os.path.exists(expected_path):
        if subset_vars is not None:
            try:
                existing_vars = set(gedi_vars_from_h5(expected_path))
                requested_vars = set(subset_vars)
                if requested_vars.issubset(existing_vars):
                    return expected_path
                else:
                    os.unlink(expected_path)
            except Exception as e:
                warnings.warn(f"Could not read existing file {os.path.basename(expected_path)}, re-downloading.")
                os.unlink(expected_path)
        else:
            try:
                _ = gedi_vars_from_h5(expected_path)
                return expected_path
            except Exception as e:
                warnings.warn(f"Could not verify existing file {os.path.basename(expected_path)}, re-downloading.")
                os.unlink(expected_path)

    opath = earthaccess.download(granule, odir_soc, threads=1, pqdm_kwargs={'disable': True})

    if len(opath) == 0:
        return

    opath = str(opath[0])

    if subset_vars is not None:
        osub = opath.replace('.h5', '_subset.h5')
        osub = gedi_subset(opath, osub, subset_vars)

        if osub is not None:
            os.unlink(opath)
            os.rename(osub, opath)

    return opath

def gedi_download(product_vars: Dict, odir: str = None, spatial = None, temporal = None, n_jobs=5, to_list=False, resume: bool = False):
    gass = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)

    prod_paths = {}
    product_vars = gedi_vars_expand(product_vars)
    
    dask_client = get_dask_client()
    if dask_client is not None:
        print(f"Using Dask client: {dask_client.dashboard_link}")
    else:
        print(f"No Dask client detected, using pqdm with {n_jobs} jobs")

    try:
        for prod, vars in product_vars.items():                
            granules = gass.search_data(product=prod)

            if odir is None:
                opaths = gass.link_s3(product=prod)
            else:
                download_func = partial(download_granule, odir=odir, subset_vars=vars, resume=resume)
                if dask_client is not None:
                    futures = dask_client.map(download_func, granules)
                    progress(futures)
                    opaths = dask_client.gather(futures)
                else:
                    opaths = pqdm(granules, download_func, n_jobs=n_jobs)

            prod_paths[prod] = opaths
            
    except Exception as e:
        raise RuntimeError(f"Error downloading {prod}: {e}")

    if to_list:
        prod_paths = list(chain(*prod_paths.values()))

    return prod_paths

def _testit():
    odir = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp'
    product_vars = {'L1B': ['minimal'], 'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['*']}
    spatial = [-50.5,0.5,-50,1]
    temporal = ('2020-01-01','2020-07-01')
    # producer_granule_id
    n_jobs=10
    
    print("testing gedi_download()")    
    try:        
        d = gedi_download(product_vars, odir, spatial=spatial, temporal=temporal, n_jobs=n_jobs)
        print("Test successful")
    except Exception as e:
        print(f"Test failed: {e}")
        
    print("testing direct dask_h5_merged()")    
    try:        
        prod_vars = {'L2A': ['shot_number', 'rh'], 'L4C': ['wsci']}
        d = gedi_download(product_vars, spatial=spatial, temporal=temporal, n_jobs=n_jobs, to_list=True)
        s3_files = soc_file_tree(d, to_list=True)
        df = dask_h5_merged(s3_files, prod_vars)
        print(df.head())
        print("Test successful")
    except Exception as e:
        print(f"Test failed: {e}")    
       
if __name__ == "__main__":    
    _testit()