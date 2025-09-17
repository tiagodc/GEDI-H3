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

# Import configuration variables
from config import GH3_DEFAULT_DOWNLOAD_DIR, GEDI_PRODUCTS
from utils import read_vector_file
from gedidriver import GEDIFile, soc_file_tree, gedi_subset, dask_h5_merged

class GEDIAccessor:
    """Main class for accessing GEDI data through various methods"""
    
    def __init__(self, authenticate: bool = True, 
                 spatial: Union[List[float], Polygon, str, gpd.GeoDataFrame] = None,
                 temporal: Union[Tuple[str, str], List[str]] = None):
        
        self.product_files = {}
        
        if spatial is not None:
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
            search_params["bounding_box"] = self.spatial

        # Handle temporal filtering
        if hasattr(self, 'spatial'):
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
            if isinstance(spatial, list) and len(spatial) == 4:
                return tuple(spatial)
            
            elif isinstance(spatial, str):
                if spatial.lower().endswith(('.shp', '.geojson', '.json', '.parquet')):
                    gdf = read_vector_file(spatial)
                    return tuple(gdf.total_bounds)
            
            elif isinstance(spatial, gpd.GeoDataFrame):
                return tuple(spatial.total_bounds)
            
            elif isinstance(spatial, Polygon):
                return spatial.bounds
            
        except Exception as e:
            warnings.warn(f"Could not process spatial filter: {e}")
            return None
    
    def _process_temporal_filter(self, temporal) -> Tuple[str, str]:
        if isinstance(temporal, (list, tuple)) and len(temporal) == 2:
            start_date, end_date = temporal
            
            # Convert to datetime objects if strings
            if isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            if isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            
            # Format as ISO strings
            start_iso = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_iso = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            return (start_iso, end_iso)
        
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


def download_granule(granule, odir: str = None, subset_vars: List[str] = None):
    gfile = GEDIFile(granule.data_links()[0])
    odir_soc = os.path.join(odir, str(gfile.date.year), gfile.date.strftime('%j'))
    os.makedirs(odir_soc, exist_ok=True)
    opath = earthaccess.download(granule, odir_soc, threads=1, pqdm_kwargs={'disable': True})
    
    if len(opath) == 0:
        return    
    
    opath = opath[0]
        
    if subset_vars is not None:
        osub = opath.replace('.h5', '_subset.h5')
        osub = gedi_subset(opath, osub, subset_vars)
                
        if osub is not None:
            os.unlink(opath)
            os.rename(osub, opath)
            
    return opath

def gedi_download(product_vars: Dict, odir: str = None, spatial = None, temporal = None, n_jobs=5, to_list=False):
    gass = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)
    
    prod_paths = {}
    for prod, vars in product_vars.items():
        if "minimal" in vars:
            vars = GEDI_PRODUCTS[prod]['default_vars']
        elif "*" in vars or "all" in vars:
            vars = None
        
        granules = gass.search_data(product=prod)
        
        if odir is None:
            opaths = gass.link_s3(product=prod)
        else:
            download_func = partial(download_granule, odir=odir, subset_vars=vars)
            opaths = pqdm(granules, download_func, n_jobs=n_jobs)
        
        prod_paths[prod] = opaths
    
    if to_list:
        prod_paths = list(chain(*prod_paths.values()))
        
    return prod_paths

def _testit():
    odir = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp'
    product_vars = {'L1B': ['minimal'], 'L2A': ['minimal'], 'L4A': ['minimal'], 'L4C': ['*']}
    spatial = [-50.5,0.5,-50,1]
    temporal = ('2020-01-01','2020-07-01')
    # producer_granule_id
    # polygon
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