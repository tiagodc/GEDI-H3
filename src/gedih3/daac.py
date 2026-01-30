import os
import time
import logging
from datetime import datetime
from typing import Union, List, Dict, Optional, Tuple, Any
from functools import partial
from itertools import chain
import earthaccess
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import warnings
from pqdm.processes import pqdm
from dask.distributed import progress
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
    RetryError,
)

# Import configuration variables
from .config import GH3_DEFAULT_DOWNLOAD_DIR, GEDI_PRODUCTS
from .utils import get_dask_client, read_vector_file, geo_to_umm, parse_temporal
from .gedidriver import GEDIFile, soc_file_tree, gedi_subset, dask_h5_merged, gedi_vars_expand, gedi_vars_from_h5
from .exceptions import (
    GediDownloadError,
    GediAuthenticationError,
    GediNetworkError,
    is_retryable_error,
    RETRY_DEFAULTS,
)
from .logging_config import get_logger

logger = get_logger(__name__)

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
    
    def login(self, strategy: str = 'netrc', persist: bool = True, max_attempts: int = 3):
        """
        Authenticate with NASA Earthdata Login.

        Parameters
        ----------
        strategy : str
            Authentication strategy ('netrc', 'environment', 'interactive').
            Default is 'netrc' which reads credentials from ~/.netrc file.
            Use 'environment' for EDL_USERNAME/EDL_PASSWORD env vars.
            Use 'interactive' only in interactive terminals (may crash VSCode).
        persist : bool
            Whether to persist credentials
        max_attempts : int
            Maximum authentication attempts

        Raises
        ------
        GediAuthenticationError
            If authentication fails after all attempts

        Notes
        -----
        For CLI usage, credentials should be stored in ~/.netrc file:

            machine urs.earthdata.nasa.gov
                login YOUR_USERNAME
                password YOUR_PASSWORD

        Create an account at https://urs.earthdata.nasa.gov/ if needed.
        """
        # Check if netrc credentials exist before attempting login
        if strategy == 'netrc':
            import netrc
            import os
            netrc_path = os.path.expanduser('~/.netrc')
            if not os.path.exists(netrc_path):
                raise GediAuthenticationError(
                    f"No ~/.netrc file found. Please create one with your NASA Earthdata credentials:\n\n"
                    f"  machine urs.earthdata.nasa.gov\n"
                    f"      login YOUR_USERNAME\n"
                    f"      password YOUR_PASSWORD\n\n"
                    f"Create an account at https://urs.earthdata.nasa.gov/ if needed."
                )
            try:
                nrc = netrc.netrc(netrc_path)
                if nrc.authenticators('urs.earthdata.nasa.gov') is None:
                    raise GediAuthenticationError(
                        f"No NASA Earthdata credentials found in ~/.netrc. Please add:\n\n"
                        f"  machine urs.earthdata.nasa.gov\n"
                        f"      login YOUR_USERNAME\n"
                        f"      password YOUR_PASSWORD\n\n"
                        f"Create an account at https://urs.earthdata.nasa.gov/ if needed."
                    )
            except netrc.NetrcParseError as e:
                raise GediAuthenticationError(f"Error parsing ~/.netrc file: {e}")

        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                self.auth = earthaccess.login(strategy=strategy, persist=persist)
                self.authenticated = True
                logger.info("Successfully authenticated with Earthdata Login")
                return
            except Exception as e:
                last_error = e
                if attempt < max_attempts:
                    wait_time = 2 ** attempt
                    logger.warning(f"Authentication attempt {attempt} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)

        logger.error(f"Authentication failed after {max_attempts} attempts: {last_error}")
        self.auth = None
        self.authenticated = False
        raise GediAuthenticationError(f"Failed to authenticate after {max_attempts} attempts: {last_error}")
    
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
        return parse_temporal(temporal)
    
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


def _download_with_retry(
    granule,
    odir_soc: str,
    max_attempts: int = RETRY_DEFAULTS['max_attempts'],
    initial_wait: float = RETRY_DEFAULTS['initial_wait'],
    max_wait: float = RETRY_DEFAULTS['max_wait']
) -> Optional[str]:
    """
    Download a granule with automatic retry on transient failures.

    Parameters
    ----------
    granule : earthaccess.Granule
        Granule object to download
    odir_soc : str
        Output directory for the downloaded file
    max_attempts : int
        Maximum number of download attempts
    initial_wait : float
        Initial wait time between retries (seconds)
    max_wait : float
        Maximum wait time between retries (seconds)

    Returns
    -------
    str or None
        Path to downloaded file, or None if download failed

    Raises
    ------
    GediDownloadError
        If download fails after all retry attempts
    """
    granule_id = None
    try:
        granule_id = granule.data_links()[0].split('/')[-1]
    except Exception:
        granule_id = str(granule)

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            opath = earthaccess.download(granule, odir_soc, threads=1, pqdm_kwargs={'disable': True})

            if len(opath) == 0:
                raise GediDownloadError(
                    f"Download returned empty result for {granule_id}",
                    granule_id=granule_id,
                    attempts=attempt
                )

            return str(opath[0])

        except Exception as e:
            last_error = e

            if not is_retryable_error(e):
                logger.error(f"Non-retryable error downloading {granule_id}: {e}")
                raise GediDownloadError(
                    f"Download failed for {granule_id}: {e}",
                    granule_id=granule_id,
                    attempts=attempt
                ) from e

            if attempt < max_attempts:
                wait_time = min(initial_wait * (2 ** (attempt - 1)), max_wait)
                logger.warning(
                    f"Download attempt {attempt}/{max_attempts} failed for {granule_id}: {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                time.sleep(wait_time)

    raise GediDownloadError(
        f"Download failed after {max_attempts} attempts for {granule_id}: {last_error}",
        granule_id=granule_id,
        attempts=max_attempts
    )


def download_granule(
    granule,
    odir: str = None,
    subset_vars: List[str] = None,
    resume: bool = False,
    max_attempts: int = RETRY_DEFAULTS['max_attempts']
) -> Optional[str]:
    """
    Download a GEDI granule with retry logic and optional variable subsetting.

    Parameters
    ----------
    granule : earthaccess.Granule
        Granule object to download
    odir : str
        Base output directory
    subset_vars : list of str, optional
        Variables to extract (subset HDF5 after download)
    resume : bool
        If True, skip already-downloaded files
    max_attempts : int
        Maximum download attempts on failure

    Returns
    -------
    str or None
        Path to downloaded/existing file, or None on failure
    """
    gfile = GEDIFile(granule.data_links()[0])
    odir_soc = os.path.join(odir, str(gfile.date.year), gfile.date.strftime('%j'))
    os.makedirs(odir_soc, exist_ok=True)

    expected_filename = gfile.full_name
    expected_path = os.path.join(odir_soc, expected_filename)

    # Check for existing file if resume mode
    if resume and os.path.exists(expected_path):
        if subset_vars is not None:
            try:
                existing_vars = set(gedi_vars_from_h5(expected_path))
                requested_vars = set(subset_vars)
                if requested_vars.issubset(existing_vars):
                    logger.debug(f"Skipping {expected_filename} (already exists with required variables)")
                    return expected_path
                else:
                    logger.debug(f"Re-downloading {expected_filename} (missing variables)")
                    os.unlink(expected_path)
            except Exception as e:
                logger.warning(f"Could not read existing file {expected_filename}, re-downloading: {e}")
                os.unlink(expected_path)
        else:
            try:
                _ = gedi_vars_from_h5(expected_path)
                logger.debug(f"Skipping {expected_filename} (already exists)")
                return expected_path
            except Exception as e:
                logger.warning(f"Could not verify existing file {expected_filename}, re-downloading: {e}")
                os.unlink(expected_path)

    # Download with retry
    try:
        opath = _download_with_retry(granule, odir_soc, max_attempts=max_attempts)
    except GediDownloadError as e:
        logger.error(f"Download failed: {e}")
        return None

    if opath is None:
        return None

    # Apply variable subsetting if requested
    if subset_vars is not None:
        osub = opath.replace('.h5', '_subset.h5')
        try:
            osub = gedi_subset(opath, osub, subset_vars)
            if osub is not None:
                os.unlink(opath)
                os.rename(osub, opath)
        except Exception as e:
            logger.warning(f"Subsetting failed for {opath}: {e}. Keeping full file.")

    return opath

def gedi_download(
    product_vars: Dict,
    odir: str = None,
    spatial = None,
    temporal = None,
    n_jobs: int = 5,
    to_list: bool = False,
    resume: bool = False,
    max_attempts: int = RETRY_DEFAULTS['max_attempts']
) -> Union[Dict[str, List[str]], List[str]]:
    """
    Download GEDI granules for specified products and variables.

    Parameters
    ----------
    product_vars : dict
        Dictionary mapping product codes to variable specifications
    odir : str, optional
        Output directory. If None, returns S3 links instead of downloading.
    spatial : various, optional
        Spatial filter (bbox, file path, or GeoDataFrame)
    temporal : tuple, optional
        Temporal filter as (start_date, end_date)
    n_jobs : int
        Number of parallel download jobs (when not using Dask)
    to_list : bool
        If True, return flat list instead of dict
    resume : bool
        If True, skip already-downloaded files
    max_attempts : int
        Maximum download attempts per granule

    Returns
    -------
    dict or list
        Downloaded file paths, organized by product or as flat list

    Raises
    ------
    GediAuthenticationError
        If authentication fails
    GediDownloadError
        If critical download errors occur
    """
    gass = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)

    prod_paths = {}
    product_vars = gedi_vars_expand(product_vars)

    dask_client = get_dask_client()
    if dask_client is not None:
        logger.info(f"Using Dask client: {dask_client.dashboard_link}")
    else:
        logger.info(f"No Dask client detected, using pqdm with {n_jobs} jobs")

    failed_products = []

    for prod, vars in product_vars.items():
        try:
            granules = gass.search_data(product=prod)

            if len(granules) == 0:
                logger.warning(f"No granules found for product {prod}")
                prod_paths[prod] = []
                continue

            if odir is None:
                opaths = gass.link_s3(product=prod)
            else:
                download_func = partial(
                    download_granule,
                    odir=odir,
                    subset_vars=vars,
                    resume=resume,
                    max_attempts=max_attempts
                )
                if dask_client is not None:
                    futures = dask_client.map(download_func, granules)
                    progress(futures)
                    opaths = dask_client.gather(futures)
                else:
                    opaths = pqdm(granules, download_func, n_jobs=n_jobs)

                # Filter out None values (failed downloads)
                successful = [p for p in opaths if p is not None]
                failed_count = len(opaths) - len(successful)

                if failed_count > 0:
                    logger.warning(f"Product {prod}: {failed_count}/{len(opaths)} granules failed to download")

                opaths = successful

            prod_paths[prod] = opaths
            logger.info(f"Product {prod}: {len(opaths)} files ready")

        except GediAuthenticationError:
            raise
        except Exception as e:
            logger.error(f"Error processing product {prod}: {e}")
            failed_products.append(prod)
            prod_paths[prod] = []

    if failed_products:
        logger.warning(f"Failed products: {failed_products}")

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