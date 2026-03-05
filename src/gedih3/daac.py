import os
import time
from typing import Union, List, Dict, Optional, Tuple, Any, Callable
from functools import partial
from itertools import chain
import earthaccess
import geopandas as gpd
from shapely.geometry import Polygon
import warnings
from pqdm.processes import pqdm
from dask.distributed import progress

# Import configuration variables
from .config import GH3_DEFAULT_DOWNLOAD_DIR, GEDI_PRODUCTS
from .utils import get_dask_client, read_vector_file, geo_to_umm, parse_temporal
from .gedidriver import GEDIFile, gedi_subset, gedi_vars_expand, gedi_vars_from_h5
from .exceptions import (
    GediDownloadError,
    GediAuthenticationError,
    GediNetworkError,
    GediProductError,
    GediValidationError,
    is_retryable_error,
    RETRY_DEFAULTS,
)
from .logging_config import get_logger

logger = get_logger(__name__)

def gedi_list_versions(product: str) -> List[dict]:
    """Query NASA CMR for all available versions of a GEDI product.

    Parameters
    ----------
    product : str
        Product code (e.g., 'L2A', 'L4A')

    Returns
    -------
    list of dict
        Available versions with metadata, sorted by version string.
    """
    product = product.upper()
    if product not in GEDI_PRODUCTS:
        raise GediProductError(f"Product must be one of: {list(GEDI_PRODUCTS.keys())}")

    short_name = GEDI_PRODUCTS[product]['short_name']
    collections = earthaccess.search_datasets(short_name=short_name)
    versions = []
    for c in collections:
        umm = c.get('umm', {})
        versions.append({
            'version': c.get('meta', {}).get('native-id', umm.get('Version', 'unknown')).split('.')[-1]
                if not umm.get('Version') else umm.get('Version'),
            'doi': umm.get('DOI', {}).get('DOI') if isinstance(umm.get('DOI'), dict) else None,
            'concept_id': c.get('meta', {}).get('concept-id'),
            'title': umm.get('EntryTitle', ''),
        })
    return sorted(versions, key=lambda x: x.get('version', ''))


def gedi_latest_version(product: str) -> str:
    """Return the latest available version string for a GEDI product.

    Parameters
    ----------
    product : str
        Product code (e.g., 'L2A', 'L4A')

    Returns
    -------
    str
        Latest version string (e.g., '002')

    Raises
    ------
    GediProductError
        If no versions are found
    """
    versions = gedi_list_versions(product)
    if not versions:
        raise GediProductError(f"No versions found for {product}")
    return versions[-1]['version']


class GEDIAccessor:
    """NASA Earthdata accessor for searching and downloading GEDI granules.

    Provides authentication, spatial/temporal filtering, granule search,
    and batch download functionality using the earthaccess library.

    Parameters
    ----------
    authenticate : bool
        If True (default), automatically call login() on initialization.
    spatial : list, Polygon, str, or GeoDataFrame, optional
        Spatial filter: bounding box ``[W, S, E, N]``, Shapely geometry,
        vector file path, or GeoDataFrame.
    temporal : tuple or list of str, optional
        Temporal filter as ``(start_date, end_date)``, e.g.
        ``('2020-01-01', '2021-01-01')``.

    Raises
    ------
    GediAuthenticationError
        If authentication fails on initialization (when ``authenticate=True``).

    Examples
    --------
    >>> accessor = GEDIAccessor(
    ...     spatial=[-51, 0, -50, 1],
    ...     temporal=('2020-01-01', '2021-01-01')
    ... )
    >>> granules = accessor.search_data('L4A')
    >>> paths = accessor.download_all('/path/to/output', product='L4A')
    """
    
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

        Token-based authentication is also supported by setting the
        EARTHDATA_TOKEN environment variable. When set, earthaccess
        uses the token directly regardless of the strategy parameter.
        This is the recommended approach for HPC environments.
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
    
    def search_data(self, product: str = None, version: str = None, **kwargs) -> List[Any]:
        """
        Search for GEDI granules with spatial and temporal filtering.

        Uses short_name + version for GEDI product searches (preferred over DOI).
        Falls back to DOI if short_name is not available.

        Parameters
        ----------
        product : str, optional
            GEDI product level ('L1B', 'L2A', 'L2B', 'L4A', 'L4C').
            If None, uses kwargs directly for custom dataset searches.
        version : str or int, optional
            GEDI data version (e.g., '002' or 2). If None, earthaccess
            returns the latest version by default.
        **kwargs : dict
            Additional search parameters passed to earthaccess.search_data().

        Returns
        -------
        list
            List of granule objects

        Examples
        --------
        >>> accessor.search_data('L4A')
        >>> accessor.search_data('L4A', version=2)
        >>> accessor.search_data(short_name='MY_PRIVATE_DATASET', version='001')
        """
        search_params = {}

        if product is not None:
            if product.upper() not in GEDI_PRODUCTS:
                raise GediProductError(f"Product must be one of: {list(GEDI_PRODUCTS.keys())}")

            self.product = GEDI_PRODUCTS[product.upper()]

            # Prefer short_name + version over DOI
            if 'short_name' in self.product:
                search_params['short_name'] = self.product['short_name']
                if version is not None:
                    # LPDAAC uses zero-padded versions (e.g., '002'), ORNLDAAC uses plain strings (e.g., '2', '2.1')
                    if self.product.get('daac') == 'LPDAAC':
                        search_params['version'] = f'{int(version):03d}'
                    else:
                        search_params['version'] = str(version)
            else:
                # Fallback to DOI
                search_params['doi'] = self.product['doi']
        else:
            self.product = None

        # Handle spatial filtering
        if hasattr(self, 'spatial'):
            if self.is_bounding_box:
                search_params['bounding_box'] = self.spatial
            else:
                search_params['polygon'] = self.spatial

        # Handle temporal filtering
        if hasattr(self, 'temporal'):
            search_params['temporal'] = self.temporal

        # Add any additional parameters
        search_params.update(kwargs)

        self.search_params = search_params
        self.granules = earthaccess.search_data(**search_params)

        # DOI fallback: if short_name search returned 0 results, retry with DOI
        if len(self.granules) == 0 and self.product is not None and 'short_name' in search_params and 'doi' in self.product:
            logger.warning(f"No granules found with short_name '{search_params['short_name']}', retrying with DOI")
            fallback_params = {k: v for k, v in search_params.items() if k not in ('short_name', 'version')}
            fallback_params['doi'] = self.product['doi']
            self.granules = earthaccess.search_data(**fallback_params)

        product_key = product.upper() if product is not None else 'CUSTOM'
        self.product_files[product_key] = self.granules

        dataset_name = product if product is not None else kwargs.get('short_name', kwargs.get('concept_id', 'custom dataset'))
        logger.info(f"Found {len(self.granules)} {dataset_name} granules")
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
        """Download all searched granules.

        Parameters
        ----------
        download_dir : str, optional
            Output directory. Defaults to GH3_DEFAULT_DOWNLOAD_DIR.
        product : str, optional
            Product key to download. If None, downloads all granules.
        show_progress : bool, optional
            Whether to display download progress bar. Passed through
            to earthaccess.download() via **kwargs.
        **kwargs
            Additional arguments passed to earthaccess.download().
        """
        if not self.authenticated:
            raise GediAuthenticationError("Must authenticate before downloading")

        if not hasattr(self, 'granules'):
            raise GediNetworkError("No granules found. Please run search_data() first.")
       
        if download_dir is None:
            download_dir = GH3_DEFAULT_DOWNLOAD_DIR
        
        granules = self.granules if product is None else self.product_files[product.upper()]
        
        os.makedirs(download_dir, exist_ok=True)        
        downloaded_files = earthaccess.download(granules, download_dir, **kwargs)
        return downloaded_files

    def link_s3(self, product: str = None):
        if not self.authenticated:
            raise GediAuthenticationError("Must authenticate before accessing S3")

        if not hasattr(self, 'granules'):
            raise GediNetworkError("No granules found. Please run search_data() first.")

        if product is None:
            granules = self.granules
        else:
            # Handle both standard product keys and 'CUSTOM'
            product_key = product.upper() if product else 'CUSTOM'
            granules = self.product_files.get(product_key, self.granules)

        s3_files = earthaccess.open(granules, show_progress=False,
                                     open_kwargs={'block_size': 16 * 1024 * 1024})
        return s3_files    
    
    def merge_paths(self, open_s3: bool = False):
        if not hasattr(self, 'product_files'):
            raise GediNetworkError("No products found. Please run search_data() first.")
        
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

    # Ensure authentication in worker processes (pqdm/Dask spawn fresh processes
    # that don't inherit the main-process earthaccess session)
    if not earthaccess.__auth__.authenticated:
        earthaccess.login(strategy='netrc', persist=False)

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            opath = earthaccess.download(granule, odir_soc, threads=1, show_progress=False)

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


def download_custom_granule(
    granule,
    odir: str,
    resume: bool = False,
    max_attempts: int = RETRY_DEFAULTS['max_attempts']
) -> Optional[str]:
    """
    Download a custom (non-GEDI) granule with retry logic.

    This function handles granules from any NASA dataset without requiring
    GEDI-specific filename parsing. Files are downloaded directly to the
    output directory without year/doy subdirectory structure.

    Parameters
    ----------
    granule : earthaccess.Granule
        Granule object to download
    odir : str
        Output directory for downloaded files
    resume : bool
        If True, skip already-downloaded files
    max_attempts : int
        Maximum download attempts on failure

    Returns
    -------
    str or None
        Path to downloaded file, or None on failure
    """
    # Extract filename from data link
    try:
        data_link = granule.data_links()[0]
        filename = data_link.split('/')[-1]
    except Exception:
        filename = None

    os.makedirs(odir, exist_ok=True)

    # Check for existing file if resume mode
    if resume and filename:
        expected_path = os.path.join(odir, filename)
        if os.path.exists(expected_path):
            logger.debug(f"Skipping {filename} (already exists)")
            return expected_path

    # Download with retry
    try:
        opath = _download_with_retry(granule, odir, max_attempts=max_attempts)
        return opath
    except GediDownloadError as e:
        logger.error(f"Download failed: {e}")
        return None


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
                    missing = requested_vars - existing_vars
                    logger.debug(f"Re-downloading {expected_filename} (missing {len(missing)} variables)")
                    # Preserve union of existing + requested vars after re-download
                    subset_vars = sorted(existing_vars | requested_vars)
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
    product_vars: Dict = None,
    odir: str = None,
    spatial = None,
    temporal = None,
    version = None,
    n_jobs: int = 5,
    to_list: bool = False,
    resume: bool = False,
    max_attempts: int = RETRY_DEFAULTS['max_attempts'],
    search_kwargs: Dict = None,
    on_granule_complete: Optional[Callable] = None,
) -> Union[Dict[str, List[str]], List[str]]:
    """
    Download GEDI granules for specified products and variables.

    Parameters
    ----------
    product_vars : dict, optional
        Dictionary mapping product codes to variable specifications.
        If None and search_kwargs is provided, downloads a custom dataset.
    odir : str, optional
        Output directory. If None, returns S3 links instead of downloading.
    spatial : various, optional
        Spatial filter (bbox, file path, or GeoDataFrame)
    temporal : tuple, optional
        Temporal filter as (start_date, end_date)
    version : int or str, optional
        GEDI data version (e.g., 2 or '002'). If None, uses latest available.
    n_jobs : int
        Number of parallel download jobs (when not using Dask)
    to_list : bool
        If True, return flat list instead of dict
    resume : bool
        If True, skip already-downloaded files
    max_attempts : int
        Maximum download attempts per granule
    search_kwargs : dict, optional
        Custom search parameters for non-GEDI datasets.
    on_granule_complete : callable, optional
        Callback ``(granule_info_dict, status_str) -> None`` called after
        each granule completes. ``granule_info_dict`` has orbit/granule/track
        keys; ``status_str`` is 'PENDING', 'DOWNLOADED', or 'FAILED'.

    Returns
    -------
    dict or list
        Downloaded file paths, organized by product or as flat list.

    Raises
    ------
    GediAuthenticationError
        If NASA Earthdata authentication fails.
    GediValidationError
        If neither ``product_vars`` nor ``search_kwargs`` is provided.
    GediDownloadError
        If downloads fail after all retry attempts.

    Examples
    --------
    >>> from gedih3.daac import gedi_download
    >>> paths = gedi_download(
    ...     product_vars={'L2A': ['default'], 'L4A': ['agbd']},
    ...     odir='/path/to/output',
    ...     spatial=[-51, 0, -50, 1],
    ...     temporal=('2020-01-01', '2021-01-01'),
    ...     resume=True,
    ... )
    """
    gass = GEDIAccessor(authenticate=True, spatial=spatial, temporal=temporal)

    prod_paths = {}

    # Handle custom dataset download (no product_vars, using search_kwargs)
    if product_vars is None and search_kwargs is not None:
        product_vars = {'CUSTOM': None}  # Placeholder for custom dataset
    elif product_vars is None:
        raise GediValidationError("Either product_vars or search_kwargs must be provided")
    else:
        product_vars = gedi_vars_expand(product_vars)

    dask_client = get_dask_client()
    if dask_client is not None:
        logger.info(f"Using Dask client: {dask_client.dashboard_link}")
    else:
        logger.info(f"No Dask client detected, using pqdm with {n_jobs} jobs")

    failed_products = []

    for prod, vars in product_vars.items():
        try:
            # Use custom search kwargs for CUSTOM product or standard product search
            if prod == 'CUSTOM' and search_kwargs is not None:
                granules = gass.search_data(product=None, **search_kwargs)
            else:
                # Use per-product config version when --gedi-version not specified
                prod_version = version if version is not None else GEDI_PRODUCTS.get(prod.upper(), {}).get('version')
                granules = gass.search_data(product=prod, version=prod_version)

            if len(granules) == 0:
                logger.warning(f"No granules found for product {prod}")
                prod_paths[prod] = []
                continue

            if odir is None:
                # Return S3 links for streaming access
                product_key = prod if prod != 'CUSTOM' else None
                opaths = gass.link_s3(product=product_key)
            else:
                # Select appropriate download function
                if prod == 'CUSTOM':
                    # Use simpler download function for custom datasets
                    download_func = partial(
                        download_custom_granule,
                        odir=odir,
                        resume=resume,
                        max_attempts=max_attempts
                    )
                else:
                    # Use GEDI-specific download with variable subsetting
                    download_func = partial(
                        download_granule,
                        odir=odir,
                        subset_vars=vars,
                        resume=resume,
                        max_attempts=max_attempts
                    )

                # Register all granules as PENDING before download
                if on_granule_complete and prod != 'CUSTOM':
                    for g in granules:
                        gfile = GEDIFile(g.data_links()[0])
                        ginfo = {'orbit': gfile.orbit, 'granule': gfile.orbit_granule, 'track': gfile.track}
                        on_granule_complete(ginfo, 'PENDING')

                if dask_client is not None:
                    futures = dask_client.map(download_func, granules)

                    if on_granule_complete and prod != 'CUSTOM':
                        # Real-time per-file tracking via as_completed
                        from distributed import as_completed as dask_as_completed
                        future_to_granule = dict(zip(futures, granules))
                        opaths_map = {}
                        n_total = len(futures)
                        for i, (future, result) in enumerate(dask_as_completed(futures, with_results=True)):
                            opaths_map[future] = result
                            g = future_to_granule[future]
                            gfile = GEDIFile(g.data_links()[0])
                            ginfo = {'orbit': gfile.orbit, 'granule': gfile.orbit_granule, 'track': gfile.track}
                            status = 'DOWNLOADED' if result else 'FAILED'
                            on_granule_complete(ginfo, status)
                            logger.info(f"[{prod}] {i+1}/{n_total} granules processed")
                        # Reconstruct in original order
                        opaths = [opaths_map[f] for f in futures]
                    else:
                        # Original path: progress bar + gather
                        progress(futures)
                        opaths = dask_client.gather(futures)
                else:
                    opaths = pqdm(granules, download_func, n_jobs=n_jobs)
                    # Batch update after pqdm completion
                    if on_granule_complete and prod != 'CUSTOM':
                        for g, result in zip(granules, opaths):
                            gfile = GEDIFile(g.data_links()[0])
                            ginfo = {'orbit': gfile.orbit, 'granule': gfile.orbit_granule, 'track': gfile.track}
                            status = 'DOWNLOADED' if result else 'FAILED'
                            on_granule_complete(ginfo, status)

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

