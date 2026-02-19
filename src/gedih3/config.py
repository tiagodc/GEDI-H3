import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from importlib.resources import files

def get_package_data_path(filename):
    try:
        return files('gedih3').joinpath('data', filename)
    except ModuleNotFoundError:
        return Path(__file__).parent.joinpath('data', filename)

ISO3_COUNTRIES_URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/country_shapes/exports/geojson/"

# Default download directories
GH3_DEFAULT_DOWNLOAD_DIR = str(Path.home() / 'gedih3_db')
GH3_DEFAULT_TMP_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'tmp')
GH3_DEFAULT_SOC_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'soc')
GH3_DEFAULT_H3_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'h3')

# Metadata filenames
BUILD_LOG_FILENAME = 'gedih3_build_log.json'
DATASET_META_FILENAME = 'gedih3_dataset.json'
PARTITION_META_FILENAME = '.metadata.json'
MANIFEST_FILENAME = '_manifest.txt'

def configure_environment(mkdirs=False):
    global GH3_DEFAULT_DOWNLOAD_DIR
    global GH3_DEFAULT_TMP_DIR
    global GH3_DEFAULT_SOC_DIR
    global GH3_DEFAULT_H3_DIR

    env_file = Path.home() / '.gedih3.env'
    if env_file.exists():
        load_dotenv(env_file, override=False)

    # Set variables according to priority
    GH3_DEFAULT_DOWNLOAD_DIR = os.getenv('GH3_DEFAULT_DOWNLOAD_DIR', GH3_DEFAULT_DOWNLOAD_DIR)
    GH3_DEFAULT_TMP_DIR = os.getenv('GH3_DEFAULT_TMP_DIR', os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'tmp'))
    GH3_DEFAULT_SOC_DIR = os.getenv('GH3_DEFAULT_SOC_DIR', os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'soc'))
    GH3_DEFAULT_H3_DIR = os.getenv('GH3_DEFAULT_H3_DIR', os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'h3'))

    # Create directories if they don't exist (skip remote paths)
    if mkdirs:
        from .utils import is_remote_path
        for directory in [GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR]:
            if not is_remote_path(directory):
                os.makedirs(directory, exist_ok=True)

configure_environment()

GEDI_START_DATE = datetime.strptime('2018-01-01', '%Y-%m-%d')
GEDI_BEAMS = ['BEAM0000','BEAM0001','BEAM0010','BEAM0011','BEAM0101','BEAM0110','BEAM1000','BEAM1011']
GEDI_L2A_ESSENTIALS = ['shot_number','delta_time','quality_flag','lat_lowestmode','lon_lowestmode','elev_lowestmode']

GEDI_PRODUCTS = {
    'L1B': {
        'short_name': 'GEDI01_B',
        'doi': '10.5067/GEDI/GEDI01_B.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Geolocated waveforms',
        'min_vars': ['shot_number','noise_mean_corrected','rx_sample_start_index','rx_sample_count','rxwaveform'],
        'default_vars_file': get_package_data_path('GEDI01_B_DATASETS_002.txt')
    },
    'L2A': {
        'short_name': 'GEDI02_A',
        'doi': '10.5067/GEDI/GEDI02_A.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Elevation and height metrics',
        'min_vars': GEDI_L2A_ESSENTIALS + ['rh'],
        'default_vars_file': get_package_data_path('GEDI02_A_DATASETS_002.txt')
    },
    'L2B': {
        'short_name': 'GEDI02_B',
        'doi': '10.5067/GEDI/GEDI02_B.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Canopy cover and vertical profile metrics',
        'min_vars': ['shot_number','cover_z','fhd_normal', 'pai_z', 'pgap_theta'],
        'default_vars_file': get_package_data_path('GEDI02_B_DATASETS_002.txt')
    },
    # 'L3': {
    #     'short_name': 'GEDI03',
    #     'doi': '10.3334/ORNLDAAC/1952',
    #     'daac': 'ORNLDAAC',
    #     'version': 2,
    #     'format': '.tif',
    #     'description': 'Gridded land surface metrics'
    # },
    'L4A': {
        'short_name': 'GEDI_L4A_AGB_Density_V2_1_2056',
        'doi': '10.3334/ORNLDAAC/2056',
        'daac': 'ORNLDAAC',
        'version': 2.1,
        'format': '.h5',
        'description': 'Footprint level aboveground biomass',
        'min_vars': ['shot_number','agbd','sensitivity','l4_quality_flag'],
        'default_vars_file': get_package_data_path('GEDI04_A_DATASETS_002.txt')
    },
    # 'L4B': {
    #     'short_name': 'GEDI04_B',
    #     'doi': '10.3334/ORNLDAAC/2299',
    #     'daac': 'ORNLDAAC',
    #     'version': 2,
    #     'format': '.tif',
    #     'description': 'Gridded aboveground biomass'
    # },
    'L4C': {
        'short_name': 'GEDI_L4C_WSCI_2338',
        'doi': '10.3334/ORNLDAAC/2338',
        'daac': 'ORNLDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Footprint level structural complexity',
        'min_vars': ['shot_number','wsci', 'wsci_pi_lower', 'wsci_pi_upper', 'wsci_quality_flag', 'land_cover_data/worldcover_class'],
        'default_vars_file': get_package_data_path('GEDI04_C_DATASETS_002.txt')
    }
    # Future products:
    # 'L4C_FUSION': {'short_name': '', 'daac': 'ORNLDAAC', 'version': '002', 'format': '.tif'},
    # 'L4D': {'short_name': '', 'daac': 'ORNLDAAC', 'version': '002', 'format': '.tif'}
}


def get_default_vars_file(product, version=None):
    """Get the default variable list file for a product, with version awareness.

    Parameters
    ----------
    product : str
        Product code (e.g., 'L2A', 'L4A')
    version : int or None
        GEDI data version. If None, falls back to known version 2.

    Returns
    -------
    Path
        Path to the variable list file
    """
    prod_info = GEDI_PRODUCTS[product.upper()]
    if version is None:
        version = 2
    short_name = prod_info['short_name']
    fname = f'{short_name}_DATASETS_{int(version):03d}.txt'
    path = get_package_data_path(fname)
    if path.is_file():
        return path
    # Fall back to the configured default
    return prod_info['default_vars_file']
