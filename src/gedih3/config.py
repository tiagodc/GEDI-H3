import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Default download directories
# GH3_DEFAULT_DOWNLOAD_DIR = str(Path.home() / 'gedih3_db')
GH3_DEFAULT_DOWNLOAD_DIR = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp'
GH3_DEFAULT_TMP_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'tmp')
GH3_DEFAULT_SOC_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'soc')
GH3_DEFAULT_H3_DIR = os.path.join(GH3_DEFAULT_DOWNLOAD_DIR, 'h3')

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

    # Create directories if they don't exist
    if mkdirs:
        for directory in [GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR]:
            os.makedirs(directory, exist_ok=True)

configure_environment()

GEDI_START_DATE = datetime.strptime('2018-01-01', '%Y-%m-%d')
GEDI_BEAMS = ['BEAM0000','BEAM0001','BEAM0010','BEAM0011','BEAM0101','BEAM0110','BEAM1000','BEAM1011']
GEDI_L2A_ESSENTIALS = ['shot_number','delta_time','quality_flag','lat_lowestmode','lon_lowestmode','elev_lowestmode']

GEDI_PRODUCTS = {
    'L1B': {
        'doi': '10.5067/GEDI/GEDI01_B.002', 
        'daac': 'LPDAAC', 
        'version': 2, 
        'format': '.h5',
        'description': 'Geolocated waveforms',
        'default_vars': ['shot_number','noise_mean_corrected','rx_sample_start_index','rx_sample_count','rxwaveform']
    },
    'L2A': {
        'doi': '10.5067/GEDI/GEDI02_A.002', 
        'daac': 'LPDAAC', 
        'version': 2, 
        'format': '.h5',
        'description': 'Elevation and height metrics',
        'default_vars': GEDI_L2A_ESSENTIALS + ['rh']
    },
    'L2B': {
        'doi': '10.5067/GEDI/GEDI02_B.002', 
        'daac': 'LPDAAC', 
        'version': 2, 
        'format': '.h5',
        'description': 'Canopy cover and vertical profile metrics',
        'default_vars': ['shot_number','cover_z','fhd_normal']
    },
    # 'L3': {
    #     'doi': '10.3334/ORNLDAAC/1952', 
    #     'daac': 'ORNLDAAC', 
    #     'version': 2, 
    #     'format': '.tif',
    #     'description': 'Gridded land surface metrics'
    # },
    'L4A': {
        'doi': '10.3334/ORNLDAAC/2056', 
        'daac': 'ORNLDAAC', 
        'version': 2.1, 
        'format': '.h5',
        'description': 'Footprint level aboveground biomass',
        'default_vars': ['shot_number','agbd']
    },
    # 'L4B': {
    #     'doi': '10.3334/ORNLDAAC/2299', 
    #     'daac': 'ORNLDAAC', 
    #     'version': 2.1, 
    #     'format': '.tif',
    #     'description': 'Gridded aboveground biomass'
    # },
    'L4C': {
        'doi': '10.3334/ORNLDAAC/2338', 
        'daac': 'ORNLDAAC', 
        'version': 2, 
        'format': '.h5',
        'description': 'Footprint level structural complexity',
        'default_vars': ['shot_number','wsci']
    }
    # Future products:
    # 'L4C_FUSION': {'doi': '', 'daac': 'ORNLDAAC', 'version': '002', 'format': '.tif'},     
    # 'L4D': {'doi': '', 'daac': 'ORNLDAAC', 'version': '002', 'format': '.tif'}
}