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

def _get_versioned(version_dict, version=None):
    """Resolve a version-keyed dict. Falls back to nearest lower version.

    Parameters
    ----------
    version_dict : dict
        Mapping of integer version numbers to values.
    version : int or None
        Target version. If None, defaults to 2.

    Returns
    -------
    object
        The value for the requested version, or the nearest lower version.
    """
    if version is None:
        version = 2
    if version in version_dict:
        return version_dict[version]
    available = sorted(v for v in version_dict if v <= version)
    if available:
        return version_dict[available[-1]]
    return version_dict[min(version_dict)]

def _resolve_identifier(value, version, *, product, field):
    """Resolve a product short_name/DOI for a requested version.

    LPDAAC products store ``value`` as a plain string (version-agnostic) and
    pass through. ORNL DAAC products store ``value`` as a ``{version: str}``
    mapping because the release ID is encoded into the identifier itself.

    Resolution for dict-shaped values:
      1. Exact match on the requested version.
      2. Major-version match (e.g. ``2`` → entry keyed ``2.1``).
      3. Raise ``ValueError`` — never silently substitute an older release.

    Parameters
    ----------
    value : str or dict
        The ``short_name`` or ``doi`` field from a GEDI_PRODUCTS entry.
    version : int, float, str, or None
        Requested data version. ``None`` resolves to the product's default
        version (handled by the caller before calling this helper).
    product : str
        Product code, used only for the error message.
    field : str
        Field name (``'short_name'`` or ``'doi'``), used only for the error.

    Returns
    -------
    str
        The resolved identifier.
    """
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"GEDI_PRODUCTS[{product!r}][{field!r}] must be str or dict, got {type(value).__name__}")
    try:
        v = float(version)
    except (TypeError, ValueError):
        raise ValueError(f"Cannot resolve {field} for {product}: version={version!r} is not numeric")
    if v in value:
        return value[v]
    int_v = int(v)
    # Major-version match: prefer the lowest minor under the same major
    # (e.g. v=2 should pick 2.0 over 2.1; v=3 should pick 3.0 over 3.5).
    candidates = sorted(k for k in value if int(float(k)) == int_v)
    if candidates:
        return value[candidates[0]]
    available = sorted(value)
    raise ValueError(
        f"No {field} registered for {product} v{version}; available versions: {available}. "
        f"If a new release has been published, add it to GEDI_PRODUCTS[{product!r}][{field!r}]."
    )


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
SOC_MANIFEST_FILENAME = '_soc_manifest.txt'

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
GEDI_MISSION_START = datetime(2018, 12, 13)
GEDI_BEAMS = ['BEAM0000','BEAM0001','BEAM0010','BEAM0011','BEAM0101','BEAM0110','BEAM1000','BEAM1011']
_GEDI_L2A_ESSENTIALS = {
    2: ['shot_number','delta_time','quality_flag','degrade_flag','sensitivity','lat_lowestmode','lon_lowestmode','elev_lowestmode'],
    3: ['shot_number','delta_time','l2a_quality_flag_rel3','degrade_flag','sensitivity','lat_lowestmode','lon_lowestmode','elev_lowestmode'],
}
# Version-keyed minimum variable sets per product.
# _get_versioned() falls back to nearest lower version, so only entries
# that differ from the previous version need to be added (e.g., v4 falls
# back to v3 automatically if no v4 entry exists).
_GEDI_MIN_VARS = {
    'L1B': {
        2: ['shot_number','stale_return_flag','noise_mean_corrected','rx_sample_start_index','rx_sample_count','rxwaveform','geolocation/elevation_bin0','geolocation/elevation_lastbin'],
        3: ['shot_number','stale_return_flag','noise_mean_corrected','rx_sample_start_index','rx_sample_count','rxwaveform','geolocation/elevation_bin0','geolocation/elevation_lastbin','rx_clipflag'],
    },
    'L2A': {
        2: _GEDI_L2A_ESSENTIALS[2] + ['rh'],
        3: _GEDI_L2A_ESSENTIALS[3] + ['rh'],
    },
    'L2B': {
        2: ['shot_number','l2b_quality_flag','cover_z','fhd_normal','pai_z','pavd_z','cover','pai'],
        3: ['shot_number','l2b_quality_flag_rel3','cover_z','fhd_normal','pai_z','pavd_z','cover','pai','rch'],
    },
    'L4A': {
        2: ['shot_number','agbd','agbd_se','l4_quality_flag'],
        3: ['shot_number','agbd','agbd_se','l4a_quality_flag_rel3','elev_highestreturn_outlier_flag'],
    },
    'L4C': {
        2: ['shot_number','wsci','wsci_xy','wsci_z','wsci_pi_lower','wsci_pi_upper','wsci_quality_flag','land_cover_data/worldcover_class'],
        3: ['shot_number','wsci','wsci_xy','wsci_z','wsci_pi_lower','wsci_pi_upper','l4c_quality_flag_rel3','land_cover_data/worldcover_class','elev_highestreturn_outlier_flag'],
    },
}

# Quality flag conditions per product and GEDI version (before product suffix).
# Each entry is a list of (flag_name, condition_str) tuples applied when --quality is used.
# degrade_flag lives in L2A only (always present via essentials); downstream products
# rely on L2A's degrade_flag rather than carrying redundant copies.
# Use _get_versioned() to resolve for a given version.
_PRODUCT_QUALITY_FLAGS = {
    'L1B': {
        2: [('stale_return_flag', '== 0')],
        3: [('stale_return_flag', '== 0'), ('rx_clipflag', '== 0')],
    },
    'L2A': {
        2: [('quality_flag', '== 1'), ('degrade_flag', '== 0')],
        3: [('l2a_quality_flag_rel3', '== 1'), ('degrade_flag', '== 0')],
    },
    'L2B': {
        2: [('l2b_quality_flag', '== 1')],
        3: [('l2b_quality_flag_rel3', '== 1')],
    },
    'L4A': {
        2: [('l4_quality_flag', '== 1')],
        3: [('l4a_quality_flag_rel3', '== 1'), ('elev_highestreturn_outlier_flag', '== 0')],
    },
    'L4C': {
        2: [('wsci_quality_flag', '== 1')],
        3: [('l4c_quality_flag_rel3', '== 1'), ('elev_highestreturn_outlier_flag', '== 0')],
    },
}

GEDI_PRODUCTS = {
    'L1B': {
        'short_name': 'GEDI01_B',
        'doi': '10.5067/GEDI/GEDI01_B.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Geolocated waveforms'
    },
    'L2A': {
        'short_name': 'GEDI02_A',
        'doi': '10.5067/GEDI/GEDI02_A.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Elevation and height metrics'
    },
    'L2B': {
        'short_name': 'GEDI02_B',
        'doi': '10.5067/GEDI/GEDI02_B.002',
        'daac': 'LPDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Canopy cover and vertical profile metrics'
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
        # ORNL DAAC short_names and DOIs encode the release ID, so they are
        # version-pinned. Resolve per requested version via _resolve_identifier().
        'short_name': {2.1: 'GEDI_L4A_AGB_Density_V2_1_2056'},
        'doi':        {2.1: '10.3334/ORNLDAAC/2056'},
        'daac': 'ORNLDAAC',
        'version': 2.1,
        'format': '.h5',
        'description': 'Footprint level aboveground biomass'
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
        # See L4A note re: ORNL DAAC version-pinned identifiers.
        'short_name': {2: 'GEDI_L4C_WSCI_2338'},
        'doi':        {2: '10.3334/ORNLDAAC/2338'},
        'daac': 'ORNLDAAC',
        'version': 2,
        'format': '.h5',
        'description': 'Footprint level structural complexity'
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
    product = product.upper()
    if product not in GEDI_PRODUCTS:
        raise ValueError(f"Unknown product: {product}")
    if version is None:
        version = 2
    # Derive canonical prefix from product key (e.g., 'L2A' → 'GEDI02_A')
    # instead of short_name, which is DAAC-specific for L4A/L4C.
    prefix = f"GEDI0{product[1]}_{product[2]}"
    fname = f'{prefix}_DATASETS_{int(version):03d}.txt'
    path = get_package_data_path(fname)
    if not path.is_file():
        raise FileNotFoundError(f"Variable list file not found: {fname}")
    return path
