"""
gedih3: GEDI Data Access and H3 Indexing Library

A comprehensive Python library for accessing GEDI (Global Ecosystem Dynamics Investigation)
data from NASA's ORNL DAAC with H3 spatial indexing support, multiple access methods,
spatial/temporal filtering, and on-the-fly subsetting.

Examples
--------
>>> import gedih3
>>> print(gedih3.__version__)
>>> ddf = gedih3.gh3_load(source='/path/to/database', columns=['agbd_l4a'])
>>> agg = gedih3.gh3_aggregate(ddf, target_res=6, agg='mean')
>>> gedih3.egi.egi_dataframe(shots_df, level=6)
>>> gedih3.raster.h3_to_raster(agg_gdf)
"""

__version__ = "0.5.2"
__author__ = "Tiago de Conto"
__email__ = "tiagodc@umd.edu"

# --- Config & environment ---------------------------------------------------
from .config import (
    GEDI_PRODUCTS,
    GEDI_BEAMS,
    GEDI_START_DATE,
    GH3_DEFAULT_DOWNLOAD_DIR,
    GH3_DEFAULT_TMP_DIR,
    GH3_DEFAULT_SOC_DIR,
    GH3_DEFAULT_H3_DIR,
    configure_environment,
    get_package_data_path,
)

# --- Remote storage ---------------------------------------------------------
from .utils import configure_storage, get_storage_options

# --- Exceptions -------------------------------------------------------------
from .exceptions import (
    GediError,
    GediNetworkError,
    GediDownloadError,
    GediAuthenticationError,
    GediS3AccessError,
    GediValidationError,
    H3ValidationError,
    EGIValidationError,
    GediProductError,
    GediVariableError,
    GediFileError,
    GediHDF5Error,
    GediParquetError,
    GediCorruptedFileError,
    GediTransactionError,
    GediDatabaseError,
    GediDatabaseNotFoundError,
    GediDatabaseCorruptedError,
    GediMergeError,
    GediSpatialError,
    GediTemporalError,
    GediProcessingError,
    GediAggregationError,
    GediRasterizationError,
    GediImageSamplingError,
    GediSpatialJoinError,
)

# --- Data access ------------------------------------------------------------
from .daac import (
    GEDIAccessor,
    gedi_download,
    gedi_list_versions,
    gedi_latest_version,
)

# --- HDF5 parsing -----------------------------------------------------------
from .gedidriver import (
    GEDIFile,
    GEDIShot,
    soc_file_tree,
    load_h5,
    load_h5_merged,
    dask_h5_merged,
)

# --- Database building ------------------------------------------------------
from .gh3builder import (
    build_h3db,
    download_soc,
)

# --- Database querying ------------------------------------------------------
from .gh3driver import (
    gh3_load,
    gh3_aggregate,
    gh3_export,
    egi_load,
    egi_aggregate,
    egi_extract,
    gh3_to_raster,
    gh3_rasterize_partitions,
)

# --- Sub-modules ------------------------------------------------------------
from . import egi
from . import raster
from . import validation

__all__ = [
    # metadata
    "__version__",
    "__author__",
    "__email__",
    # config
    "GEDI_PRODUCTS",
    "GEDI_BEAMS",
    "GEDI_START_DATE",
    "GH3_DEFAULT_DOWNLOAD_DIR",
    "GH3_DEFAULT_TMP_DIR",
    "GH3_DEFAULT_SOC_DIR",
    "GH3_DEFAULT_H3_DIR",
    "configure_environment",
    "get_package_data_path",
    # storage
    "configure_storage",
    "get_storage_options",
    # exceptions
    "GediError",
    "GediNetworkError",
    "GediDownloadError",
    "GediAuthenticationError",
    "GediS3AccessError",
    "GediValidationError",
    "H3ValidationError",
    "EGIValidationError",
    "GediProductError",
    "GediVariableError",
    "GediFileError",
    "GediHDF5Error",
    "GediParquetError",
    "GediCorruptedFileError",
    "GediTransactionError",
    "GediDatabaseError",
    "GediDatabaseNotFoundError",
    "GediDatabaseCorruptedError",
    "GediMergeError",
    "GediSpatialError",
    "GediTemporalError",
    "GediProcessingError",
    "GediAggregationError",
    "GediRasterizationError",
    "GediImageSamplingError",
    "GediSpatialJoinError",
    # data access
    "GEDIAccessor",
    "gedi_download",
    "gedi_list_versions",
    "gedi_latest_version",
    # HDF5 parsing
    "GEDIFile",
    "GEDIShot",
    "soc_file_tree",
    "load_h5",
    "load_h5_merged",
    "dask_h5_merged",
    # database building
    "build_h3db",
    "download_soc",
    # database querying
    "gh3_load",
    "gh3_aggregate",
    "gh3_export",
    "egi_load",
    "egi_aggregate",
    "egi_extract",
    "gh3_to_raster",
    "gh3_rasterize_partitions",
    # sub-modules
    "egi",
    "raster",
    "validation",
]
