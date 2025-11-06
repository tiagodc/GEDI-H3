"""
gedih3: GEDI Data Access and H3 Indexing Library

A comprehensive Python library for accessing GEDI (Global Ecosystem Dynamics Investigation)
data from NASA's ORNL DAAC with H3 spatial indexing support, multiple access methods,
spatial/temporal filtering, and on-the-fly subsetting.

Main modules:
- gedih3.config: Package configuration and GEDI product metadata
- gedih3.daac: NASA Earthdata access with GEDIAccessor class and download functions
- gedih3.gedidriver: Low-level GEDI HDF5 file operations and data loading
- gedih3.gh3builder: H3-indexed database building from GEDI data
- gedih3.gh3driver: H3 database query and access functions
- gedih3.h3utils: H3 geospatial indexing utilities
- gedih3.utils: General utility functions for file I/O and geospatial operations

Usage examples:
    >>> import gedih3 as gh3
    >>> gh3.config.GEDI_PRODUCTS
    >>> accessor = gh3.daac.GEDIAccessor()
    >>> gh3.gh3builder.build_h3db(...)

    >>> from gedih3.daac import GEDIAccessor
    >>> from gedih3.config import GEDI_PRODUCTS
"""

__version__ = "0.0.1"
__author__ = "Tiago de Conto"
__email__ = "tiagodc@umd.edu"

# from . import config
# from . import utils
# from . import daac
# from . import h3utils
# from . import sqlutils
# from . import cliutils
# from . import gedidriver
# from . import gh3driver
# from . import gh3builder

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    # "config",
    # "daac",
    # "gedidriver",
    # "gh3builder",
    # "gh3driver",
    # "utils",
    # "h3utils",
    # "sqlutils",
    # "cliutils"
]