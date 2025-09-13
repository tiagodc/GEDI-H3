"""
GEDI Data Access Library

A comprehensive Python library for accessing GEDI (Global Ecosystem Dynamics Investigation) 
data from NASA's ORNL DAAC with support for multiple access methods, spatial/temporal filtering, 
and on-the-fly subsetting.

Main components:
- gedi_access: Core module with GEDIAccessor class and convenience functions
- utils: Utility functions for geospatial operations
"""

from .daac import GEDIAccessor, search_gedi_data, download_gedi_data

__version__ = "1.0.0"
__author__ = "GEDI Access Team"
__email__ = ""

# Make key classes and functions available at package level
__all__ = [
    'GEDIAccessor',
    'search_gedi_data', 
    'download_gedi_data'
]

# Also make configuration available for advanced users
try:
    from .config import (
        GEDI_PRODUCTS, GEDI_DOIS, GEDI_VARIABLES,
        DEFAULT_DOWNLOAD_DIR, DOWNLOAD_METHODS
    )
    __all__.extend([
        'GEDI_PRODUCTS', 'GEDI_DOIS', 'GEDI_VARIABLES',
        'DEFAULT_DOWNLOAD_DIR', 'DOWNLOAD_METHODS'
    ])
except ImportError:
    pass  # Config import failed, continue without it

# Optional: Set up logging
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())