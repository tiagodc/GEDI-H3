import os
import geopandas as gpd
from typing import Dict
from datetime import datetime

from .config import GEDI_PRODUCTS
from .utils import json_read, json_write, read_as_geojson, to_geojson
from .gedidriver import gedi_vars_expand

def get_package_version():
    """Get the current package version"""
    try:
        from importlib.metadata import version
        return version('gedih3')
    except ImportError:
        try:
            from . import __version__
            return __version__
        except:
            return "unknown"


class H3BuildLogger:
    """
    Logger class for managing SOC and H3 database operations and metadata.

    This class creates and maintains a structured JSON log file containing
    information about both SOC (Science Operations Center) and H3 (Hexagonal
    Hierarchical Geospatial Indexing) databases.

    The log file structure is:
    {
        "soc": {
            "config": {...},
            "products": {...},
            "status": "..."
        },
        "h3": {
            "config": {...},
            "products": {...},
            "status": "..."
        },
        "metadata": {...}
    }
    """

    _LOG_FILE_NAME = 'build_log.json'
    _VALID_STATUSES = ('INITIALIZED', 'DOWNLOADING', 'PARTITIONING', 'MERGING', 'COMPLETED', 'FAILED')
    _VALID_DB_TYPES = ('soc', 'h3', 'both')

    def __init__(self, odir, prod_vars, res=12, part=3, spatial=None, temporal=None, resume=False, update=False, db_type='both'):
        """
        Initialize the H3BuildLogger.

        Parameters:
        -----------
        odir : str
            Output directory where the log file will be saved
        prod_vars : dict
            Product variables dictionary
        res : int, default=12
            H3 resolution
        part : int, default=3
            H3 partition level
        spatial : various, optional
            Spatial filter (bounding box, file path, or GeoDataFrame)
        temporal : tuple, optional
            Temporal filter (start_date, end_date)
        resume : bool, default=False
            Whether to resume from existing log
        update : bool, default=False
            Whether to update existing log with new parameters
        db_type : str, default='both'
            Database type to manage ('soc', 'h3', or 'both')
        """
        self.odir = odir
        self.db_type = db_type.lower()
        prod_vars = gedi_vars_expand(prod_vars)

        if self.db_type not in self._VALID_DB_TYPES:
            raise ValueError(f"Invalid db_type '{db_type}'. Must be one of {self._VALID_DB_TYPES}")

        self.log_file = os.path.join(self.odir, self._LOG_FILE_NAME)
        self.current_timestamp = datetime.now().isoformat()

        # Initialize log structure
        log_data = self._load_log_data()

        # Process spatial filter
        processed_spatial = self._process_spatial(spatial)

        # Handle resume/update logic based on database type
        if resume and log_data:
            self._handle_resume_mode(log_data, prod_vars, res, part, processed_spatial, temporal)
        elif update and log_data:
            self._handle_update_mode(log_data, prod_vars, res, part, processed_spatial, temporal)
        else:
            self._handle_normal_mode(prod_vars, res, part, processed_spatial, temporal)

        # Initialize database sections
        self._initialize_database_sections(log_data)

    def _load_log_data(self):
        """Load existing log data or initialize empty structure"""
        if os.path.exists(self.log_file):
            log_data = json_read(self.log_file)
            # Ensure proper structure
            if 'soc' not in log_data:
                log_data['soc'] = {}
            if 'h3' not in log_data:
                log_data['h3'] = {}
            return log_data
        else:
            return {'soc': {}, 'h3': {}}

    def _initialize_database_sections(self, log_data):
        """Initialize SOC and H3 database sections"""
        self.soc_data = log_data.get('soc', {})
        self.h3_data = log_data.get('h3', {})

        # Initialize SOC section if working with SOC database
        if self.db_type in ('soc', 'both'):
            if 'config' not in self.soc_data:
                self.soc_data['config'] = {}
            if 'products' not in self.soc_data:
                self.soc_data['products'] = {}
            if 'status' not in self.soc_data:
                self.soc_data['status'] = 'INITIALIZED'

            # Update SOC configuration
            self.soc_data['config'].update({
                'product_variables': self.prod_vars,
                'spatial_filter': self.spatial,
                'temporal_filter': self.temporal,
                'last_updated': self.current_timestamp
            })

        # Initialize H3 section if working with H3 database
        if self.db_type in ('h3', 'both'):
            if 'config' not in self.h3_data:
                self.h3_data['config'] = {}
            if 'products' not in self.h3_data:
                self.h3_data['products'] = {}
            if 'status' not in self.h3_data:
                self.h3_data['status'] = 'INITIALIZED'

            # Update H3 configuration
            self.h3_data['config'].update({
                'product_variables': self.prod_vars,
                'h3_resolution': self.res,
                'h3_partition': self.part,
                'spatial_filter': self.spatial,
                'temporal_filter': self.temporal,
                'last_updated': self.current_timestamp
            })

    def _handle_resume_mode(self, log_data, prod_vars, res, part, spatial, temporal):
        """Handle resume mode - use existing parameters"""
        soc_config = log_data.get('soc', {}).get('config', {})
        h3_config = log_data.get('h3', {}).get('config', {})

        # Use SOC config if available, otherwise H3 config, otherwise provided params
        if self.db_type == 'soc' and soc_config:
            self.prod_vars = soc_config.get('product_variables', prod_vars)
            self.spatial = soc_config.get('spatial_filter', spatial)
            self.temporal = soc_config.get('temporal_filter', temporal)
        elif self.db_type == 'h3' and h3_config:
            self.prod_vars = h3_config.get('product_variables', prod_vars)
            self.res = h3_config.get('h3_resolution', res)
            self.part = h3_config.get('h3_partition', part)
            self.spatial = h3_config.get('spatial_filter', spatial)
            self.temporal = h3_config.get('temporal_filter', temporal)
        elif self.db_type == 'both':
            # For both, prefer H3 config, fallback to SOC config
            config = h3_config if h3_config else soc_config
            self.prod_vars = config.get('product_variables', prod_vars)
            self.res = config.get('h3_resolution', res) if 'h3_resolution' in config else res
            self.part = config.get('h3_partition', part) if 'h3_partition' in config else part
            self.spatial = config.get('spatial_filter', spatial)
            self.temporal = config.get('temporal_filter', temporal)
        else:
            self._handle_normal_mode(prod_vars, res, part, spatial, temporal)

    def _handle_update_mode(self, log_data, prod_vars, res, part, spatial, temporal):
        """Handle update mode - merge new parameters with existing ones"""
        soc_config = log_data.get('soc', {}).get('config', {})
        h3_config = log_data.get('h3', {}).get('config', {})

        if self.db_type in ('soc', 'both') and soc_config:
            existing_vars = soc_config.get('product_variables', {})
            self.prod_vars = self._merge_product_vars(existing_vars, prod_vars)
            self.spatial = self._merge_spatial(soc_config.get('spatial_filter'), spatial)
            self.temporal = self._merge_temporal(soc_config.get('temporal_filter'), temporal)
        else:
            self.prod_vars = prod_vars
            self.spatial = spatial
            self.temporal = temporal

        if self.db_type in ('h3', 'both') and h3_config:
            self.res = h3_config.get('h3_resolution', res)
            self.part = h3_config.get('h3_partition', part)
        else:
            self.res = res
            self.part = part

    def _handle_normal_mode(self, prod_vars, res, part, spatial, temporal):
        """Handle normal initialization mode"""
        self.prod_vars = prod_vars
        self.res = res
        self.part = part
        self.spatial = spatial
        self.temporal = temporal

    def _process_spatial(self, spatial):
        """Process spatial input into standardized format"""
        if spatial is None:
            return None

        if isinstance(spatial, dict) and 'shapefile' in spatial:
            pass
        elif isinstance(spatial, list) and len(spatial) == 4:
            spatial = tuple(spatial)
        elif isinstance(spatial, str):
            spatial = read_as_geojson(spatial)
        elif isinstance(spatial, gpd.GeoDataFrame):
            spatial = to_geojson(spatial.to_crs(4326))
        else:
            raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
        return spatial

    def _merge_product_vars(self, existing_vars, new_vars):
        """Merge new product variables with existing ones"""
        if not existing_vars:
            return new_vars
        if not new_vars:
            return existing_vars

        merged = existing_vars.copy()
        for prod, vars_list in new_vars.items():
            if prod not in merged:
                merged[prod] = vars_list
            elif vars_list is not None:
                if merged[prod] is None:
                    merged[prod] = vars_list
                else:
                    existing_set = set(merged[prod]) if merged[prod] else set()
                    new_set = set(vars_list) if vars_list else set()
                    merged[prod] = list(existing_set | new_set)
        return merged

    def _merge_temporal(self, existing_temporal, new_temporal):
        """Merge temporal filters - expand time range to include both"""
        if not existing_temporal:
            return new_temporal
        if not new_temporal:
            return existing_temporal

        # Parse existing temporal
        if isinstance(existing_temporal, (list, tuple)):
            start1, end1 = existing_temporal
        else:
            return new_temporal

        # Parse new temporal
        if isinstance(new_temporal, (list, tuple)):
            start2, end2 = new_temporal
        else:
            return existing_temporal

        # Convert to datetime for comparison
        start1_dt = datetime.fromisoformat(start1.replace('Z', '+00:00')) if isinstance(start1, str) else start1
        end1_dt = datetime.fromisoformat(end1.replace('Z', '+00:00')) if isinstance(end1, str) else end1
        start2_dt = datetime.fromisoformat(start2.replace('Z', '+00:00')) if isinstance(start2, str) else start2
        end2_dt = datetime.fromisoformat(end2.replace('Z', '+00:00')) if isinstance(end2, str) else end2

        # Take earliest start and latest end
        merged_start = min(start1_dt, start2_dt)
        merged_end = max(end1_dt, end2_dt)

        # Return in same format as input
        return (merged_start.strftime('%Y-%m-%d'), merged_end.strftime('%Y-%m-%d'))

    def _merge_spatial(self, existing_spatial, new_spatial):
        """Merge spatial filters - union of geometries"""
        if not existing_spatial:
            return self._process_spatial(new_spatial)
        if not new_spatial:
            return existing_spatial

        # For now, keep the existing spatial filter when merging
        # More complex spatial merging could be implemented later
        return existing_spatial

    def set_status(self, new_status: str, db_target=None):
        """Set status for specific database or both"""
        if new_status not in self._VALID_STATUSES:
            raise ValueError(f"Invalid status '{new_status}'. Must be one of {self._VALID_STATUSES}")

        if db_target is None:
            db_target = self.db_type

        if db_target in ('soc', 'both') and self.db_type in ('soc', 'both'):
            self.soc_data['status'] = new_status
        if db_target in ('h3', 'both') and self.db_type in ('h3', 'both'):
            self.h3_data['status'] = new_status

    def set_current_level(self, gedi_prod_level: str = None):
        """Set currently building product level"""
        if gedi_prod_level is None:
            if hasattr(self, 'building_product_level'):
                del self.building_product_level
            return
        self.building_product_level = gedi_prod_level.upper()

    def update_product_info(self, product_level: str, info_dict: Dict, db_target=None):
        """Update product information for SOC or H3 database"""
        if db_target is None:
            db_target = self.db_type

        product_level = product_level.upper()
        timestamp = datetime.now().isoformat()

        # Update product info with essential metadata
        product_info = {
            'last_updated': timestamp,
            'variables': info_dict.get('variables', []),
            'file_count': info_dict.get('file_count', 0),
            'size_gb': info_dict.get('size_gb', 0),
        }

        # Add database-specific information
        if db_target in ('soc', 'both') and self.db_type in ('soc', 'both'):
            if 'products' not in self.soc_data:
                self.soc_data['products'] = {}
            self.soc_data['products'][product_level] = product_info.copy()
            self.soc_data['products'][product_level].update({
                'date_range': info_dict.get('date_range', None),
                'orbit_range': info_dict.get('orbit_range', None),
                'version_info': {
                    'doi': GEDI_PRODUCTS.get(product_level, {}).get('doi', ''),
                    'version': GEDI_PRODUCTS.get(product_level, {}).get('version', ''),
                }
            })


        if db_target in ('h3', 'both') and self.db_type in ('h3', 'both'):
            if 'products' not in self.h3_data:
                self.h3_data['products'] = {}
            self.h3_data['products'][product_level] = product_info.copy()
            # Add H3-specific info
            self.h3_data['products'][product_level].update({
                'h3_tiles': info_dict.get('h3_tiles', []),
                'parquet_files': info_dict.get('parquet_files', []),
                'indexed_shots': info_dict.get('indexed_shots', 0),
                'h3_resolution': self.res,
                'h3_partition': self.part
            })

    def get_summary(self):
        """Get comprehensive summary of both databases"""
        summary = {
            'log_file': self.log_file,
            'db_type': self.db_type,
            'last_activity': self.current_timestamp
        }

        if self.db_type in ('soc', 'both'):
            soc_products = list(self.soc_data.get('products', {}).keys())
            summary['soc'] = {
                'status': self.soc_data.get('status', 'UNKNOWN'),
                'products_available': soc_products,
                'total_products': len(soc_products),
                'last_updated': self.soc_data.get('config', {}).get('last_updated', 'Never'),
                'config': self.soc_data.get('config', {})
            }

        if self.db_type in ('h3', 'both'):
            h3_products = list(self.h3_data.get('products', {}).keys())
            summary['h3'] = {
                'status': self.h3_data.get('status', 'UNKNOWN'),
                'products_available': h3_products,
                'total_products': len(h3_products),
                'last_updated': self.h3_data.get('config', {}).get('last_updated', 'Never'),
                'config': self.h3_data.get('config', {})
            }

        return summary

    def to_dict(self):
        """Convert logger to dictionary format for JSON serialization"""
        log_dict = {
            'soc': self.soc_data,
            'h3': self.h3_data,
            'metadata': {
                'package_version': get_package_version(),
                'db_type': self.db_type,
                'last_modified': self.current_timestamp
            }
        }

        # Add legacy fields for backward compatibility if needed
        if hasattr(self, 'building_product_level'):
            log_dict['metadata']['building_product_level'] = self.building_product_level

        return log_dict

    def save_log(self):
        """Save the complete log structure to JSON file"""
        json_write(self.to_dict(), self.log_file, mode='w', rewrite=True)