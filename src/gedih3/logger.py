import os
import geopandas as gpd
from typing import Dict
from datetime import datetime

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR
from .utils import json_read, json_write, read_vector_file, to_geojson, from_geojson
from .gedidriver import GEDIFile, gedi_vars_expand, soc_file_tree, check_soc_file_vars, validate_soc_files

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
    _LOG_FILE_NAME = 'gh3_build_log.json'
    _VALID_STATUSES = ('INITIALIZED', 'DOWNLOADING', 'PARTITIONING', 'MERGING', 'COMPLETED', 'FAILED', 'INTERRUPTED', 'UNKNOWN')
    _VALID_DB_TYPES = ('soc', 'h3', 'both')

    def __init__(self, prod_vars, res=12, part=3, spatial=None, temporal=None, resume=False, update=False, db_type='both'):
        self.odir = GH3_DEFAULT_DOWNLOAD_DIR
        self.db_type = db_type.lower()
        
        self.s3_access = False  # Placeholder for future S3 access handling
        self.soc_data = {}
        self.h3_data = {}
        self.processing_data = {}

        if self.db_type not in self._VALID_DB_TYPES:
            raise ValueError(f"Invalid db_type '{db_type}'. Must be one of {self._VALID_DB_TYPES}")

        self.log_file = os.path.join(self.odir, self._LOG_FILE_NAME)
        log_data = self._load_log_data()
        self._parse_log(log_data)
    
        if update:
            self._merge_spatial(spatial)
            self._merge_temporal(temporal)
            self._merge_product_vars(prod_vars)
        
        if not resume and not update:
            if log_data:
                raise ValueError(f"Log file '{self.log_file}' already exists. Use resume or update mode to modify existing log.")

            self.spatial = self._process_spatial(spatial)
            self.temporal = self._process_temporal(temporal)
            self.product_vars = gedi_vars_expand(prod_vars)
            
            if db_type in ('h3', 'both'):
                self.res = res
                self.part = part

    def _now(self):
        return datetime.now().isoformat()
    
    def _load_log_data(self):
        if os.path.exists(self.log_file):
            return json_read(self.log_file)
        else:
            return {}
    
    def _parse_log(self, log_data):
        if not log_data:
            return
        
        spatial = log_data.get("spatial_filter")
        temporal = log_data.get("temporal_filter")
        
        self.soc_data.update(log_data.get("soc", {}))
        self.h3_data.update(log_data.get("h3", {}))
        
        self.spatial = self._process_spatial(spatial)
        self.temporal = self._process_temporal(temporal)
        
        self.product_vars = log_data.get("soc",{}).get("products",{})
        if self.product_vars:
            self.product_vars = {k:val.get('variables') for k,val in self.product_vars.items()}
    
    def _process_spatial(self, spatial):
        if spatial is None:
            return None
        
        if isinstance(spatial, dict) and 'shapefile' in spatial:
            spatial = from_geojson(spatial)
        elif isinstance(spatial, list) and len(spatial) == 4:
            from shapely.geometry import box
            spatial = gpd.GeoDataFrame(geometry=[box(*spatial)], crs=4326, index=[0])
        elif isinstance(spatial, str):
            if not os.path.exists(spatial):
                raise FileNotFoundError(f"Spatial file '{spatial}' does not exist.")
            spatial = read_vector_file(spatial, crs=4326)
        elif isinstance(spatial, gpd.GeoDataFrame):
            spatial = spatial.to_crs(epsg=4326)
        else:
            raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
        
        return spatial
    
    def _process_temporal(self, temporal):
        if temporal is None:
            return None
        
        if isinstance(temporal, (list, tuple)) and len(temporal) == 2:
            start, end = temporal
            if isinstance(start, str):
                start = datetime.fromisoformat(start.replace('Z', '+00:00'))
                start = start.strftime('%Y-%m-%d')
            if isinstance(end, str):
                end = datetime.fromisoformat(end.replace('Z', '+00:00'))
                end = end.strftime('%Y-%m-%d')
            return (start, end)
        else:
            raise ValueError("Invalid temporal input. Must be a list or tuple of two dates.")
    
    def _merge_spatial(self, new_spatial):
        if new_spatial is None:
            return
        
        if self.spatial is None:
            self.spatial = self._process_spatial(new_spatial)        
        else:
            self.spatial = gpd.overlay(self.spatial, self._process_spatial(new_spatial), how='union')            

    def _merge_temporal(self, new_temporal):
        if new_temporal is None:
            return
        if self.temporal is None:
            self.temporal = self._process_temporal(new_temporal)
        else:
            start1, end1 = self.temporal
            start2, end2 = self._process_temporal(new_temporal)
            
            new_start = None if None in (start1, start2) else min(start1, start2)
            new_end = None if None in (end1, end2) else max(end1, end2)
            
            self.temporal = (new_start, new_end)

    def _merge_product_vars(self, new_prod_vars):
        if not new_prod_vars:
            return
        
        for prod, vars_list in new_prod_vars.items():
            if prod not in self.product_vars:
                self.product_vars[prod] = vars_list
            elif self.product_vars[prod] is None or vars_list is None:
                self.product_vars[prod] = None
            else:
                existing_set = set(self.product_vars[prod])
                new_set = set(vars_list)
                self.product_vars[prod] = list(existing_set | new_set)

    def set_status(self, new_status: str, which_product: str=None):
        if new_status not in self._VALID_STATUSES:
            raise ValueError(f"Invalid status '{new_status}'. Must be one of {self._VALID_STATUSES}")

        self.processing_data = {
            "db_type": self.db_type,
            "product": which_product if which_product else None,
            "status": new_status
        }
        
    def get_product_info(self, prod: str, status: str = 'COMPLETED'):
        prod = prod.upper()
        if prod not in GEDI_PRODUCTS:
            raise ValueError(f"Invalid product '{prod}'. Must be one of {list(GEDI_PRODUCTS.keys())}")        
        
        if self.db_type in ('soc', 'both'):
            self.soc_data.setdefault('products', {})
            self.soc_data['s3_access'] = self.s3_access
            
            if prod is None:
                self.soc_data['status'] = status
            else:
                self.soc_data['products'].setdefault(prod, {})
                self.soc_data['last_modified'] = self._now()
                
                prod_log = self.soc_data['products'][prod]
                prod_log.setdefault('version_info', [])
                
                versions = [i.get('doi') for i in prod_log['version_info']]
                if GEDI_PRODUCTS[prod]['doi'] not in versions:
                    prod_log['version_info'].append({
                        'doi': GEDI_PRODUCTS[prod]['doi'],
                        'version': GEDI_PRODUCTS[prod]['version']
                    })                                                    
                
                prod_log['status'] = status
                prod_log['last_modified'] = self._now()
                prod_log['variables'] = self.product_vars.get(prod)
                
                if not self.s3_access:
                    gedi_files = soc_file_tree(GH3_DEFAULT_SOC_DIR, to_list=True)
        
                    prod_files = [f[prod] for f in gedi_files]
                    gedi_prods = [GEDIFile(f) for f in prod_files if f and os.path.exists(f)]
                    gedi_dates = [f.date.strftime('%Y-%m-%d') for f in gedi_prods]
                    gedi_orbits = [f.orbit for f in gedi_prods]
                                
                    prod_log['file_count'] = len(prod_files)
                    prod_log['size_gb'] = sum(f.file_size for f in gedi_prods)
                    prod_log['date_range'] = (min(gedi_dates), max(gedi_dates))
                    prod_log['orbit_range'] = (min(gedi_orbits), max(gedi_orbits))
            
        if self.db_type in ('h3', 'both'):
            pass # Placeholder for future H3-specific logging
    
    def set_product_info(self, validate: bool = True):
        if self.db_type in ('soc', 'both'):
            self.soc_data['products'] = {}
            if not self.s3_access:
                gedi_files = soc_file_tree(GH3_DEFAULT_SOC_DIR, to_list=True, glob_kwargs=None)
                prod_vars = check_soc_file_vars(soc_file=gedi_files[0], available_products=GEDI_PRODUCTS)
                
                if validate:
                    val_report = validate_soc_files(product_vars=prod_vars, soc_dir=GH3_DEFAULT_SOC_DIR)
                    if not val_report['can_skip']:
                        raise ValueError(f"SOC files validation failed: {val_report['error_msg']}")                
                
                self.product_vars = prod_vars
                
                for prod in self.soc_data['products'].keys():
                    self.get_product_info(prod, status='COMPLETED')
                self.get_product_info(None, status='COMPLETED')
        
        if self.db_type in ('h3', 'both'):
            pass # Placeholder for future H3-specific logging

    def to_dict(self):
        """Convert logger to dictionary format for JSON serialization"""
        log_dict = {
            'spatial_filter': to_geojson(self.spatial) if isinstance(self.spatial, gpd.GeoDataFrame) else self.spatial,
            'temporal_filter': self.temporal,
            'soc': self.soc_data,
            'h3': self.h3_data,
            'processing': self.processing_data,
            'metadata': {
                'package_version': get_package_version(),
                'last_modified': self._now()
            }            
        }

        return log_dict

    def save_log(self):
        json_write(self.to_dict(), self.log_file, mode='w', rewrite=True)