import os
import geopandas as gpd
from typing import Dict
from datetime import datetime

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR
from .utils import json_read, json_write, read_vector_file, to_geojson, from_geojson
from .gedidriver import GEDIFile, gedi_vars_expand, soc_file_tree, check_soc_file_vars, validate_soc_files
from .gh3driver import gh3_list_files

_VALID_STATUSES = ('INITIALIZING', 'DOWNLOADING','PROCESSING', 'PARTITIONING', 'MERGING', 'COMPLETED', 'FAILED', 'INTERRUPTED', 'UNKNOWN')

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

def load_log_data(file_path):
    if os.path.exists(file_path):
        return json_read(file_path)
    else:
        return {}

def now():
    return datetime.now().isoformat()

def parse_spatial(spatial):
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

def merge_spatial(existing, new):
    if new is None:
        return existing, None
    
    new = parse_spatial(new)
    
    if existing is None:
        return new, None

    gdf_union = gpd.overlay(existing, new, how='union').union_all()
    gdf_union = gpd.GeoDataFrame(geometry=[gdf_union], crs=existing.crs)
    
    gdf_sdiff = gpd.overlay(existing, new, how='symmetric_difference').union_all()
    gdf_sdiff = gpd.GeoDataFrame(geometry=[gdf_sdiff], crs=existing.crs)

    if gdf_sdiff.geometry.iloc[0].is_empty:
        gdf_sdiff = None

    return gdf_union, gdf_sdiff

def parse_temporal(temporal):
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

def merge_temporal(existing, new):
    if existing is None:
        return None, None

    if new is None:
        return existing, None
    
    new = parse_temporal(new)
    
    start1, end1 = existing
    start2, end2 = new

    new_start = None if None in (start1, start2) else min(start1, start2)
    new_end = None if None in (end1, end2) else max(end1, end2)    
    
    if new_start is None and new_end is None:
        return None, None
    
    t_full = (new_start, new_end)
    t_diff = list(t_full)
    
    if new_start is not None and new_start < start1:
        t_diff[0] = new_start
    if new_end is not None and new_end > end1:
        t_diff[1] = new_end
    
    t_diff = tuple(t_diff)
    if t_full == t_diff:
        t_diff = None    
    
    return t_full, t_diff

def merge_product_vars(existing_product_vars, new_product_vars=None):
    if not new_product_vars:
        return existing_product_vars, None

    merged_product_vars = existing_product_vars.copy()

    update_prods = set()
    for prod, vars_list in new_product_vars.items():
        if prod not in merged_product_vars:
            merged_product_vars[prod] = vars_list
            update_prods.add(prod)
        elif merged_product_vars[prod] is None or vars_list is None:
            merged_product_vars[prod] = None
            if vars_list is None and existing_product_vars[prod] is not None:
                update_prods.add(prod)
        else:
            existing_set = set(merged_product_vars[prod])
            new_set = set(vars_list)
            merged_set = existing_set | new_set
            merged_product_vars[prod] = list(merged_set)
            if merged_set != existing_set:
                update_prods.add(prod)

    update_prods = list(update_prods) if len(update_prods) > 0 else None
    return merged_product_vars, update_prods

class SOCDownloadLogger:
    _LOG_FILE_NAME = 'download_log.json'
    _PARENT_DIR = GH3_DEFAULT_SOC_DIR

    def __init__(self, product_vars, spatial=None, temporal=None, dir=None):
        if dir is not None:
            self._PARENT_DIR = dir

        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.update = False
        
        if not self.log_data:
            self.product_vars = gedi_vars_expand(product_vars)
            self.spatial = parse_spatial(spatial)
            self.temporal = parse_temporal(temporal)
            return
        
        self._load_filters_from_log()
        
        self.new_spatial = None
        self.new_temporal = None
        self.update_products = None
        
        if spatial is not None:
            self.spatial, self.new_spatial = merge_spatial(self.spatial, spatial)
        if temporal is not None:
            self.temporal, self.new_temporal = merge_temporal(self.temporal, temporal)
        if product_vars:
            self.product_vars, self.update_products = merge_product_vars(self.product_vars, product_vars)

        self.set_update_status()

    def _load_filters_from_log(self):
        self.product_vars = self.log_data.get('product_vars', {})
        self.spatial = parse_spatial(self.log_data.get('spatial_filter', None))
        self.temporal = parse_temporal(self.log_data.get('temporal_filter', None))
        
    def set_update_status(self):
        self.update = self.new_spatial is not None or self.new_temporal is not None or self.update_products is not None

class H3BuildLogger:
    _LOG_FILE_NAME = 'build_log.json'
    _VALID_STATUSES = ('INITIALIZING', 'DOWNLOADING','PROCESSING', 'PARTITIONING', 'MERGING', 'COMPLETED', 'FAILED', 'INTERRUPTED', 'UNKNOWN')

    def __init__(self, product_vars, res:int=12, part:int=3, spatial=None, temporal=None, db_type='both'):
        self.odir = GH3_DEFAULT_DOWNLOAD_DIR
        self.set_db_type(db_type)
        
        self.s3_access = False  # Placeholder for future S3 access handling
        self.soc_data = {}
        self.h3_data = {}
        self.processing_data = {}

        self.log_file = os.path.join(self.odir, self._LOG_FILE_NAME)
        log_data = self._load_log_data()
        self._parse_log(log_data)

        if not resuming and not log_data:
            self.spatial = self._process_spatial(spatial)
            self.temporal = self._process_temporal(temporal)
            self.product_vars = gedi_vars_expand(product_vars)

        if self.db_type in ('h3', 'both'):
            self._set_h3_resolutions(res, part)

        if update:
            self._merge_spatial(spatial)
            self._merge_temporal(temporal)
            self._merge_product_vars(product_vars)
                
    
    def set_db_type(self, db):
        db = db.lower()
        if db not in self._VALID_DB_TYPES:
            raise ValueError(f"Invalid db_type '{db}'. Must be one of {self._VALID_DB_TYPES}")
        self.db_type = db
    
    def _set_h3_resolutions(self, res=None, part=None):
        self.res = self.h3_data.get("h3_resolution_level", res)
        self.part = self.h3_data.get("h3_partition_level", part)
        
        if not (self.res and self.part):
            raise ValueError("H3 resolution and partition levels must be specified for H3 database.")
          
        self.h3_data['h3_resolution_level'] = self.res
        self.h3_data['h3_partition_level'] = self.part
    
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
        
        self.product_vars = log_data.get(self.db_type,{}).get("products",{})
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

    def _merge_product_vars(self, new_product_vars):
        if not new_product_vars:
            return
        
        for prod, vars_list in new_product_vars.items():
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
        
        if which_product is None:
            if self.db_type in ('soc', 'both'):
                self.soc_data['status'] = new_status
            if self.db_type in ('h3', 'both'):
                self.h3_data['status'] = new_status
        
    def get_product_info(self, prod: str, status: str = 'COMPLETED'):
        prod = prod.upper()
        
        if prod not in GEDI_PRODUCTS:
            raise ValueError(f"Invalid product '{prod}'. Must be one of {list(GEDI_PRODUCTS.keys())}")        
        
        if self.db_type in ('soc', 'both'):
            self.soc_data.setdefault('products', {})
            self.soc_data['s3_access'] = self.s3_access
            
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
            self.h3_data.setdefault('products', {})
            
            self.h3_data['products'].setdefault(prod, {})
            self.h3_data['last_modified'] = self._now()
            
            prod_log = self.h3_data['products'][prod]
            prod_log.setdefault('soc_version_info', {})            
            prod_log['soc_version_info']['version'] = GEDI_PRODUCTS[prod]['version']
            
            prod_log['status'] = status
            prod_log['last_modified'] = self._now()
            prod_log['variables'] = self.product_vars.get(prod) # change to h3 vars after build
                
            h3_files = gh3_list_files(GH3_DEFAULT_H3_DIR, product=prod)
                        
            prod_log['file_count'] = len(h3_files)
            prod_log['size_gb'] = sum((os.path.getsize(f) / 1e9) for f in h3_files)
            prod_log['date_range'] = None
            prod_log['orbit_range'] = None
    
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
                self.set_status('COMPLETED')
        
        if self.db_type in ('h3', 'both'):
            pass # Placeholder for future H3-specific logging

    def to_dict(self):
        """Convert logger to dictionary format for JSON serialization"""
        log_dict = {
            'spatial_filter': to_geojson(self.spatial),
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