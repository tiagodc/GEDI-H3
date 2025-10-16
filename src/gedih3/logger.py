import os, glob
import geopandas as gpd
from typing import Dict
from datetime import datetime

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR
from .utils import now, json_read, json_write, read_vector_file, to_geojson, from_geojson
from .gedidriver import GEDIFile, gedi_vars_expand, soc_file_tree, check_soc_file_vars, validate_soc_files
from .gh3driver import gh3_list_files

_VALID_STATUSES = (
    'DOWNLOADING',
    'PROCESSING',
    'PARTITIONING',
    'MERGING',
    'COMPLETED',
    'FAILED',
    'INTERRUPTED',
    'UNKNOWN'
)

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
        log =  json_read(file_path)
        if log.get('status') in ['FAILED', 'INTERRUPTED']:
            return {}
        return log
    else:
        return {}

def parse_spatial(spatial):
    if spatial is None:
        return None
    
    if isinstance(spatial, dict):
        spatial = from_geojson(spatial)
    elif isinstance(spatial, str):        
        if os.path.exists(spatial):
            spatial = read_vector_file(spatial, crs=4326)
        else:
            try:
                spatial = from_geojson(spatial)
            except:
                raise ValueError("Invalid spatial input. Must be bounding box list, file path, or GeoDataFrame.")
    elif isinstance(spatial, list) and len(spatial) == 4:
        from shapely.geometry import box
        spatial = gpd.GeoDataFrame(geometry=[box(*spatial)], crs=4326, index=[0])
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
    t_full = (new_start, new_end)
    
    if t_full == (None, None):
        return t_full, t_full

    t_diff = list(t_full)
    
    if new_start is not None and new_start >= start1 and end1 is not None:
        t_diff[0] = end1
    if new_end is not None and new_end <= end1 and start1 is not None:
        t_diff[1] = start1

    t_diff = tuple(t_diff)
    if t_full == t_diff or t_diff[0] >= t_diff[1]:
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

    new_product_vars = None
    if len(update_prods) > 0:
        new_product_vars = {k:val for k, val in merged_product_vars.items() if k in update_prods}

    return merged_product_vars, new_product_vars

class SOCDownloadLogger:
    _LOG_FILE_NAME = 'gh3_download_log.json'
    _PARENT_DIR = GH3_DEFAULT_SOC_DIR

    def __init__(self, product_vars, spatial=None, temporal=None, dir=None):
        if dir is not None:
            self._PARENT_DIR = dir

        if product_vars:
            product_vars = gedi_vars_expand(product_vars)
        
        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.updating = False
        
        if not self.log_data:
            self.product_vars = product_vars
            self.spatial = parse_spatial(spatial)
            self.temporal = parse_temporal(temporal)
            return
        
        self._load_filters_from_log()
        
        self.updating = True
        self.new_spatial = None
        self.new_temporal = None
        self.new_product_vars = None
        
        if temporal is not None:
            self.temporal, self.new_temporal = merge_temporal(self.temporal, temporal)

        if spatial is not None:
            self.spatial, self.new_spatial = merge_spatial(self.spatial, spatial)
        
        if product_vars is not None:
            self.product_vars, self.new_product_vars = merge_product_vars(self.product_vars, product_vars)

    def _load_filters_from_log(self):
        self.product_vars = self.log_data.get('products', {})
        self.product_vars = {k:val.get('variables') for k,val in self.product_vars.items()}
        
        self.spatial = parse_spatial(self.log_data.get('spatial_filter'))
        self.temporal = parse_temporal(self.log_data.get('temporal_filter'))
    
    def get_temporal(self):
        if not self.updating or self.new_temporal is None:
            return self.temporal

        if self.new_spatial is None and self.new_product_vars is None:
            return self.new_temporal

        return self.temporal

    def get_spatial(self):
        if not self.updating or self.new_spatial is None:
            return self.spatial

        if self.new_product_vars is None and self.new_temporal is None:
            return self.new_spatial

        return self.spatial

    def get_product_vars(self):
        if not self.updating or self.new_product_vars is None:
            return self.product_vars

        if self.new_spatial is None and self.new_temporal is None:
            return self.new_product_vars

        return self.product_vars

    def to_dict(self, status):
        if status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of {_VALID_STATUSES}")
        
        product_logs = self.log_data.get('products', {})
        for prod in self.get_product_vars().keys():
            product_logs[prod] = product_logs.get(prod, {})
            product_logs[prod]['status'] = status
            product_logs[prod]['last_modified'] = now()
            product_logs[prod]['variables'] = self.product_vars.get(prod)
        
        log_dict = {
            'metadata': {
                'package_version': get_package_version()
            },
            'status': status,
            'last_modified': now(),
            'spatial_filter': None if self.spatial is None else to_geojson(self.spatial),
            'temporal_filter': self.temporal,
            "s3_access": False,
            'products': product_logs
        }

        return log_dict

    def save_log(self, status):
        json_write(self.to_dict(status), self.log_file, mode='w', rewrite=True)

class H3BuildLogger:
    _LOG_FILE_NAME = 'gh3_build_log.json'
    _PARENT_DIR = GH3_DEFAULT_H3_DIR

    def __init__(self, product_vars, spatial=None, res:int=12, part:int=3, version:int=None, dir=None):
        if dir is not None:
            self._PARENT_DIR = dir
            
        if product_vars:
            product_vars = gedi_vars_expand(product_vars)
            
        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.updating = False
        
        if not self.log_data:
            self.product_vars = product_vars
            self.spatial = parse_spatial(spatial)
            self.res = res
            self.part = part
            self.gedi_version = version
            return
        
        self._load_filters_from_log()
        
        self.updating = True
        self.new_spatial = None
        self.new_product_vars = None        

        if spatial is not None:
            self.spatial, self.new_spatial = merge_spatial(self.spatial, spatial)
        
        if product_vars is not None:
            self.product_vars, self.new_product_vars = merge_product_vars(self.product_vars, product_vars)


    def _load_filters_from_log(self):
        self.product_vars = self.log_data.get('products', {})
        self.product_vars = {k:val.get('variables') for k,val in self.product_vars.items()}        
        
        self.spatial = parse_spatial(self.log_data.get('spatial_filter'))
        self.res = self.log_data.get('h3_resolution_level')
        self.part = self.log_data.get('h3_partition_level')
        self.gedi_version = self.log_data.get('gedi_version')
        
        if 'granules' in self.log_data:
            self.granule_info = self.log_data.get('granules')

    def get_spatial(self):
        if not self.updating or self.new_spatial is None:
            return self.spatial

        if self.new_product_vars is None:
            return self.new_spatial

        return self.spatial

    def get_product_vars(self):
        if not self.updating or self.new_product_vars is None:
            return self.product_vars

        if self.new_spatial is None:
            return self.new_product_vars

        return self.product_vars

    def set_granule_info(self):
        metadata_files = glob.glob(os.path.join(self._PARENT_DIR, '*', '*.metadata.json'))
        if len(metadata_files) == 0:
            return   
        
        granule_info = []
        for f in metadata_files:
            fmeta = json_read(f)
            gran = fmeta.get('granules', []) 
            for g in gran:
                if g not in granule_info:
                    granule_info.append(g)
        
        self.granule_info = granule_info
    
    def to_dict(self, status):
        if status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of {_VALID_STATUSES}")
        
        product_logs = self.log_data.get('products', {})
        for prod in self.get_product_vars().keys():
            product_logs[prod] = product_logs.get(prod, {})
            product_logs[prod]['status'] = status
            product_logs[prod]['last_modified'] = now()
            product_logs[prod]['variables'] = self.product_vars.get(prod)        
        
        log_dict = {
            'metadata': {
                'package_version': get_package_version()
            },
            'gedi_version': self.gedi_version,
            'h3_resolution_level': self.res,
            'h3_partition_level': self.part,
            'status': status,
            'last_modified': now(),
            'spatial_filter': None if self.spatial is None else to_geojson(self.spatial),
            'products': product_logs
        }
        
        if hasattr(self, 'granule_info'):
            log_dict['granules'] = self.granule_info

        return log_dict

    def save_log(self, status):
        json_write(self.to_dict(status), self.log_file, mode='w', rewrite=True)