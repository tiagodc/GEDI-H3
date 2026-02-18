import os, glob
import geopandas as gpd
from typing import Dict
from datetime import datetime, timezone

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, BUILD_LOG_FILENAME, PARTITION_META_FILENAME
from .utils import now, json_read, json_write, read_vector_file, to_geojson, from_geojson, parse_spatial, merge_spatial, parse_temporal, get_package_version
from .h3utils import intersect_h3_geometries
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

def load_log_data(file_path):
    if os.path.exists(file_path):
        log = json_read(file_path)
        if log.get('status') in ['FAILED', 'INTERRUPTED']:
            log['_resuming'] = True
        return log
    else:
        return {}

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
    _LOG_FILE_NAME = 'gedih3_download_log.json'
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

        # When resuming from FAILED/INTERRUPTED, skip filter merging
        # (resume from where we left off using existing filters)
        if self.log_data.get('_resuming'):
            return

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
    _LOG_FILE_NAME = BUILD_LOG_FILENAME
    _PARENT_DIR = GH3_DEFAULT_H3_DIR

    def __init__(self, product_vars, spatial=None, temporal=None, res:int=12, part:int=3, version:int=None, dir=None, source_mode=None):
        if dir is not None:
            self._PARENT_DIR = dir

        if product_vars:
            product_vars = gedi_vars_expand(product_vars)

        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.updating = False
        self.source_mode = source_mode
        self.build_start_time = datetime.now(timezone.utc)
        self.previous_status = self.log_data.get('status')

        if not self.log_data:
            self.product_vars = product_vars
            self.spatial = parse_spatial(spatial)
            self.temporal = parse_temporal(temporal)
            self.res = res
            self.part = part
            self.gedi_version = version
            return

        self._load_filters_from_log()

        self.updating = True
        self.new_spatial = None
        self.new_temporal = None
        self.new_product_vars = None

        # When resuming from FAILED/INTERRUPTED, skip filter merging
        # (resume from where we left off using existing filters)
        if self.log_data.get('_resuming'):
            return

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
        self.res = self.log_data.get('h3_resolution_level')
        self.part = self.log_data.get('h3_partition_level')
        self.gedi_version = self.log_data.get('gedi_version')

        if 'granules' in self.log_data:
            self.granule_info = self.log_data.get('granules')

        if 'h3_columns' in self.log_data:
            self.h3_columns = self.log_data.get('h3_columns')

        if 'h3_partition_ids' in self.log_data:
            self.h3_partition_ids = self.log_data.get('h3_partition_ids')

        if 'date_range' in self.log_data:
            self.date_range = self.log_data.get('date_range')

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

    def _adding_h3_parts(self):
        if not hasattr(self, 'h3_partition_ids'):
            return True
        
        if self.new_spatial is None:
            return False

        new_h3_parts = set(intersect_h3_geometries(self.new_spatial, res=self.part))
        existing_parts = set(self.h3_partition_ids)

        return not new_h3_parts.issubset(existing_parts)

    def get_finished_granules(self):
        if (hasattr(self, 'granule_info')
                and self.new_product_vars is None
                and self.new_temporal is None
                and not self._adding_h3_parts()):
            return self.granule_info
        return None

    def set_post_build_info(self):
        metadata_files = glob.glob(os.path.join(self._PARENT_DIR, '*', f'*{PARTITION_META_FILENAME}'))
        if len(metadata_files) == 0:
            return   
        
        granule_info = []
        h3_parts = []        
        date_min = None
        date_max = None
        for f in metadata_files:
            fmeta = json_read(f)
            gran = fmeta.get('granules', [])
            drange = fmeta.get('date_range')
            
            if date_min is None:
                date_min = drange[0]
            if date_max is None:
                date_max = drange[1]
                
            date_min = min(date_min, drange[0])
            date_max = max(date_max, drange[1])
            
            h3_parts.append(fmeta.get('h3_partition'))
            for g in gran:
                if g not in granule_info:
                    granule_info.append(g)

        self.date_range = (date_min, date_max)
        self.granule_info = granule_info
        self.h3_columns = fmeta.get('columns')
        self.h3_partition_ids = h3_parts

    def to_dict(self, status):
        if status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of {_VALID_STATUSES}")
        
        product_logs = self.log_data.get('products', {})
        for prod in self.get_product_vars().keys():
            product_logs[prod] = product_logs.get(prod, {})
            product_logs[prod]['status'] = status
            product_logs[prod]['last_modified'] = now()
            product_logs[prod]['variables'] = self.product_vars.get(prod)
        
        # Calculate build duration
        build_duration = None
        if self.build_start_time:
            build_duration = (datetime.now(timezone.utc) - self.build_start_time).total_seconds()

        log_dict = {
            'metadata': {
                'package_version': get_package_version()
            },
            'gedi_version': self.gedi_version,
            'h3_resolution_level': self.res,
            'h3_partition_level': self.part,
            'status': status,
            'previous_status': self.previous_status,
            'source_mode': self.source_mode,
            'last_modified': now(),
            'build_duration_seconds': build_duration,
            'spatial_filter': None if self.spatial is None else to_geojson(self.spatial),
            'temporal_filter': self.temporal,
            'products': product_logs
        }

        if hasattr(self, 'granule_info'):
            log_dict['granules'] = self.granule_info

        if hasattr(self, 'h3_columns'):
            log_dict['h3_columns'] = self.h3_columns

        if hasattr(self, 'h3_partition_ids'):
            log_dict['h3_partition_ids'] = self.h3_partition_ids

        if hasattr(self, 'date_range'):
            log_dict['date_range'] = self.date_range

        return log_dict

    def save_log(self, status):
        json_write(self.to_dict(status), self.log_file, mode='w', rewrite=True)