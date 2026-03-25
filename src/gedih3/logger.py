import os, glob
from typing import Dict
from datetime import datetime, timezone

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, BUILD_LOG_FILENAME, PARTITION_META_FILENAME
from .exceptions import GediValidationError
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
        return json_read(file_path)
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

    def __init__(self, product_vars, spatial=None, temporal=None, version=None, dir=None):
        if dir is not None:
            self._PARENT_DIR = dir

        if product_vars:
            product_vars = gedi_vars_expand(product_vars, version=version)

        self.gedi_version = version
        self.s3_access = False

        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.updating = False

        if not self.log_data:
            self.product_vars = product_vars
            self.spatial = parse_spatial(spatial)
            self.temporal = parse_temporal(temporal)
            self.new_spatial = None
            self.new_temporal = None
            self.new_product_vars = None
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
        self.gedi_version = self.log_data.get('gedi_version', self.gedi_version)
        self.s3_access = self.log_data.get('s3_access', False)

        if 'granules' in self.log_data:
            self.granule_info = self.log_data.get('granules')

    def register_pending_granules(self, granule_list):
        """Register granules as PENDING. Called before download starts.

        Parameters
        ----------
        granule_list : list of dict
            Each dict must have 'orbit', 'granule', 'track' keys.
        """
        if not hasattr(self, 'granule_info'):
            self.granule_info = []
        existing = {(g['orbit'], g['granule'], g['track']) for g in self.granule_info}
        for g in granule_list:
            key = (g['orbit'], g['granule'], g['track'])
            if key not in existing:
                self.granule_info.append({**g, 'status': 'PENDING'})
                existing.add(key)

    def update_granule_status(self, gran_key, status):
        """Update status of a single granule (does NOT auto-save).

        Caller controls save frequency via explicit save_log() calls.

        Parameters
        ----------
        gran_key : dict
            Dict with 'orbit', 'granule', 'track' keys.
        status : str
            'DOWNLOADED' or 'FAILED'.
        """
        if not hasattr(self, 'granule_info'):
            return
        key = (gran_key['orbit'], gran_key['granule'], gran_key['track'])
        for g in self.granule_info:
            if (g['orbit'], g['granule'], g['track']) == key:
                g['status'] = status
                break

    def set_post_download_info(self):
        """Scan SOC directory and record downloaded granules."""
        soc_files = soc_file_tree(self._PARENT_DIR, to_list=True)
        granule_info = []
        for soc in soc_files:
            first_file = list(soc.values())[0]
            gfile = GEDIFile(first_file)
            gran = {'orbit': gfile.orbit, 'granule': gfile.orbit_granule, 'track': gfile.track}
            if gran not in granule_info:
                granule_info.append(gran)
            if self.gedi_version is None:
                self.gedi_version = gfile.version
        # Merge with existing tracked granules (preserve status from real-time tracking)
        if hasattr(self, 'granule_info') and self.granule_info:
            existing = {(g['orbit'], g['granule'], g['track']): g for g in self.granule_info}
            for g in granule_info:
                key = (g['orbit'], g['granule'], g['track'])
                if key in existing:
                    existing[key].update({k: v for k, v in g.items() if k != 'status'})
                    if existing[key].get('status') != 'FAILED':
                        existing[key]['status'] = 'DOWNLOADED'
                else:
                    existing[key] = {**g, 'status': 'DOWNLOADED'}
            self.granule_info = list(existing.values())
        else:
            self.granule_info = [{**g, 'status': 'DOWNLOADED'} for g in granule_info]

    def get_finished_granules(self):
        """Return skip list when resuming with same filters.

        Only returns successfully completed granules, with the status
        field stripped for backward compatibility with _filter_granules()
        dict comparison. Legacy logs without status fields are treated
        as successful.
        """
        if (hasattr(self, 'granule_info')
                and getattr(self, 'new_product_vars', None) is None
                and getattr(self, 'new_temporal', None) is None):
            return [
                {k: v for k, v in g.items() if k != 'status'}
                for g in self.granule_info
                if g.get('status') in ('DOWNLOADED', None)
            ]
        return None

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
            vars_list = self.product_vars.get(prod)
            product_logs[prod]['variables'] = sorted(vars_list) if vars_list else vars_list

        log_dict = {
            'metadata': {
                'package_version': get_package_version()
            },
            'gedi_version': self.gedi_version,
            'status': status,
            'last_modified': now(),
            'spatial_filter': None if self.spatial is None else to_geojson(self.spatial),
            'temporal_filter': self.temporal,
            "s3_access": self.s3_access,
            'products': product_logs
        }

        if hasattr(self, 'granule_info'):
            log_dict['granules'] = self.granule_info

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
            product_vars = gedi_vars_expand(product_vars, version=version)

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
            self.new_spatial = None
            self.new_temporal = None
            self.new_product_vars = None
            return

        self._load_filters_from_log()

        if version is not None and self.gedi_version is not None and version != self.gedi_version:
            raise GediValidationError(
                f"GEDI version mismatch: existing database is version {self.gedi_version}, "
                f"but version {version} was requested. Different versions require separate databases."
            )

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

    def register_pending_granules(self, granule_list):
        """Register granules as PENDING. Called before build starts.

        Parameters
        ----------
        granule_list : list of dict
            Each dict must have 'orbit', 'granule', 'track' keys.
        """
        if not hasattr(self, 'granule_info'):
            self.granule_info = []
        existing = {(g['orbit'], g['granule'], g['track']) for g in self.granule_info}
        for g in granule_list:
            key = (g['orbit'], g['granule'], g['track'])
            if key not in existing:
                self.granule_info.append({**g, 'status': 'PENDING'})
                existing.add(key)

    def update_granule_status(self, gran_key, status):
        """Update status of a single granule (does NOT auto-save).

        Caller controls save frequency via explicit save_log() calls.

        Parameters
        ----------
        gran_key : dict
            Dict with 'orbit', 'granule', 'track' keys.
        status : str
            'INDEXED' or 'PENDING'.
        """
        if not hasattr(self, 'granule_info'):
            return
        key = (gran_key['orbit'], gran_key['granule'], gran_key['track'])
        for g in self.granule_info:
            if (g['orbit'], g['granule'], g['track']) == key:
                g['status'] = status
                break

    def get_finished_granules(self):
        """Return skip list when resuming with same filters.

        Only returns successfully completed granules, with the status
        field stripped for backward compatibility with _filter_granules()
        dict comparison. Legacy logs without status fields are treated
        as successful.
        """
        if (hasattr(self, 'granule_info')
                and getattr(self, 'new_product_vars', None) is None
                and getattr(self, 'new_temporal', None) is None
                and not self._adding_h3_parts()):
            return [
                {k: v for k, v in g.items() if k != 'status'}
                for g in self.granule_info
                if g.get('status') in ('INDEXED', None)
            ]
        return None

    def is_up_to_date(self):
        """Check if database already matches requested parameters.

        Returns True when all of these hold:
        - Log exists (updating=True)
        - No new products, spatial, or temporal changes detected
        - No pending variable update from a previous crash
        - All tracked granules are INDEXED
        """
        if not self.updating:
            return False
        if getattr(self, 'new_product_vars', None) is not None:
            return False
        if getattr(self, 'new_spatial', None) is not None:
            return False
        if getattr(self, 'new_temporal', None) is not None:
            return False
        if self.log_data.get('_pending_variable_update'):
            return False
        if hasattr(self, 'granule_info') and self.granule_info:
            if any(g.get('status') not in ('INDEXED', None) for g in self.granule_info):
                return False
        return True

    def set_post_build_info(self):
        metadata_files = glob.glob(os.path.join(self._PARENT_DIR, '*', f'*{PARTITION_META_FILENAME}'))
        if len(metadata_files) == 0:
            return

        # Collect indexed granules from partition metadata
        indexed_granules = []
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
                if g not in indexed_granules:
                    indexed_granules.append(g)
            if self.gedi_version is None:
                self.gedi_version = fmeta.get('l2a_version')

        self.date_range = (date_min, date_max)
        self.h3_columns = sorted(fmeta.get('columns', []))
        self.h3_partition_ids = sorted(h3_parts)

        # Merge with existing tracked granules (preserve PENDING for unindexed)
        indexed_keys = {(g['orbit'], g['granule'], g['track']) for g in indexed_granules}
        if hasattr(self, 'granule_info') and self.granule_info:
            for g in self.granule_info:
                key = (g['orbit'], g['granule'], g['track'])
                if key in indexed_keys:
                    g['status'] = 'INDEXED'
                # else: keep existing status (PENDING)

            # Add any newly discovered granules not previously tracked
            existing_keys = {(g['orbit'], g['granule'], g['track']) for g in self.granule_info}
            for g in indexed_granules:
                key = (g['orbit'], g['granule'], g['track'])
                if key not in existing_keys:
                    self.granule_info.append({**g, 'status': 'INDEXED'})
        else:
            self.granule_info = [{**g, 'status': 'INDEXED'} for g in indexed_granules]

        self.granule_info = sorted(self.granule_info, key=lambda g: (g.get('orbit', 0), g.get('granule', 0), g.get('track', 0)))

    def to_dict(self, status):
        if status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of {_VALID_STATUSES}")
        
        product_logs = self.log_data.get('products', {})
        for prod in self.get_product_vars().keys():
            product_logs[prod] = product_logs.get(prod, {})
            product_logs[prod]['status'] = status
            product_logs[prod]['last_modified'] = now()
            vars_list = self.product_vars.get(prod)
            product_logs[prod]['variables'] = sorted(vars_list) if vars_list else vars_list

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

        # Append to update history on terminal statuses
        if status in ('COMPLETED', 'FAILED', 'INTERRUPTED'):
            action = 'variable_update' if getattr(self, 'new_product_vars', None) and getattr(self, 'new_spatial', None) is None and getattr(self, 'new_temporal', None) is None else 'build'
            update_entry = {
                'timestamp': now(),
                'action': action,
                'status': status,
                'duration_seconds': build_duration,
                'new_products': sorted(self.new_product_vars.keys()) if getattr(self, 'new_product_vars', None) else None,
            }
            existing_history = self.log_data.get('update_history', [])
            existing_history.append(update_entry)
            log_dict['update_history'] = existing_history[-50:]  # Keep last 50

        # Preserve pending variable update state for crash recovery (P0-B)
        if '_pending_variable_update' in self.log_data:
            log_dict['_pending_variable_update'] = self.log_data['_pending_variable_update']

        return log_dict

    def save_log(self, status):
        json_write(self.to_dict(status), self.log_file, mode='w', rewrite=True)