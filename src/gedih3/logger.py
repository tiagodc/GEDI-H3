# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

import os, glob
from typing import Dict
from datetime import datetime, timezone

from .config import GEDI_PRODUCTS, GH3_DEFAULT_DOWNLOAD_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR, BUILD_LOG_FILENAME, PARTITION_META_FILENAME
from .exceptions import GediValidationError
from .utils import now, json_read, json_write, read_vector_file, to_geojson, from_geojson, parse_spatial, merge_spatial, parse_temporal, get_package_version
from .h3utils import intersect_h3_geometries
from .gedidriver import GEDIFile, gedi_vars_expand, soc_file_tree, check_soc_file_vars, validate_soc_files, write_soc_manifest
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

# Per-granule per-product status vocabulary (used by gh3_doctor and lazy upgrade
# of legacy log entries). Top-level granule 'status' remains the worst per-product
# value for backwards compatibility with readers that ignore the products map.
PRODUCT_STATUS_INDEXED = 'INDEXED'
PRODUCT_STATUS_PARTIAL_NAN = 'PARTIAL_NAN'
PRODUCT_STATUS_MISSING_COLUMN = 'MISSING_COLUMN'
PRODUCT_STATUS_MISSING_SOURCE = 'MISSING_SOURCE'
PRODUCT_STATUS_FAILED = 'FAILED'
PRODUCT_STATUS_PENDING = 'PENDING'

_VALID_PRODUCT_STATUSES = (
    PRODUCT_STATUS_INDEXED,
    PRODUCT_STATUS_PARTIAL_NAN,
    PRODUCT_STATUS_MISSING_COLUMN,
    PRODUCT_STATUS_MISSING_SOURCE,
    PRODUCT_STATUS_FAILED,
    PRODUCT_STATUS_PENDING,
)


def _products_from_columns(columns, active_products):
    """Return the subset of ``active_products`` whose suffix appears in ``columns``.

    A product P is considered present if any column ends with ``_<p>`` (the
    suffix convention used by `_add_variables_to_year_file` and `load_h5_merged`).
    """
    if not columns or not active_products:
        return set()
    cols_lower = [str(c).lower() for c in columns]
    return {p for p in active_products if any(c.endswith(f"_{p.lower()}") for c in cols_lower)}


def _per_product_status_from_observed(active_products, observed_products):
    """Map active products to INDEXED if observed, else MISSING_COLUMN.

    PARTIAL_NAN is not derived here because it requires a row-level scan;
    it is populated by gh3_doctor after `_load_filters_from_log`.
    """
    return {
        p: PRODUCT_STATUS_INDEXED if p in observed_products else PRODUCT_STATUS_MISSING_COLUMN
        for p in active_products
    }


def _scan_partition_meta_post_build_info(partition_dir, *, meta_filename, active_products):
    """Worker: read PARTITION_META JSONs under one h3_* partition and return
    the compact aggregate the driver needs to fold into set_post_build_info.

    Module-level so it pickles for dask. Per-file errors are non-fatal — the
    affected file contributes nothing, matching the legacy serial behavior
    (any json_read raise would have aborted the whole scan; we instead skip).
    """
    out = {
        'granules': [],
        'date_min': None,
        'date_max': None,
        'h3_partitions': [],
        'columns': [],
        'l2a_version': None,
        'column_dtypes': {},
        'partition_products': set(),
    }
    cols_union: set = set()
    for mf in glob.glob(os.path.join(partition_dir, f'*{meta_filename}')):
        try:
            fmeta = json_read(mf) or {}
        except Exception:
            continue
        gran = fmeta.get('granules', []) or []
        drange = fmeta.get('date_range')
        cols = fmeta.get('columns', []) or []
        if drange:
            if out['date_min'] is None or drange[0] < out['date_min']:
                out['date_min'] = drange[0]
            if out['date_max'] is None or drange[1] > out['date_max']:
                out['date_max'] = drange[1]
        h3p = fmeta.get('h3_partition')
        if h3p is not None:
            out['h3_partitions'].append(h3p)
        out['granules'].extend(gran)
        out['partition_products'].update(_products_from_columns(cols, active_products))
        if out['l2a_version'] is None and fmeta.get('l2a_version') is not None:
            out['l2a_version'] = fmeta.get('l2a_version')
        cols_union.update(cols)
        cd = fmeta.get('column_dtypes') or {}
        for col_name, dtype in cd.items():
            out['column_dtypes'].setdefault(col_name, dtype)
    out['columns'] = list(cols_union)
    return out


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

        self.gedi_version = version
        self.s3_access = False

        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)
        self.updating = False

        # Resume-mode `default`/`minimal` expansion must target the
        # existing DB's version, not the package fallback (v2). Otherwise
        # `gh3_download -l4c default` against a v3 download tree expands
        # against the v2 manifest and writes the wrong variable names.
        # Peek the log before expansion so an absent `--gedi-version` CLI
        # arg adopts the persisted version.
        effective_version = version
        if version is None and self.log_data:
            effective_version = self.log_data.get('gedi_version', version)

        if product_vars:
            product_vars = gedi_vars_expand(product_vars, version=effective_version)

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
        """Scan SOC directory and record downloaded granules.

        Refreshes ``_soc_manifest.txt`` first so subsequent
        ``soc_file_tree`` calls (here and on the next resume) skip the
        recursive glob over the SOC tree. This is the same pattern the
        H3 database uses via ``MANIFEST_FILENAME``.
        """
        try:
            n_files = write_soc_manifest(self._PARENT_DIR)
        except OSError:
            # Manifest is an optimization, not a correctness gate; if the
            # SOC root is read-only or transiently unavailable the
            # downstream glob fallback still produces correct results.
            # Defer the count to the post-glob fallback below.
            n_files = None
        soc_files = soc_file_tree(self._PARENT_DIR, to_list=True)
        if n_files is None:
            # Manifest write failed; recover the count from the discovery
            # step so downstream summary lines stay correct.
            n_files = sum(len(d) for d in soc_files)
        self.n_files = n_files
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

        # Capture which products were requested via the `default` keyword
        # before expansion replaces it with the literal manifest list. This
        # is the bit the resume-path validator gate needs in `gh3_build.py`:
        # only the static manifest in `src/gedih3/data/GEDI0*_DATASETS_*.txt`
        # is the contract for a `default` request; any other shape (explicit
        # list, file path, `*`/`all`, or unmodified resume) treats the build
        # log as authoritative and skips manifest consultation entirely.
        # Runtime-only — never persisted to disk.
        self.default_products = {
            prod for prod, v in (product_vars or {}).items()
            if isinstance(v, list) and any(
                isinstance(s, str) and s in ('default', 'def') for s in v
            )
        }

        self.log_file = os.path.join(self._PARENT_DIR, self._LOG_FILE_NAME)
        self.log_data = load_log_data(self.log_file)

        # Resume-mode `default`/`minimal` expansion must target the
        # existing DB's gedi_version, not the package fallback (v2).
        # Otherwise `gh3_build -l4c default` against an existing v3 DB
        # expands against the v2 manifest, requesting variable names that
        # don't exist in the v3 h5 files and aborting at validation. Peek
        # the log before expansion so an absent `--gedi-version` CLI arg
        # adopts the persisted version.
        effective_version = version
        if version is None and self.log_data:
            effective_version = self.log_data.get('gedi_version', version)

        if product_vars:
            product_vars = gedi_vars_expand(product_vars, version=effective_version)
        self.updating = False
        self.source_mode = source_mode
        self.build_start_time = datetime.now(timezone.utc)
        self.previous_status = self.log_data.get('status')

        if not self.log_data:
            self.product_vars = product_vars
            self.spatial = parse_spatial(spatial)
            self.temporal = parse_temporal(temporal)
            # Fresh-build fall-back: argparse defaults are now None (so the
            # resume-mismatch check below can be strict). Apply the package
            # defaults here when the user didn't specify them.
            self.res = res if res is not None else 12
            self.part = part if part is not None else 3
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
        # H3 levels are baked into the database layout (partition
        # directory names + per-shot index column). Changing them
        # silently on resume produces an inconsistent DB; refuse to
        # start, matching the gedi_version pattern above. Only fires
        # when the user explicitly passed -h3r/-h3p (argparse defaults
        # are None so a naked resume on a non-default DB is safe).
        if res is not None and self.res is not None and res != self.res:
            raise GediValidationError(
                f"H3 resolution mismatch: existing database has h3_resolution_level={self.res}, "
                f"but -h3r {res} was requested. The H3 index level is baked into the partition "
                f"layout — different levels require separate databases. Drop -h3r to resume against "
                f"the existing level, or build a fresh database in a different output directory."
            )
        if part is not None and self.part is not None and part != self.part:
            raise GediValidationError(
                f"H3 partition mismatch: existing database has h3_partition_level={self.part}, "
                f"but -h3p {part} was requested. The H3 partition level is baked into the directory "
                f"layout — different levels require separate databases. Drop -h3p to resume against "
                f"the existing level, or build a fresh database in a different output directory."
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
            # Lazy upgrade: legacy entries lacking the `products` map get one
            # derived from their top-level status, matching pre-doctor semantics
            # (granule INDEXED ⇒ all then-listed products were INDEXED). The
            # on-disk file is only rewritten on the next save_log() call.
            active_products = list(self.product_vars.keys())
            for g in self.granule_info:
                if 'products' not in g and active_products:
                    existing_status = g.get('status', PRODUCT_STATUS_INDEXED)
                    g['products'] = {p: existing_status for p in active_products}

        if 'h3_columns' in self.log_data:
            self.h3_columns = self.log_data.get('h3_columns')

        if 'h3_columns_dtypes' in self.log_data:
            self.h3_columns_dtypes = dict(self.log_data.get('h3_columns_dtypes') or {})

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
        # No expansion requested → not adding partitions, period. This must
        # be checked BEFORE the h3_partition_ids attribute guard: on a resume
        # after the first write phase was killed before persisting the
        # partition list, `h3_partition_ids` is absent on the loaded log even
        # though we are resuming the same spatial plan. Returning True here
        # would disable the skip filter in get_finished_granules and cause
        # the resume to re-process every already-INDEXED granule.
        if self.new_spatial is None:
            return False

        if not hasattr(self, 'h3_partition_ids'):
            # Expansion requested but we have no recorded partition set to
            # compare against — treat the entire new set as "added".
            return True

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

        Only returns successfully completed granules, with the status and
        products fields stripped for backward compatibility with
        ``_filter_granules()`` dict comparison (which expects bare
        ``{'orbit', 'granule', 'track'}`` dicts). Legacy logs without status
        fields are treated as successful.
        """
        # Keys not part of the original granule identity. ``products`` was
        # added by the per-product status extension; both must be stripped so
        # _filter_granules' dict equality keeps matching legacy entries.
        _strip = {'status', 'products'}
        if (hasattr(self, 'granule_info')
                and getattr(self, 'new_product_vars', None) is None
                and getattr(self, 'new_temporal', None) is None
                and not self._adding_h3_parts()):
            return [
                {k: v for k, v in g.items() if k not in _strip}
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
        # Enumerate partition dirs via os.scandir — replaces the prior
        # glob.glob('*/*<meta>') which paid 10k+ GPFS metadata round-trips
        # in a single driver thread. We then dispatch per-partition JSON
        # reads in parallel via parallel_map so the scan plateaus at
        # ~seconds instead of tens of minutes on continental-scale DBs.
        try:
            partition_dirs = sorted(
                e.path for e in os.scandir(self._PARENT_DIR)
                if e.is_dir(follow_symlinks=False) and e.name.startswith('h3_')
            )
        except OSError:
            return
        if not partition_dirs:
            return

        # Collect indexed granules from partition metadata
        indexed_granules = []
        h3_parts = []
        date_min = None
        date_max = None
        # Per-granule observed product set: union of products whose suffix is
        # present in the columns of any partition that contains the granule.
        # Used to derive per-granule per-product status (INDEXED vs MISSING_COLUMN).
        active_products = set(self.product_vars.keys())
        gran_observed_products = {}
        # Schema accumulators: track the union of columns observed and
        # the first non-empty per-column dtype map. Aggregating inside
        # the loop (rather than reading the last fmeta) survives mixed
        # builds where some partitions predate ``column_dtypes`` — the
        # last metadata in glob order may be a legacy partition without
        # the field, and reading from it would silently zero the cache
        # for the whole DB.
        observed_columns: set = set()
        observed_dtypes: dict = {}

        # Stream per-partition results. parallel_map yields (item, result)
        # pairs as workers finish; we fold them into the aggregates with
        # identical semantics to the legacy serial loop.
        def _fold(part_result):
            nonlocal date_min, date_max
            if part_result.get('date_min') is not None:
                if date_min is None or part_result['date_min'] < date_min:
                    date_min = part_result['date_min']
            if part_result.get('date_max') is not None:
                if date_max is None or part_result['date_max'] > date_max:
                    date_max = part_result['date_max']
            h3_parts.extend(part_result.get('h3_partitions', []))
            partition_products = part_result.get('partition_products') or set()
            seen_keys = set()
            for g in part_result.get('granules', []):
                try:
                    key = (g['orbit'], g['granule'], g['track'])
                except (KeyError, TypeError):
                    continue
                if key not in seen_keys:
                    indexed_granules.append(g)
                    seen_keys.add(key)
                gran_observed_products.setdefault(key, set()).update(partition_products)
            if self.gedi_version is None and part_result.get('l2a_version') is not None:
                self.gedi_version = part_result['l2a_version']
            observed_columns.update(part_result.get('columns', []))
            for col_name, dtype in (part_result.get('column_dtypes') or {}).items():
                observed_dtypes.setdefault(col_name, dtype)

        from .utils import get_dask_client
        client = None
        try:
            client = get_dask_client()
        except Exception:
            pass
        if client is not None and len(partition_dirs) > 100:
            from .parallel import parallel_map
            for _, res in parallel_map(
                partition_dirs,
                _scan_partition_meta_post_build_info,
                desc='Scanning partition metadata',
                unit='part',
                meta_filename=PARTITION_META_FILENAME,
                active_products=active_products,
            ):
                if isinstance(res, Exception):
                    continue
                _fold(res)
        else:
            for pd in partition_dirs:
                _fold(_scan_partition_meta_post_build_info(
                    pd,
                    meta_filename=PARTITION_META_FILENAME,
                    active_products=active_products,
                ))

        # Deduplicate against any granules that may already have appeared
        # in a prior in-memory accumulation — the per-partition seen_keys
        # only dedupes within one partition's result.
        _seen = set()
        _deduped = []
        for g in indexed_granules:
            try:
                k = (g['orbit'], g['granule'], g['track'])
            except (KeyError, TypeError):
                continue
            if k in _seen:
                continue
            _seen.add(k)
            _deduped.append(g)
        indexed_granules = _deduped

        self.date_range = (date_min, date_max)
        self.h3_columns = sorted(observed_columns)
        # Empty dict when every partition metadata predates this field.
        self.h3_columns_dtypes = dict(observed_dtypes)
        self.h3_partition_ids = sorted(h3_parts)

        # Merge with existing tracked granules (preserve PENDING for unindexed)
        indexed_keys = {(g['orbit'], g['granule'], g['track']) for g in indexed_granules}
        if hasattr(self, 'granule_info') and self.granule_info:
            for g in self.granule_info:
                key = (g['orbit'], g['granule'], g['track'])
                if key in indexed_keys:
                    g['status'] = 'INDEXED'
                    g['products'] = _per_product_status_from_observed(
                        active_products, gran_observed_products.get(key, set())
                    )
                # else: keep existing status (PENDING)

            # Add any newly discovered granules not previously tracked
            existing_keys = {(g['orbit'], g['granule'], g['track']) for g in self.granule_info}
            for g in indexed_granules:
                key = (g['orbit'], g['granule'], g['track'])
                if key not in existing_keys:
                    self.granule_info.append({
                        **g,
                        'status': 'INDEXED',
                        'products': _per_product_status_from_observed(
                            active_products, gran_observed_products.get(key, set())
                        ),
                    })
        else:
            self.granule_info = [
                {
                    **g,
                    'status': 'INDEXED',
                    'products': _per_product_status_from_observed(
                        active_products,
                        gran_observed_products.get((g['orbit'], g['granule'], g['track']), set()),
                    ),
                }
                for g in indexed_granules
            ]

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

        if getattr(self, 'h3_columns_dtypes', None):
            log_dict['h3_columns_dtypes'] = self.h3_columns_dtypes

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

    def get_granule_product_status(self, gran_key, product):
        """Return per-product status for a granule, or None if unknown.

        Parameters
        ----------
        gran_key : dict or tuple
            Either ``{'orbit', 'granule', 'track'}`` or ``(orbit, granule, track)``.
        product : str
            Product code (e.g. 'L4A').
        """
        if not hasattr(self, 'granule_info'):
            return None
        if isinstance(gran_key, dict):
            key = (gran_key['orbit'], gran_key['granule'], gran_key['track'])
        else:
            key = tuple(gran_key)
        for g in self.granule_info:
            if (g['orbit'], g['granule'], g['track']) == key:
                return (g.get('products') or {}).get(product)
        return None

    def get_product_gaps(self, active_products=None, gap_statuses=None):
        """List granules where any active product is not INDEXED.

        Returns a list of ``(gran_key_dict, [missing_products])`` pairs.
        Used by gh3_doctor as the fast-path audit (log-only, no parquet scan).

        Parameters
        ----------
        active_products : iterable of str, optional
            Products to check. Defaults to ``self.product_vars.keys()``.
        gap_statuses : iterable of str, optional
            Per-product statuses considered as gaps. Defaults to everything except
            ``INDEXED`` (i.e. PARTIAL_NAN, MISSING_COLUMN, MISSING_SOURCE, FAILED, PENDING).
        """
        if not hasattr(self, 'granule_info'):
            return []
        if active_products is None:
            active_products = list(self.product_vars.keys())
        active_products = list(active_products)
        if gap_statuses is None:
            gap_statuses = {
                PRODUCT_STATUS_PARTIAL_NAN,
                PRODUCT_STATUS_MISSING_COLUMN,
                PRODUCT_STATUS_MISSING_SOURCE,
                PRODUCT_STATUS_FAILED,
                PRODUCT_STATUS_PENDING,
            }
        else:
            gap_statuses = set(gap_statuses)

        gaps = []
        for g in self.granule_info:
            products_map = g.get('products') or {}
            missing = []
            for p in active_products:
                # Unknown for this entry → treat as PENDING so the doctor
                # inspects rather than silently skipping.
                status = products_map.get(p, PRODUCT_STATUS_PENDING)
                if status in gap_statuses:
                    missing.append(p)
            if missing:
                gaps.append(({'orbit': g['orbit'], 'granule': g['granule'], 'track': g['track']}, missing))
        return gaps

    def mark_granule_product(self, gran_key, product, status):
        """Set per-product status for a granule (does not auto-save).

        Caller is responsible for invoking ``save_log()`` to persist.
        Used by gh3_doctor after a fix or verification step.
        """
        if status not in _VALID_PRODUCT_STATUSES:
            raise GediValidationError(
                f"Invalid per-product status '{status}'. Must be one of {_VALID_PRODUCT_STATUSES}"
            )
        if not hasattr(self, 'granule_info'):
            return False
        if isinstance(gran_key, dict):
            key = (gran_key['orbit'], gran_key['granule'], gran_key['track'])
        else:
            key = tuple(gran_key)
        for g in self.granule_info:
            if (g['orbit'], g['granule'], g['track']) == key:
                products_map = g.get('products')
                if products_map is None:
                    products_map = {}
                    g['products'] = products_map
                products_map[product] = status
                return True
        return False