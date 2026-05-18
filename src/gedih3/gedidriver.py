# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
GEDI data file operations and HDF5 I/O.

This module provides classes and functions for working with GEDI HDF5 files,
including parsing filenames, extracting data, and building Dask DataFrames.
"""

import os
import re
import glob
import itertools
from typing import Dict, List, Optional, Union
from pathlib import Path

import h5py
import yaml
import numpy as np
import pandas as pd
import geopandas as gpd
from numba import njit
from datetime import datetime
import dask
import dask.dataframe
from earthaccess.store import EarthAccessFile

from .config import GEDI_BEAMS, GEDI_MISSION_START, GEDI_START_DATE, GH3_DEFAULT_SOC_DIR, SOC_MANIFEST_FILENAME, get_default_vars_file, _get_versioned, _GEDI_MIN_VARS
from .utils import h5_copy_subset, h5_info, _read_manifest, generate_manifest
from .logging_config import get_logger
from .exceptions import GediValidationError, GediProductError, GediVariableError, GediFileError

logger = get_logger(__name__)


def soc_prod_from_file(file_path: Union[str, Path]) -> str:
    """
    Extract GEDI product code from a file path.

    Parameters
    ----------
    file_path : str or Path
        Path to a GEDI HDF5 file

    Returns
    -------
    str
        Product code (e.g., 'L2A', 'L4A')
    """
    f = GEDIFile(file_path)
    prod_str = f"L{f.product[-1]}{f.level}"
    return prod_str


def soc_from_file(file_path: Union[str, Path]) -> str:
    """
    Get the SOC root directory from a GEDI file path.

    Parameters
    ----------
    file_path : str or Path
        Path to a GEDI HDF5 file

    Returns
    -------
    str
        Path to the SOC root directory
    """
    return os.path.dirname(os.path.dirname(os.path.dirname(file_path)))


def _soc_manifest_path(soc_dir: str) -> str:
    """Return the absolute path of the SOC manifest sentinel for *soc_dir*."""
    return os.path.join(soc_dir.rstrip('/'), SOC_MANIFEST_FILENAME)


def _read_soc_manifest(soc_dir: str) -> Optional[List[str]]:
    """Read ``_soc_manifest.txt`` from a SOC root and return absolute paths.

    Returns ``None`` when the manifest does not exist; callers should fall
    back to the recursive glob. Thin wrapper around the shared
    :func:`gedih3.utils._read_manifest` (cached, manifest-name aware) —
    the SOC manifest stores relative paths just like the H3 database
    manifest, so the only SOC-specific behavior is filling in the
    sentinel filename and joining results back to absolute paths.
    """
    rels = _read_manifest(soc_dir, manifest_filename=SOC_MANIFEST_FILENAME)
    if rels is None:
        return None
    root = soc_dir.rstrip('/')
    return [os.path.join(root, rel) for rel in rels]


def write_soc_manifest(soc_dir: str, files=None) -> int:
    """(Re)generate the SOC manifest sentinel by walking *soc_dir* once.

    Producer-driven refresh (R2): every code path that mutates a SOC
    tree (`gh3_download`, `gh3_build --download`, `s3_etl_subset`,
    `gh3_build -i` exit, `gh3_doctor --fix soc_health`) calls this on
    exit. Cheaper than letting every consumer re-walk the tree.

    Walks the SOC tree in parallel via
    :func:`gedih3.parallel.walk_soc_parallel` (year/doy fan-out on the
    registered dask Client). Returns the number of files written to
    the manifest (0 when *soc_dir* is missing / empty).

    Parameters
    ----------
    soc_dir
        SOC root directory.
    files : list[str], optional
        Pre-computed absolute file list to use instead of walking. When
        the caller has already enumerated the tree (e.g.
        `cli/gh3_build.py` after its existing-h5 listing), passing the
        list here avoids the redundant parallel walk.
    """
    if not os.path.isdir(soc_dir):
        return 0
    if files is None:
        from .parallel import walk_soc_parallel
        files = walk_soc_parallel(soc_dir, pattern='GEDI*.h5')
    if not files:
        return 0
    generate_manifest(soc_dir, pattern='GEDI*.h5',
                      manifest_filename=SOC_MANIFEST_FILENAME,
                      tree_shape='soc', files=files)
    return len(files)


def soc_file_tree(
    file_struct: Union[str, List[str], List[EarthAccessFile]],
    to_list: bool = False,
    glob_kwargs: Optional[Dict] = None,
    exclude: Optional[List[str]] = None,
) -> Union[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    """
    Build a structured tree of GEDI SOC files grouped by orbit/track.

    Organizes GEDI HDF5 files into a nested structure for easy access to
    related files across different products (L2A, L2B, L4A, etc.).

    Parameters
    ----------
    file_struct : str, list of str, or list of EarthAccessFile
        Either a directory path containing GEDI files, or a list of file
        paths or EarthAccessFile objects.
    to_list : bool, default False
        If True, return a list of dicts instead of a nested dict.
    glob_kwargs : dict, optional
        Keyword arguments passed to gedi_file_glob() for filtering files
        (e.g., version, orbit).
    exclude : list of str, optional
        fnmatch-style patterns matched against each file's basename.
        Any file whose basename matches any pattern is dropped from the
        result. Useful to exclude internal/SGS variants (e.g.
        ``['*_SGS.h5']``) that share the SOC tree with public release files.

    Returns
    -------
    dict or list
        If to_list=False: dict keyed by orbit_track identifier, with values
        being dicts mapping product codes to file paths.
        If to_list=True: list of product dicts.

    Examples
    --------
    >>> tree = soc_file_tree('/path/to/soc')
    >>> tree['O12345_01_T00001']
    {'L2A': '/path/to/GEDI02_A_...h5', 'L4A': '/path/to/GEDI04_A_...h5'}
    >>> # Exclude NASA SGS internal files from discovery
    >>> tree = soc_file_tree('/path/to/soc', exclude=['*_SGS.h5'])
    """
    direct_access = False
    if isinstance(file_struct, str) and os.path.isdir(file_struct):
        glob_pattern = gedi_file_glob(**glob_kwargs) if glob_kwargs else 'GEDI*.h5'
        # Prefer the cached SOC manifest when present — recursive glob over a
        # multi-million-file SOC tree on shared GPFS is otherwise the
        # dominant cost of resume / post-download reconciliation. The
        # manifest is regenerated by ``write_soc_manifest`` after every
        # successful download (see ``SOCDownloadLogger.set_post_download_info``).
        manifest_files = _read_soc_manifest(file_struct)
        if manifest_files is not None:
            import fnmatch
            file_list = [
                p for p in manifest_files
                if fnmatch.fnmatch(os.path.basename(p), glob_pattern)
            ]
        else:
            # No manifest — fall back to a parallel year/doy walk. The
            # serial recursive glob the prior implementation used was the
            # dominant cost of `gh3_build` resume on multi-million-file
            # SOC trees (5–15 min on cold GPFS). The parallel walk uses
            # the same dask Client every CLI tool already establishes.
            from .parallel import walk_soc_parallel
            file_list = walk_soc_parallel(file_struct, pattern=glob_pattern)
    elif (direct_access := isinstance(file_struct[0], EarthAccessFile)):
        file_list = [i.path for i in file_struct]
    elif isinstance(file_struct[0], str):
        file_list = file_struct
    else:
        raise GediValidationError("file_struct must be a directory or a list of file paths (or s3 links)")

    if exclude:
        import fnmatch
        before = len(file_list)
        keep_idx = [
            i for i, p in enumerate(file_list)
            if not any(fnmatch.fnmatch(os.path.basename(p), pat) for pat in exclude)
        ]
        if len(keep_idx) < before:
            file_list = [file_list[i] for i in keep_idx]
            if direct_access:
                file_struct = [file_struct[i] for i in keep_idx]
            # Library is silent about exclusions on purpose: soc_file_tree
            # is called multiple times per build and the CLI emits one
            # user-facing summary at the existing_h5 filter point.

    if not file_list:
        return [] if to_list else {}

    file_array = np.array(file_struct) if direct_access else np.array(file_list)

    flist = pd.DataFrame({'file_paths': file_list, 'file_links': file_array})
    fidx = flist.file_paths.str.extract(r'.*GEDI(\d{2})_([A-Z])_.*_(O\d+_\d{2}_T\d+).*\.h5$')

    valid = fidx.notna().all(axis=1)
    flist = flist.loc[valid].copy()
    fidx = fidx.loc[valid]

    flist['prod'] = 'L' + fidx[0].astype(int).astype(str) + fidx[1]
    flist['orb_track'] = fidx[2]
    flist = flist.sort_values(['orb_track', 'file_paths', 'prod'])
    flist = flist.pivot_table(index='orb_track', columns='prod', values='file_links', aggfunc='last')
    flist = flist.dropna()

    soc_tree = flist.T.to_dict()

    if to_list:
        soc_tree = list(soc_tree.values())

    return soc_tree

def gedi_subset(source_file, dest_file, variables, subset_beams=None):
    beams = [b for b in GEDI_BEAMS if b in subset_beams] if subset_beams else list(GEDI_BEAMS)
    beam_variables = [f"{b}/{v}" for b in beams for v in variables]
    h5_copy_subset(source_file, dest_file, beam_variables)

    if os.path.exists(dest_file):
        return dest_file
    return None

def gedi_file_glob(orbit=None, orbit_granule=None, track=None, product: int=None, level: str=None, ppds: int=None, pge:int=None, generation:int=None, version:int=None):
    str_build = lambda x, digits=2: '*' if x is None else f"{x:0{digits}d}"
    lev_str = '*' if level is None else level.upper()    
    prd_str = str_build(product)
    orb_str = str_build(orbit, 5)
    ogr_str = str_build(orbit_granule)
    trk_str = str_build(track, 5)
    pge_str = str_build(pge, 3)    
    gen_str = str_build(generation)
    ver_str = str_build(version, 3)
    ppd_str = str_build(ppds)    
    return f"GEDI{prd_str}_{lev_str}_*_O{orb_str}_{ogr_str}_T{trk_str}_{ppd_str}_{pge_str}_{gen_str}_V{ver_str}*.h5"

def gedi_vars_expand(product_vars, version=None):
    for prod, vars in product_vars.items():
        if vars is None:
            continue
        if isinstance(vars, list) and len(vars) == 0:
            # Bare flag (e.g. -l2a with no args) → dump everything
            product_vars[prod] = None
        elif os.path.isfile(vars[0]) and len(vars) == 1:
            with open(vars[0], 'r') as f:
                product_vars[prod] = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        elif "minimal" in vars or "min" in vars:
            product_vars[prod] = _get_versioned(_GEDI_MIN_VARS[prod], version)
        elif 'default' in vars or 'def' in vars:
            with open(get_default_vars_file(prod, version=version), 'r') as f:
                product_vars[prod] = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        elif "*" in vars or "all" in vars:
            product_vars[prod] = None
        elif isinstance(vars, list):
            continue
        else:
            raise GediProductError(f"Unknown variable specification for product {prod}: {vars}")
    return product_vars

def expand_var_wildcards(var_specs, available_vars):
    """Expand fnmatch-style wildcard patterns in variable specifications.

    Supports ``*``, ``?``, ``[seq]``, ``[!seq]``.  Non-pattern specs pass
    through unchanged.  Raises :class:`GediValidationError` if a pattern
    matches nothing.
    """
    import fnmatch
    expanded = []
    for spec in var_specs:
        if any(c in spec for c in ('*', '?', '[', ']')):
            matched = fnmatch.filter(available_vars, spec)
            if not matched:
                raise GediValidationError(
                    f"Wildcard pattern '{spec}' matched no available variables"
                )
            expanded.extend(matched)
        else:
            expanded.append(spec)
    return list(dict.fromkeys(expanded))  # deduplicate, preserve order

def gedi_vars_from_h5(gedi_file):
    with h5py.File(gedi_file, 'r') as f:
        b = [i for i in f.keys() if i.upper().startswith('BEAM')][0]
    pl_info = h5_info(gedi_file, root=b)
    return pl_info.path.str.replace(b,'').str.lstrip('/').tolist()


_static_vars_cache: Dict = {}


def gedi_vars_static(product, version=None):
    """Return the canonical variable list for a GEDI ``(product, version)``
    from the static manifest shipped in :mod:`gedih3.data` — no I/O on any
    target file.

    NASA release files of the same ``(product, version)`` always carry an
    identical dataset schema, so this is equivalent to
    :func:`gedi_vars_from_h5` for canonical release files but free.

    Returns ``None`` when no static manifest exists for the requested
    combination; callers should fall back to :func:`gedi_vars_from_h5`.

    Note: the result reflects a NASA release file's schema. Files that
    were previously subset (e.g. via :func:`gedi_subset` or S3 ETL mode)
    contain a *subset* of these variables — for those, use
    :func:`gedi_vars_from_h5` to introspect what is actually present.
    """
    key = (product, version)
    if key in _static_vars_cache:
        return _static_vars_cache[key]
    try:
        with open(get_default_vars_file(product, version=version)) as f:
            vars_list = [ln.strip() for ln in f if ln.strip() and not ln.startswith('#')]
    except (FileNotFoundError, ValueError):
        vars_list = None
    _static_vars_cache[key] = vars_list
    return vars_list

def check_soc_file_vars(soc_file, available_products):
    file_products = {}
    for prod in available_products.keys():
        if prod in soc_file:
            try:
                available_vars = gedi_vars_from_h5(soc_file[prod])
            except Exception as e:
                # A single broken granule (truncated download, no BEAM
                # group, etc.) must not abort the whole validation bag.
                # _filter_granules will drop the file via h5_is_valid.
                logger.warning(
                    f"validate_soc_files: skipping unreadable {prod} file "
                    f"{soc_file[prod]}: {type(e).__name__}: {e}"
                )
                continue
            file_products[prod] = available_vars
    return file_products

@dask.delayed
def _check_soc_file_vars(soc_file, available_products):
    return check_soc_file_vars(soc_file, available_products)

def validate_soc_files(product_vars: Dict, soc_dir: str = GH3_DEFAULT_SOC_DIR,
                        version: Optional[int] = None,
                        exclude: Optional[List[str]] = None):
    """Validate ``product_vars`` against the shipped static manifests.

    Only authoritative for **canonical NASA release files** validated against
    the per-product manifest in ``src/gedih3/data/GEDI*_DATASETS_*.txt`` —
    that is, for the fresh-build ``default`` request (Regime A) or for a
    resume where the user explicitly re-requests ``default`` for a product
    (Regime C). On all other resume paths the build log is the contract,
    and ``gh3_build.py`` bypasses this check entirely. Callers are
    responsible for filtering ``product_vars`` to only the products they
    actually want to validate against the static manifest — passing the
    full union of existing + new vars on a resume will produce false
    negatives whenever the shipped manifest has drifted relative to the
    list saved in the build log.
    """
    glob_kwargs = {'version': version} if version is not None else None
    soc_files = soc_file_tree(soc_dir, to_list=True, glob_kwargs=glob_kwargs, exclude=exclude)

    if not soc_files:
        return False, {"error": "No SOC files found in directory"}

    from .config import get_default_vars_file
    available_products = {}
    for prod in soc_files[0].keys():
        try:
            with open(get_default_vars_file(prod, version=version)) as f:
                available_products[prod] = [
                    ln.strip() for ln in f
                    if ln.strip() and not ln.startswith('#')
                ]
        except (FileNotFoundError, ValueError) as e:
            logger.warning(
                f"validate_soc_files: no static manifest for {prod} v{version}; "
                f"skipping variable check for this product ({e})"
            )
            available_products[prod] = []

    validation_report = {
        "available_products": available_products,
        "missing_products": [],
        "missing_variables": {},
        "can_skip": True
    }

    for prod, required_vars in product_vars.items():
        if prod not in available_products:
            validation_report["missing_products"].append(prod)
            validation_report["can_skip"] = False
        elif required_vars is not None:
            try:
                required_vars = expand_var_wildcards(required_vars, available_products[prod])
            except GediValidationError:
                pass  # unmatched patterns will surface as missing vars below
            missing_vars = [v for v in required_vars if v not in set(available_products[prod])]
            if missing_vars:
                validation_report["missing_variables"][prod] = missing_vars
                validation_report["can_skip"] = False

    if not validation_report["can_skip"]:
        error_msg = "Cannot continue due to missing data.\n"
        if validation_report.get("missing_products"):
            error_msg += f"Missing products: {validation_report['missing_products']}\n"
        if validation_report.get("missing_variables"):
            for prod, missing_vars in validation_report["missing_variables"].items():
                error_msg += f"Missing variables in {prod}: {missing_vars}\n"    
        validation_report['error_msg'] = error_msg.strip()    
    
    return validation_report

class GEDIFile:
    """
    Parser for GEDI HDF5 filenames.

    Extracts metadata from GEDI filename convention including orbit, granule,
    track, version, and date information.

    Attributes
    ----------
    file_path : str
        Full path to the GEDI file
    full_name : str
        Basename of the file
    product : str
        Product identifier (e.g., 'GEDI02')
    level : str
        Processing level (e.g., 'A', 'B')
    date : datetime
        Acquisition date and time
    product_code : str
        Combined product code (e.g., 'L2A', 'L4A')
    orbit : int
        Orbit number
    orbit_granule : int
        Granule number within orbit
    track : int
        Ground track number
    version : int
        Data version number
    year : str
        Year string (e.g., '2019')
    doy : str
        Day-of-year string (e.g., '338')
    mission_week : int
        Mission week number since GEDI launch (2018-12-13)
    suffix : str or None
        Extra filename parts after version field, if any

    Examples
    --------
    >>> gf = GEDIFile('/path/to/GEDI02_A_2020123120000_O12345_01_T00001_02_003_01_V002.h5')
    >>> gf.orbit
    12345
    >>> gf.version
    2
    """

    def __init__(self, file_path: Union[str, Path, EarthAccessFile]):
        if isinstance(file_path, EarthAccessFile):
            file_path = file_path.path
        self.parse_file(file_path)

    def __str__(self) -> str:
        return yaml.dump(self)

    def parse_file(self, f: str) -> None:
        # check if f is str or s3 link
        self.file_path = f
        self.file_size = (os.path.getsize(f) / 1e9) if os.path.exists(f) else None
        self.full_name = os.path.basename(f)
        f_base = re.sub(r'\.h5$', '', self.full_name)
        fl = f_base.split('_')
        self.product = fl[0]
        self.level = fl[1]
        self.date = datetime.strptime(fl[2], '%Y%j%H%M%S')
        self.date_str = fl[2]
        self.doy_date_str = self.date.strftime('%Y%m%d')
        self.julian_date_str = self.date.strftime('%Y%j')
        self.time_str = self.date.strftime('%H%M%S')
        self.orbit = int(fl[3][1:])
        self.orbit_granule = int(fl[4])
        self.track = int(fl[5][1:])
        self.positioning = int(fl[6])
        self.pge = int(fl[7])
        self.generation = int(fl[8])
        self.version = int(fl[9][1:])
        self.product_code = f"L{self.product[-1]}{self.level}"
        self.year = str(self.date.year)
        self.doy = self.date.strftime('%j')
        self.mission_week = (self.date - GEDI_MISSION_START).days // 7 + 1
        self.suffix = '_'.join(fl[10:]) if len(fl) > 10 else None
    
    def get_glob_pattern(self, product:int=1, level:str='B', ppds:int=2):
        return gedi_file_glob(self.orbit, self.orbit_granule, self.track, product, level, ppds)
    
    @classmethod
    def from_filename(cls, file_path):
        return cls(file_path)

    def search_file(self, soc_dir: str = None, product:int=1, level:str='B', other_files=None): # other_files = e.g. http paths from direct s3
        pattern = self.get_glob_pattern(product, level)
        
        if other_files is None:
            if soc_dir is None:
                soc_dir = soc_from_file(self.file_path)
            files = glob.glob(os.path.join(soc_dir, '**', pattern), recursive=True)
        else:
            files = [f for f in other_files if re.match(pattern.replace('*','.*'), os.path.basename(f))]
        
        if not files:
            return None
        
        if len(files) == 1:
            return files[0]

        parsed_files = [GEDIFile.from_filename(f) for f in files]
        version_matches = [f for f in parsed_files if f.version == self.version]
        if version_matches:
            version_matches.sort(key=lambda x: (x.generation, x.pge), reverse=True)
            return version_matches[0].file_path

        parsed_files.sort(key=lambda x: (x.version, x.generation, x.pge), reverse=True)
        return parsed_files[0].file_path

class GEDIShot(GEDIFile):
    """
    Decoder for GEDI shot numbers.

    Extracts orbit, beam, track, and other metadata encoded in GEDI shot numbers.
    The shot number is a 64-bit integer encoding multiple fields.

    Attributes
    ----------
    shot : int or array
        The shot number(s)
    orbit : int or array
        Orbit number
    orbit_granule : int or array
        Granule number within orbit
    track : int or array
        Ground track number
    beam : int or array
        Beam number (0-7)
    beam_str : str or list
        Beam identifier string (e.g., 'BEAM0101')
    power : bool or array
        True if power beam (beam > 3)
    shot_index : int or array
        Shot index within the granule

    Examples
    --------
    >>> shot = GEDIShot(12345678901234567890)
    >>> shot.beam_str
    'BEAM0101'
    """

    def __init__(self, shot: Union[int, np.ndarray], build_glob: bool = False):
        self.parse_shot(shot)
        if build_glob:
            self.get_file_glob()

    def parse_shot(self, shot: Union[int, np.ndarray]) -> None:
        self.is_scalar = np.isscalar(shot)
        self.shot = shot
        self.orbit = shot // 10000000000000
        self.orbit_granule = shot % 1000000000 // 100000000
        self.track = shot // 100000000000
        self.beam = shot % 10000000000000 // 100000000000
        self.power = self.beam > 3
        self.reserved  = shot % 1000000000000 // 1000000000 % 100
        self.shot_index = shot % 100000000
        
        if self.is_scalar:
            self.beam_str = f'BEAM{self.beam:04b}'
        else:
            self.beam_str = [f'BEAM{b:04b}' for b in self.beam]    
    
    def get_glob_pattern(self, product: int=1, level: str='B', ppds: int=2, pge:int=None, generation:int=None, version:int=None):
        if self.is_scalar:
            self.file_glob_pattern = gedi_file_glob(self.orbit, self.orbit_granule, self.track, product, level, ppds, pge, generation, version)
        else:
            self.file_glob_pattern = [gedi_file_glob(self.orbit[i], self.orbit_granule[i], self.track[i], product, level, ppds, pge, generation, version) for i in range(len(self.shot))]
            self.file_glob_pattern - list(set(self.file_pattern))
        
    def search_file(self, soc_dir: str):
        if self.is_scalar:
            files = glob.glob(os.path.join(soc_dir, self.file_glob_pattern))
        else:
            files = []
            for fp in self.file_glob_pattern:
                files.extend(glob.glob(os.path.join(soc_dir, fp)))
            files = list(set(files))

        return files
            
@njit
def wfm_pad(wave, n_bins=1420, cval=0):
    bin_diff = len(wave) - n_bins
    if bin_diff == 0:
        return wave    

    if bin_diff < 0:
        padded = np.full(n_bins, cval, dtype=wave.dtype)
        padded[:len(wave)] = wave
        return padded
    
    if bin_diff > 0:
        return wave[:n_bins]

@njit
def process_waveforms(starts, ends, noises, wfs, n_bins=1420):
    n_waves = len(starts)
    waves = np.zeros((n_waves, n_bins), dtype=np.float32)
    
    for i in range(n_waves):
        start = starts[i]
        end = ends[i]
        noise = noises[i]
        
        if start < 0 or end > len(wfs):
            waves[i] = np.full(n_bins, noise)
            continue
            
        wf = wfs[start:end]
        waves[i] = wfm_pad(wf, n_bins, cval=noise)
    
    return waves

def wfm_extract(h5_file, beam, idx, tx=False):
    init = 't' if tx else 'r'

    noises = h5_file[f'/{beam}/noise_mean_corrected'][:][idx]
    starts = h5_file[f'/{beam}/{init}x_sample_start_index'][:][idx] - 1
    counts = h5_file[f'/{beam}/{init}x_sample_count'][:][idx]
    ends = starts + counts
    wfs = h5_file[f'/{beam}/{init}xwaveform'][:]

    return process_waveforms(starts, ends, noises, wfs)


def _validate_h5_columns(columns: List[str]) -> List[str]:
    """
    Validate and normalize the column list for HDF5 extraction.

    Ensures shot_number is included and adds waveform dependencies.

    Parameters
    ----------
    columns : list of str
        Requested column names

    Returns
    -------
    list of str
        Normalized column list with dependencies added
    """
    columns = list(columns)  # Make a copy

    if 'shot_number' not in columns:
        columns.append('shot_number')

    if 'rxwaveform' in columns:
        columns += ['noise_mean_corrected', 'rx_sample_start_index', 'rx_sample_count']
        columns = list(set(columns))

    if 'txwaveform' in columns:
        columns += ['noise_mean_corrected', 'tx_sample_start_index', 'tx_sample_count']
        columns = list(set(columns))

    return columns


def _get_beams_to_load(
    h5_file: h5py.File,
    shots: Optional[np.ndarray],
    which_beams: Optional[List[str]]
) -> tuple:
    """
    Determine which beams to load based on shots and beam filters.

    Parameters
    ----------
    h5_file : h5py.File
        Open HDF5 file handle
    shots : array, optional
        Specific shot numbers to extract
    which_beams : list of str, optional
        Specific beams to load

    Returns
    -------
    tuple
        (beams_to_load, all_beams, shot_beams_array_or_none)
    """
    all_beams = [k for k in h5_file.keys() if k.startswith('BEAM')]
    beams = all_beams
    shot_beams = None

    if shots is not None:
        shot_beams = np.uint16(shots % 10000000000000 // 100000000000)
        beams = [f'BEAM{b:04b}' for b in np.unique(shot_beams)]

    if which_beams is not None:
        beams = [b for b in beams if b in which_beams]

    return beams, all_beams, shot_beams


def _extract_beam_data(
    h5_file: h5py.File,
    beam: str,
    columns: List[str],
    shots: Optional[np.ndarray],
    shot_beams: Optional[np.ndarray],
    include_source: bool
) -> Optional[pd.DataFrame]:
    """
    Extract data from a single beam in an HDF5 file.

    Parameters
    ----------
    h5_file : h5py.File
        Open HDF5 file handle
    beam : str
        Beam identifier (e.g., 'BEAM0101')
    columns : list of str
        Variables to extract
    shots : array, optional
        Specific shot numbers to extract
    shot_beams : array, optional
        Beam numbers for each shot (pre-computed)
    include_source : bool
        Add root_beam column

    Returns
    -------
    pd.DataFrame or None
        DataFrame with extracted data, or None if no matching shots
    """
    shot_ids = h5_file[f"{beam}/shot_number"][:]

    if shots is not None:
        kint = int(beam[-4:], base=2)
        k_shots = shots[shot_beams == kint]
        idx = np.where(np.isin(shot_ids, k_shots))
    else:
        idx = np.arange(0, len(shot_ids))

    if len(idx) == 0:
        return None

    dfs = {}
    for j in columns:
        try:
            if is_wave := (j == 'rxwaveform' or j == 'txwaveform'):
                is_tx = j == 'txwaveform'
                d = wfm_extract(h5_file, beam, idx, is_tx)
            else:
                d = h5_file[f"{beam}/{j}"][:][idx]
        except KeyError as e:
            # Normalize h5py's two missing-path error shapes ("object 'X'
            # doesn't exist" vs "component not found") into the single form
            # _MISSING_VAR_RE picks up, so _classify_load_h5_failure tags the
            # failure as missing_var with the actual variable name and the
            # end-of-build recovery advisory groups it correctly.
            raise KeyError(f"object '{j}' doesn't exist (h5py: {e})") from e

        if d.ndim == 2:
            for col in range(d.shape[-1]):
                jj = f"{j}_{col:0{4 if is_wave else 3}d}"
                dfs[jj] = d[:, col]
        else:
            dfs[j] = d

    df = pd.DataFrame(dfs)
    if include_source:
        df = df.assign(root_beam=beam)

    return df


def _build_dataframe(
    beam_frames: List[pd.DataFrame],
    fpath: Union[str, Path, EarthAccessFile],
    include_source: bool,
    dropna: bool
) -> pd.DataFrame:
    """
    Build the final DataFrame from extracted beam data.

    Parameters
    ----------
    beam_frames : list of pd.DataFrame
        DataFrames from each beam
    fpath : str, Path, or EarthAccessFile
        Source file path (for root_file column)
    include_source : bool
        Add root_file column
    dropna : bool
        Drop rows with NaN values

    Returns
    -------
    pd.DataFrame
        Combined DataFrame indexed by shot_number
    """
    full_df = pd.concat(beam_frames, copy=True)
    full_df = full_df.set_index('shot_number')

    if include_source:
        full_df = full_df.assign(
            root_file=os.path.basename(fpath.path if isinstance(fpath, EarthAccessFile) else fpath)
        )

    if dropna:
        full_df = full_df.dropna()

    return full_df


def load_h5(
    fpath: Union[str, Path, EarthAccessFile],
    columns: List[str],
    which_beams: Optional[List[str]] = None,
    shots: Optional[np.ndarray] = None,
    include_source: bool = True,
    dropna: bool = True
) -> pd.DataFrame:
    """
    Load data from a GEDI HDF5 file into a pandas DataFrame.

    Parameters
    ----------
    fpath : str, Path, or EarthAccessFile
        Path to the GEDI HDF5 file
    columns : list of str
        Variable names to extract from the file
    which_beams : list of str, optional
        Specific beams to load (e.g., ['BEAM0101', 'BEAM1000']).
        If None, loads all beams.
    shots : array, optional
        Specific shot numbers to extract. If None, loads all shots.
    include_source : bool, default True
        Add root_file and root_beam columns to output
    dropna : bool, default True
        Drop rows with NaN values

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by shot_number containing requested variables
    """
    f = h5py.File(fpath, mode='r', locking=False)

    # Validate and normalize columns
    columns = _validate_h5_columns(columns)

    # Determine which beams to load
    beams, all_beams, shot_beams = _get_beams_to_load(f, shots, which_beams)

    # Handle case where no beams match
    if len(beams) == 0:
        f.close()
        return load_h5(fpath=fpath, columns=columns, include_source=include_source, which_beams=all_beams[:1]).head(0)

    # Extract data from each beam
    beam_frames = []
    for beam in beams:
        beam_df = _extract_beam_data(f, beam, columns, shots, shot_beams, include_source)
        if beam_df is not None:
            beam_frames.append(beam_df)

    f.close()

    # Build final DataFrame
    return _build_dataframe(beam_frames, fpath, include_source, dropna)

def load_h5_merged(
    prod_files: Dict[str, str],
    product_vars: Dict[str, List[str]],
    which_beams: Optional[List[str]] = None,
    shots: Optional[np.ndarray] = None,
    dropna: bool = True,
    suffix_all: bool = False
) -> Optional[pd.DataFrame]:
    """
    Load and merge data from multiple GEDI product files.

    Combines data from different GEDI products (L2A, L2B, L4A, etc.) into a
    single DataFrame, joining on shot_number.

    Parameters
    ----------
    prod_files : dict
        Mapping of product codes to file paths (e.g., {'L2A': '/path/to/l2a.h5'})
    product_vars : dict
        Mapping of product codes to lists of variable names to extract
    which_beams : list of str, optional
        Specific beams to load
    shots : array, optional
        Specific shot numbers to extract
    dropna : bool, default True
        Drop rows with NaN values
    suffix_all : bool, default False
        If True, suffix all columns with product code (e.g., 'rh_l2a').
        Required when variables have the same name across products.

    Returns
    -------
    pd.DataFrame or None
        Merged DataFrame indexed by shot_number, or None if no data
    """
    frames = []
    for i, (p, vars) in enumerate(product_vars.items()):
        ppath = prod_files.get(p)
        
        if ppath is None:
            continue
        
        idf = load_h5(fpath=ppath, columns=vars, include_source = suffix_all or i==0, which_beams=which_beams, shots=shots, dropna=dropna)
        
        suffix = f"_{p.lower()}"
        if suffix_all:
            idf = idf.rename(columns=lambda x: x if x.endswith(suffix) else f"{x}{suffix}")

        frames.append(idf)
    
    if not frames:
        return None
    
    if not suffix_all:
        all_cols = [col for frame in frames for col in frame.columns]
        if len(all_cols) != len(set(all_cols)):
            raise GediVariableError("Duplicate columns detected. Remove duplicates or use suffix_all=True to avoid conflicts.")

    df = pd.concat(frames, axis=1, join='inner', copy=True)
    return df

def dask_h5_merged(
    prod_files_list: List[Dict[str, str]],
    product_vars: Dict[str, List[str]],
    which_beams: Optional[List[str]] = None,
    shots: Optional[np.ndarray] = None,
    dropna: bool = True,
    suffix_all: bool = False,
    by_beam: bool = False
) -> dask.dataframe.DataFrame:
    """
    Create a Dask DataFrame from multiple GEDI product file sets.

    Lazily loads and merges GEDI data for distributed processing.

    Parameters
    ----------
    prod_files_list : list of dict
        List of product file dictionaries from soc_file_tree()
    product_vars : dict
        Mapping of product codes to variable lists
    which_beams : list of str, optional
        Specific beams to load
    shots : array, optional
        Specific shot numbers to extract
    dropna : bool, default True
        Drop rows with NaN values
    suffix_all : bool, default False
        Suffix all columns with product code
    by_beam : bool, default False
        If True, create separate partitions for each beam (more parallelism)

    Returns
    -------
    dask.dataframe.DataFrame
        Lazy Dask DataFrame for distributed processing
    """
    # Probe files in order to derive the dask metadata schema. A single broken
    # granule at the head of the list (corrupt HDF5, missing variable inside a
    # BEAM, etc.) must not abort the whole build — skip it with a warning and
    # try the next. Per-partition errors at runtime are already swallowed by
    # the `_load_h5_merged` closure below.
    _meta = None
    _skipped = 0
    for i, _pf in enumerate(prod_files_list):
        try:
            _meta = load_h5_merged(
                _pf, product_vars=product_vars,
                which_beams=GEDI_BEAMS[:1],
                dropna=dropna, suffix_all=suffix_all,
            ).head(0)
            if i > 0:
                logger.warning(
                    f"Skipped {i} broken file(s) while deriving metadata schema; using {_pf} as the schema sample"
                )
            break
        except Exception as e:
            _skipped += 1
            logger.warning(f"Skipping broken file for metadata sample: {_pf} ({e})")
            continue
    if _meta is None:
        raise GediFileError(
            f"Could not derive a metadata schema: all {_skipped} candidate file(s) failed to read"
        )

    def _load_h5_merged(prod_files, **kwargs):
        try:
            return load_h5_merged(prod_files, **kwargs)
        except Exception as e:
            logger.warning(f"Error loading file combination: {prod_files}")
            return _meta
    
    if by_beam:
        beams = GEDI_BEAMS if which_beams is None else which_beams        
        
        def load_by_beam(pfiles_beam_tuple, product_vars, shots, dropna, suffix_all):
            pfiles, beam = pfiles_beam_tuple
            return _load_h5_merged(pfiles, product_vars=product_vars, which_beams=[beam], shots=shots, dropna=dropna, suffix_all=suffix_all)

        file_beam_combinations = list(itertools.product(prod_files_list, beams))
        return dask.dataframe.from_map(load_by_beam, file_beam_combinations, product_vars=product_vars, shots=shots, dropna=dropna, suffix_all=suffix_all, meta=_meta)

    return dask.dataframe.from_map(_load_h5_merged, prod_files_list, which_beams=which_beams, shots=shots, product_vars=product_vars, dropna=dropna, suffix_all=suffix_all, meta=_meta)

def add_special_columns(df, lon_col:str=None, lat_col:str=None, dat_col:str=None):
    if dat_col:
        df = df.assign(datetime=pd.to_datetime(df[dat_col] + GEDI_START_DATE.timestamp(), unit='s').dt.as_unit('s'))
    if lon_col and lat_col:
        df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col], crs='EPSG:4326'))
    return df.copy()