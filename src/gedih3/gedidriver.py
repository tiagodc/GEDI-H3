import os, h5py, re, glob, yaml, dask
import itertools
from typing import Dict, Union
import numpy as np
import pandas as pd
from numba import njit
from datetime import datetime
import dask
import dask.dataframe
from earthaccess.store import EarthAccessFile

from .config import GEDI_PRODUCTS, GEDI_BEAMS, GH3_DEFAULT_SOC_DIR
from .utils import h5_copy_subset, h5_info

def soc_prod_from_file(file_path):
    f = GEDIFile(file_path)
    prod_str = f"L{f.product[-1]}{f.level}"
    return prod_str

def soc_from_file(file_path):
    return os.path.dirname(os.path.dirname(os.path.dirname(file_path)))

def soc_file_tree(file_struct: Union[str, list], to_list=False, glob_kwargs=None):
    direct_access = False
    if isinstance(file_struct, str) and os.path.isdir(file_struct):
        glob_pattern = gedi_file_glob(**glob_kwargs) if glob_kwargs else '*.h5'
        file_list = glob.glob(os.path.join(file_struct, '**', glob_pattern), recursive=True)
    elif (direct_access := isinstance(file_struct[0], EarthAccessFile)):
        file_list = [i.path for i in file_struct]
    elif isinstance(file_struct[0], str):
        file_list = file_struct
    else:
        raise ValueError("file_struct must be a directory or a list of file paths (or s3 links)")
    
    file_globs = np.array([GEDIFile.from_filename(f).get_glob_pattern(product=None, level=None, ppds=None) for f in file_list])    
    file_array = np.array(file_struct) if direct_access else np.array(file_list)
    
    soc_tree = {}
    for f in np.unique(file_globs):
        orb_track = re.sub(r'^GEDI\*_.*_(O\d{5}_\d{2}_T\d{5}).*\.h5$', r'\1', f)
        f_prods = file_array[file_globs == f]
        f_obj = {soc_prod_from_file(fp):fp for fp in f_prods.tolist()}
        soc_tree[orb_track] = dict(sorted(f_obj.items()))
    
    if to_list:
        soc_tree = list(soc_tree.values())
    
    return soc_tree

def gedi_subset(source_file, dest_file, variables, subset_beams=None):
    with h5py.File(source_file, 'r') as f:
        beams = [k for k in f.keys() if k.startswith('BEAM')]
    
    if subset_beams is not None:
        beams = [b for b in beams if b in subset_beams]
    
    beam_variables = [f"{b}/{v}" for b in beams for v in variables]
    h5_copy_subset(source_file, dest_file, beam_variables)
    
    if os.path.exists(dest_file):
        return dest_file
    return None

def gedi_file_glob(orbit=None, orbit_granule=None, track=None, product: int=1, level: str='B', ppds: int=None, pge:int=None, generation:int=None, version:int=None):
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

def gedi_vars_expand(product_vars):
    for prod, vars in product_vars.items():
        if vars is None:
            continue
        if os.path.isfile(vars[0]) and len(vars) == 1:
            with open(vars[0], 'r') as f:
                product_vars[prod] = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        elif "minimal" in vars or "min" in vars:
            product_vars[prod] = GEDI_PRODUCTS[prod]['default_vars']
        elif "*" in vars or "all" in vars:
            product_vars[prod] = None
        elif isinstance(vars, list):
            continue
        else:
            raise ValueError(f"Unknown variable specification for product {prod}: {vars}")
    return product_vars

def gedi_vars_from_h5(gedi_file):
    with h5py.File(gedi_file, 'r') as f:
        b = [i for i in f.keys() if i.upper().startswith('BEAM')][0]
    pl_info = h5_info(gedi_file, root=b)
    return pl_info.path.str.replace(b,'').str.lstrip('/').tolist()

def check_soc_file_vars(soc_file, available_products):
    file_products = {}
    for prod in available_products.keys():
        if prod in soc_file:
            available_vars = gedi_vars_from_h5(soc_file[prod])
            file_products[prod] = available_vars
    return file_products

@dask.delayed
def _check_soc_file_vars(soc_file, available_products):
    return check_soc_file_vars(soc_file, available_products)

def validate_soc_files(product_vars: Dict, soc_dir: str = GH3_DEFAULT_SOC_DIR):
    soc_files = soc_file_tree(soc_dir, to_list=True)

    if not soc_files:
        return False, {"error": "No SOC files found in directory"}

    available_products = {k: set() for k in soc_files[0].keys()}
    delayed_tasks = [_check_soc_file_vars(soc_file, available_products) for soc_file in soc_files]
    file_results = dask.compute(*delayed_tasks)

    for file_products in file_results:
        for prod, vars_list in file_products.items():
            available_products[prod].update(vars_list)

    available_products = {k: list(v) for k, v in available_products.items()}

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
    def __init__(self, file_path):
        if isinstance(file_path, EarthAccessFile):
            file_path = file_path.path
        self.parse_file(file_path)
    
    def __str__(self):
        return yaml.dump(self)
    
    def parse_file(self, f):
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
    
    def __init__(self, shot, build_glob: bool = False):
        self.parse_shot(shot)
        if build_glob:
            self.get_file_glob()
    
    def parse_shot(self, shot):
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

def load_h5(fpath, columns, which_beams=None, shots=None, include_source=True, dropna=True):
    f = h5py.File(fpath, 'r')
    if 'shot_number' not in columns:
        columns.append('shot_number')
    
    if 'rxwaveform' in columns:
        columns += ['noise_mean_corrected','rx_sample_start_index','rx_sample_count']
        columns = list(set(columns))
    
    if 'txwaveform' in columns:
        columns += ['noise_mean_corrected','tx_sample_start_index','tx_sample_count']
        columns = list(set(columns))    
    
    beams = all_beams = [k for k in f.keys() if k.startswith('BEAM')]
    
    if shots is not None:
        shot_beams = np.uint16(shots % 10000000000000 // 100000000000)
        beams = [f'BEAM{b:04b}' for b in np.unique(shot_beams)]
        
    if which_beams is not None:
        beams = [b for b in beams if b in which_beams]
        
    if len(beams) == 0:
        f.close()
        return load_h5(fpath=fpath, columns=columns, include_source=include_source, which_beams=all_beams[[0]]).head(0)
  
    full_df = []
    for k in beams:
        shot_ids = f[f"{k}/shot_number"][:]
        
        if shots is not None:
            kint = int(k[-4:],base=2)
            k_shots = shots[shot_beams == kint]
            idx = np.where(np.isin(shot_ids, k_shots))
        else:
            idx = np.arange(0,len(shot_ids))
            
        if len(idx) == 0: 
            continue

        dfs = {}
        for j in columns:
            if is_wave := (j == 'rxwaveform' or j == 'txwaveform'):
                is_tx = j == 'txwaveform'
                d = wfm_extract(f, k, idx, is_tx)
            else:
                d = f[f"{k}/{j}"][:][idx]
                            
            if d.ndim == 2:
                for col in range(d.shape[-1]):
                    jj = f"{j}_{col:0{4 if is_wave else 3}d}"
                    dfs[jj] = d[:,col]
            else:
                dfs[j] = d

        dfs = pd.DataFrame(dfs)
        if include_source: 
            dfs['root_beam'] = k
        full_df.append(dfs)
    
    f.close()    
    full_df = pd.concat(full_df)
    full_df = full_df.set_index('shot_number')
    
    if include_source:
        full_df['root_file'] = os.path.basename(fpath.path if isinstance(fpath, EarthAccessFile) else fpath)        
    
    if dropna:
        full_df.dropna()
    
    return full_df

def load_h5_merged(prod_files, product_vars, which_beams=None, shots=None, dropna=True, suffix_all=False):
    df = None
    for i, (p, vars) in enumerate(product_vars.items()):
        ppath = prod_files.get(p)
        
        if ppath is None:
            continue
        
        idf = load_h5(fpath=ppath, columns=vars, include_source = suffix_all or i==0, which_beams=which_beams, shots=shots, dropna=dropna)
        
        suffix = f"_{p.lower()}"
        if suffix_all:
            idf = idf.rename(columns=lambda x: x if x.endswith(suffix) else f"{x}{suffix}")

        if df is None:
            df = idf
        else:
            df = df.join(idf, how='inner', rsuffix=suffix)
    return df

def dask_h5_merged(prod_files_list, product_vars, which_beams=None, shots=None, dropna=True, suffix_all=False, by_beam=False):
    if by_beam:
        beams = GEDI_BEAMS if which_beams is None else which_beams        
        
        def load_by_beam(pfiles_beam_tuple, product_vars, shots, dropna, suffix_all):
            pfiles, beam = pfiles_beam_tuple
            return load_h5_merged(pfiles, product_vars=product_vars, which_beams=[beam], shots=shots, dropna=dropna, suffix_all=suffix_all)

        file_beam_combinations = list(itertools.product(prod_files_list, beams))
        return dask.dataframe.from_map(load_by_beam, file_beam_combinations, product_vars=product_vars, shots=shots, dropna=dropna, suffix_all=suffix_all)                            

    return dask.dataframe.from_map(load_h5_merged, prod_files_list, which_beams=which_beams, shots=shots, product_vars=product_vars, dropna=dropna, suffix_all=suffix_all)

def _testit():
    soc_dir = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/soc'    
    fpath = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/soc/2020/014/GEDI02_A_2020014213209_O06178_02_T00931_02_003_01_V002.h5'
    product_vars = {'L1B':['rxwaveform'], 'L2A': ['shot_number', 'rh'], 'L4C': ['wsci']}
    
    gfile = GEDIFile(fpath)
    pfiles = {p:gfile.search_file(product=int(p[1]), level=p[-1]) for p in product_vars.keys()}
    
    print("testing load_h5_merged()")
    try:
        df = load_h5_merged(pfiles, product_vars)
        print("Test successful")
    except Exception as e:
        print(f"Test failed: {e}")
    
    print("testing dask_h5_merged()")
    try:
        all_files = soc_file_tree(soc_dir, to_list=True)
        df = dask_h5_merged(all_files, product_vars)
        print(df.head())
        print("Test successful")
    except Exception as e:
        print(f"Test failed: {e}")        
        
if __name__ == "__main__":
    _testit()