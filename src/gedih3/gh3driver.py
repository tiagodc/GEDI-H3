import os, glob

from.config import GH3_DEFAULT_H3_DIR

def gh3_list_files(gh3_root_dir=GH3_DEFAULT_H3_DIR, product=None):    
    if recursive := product is None:
        product = '**'
    return glob.glob(os.path.join(gh3_root_dir, product.lower(), '*.parquet'), recursive=recursive)