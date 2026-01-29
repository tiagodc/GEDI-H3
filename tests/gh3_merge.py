if __name__ == "__main__":
    import os, glob
    from pqdm.processes import pqdm
    from gedih3.utils import parquet_join_columns
    from gedih3.gh3builder import h3_write_metadata, h3_merge_metadata
    
    src_dir = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world/'
    join_dir = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_a10/'
    out_dir = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_merged/'
    
    def join_gh3_files(src_path):
        try:
            join_path = src_path.replace('database_world', 'database_world_a10')
            out_path = src_path.replace('database_world', 'database_world_merged')
            
            if not os.path.exists(join_path):
                return
            
            flist = [src_path, join_path]
            
            parquet_join_columns(flist=flist, 
                                ofile=out_path, 
                                key_col='shot_number',
                                tmp_suffix='.join.tmp',
                                join_how='left')
            
            mfile = h3_write_metadata(out_path)        
            return out_path
        except Exception as e:
            print(f"Error processing {src_path}: {e}")
            return None
    
    print("Finding source files...")    
    src_files = glob.glob(f'{src_dir}*/*/*.parquet')
    
    print(f"Found {len(src_files)} source files.")
    
    print("Merging files...")
    n_cpus = os.cpu_count() // 4
    results = pqdm(src_files, join_gh3_files, n_jobs=n_cpus)
    
    print("Merging metadata...")
    h3_subdirs = glob.glob(os.path.join(out_dir,'h3_*/'))
    mmetas = pqdm(h3_subdirs, h3_merge_metadata, n_jobs=n_cpus)