import os, re, glob
import shutil
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import h3pandas
import dask.dataframe
    
from config import GH3_DEFAULT_TMP_DIR, GH3_DEFAULT_SOC_DIR, GH3_DEFAULT_H3_DIR
from gedidriver import soc_file_tree, dask_h5_merged

def parquet_append_rows(df: pd.DataFrame, f: str, id_col: str = 'shot_number', tmp_suffix: str = '.row.tmp'):    
    parquet_file = pq.ParquetFile(f)
    
    if id_col:
        idx = parquet_file.read([id_col]).to_pandas().values.flatten()
        df = df[~df[id_col].isin(idx)]
    
    if df.empty:
        return
    
    new_table = pa.Table.from_pandas(df)
    
    temp_f = f + tmp_suffix
    with pq.ParquetWriter(temp_f, parquet_file.schema.to_arrow_schema()) as writer:
        for batch in parquet_file.iter_batches():
            writer.write_batch(batch)        
        writer.write_table(new_table)
    
    os.replace(temp_f, f)

def parquet_append_columns(df: pd.DataFrame, f: str, tmp_suffix:str = '.col.tmp'):
    parquet_file = pq.ParquetFile(f)
    new_table = pa.Table.from_pandas(df)
    
    existing_schema = parquet_file.schema.to_arrow_schema()
    existing_fields = list(existing_schema)
    new_fields = [field for field in new_table.schema if field.name not in existing_schema.names]
    combined_schema = pa.schema(existing_fields + new_fields)

    temp_f = f + tmp_suffix
    with pq.ParquetWriter(temp_f, combined_schema) as writer:
        for batch in parquet_file.iter_batches():
            batch_dict = batch.to_pydict()
            for field in new_table.schema:
                if field.name not in batch.schema.names:
                    batch_dict[field.name] = [None] * len(batch)
            writer.write_batch(pa.RecordBatch.from_pydict(batch_dict, combined_schema))

        new_batch_dict = new_table.to_pydict()
        for field in existing_schema:
            if field.name not in new_table.schema.names:
                new_batch_dict[field.name] = [None] * len(new_table)
        writer.write_batch(pa.RecordBatch.from_pydict(new_batch_dict, combined_schema))
    
    os.replace(temp_f, f)

def parquet_merge_files(ofile, flist, check_shots=True, rm_src=False):
    shots = np.array([], dtype=np.uint64)
    pqwriter = None
    schema = None
    
    try:
        for f in flist:
            if not os.path.exists(f):
                continue
                
            parquet_file = pq.ParquetFile(f)            
            if schema is None:
                schema = parquet_file.schema.to_arrow_schema()
                pqwriter = pq.ParquetWriter(ofile, schema)
            
            for batch in parquet_file.iter_batches():
                df = batch.to_pandas()
                
                if check_shots and 'shot_number' in df.columns:
                    new_shots = df['shot_number'].values.astype(np.uint64)
                    mask = ~np.isin(new_shots, shots)
                    df = df[mask]
                    shots = np.concatenate([shots, new_shots[mask]])
                
                if len(df) > 0:
                    table = pa.Table.from_pandas(df)
                    table = table.cast(schema)
                    pqwriter.write_table(table)
            
            if rm_src:
                os.unlink(f)
        
    finally:
        if pqwriter is not None:
            pqwriter.close()

def h3_index_df(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode'):
    import h3pandas
    return df.reset_index().h3.geo_to_h3(res, lat_col=lat_col, lng_col=lon_col).h3.h3_to_parent(part).reset_index().set_index(df.index.name)

def h3_tmp_files(df, res=12, part=3, lat_col='lat_lowestmode', lon_col='lon_lowestmode', dir_path=GH3_DEFAULT_TMP_DIR, roi_tiles=[]):
    if df.empty:
        return
    
    df = h3_index_df(df, res=res, part=part, lat_col=lat_col, lon_col=lon_col)
    df = df.reset_index().set_index(f'h3_{part:02d}')
    
    files = []
    for i in df.index.unique():
        if len(roi_tiles) > 0 and i not in roi_tiles:
            continue
        
        hex_path = os.path.join(dir_path,i)        
        hex_df = df.loc[[i]]
        gedi_name = re.sub('\\.h5$','.parquet', hex_df.root_file.iloc[0])        
        f = os.path.join(hex_path, gedi_name)
        
        if f.endswith('.parquet'):
            os.makedirs(hex_path, exist_ok=True)
            if os.path.exists(f):
                parquet_append_columns(hex_df, f)
            else:
                hex_df.to_parquet(f, engine='pyarrow')
            
        files.append(f)
        del hex_df
    
    del df    
    return files

def h3_merge_files(in_dir, out_dir=GH3_DEFAULT_H3_DIR, rm_src=True, replace=False):
    files = glob.glob(os.path.join(in_dir,'*.parquet'))
    
    if len(files) == 0:
        return    
    
    out_file = os.path.join(out_dir, os.path.basename(in_dir.rstrip('/'))+'.parquet')
    
    if is_temp := (os.path.exists(out_file) and not replace):
        files.insert(0,out_file)
        files = list(set(files))
        in_file = out_file
        out_file += '.tmp'
    
    parquet_merge_files(out_file, files, check_shots=True, rm_src=rm_src)
    
    if is_temp:
        os.replace(out_file, in_file)
    if rm_src:
        shutil.rmtree(in_dir, ignore_errors=True)
    return out_file

def _testit():
    from dask.distributed import Client, progress
    with Client() as client:
        print(client.dashboard_link)
        
        # prod_vars = {'L1B':['rxwaveform'], 'L2A': ['shot_number', 'rh'], 'L4A':['agbd'], 'L4C': ['wsci']}
        prod_vars = {'L2A': ['shot_number','lon_lowestmode','lat_lowestmode','elev_lowestmode','rh']}
        all_files = soc_file_tree(GH3_DEFAULT_SOC_DIR, to_list=True)
        ddf = dask_h5_merged(all_files, prod_vars)
        
        print("testing tmp files")    
        tmp_files = ddf.map_partitions(h3_tmp_files)
        tmp_files = tmp_files.persist(optimize_graph=False)
        progress(tmp_files)

        print("testing h3 files")    
        tmp_h3_dirs = glob.glob(os.path.join(GH3_DEFAULT_TMP_DIR, '*/'))
        h3_files = dask.dataframe.from_map(h3_merge_files, tmp_h3_dirs, rm_src=True)
        h3_files = h3_files.persist(optimize_graph=False)
        h3_files = progress(h3_files)
    
if __name__ == '__main__':
    _testit()