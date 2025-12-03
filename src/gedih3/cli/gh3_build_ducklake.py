import tqdm
import shutil
import pathlib
import datetime as dt

from gedih3 import sqlutils
from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR

def get_file_list():
    """Generate a list of all the possible parquet files in the database."""
    folders = list(pathlib.Path(GH3_DEFAULT_H3_DIR).glob('h3_03=*'))
    years = range(2019, dt.datetime.now().year + 1)
    files = []

    for f in folders:
        hex_id = f.name.split('=')[1]
        for y in years:
            files.append(f / f'year={y}' / f'{hex_id}.{y}.0.parquet')
    
    # guarantee that the first element of the list exists
    # because it will be used to create the table schema
    while files and not files[0].exists():
        files.pop(0)

    return files

def main():
    con = sqlutils.init_duckdb()

    # Ideally this would be done with a triple glob, but the file system
    # is very slow at this. I.e., 
    # data_spec = f"{GH3_DEFAULT_H3_DIR}/database_world/*/*/*.parquet"
    # files_df = con.sql(f"SELECT * FROM glob('{data_spec}')").df()
    print("Generating list of parquet files...")
    files = get_file_list()
    print(f"Found {len(files)} files to process.")

    # The DATA_PATH will not store any data and can be deleted after load
    # because the actual data is already stored in the parquet files.
    con.sql(f"""--sql
                ATTACH 'ducklake:gedi.ducklake' AS gedi_dl (
                DATA_PATH '{GH3_DEFAULT_TMP_DIR}/ducklake_temp');
            """)
    
    # Create the table schema based on one of the parquet files.
    # Exclude geometry: ducklake only supports geoparquet V2,
    # but the existing files are V1 without native geometry support.
    con.sql(f"""--sql
        CREATE OR REPLACE TABLE gedi_dl.data AS
        SELECT * EXCLUDE geometry
        FROM read_parquet('{files[0]}', hive_partitioning=true)
        WITH NO DATA;
    """)

    for file in tqdm.tqdm(files):
        # fmt: off
        if file.exists():
            con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{file.as_posix()}', ignore_extra_columns => true);")
        # fmt: on
    
    print(f"Saving ducklake metadata in {GH3_DEFAULT_H3_DIR}/gedi.ducklake ...")
    shutil.move("gedi.ducklake", f"{GH3_DEFAULT_H3_DIR}/gedi.ducklake")

if __name__ == "__main__":
    print("""This script assumes that all files follow the format:\n
    database_world/h3_03=*/year=*/<h3_03>.<year>.0.parquet\n
    If this is not the case (e.g. multiple parquet files within a year, etc.),
    the script will need to be modified accordingly.
    """)
    input("To acknowledge and proceed, press Enter >> ")
    main()
