import os

import duckdb

from .config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
from .utils import get_system_resources


def init_duckdb(threads=None, memory_limit=None, temp_directory=None, max_temp_size=None):
    temp_directory = temp_directory if temp_directory is not None else f"{GH3_DEFAULT_TMP_DIR}/duckdb"
    os.makedirs(temp_directory, exist_ok=True)

    cpus, ram, storage = get_system_resources(disk_path=temp_directory)
    memory_limit = memory_limit if memory_limit is not None else int(ram * 0.75)
    max_temp_size = max_temp_size if max_temp_size is not None else storage // 4
    threads = threads if threads is not None else max(1, cpus // 4)

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET parquet_metadata_cache = true;")
    con.execute(f"SET memory_limit='{memory_limit}GB';")
    con.execute(f"SET temp_directory='{temp_directory}';")
    con.execute(f"SET max_temp_directory_size='{max_temp_size}GB';")
    con.execute(f"PRAGMA threads={threads};")
    return con


def attach_ducklake_db(con, name="gedi_dl"):
    """Attach existing ducklake database located in GH3_DEFAULT_H3_DIR.

    Once this function is called, the gedi data can be queried using
        `SELECT ... FROM {name}.data`
    """
    con.sql(f"""--sql
        ATTACH 'ducklake:{GH3_DEFAULT_H3_DIR}/gedi.ducklake' AS {name} (READ_ONLY);
    """)
