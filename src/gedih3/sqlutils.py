import os,duckdb

from .config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
from .utils import get_system_resources

def init_duckdb(threads=None, memory_limit=None, tmp_directory=None, max_tmp_size=None):
    tmp_directory = tmp_directory if tmp_directory is not None else f"{GH3_DEFAULT_TMP_DIR}/duckdb"
    os.makedirs(tmp_directory, exist_ok=True)

    cpus, ram, storage = get_system_resources(disk_path=tmp_directory)
    memory_limit = memory_limit if memory_limit is not None else int(ram * 0.75)
    max_tmp_size = max_tmp_size if max_tmp_size is not None else storage // 4
    threads = threads if threads is not None else max(1, cpus // 4)

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute(f"SET memory_limit='{memory_limit}GB';")
    con.execute(f"SET tmp_directory='{tmp_directory}';")
    con.execute(f"SET max_tmp_directory_size='{max_tmp_size}GB';")
    con.execute(f"PRAGMA threads={threads};")
    return con