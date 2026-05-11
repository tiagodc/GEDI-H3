#! python

# Copyright (C) 2025, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

import sys
import shutil
import pathlib
import argparse
import datetime as dt


def get_cmd_args():
    from gedih3.cliutils import add_verbosity_args

    p = argparse.ArgumentParser(
        description=(
            "Build a DuckLake metadata table from an existing H3 parquet database "
            "to enable SQL queries on GEDI data via DuckDB. "
            "Files are expected to follow the layout: "
            "h3_XX=*/year=*/<hex>.<year>.0.parquet "
            "where XX is the H3 partition level stored in the build log."
        )
    )
    p.add_argument(
        "-d", "--database",
        dest="database",
        type=str,
        default=None,
        help="H3 database directory (default: GH3_DEFAULT_H3_DIR)",
    )
    p.add_argument(
        "-t", "--tmpdir",
        dest="tmpdir",
        type=str,
        default=None,
        help="temporary directory for DuckLake data files (default: GH3_DEFAULT_TMP_DIR/ducklake_temp)",
    )
    add_verbosity_args(p)
    return p.parse_args()


def get_file_list(root_dir):
    """Generate a list of all expected parquet files in the H3 database.

    Parameters
    ----------
    root_dir : str or pathlib.Path
        Root directory of the H3 database.

    Returns
    -------
    list of pathlib.Path
        Paths to parquet files that exist on disk, ordered so the first element
        is guaranteed to exist (used to infer the table schema).
    """
    from gedih3.gh3driver import gh3_read_meta

    root_dir = pathlib.Path(root_dir)
    part_level = gh3_read_meta("h3_partition_level", gh3_root_dir=str(root_dir))
    if part_level is None:
        part_level = 3

    folders = list(root_dir.glob(f"h3_{part_level:02d}=*"))
    years = range(2019, dt.datetime.now().year + 1)
    files = []

    for f in folders:
        hex_id = f.name.split("=")[1]
        for y in years:
            files.append(f / f"year={y}" / f"{hex_id}.{y}.0.parquet")

    # Guarantee the first element exists — it is used to create the table schema.
    while files and not files[0].exists():
        files.pop(0)

    return files


def main():
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import setup_logging, print_banner, print_success, cli_exception_handler
    from gedih3 import sqlutils
    import tqdm

    args = get_cmd_args()
    logger = setup_logging(args, __name__)
    print_banner("GEDI DuckLake Builder Tool", logger=logger)

    database = pathlib.Path(args.database or GH3_DEFAULT_H3_DIR)
    tmpdir = args.tmpdir or f"{GH3_DEFAULT_TMP_DIR}/ducklake_temp"

    with cli_exception_handler(args, logger=logger):
        logger.info("Generating list of parquet files ...")
        files = get_file_list(database)

        if not files:
            logger.error(f"No parquet files found in {database}")
            sys.exit(2)

        logger.info(f"Found {len(files)} files to process")

        con = sqlutils.init_duckdb(temp_directory=tmpdir)

        # The DATA_PATH stores no actual data and can be deleted after load
        # because the data lives in the existing parquet files.
        con.sql(f"""--sql
            ATTACH 'ducklake:gedi.ducklake' AS gedi_dl (
            DATA_PATH '{tmpdir}');
        """)

        # Build schema from the first file. Geometry is excluded because
        # ducklake only supports GeoParquet V2, but the H3 files are V1.
        con.sql(f"""--sql
            CREATE OR REPLACE TABLE gedi_dl.data AS
            SELECT * EXCLUDE geometry
            FROM read_parquet('{files[0]}', hive_partitioning=true)
            WITH NO DATA;
        """)

        for file in tqdm.tqdm(files, desc="Loading parquet files", disable=args.quiet):
            # fmt: off
            if file.exists():
                con.execute(f"CALL ducklake_add_data_files('gedi_dl', 'data', '{file.as_posix()}', ignore_extra_columns => true);")
            # fmt: on

        ducklake_dest = database / "gedi.ducklake"
        logger.info(f"Saving DuckLake metadata to {ducklake_dest} ...")
        shutil.move("gedi.ducklake", ducklake_dest)

        print_success(f"DuckLake metadata saved to {ducklake_dest}", logger=logger)


if __name__ == "__main__":
    main()
