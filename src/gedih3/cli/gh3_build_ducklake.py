#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

import sys
import shutil
import pathlib
import argparse


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


def find_sample_parquet(root_dir, part_level):
    """Return one existing parquet path under the database, used to seed the schema."""
    root_dir = pathlib.Path(root_dir)
    for f in root_dir.glob(f"h3_{part_level:02d}=*/year=*/*.parquet"):
        return f
    return None


def main():
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import setup_logging, print_banner, print_success, cli_exception_handler
    from gedih3.gh3driver import gh3_read_meta
    from gedih3 import sqlutils

    args = get_cmd_args()
    logger = setup_logging(args, __name__)
    print_banner("GEDI DuckLake Builder Tool", logger=logger)

    database = pathlib.Path(args.database or GH3_DEFAULT_H3_DIR)
    tmpdir = args.tmpdir or f"{GH3_DEFAULT_TMP_DIR}/ducklake_temp"

    with cli_exception_handler(args, logger=logger):
        part_level = gh3_read_meta("h3_partition_level", gh3_root_dir=str(database))
        if part_level is None:
            part_level = 3

        sample = find_sample_parquet(database, part_level)
        if sample is None:
            logger.error(f"No parquet files found under {database}")
            sys.exit(2)

        glob_pattern = (database / f"h3_{part_level:02d}=*" / "year=*" / "*.parquet").as_posix()
        logger.info(f"Registering files via glob: {glob_pattern}")

        con = sqlutils.init_duckdb(temp_directory=tmpdir)

        # The DATA_PATH stores no actual data and can be deleted after load
        # because the data lives in the existing parquet files.
        con.sql(f"""--sql
            ATTACH 'ducklake:gedi.ducklake' AS gedi_dl (
            DATA_PATH '{tmpdir}');
        """)

        # Build schema from a sample file. Geometry is excluded because
        # ducklake only supports GeoParquet V2, but the H3 files are V1.
        con.sql(f"""--sql
            CREATE OR REPLACE TABLE gedi_dl.data AS
            SELECT * EXCLUDE geometry
            FROM read_parquet('{sample.as_posix()}', hive_partitioning=true)
            WITH NO DATA;
        """)

        # Single bulk CALL via glob: ~35x faster than per-file CALLs in DuckDB 1.4
        # (per-file overhead dominates; one glob call processes thousands of files
        # in a single ducklake transaction). hive_partitioning is required so the
        # h3_XX / year partition columns are populated from the path.
        con.execute(f"""--sql
            CALL ducklake_add_data_files('gedi_dl', 'data', '{glob_pattern}',
                                         hive_partitioning => true,
                                         ignore_extra_columns => true);
        """)

        row = con.execute(
            "SELECT file_count, file_size_bytes FROM ducklake_table_info('gedi_dl') WHERE table_name = 'data';"
        ).fetchone()
        if row:
            logger.info(f"Registered {row[0]} parquet files ({row[1] / 1e9:.1f} GB)")

        ducklake_dest = database / "gedi.ducklake"
        logger.info(f"Saving DuckLake metadata to {ducklake_dest} ...")
        shutil.move("gedi.ducklake", ducklake_dest)

        print_success(f"DuckLake metadata saved to {ducklake_dest}", logger=logger)


if __name__ == "__main__":
    main()
