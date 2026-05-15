#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

import os
import sys
import shutil
import pathlib
import argparse


HEX_CHARS = "0123456789abcdef"


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
    p.add_argument(
        "--batches",
        dest="batches",
        type=int,
        default=16,
        choices=(1, 2, 4, 8, 16),
        help=(
            "split the bulk add_data_files CALL into N batches grouped by leading "
            "hex char of the h3 cell id (default: 16; use 1 for a single CALL — "
            "faster on tiny DBs but with no progress visibility and peak memory "
            "proportional to file count)."
        ),
    )
    add_verbosity_args(p)
    return p.parse_args()


def find_sample_parquet(root_dir, part_level):
    """Return one existing parquet path under the database, used to seed the schema."""
    root_dir = pathlib.Path(root_dir)
    for f in root_dir.glob(f"h3_{part_level:02d}=*/year=*/*.parquet"):
        return f
    return None


def batch_prefixes(root_dir, part_level, n_batches):
    """Group hex prefix chars into n_batches non-empty batches.

    Each batch is a string of hex chars whose combined `h3_XX=<c>*` glob has at
    least one matching partition dir. Empty groups are dropped (ducklake_add_data_files
    raises on a glob that matches nothing). Returns [] if n_batches == 1 — caller
    should use the unsharded glob.
    """
    if n_batches <= 1:
        return []

    prefix = f"h3_{part_level:02d}="
    present = set()
    with os.scandir(root_dir) as it:
        for entry in it:
            if entry.is_dir() and entry.name.startswith(prefix):
                tail = entry.name[len(prefix):]
                if tail:
                    present.add(tail[0].lower())

    group_size = 16 // n_batches  # 1, 2, 4, 8, 16 → 16, 8, 4, 2, 1
    batches = []
    for i in range(0, 16, group_size):
        group = "".join(c for c in HEX_CHARS[i:i + group_size] if c in present)
        if group:
            batches.append(group)
    return batches


def main():
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_TMP_DIR
    from gedih3.cliutils import setup_logging, print_banner, print_success, cli_exception_handler
    from gedih3.gh3driver import gh3_read_meta
    from gedih3 import sqlutils
    import tqdm

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

        batches = batch_prefixes(database, part_level, args.batches)
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

        # Bulk CALL via glob — ducklake's metadata-registration path is single-threaded
        # internally (duckdb/ducklake#404), so we shard the glob into N batches grouped
        # by leading hex char of the h3 cell id. Each batch is its own transaction:
        # caps memory growth (~50 GB peak collapsed to ~50/N GB) and gives tqdm a tick
        # per batch. hive_partitioning is required so the h3_XX / year partition
        # columns are populated from the path. n=1 keeps the unsharded glob path.
        prefix_glob = f"h3_{part_level:02d}="
        if batches:
            iterator = tqdm.tqdm(
                batches, desc="Registering parquet batches", disable=args.quiet
            )
            for group in iterator:
                # [chars] is a glob char class: matches files under any h3_XX=<c>... dir
                # whose first hex char is in group.
                glob_pattern = (
                    database / f"{prefix_glob}[{group}]*" / "year=*" / "*.parquet"
                ).as_posix()
                if hasattr(iterator, "set_postfix_str"):
                    iterator.set_postfix_str(f"prefix=[{group}]")
                con.execute(f"""--sql
                    CALL ducklake_add_data_files('gedi_dl', 'data', '{glob_pattern}',
                                                 hive_partitioning => true,
                                                 ignore_extra_columns => true);
                """)
        else:
            glob_pattern = (database / f"{prefix_glob}*" / "year=*" / "*.parquet").as_posix()
            logger.info(f"Registering files via glob: {glob_pattern}")
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
