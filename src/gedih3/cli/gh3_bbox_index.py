#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

import sys
import argparse


def get_cmd_args():
    from gedih3.cliutils import add_verbosity_args, add_dask_args

    p = argparse.ArgumentParser(
        description=(
            "Build the _bbox_index.parquet root sidecar for an H3 database: "
            "one row per partition year-file with the true data envelope, "
            "derived from existing parquet row-group statistics (footer-only "
            "scan, no data read). Query tools then skip files a region or "
            "EGI tile provably cannot touch. Safe to re-run at any time; "
            "gh3_build removes the index automatically when a merge changes "
            "the data."
        )
    )
    p.add_argument(
        "-d", "--database",
        dest="database",
        type=str,
        default=None,
        help="H3 database directory (default: GH3_DEFAULT_H3_DIR). Must be "
             "a local path - the index is written at the database root",
    )
    add_dask_args(p)
    add_verbosity_args(p)
    return p.parse_args()


def main():
    from gedih3.cliutils import (setup_logging, print_banner, print_success,
                                 cli_exception_handler, resolve_path_args,
                                 parse_dask_args)

    args = get_cmd_args()
    logger = setup_logging(args, __name__)
    print_banner("GEDI H3 BBox Index Builder", logger=logger)

    resolve_path_args(args, ['database'], logger=logger)

    with cli_exception_handler(args, logger=logger):
        from dask.distributed import Client
        from gedih3.gh3driver import gh3_build_bbox_index
        from gedih3.config import GH3_DEFAULT_H3_DIR

        database = args.database or GH3_DEFAULT_H3_DIR
        logger.info(f"Database: {database}")

        dask_kwargs = parse_dask_args(args)
        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard available at: {client.dashboard_link}")
            opath = gh3_build_bbox_index(database)

        print_success(f"BBox index written to {opath}", logger=logger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
