#! python
"""
GEDI H3/EGI Rasterization Tool

Convert pre-aggregated GEDI datasets to GeoTIFF raster format.

This tool reads datasets produced by gh3_aggregate or gh3_extract and
converts them to GeoTIFF rasters. For EGI datasets, the output is natively
aligned to the EASE-Grid 2.0 projection. For H3 datasets, interpolation
is used to approximate hexagonal data on a regular grid.

IMPORTANT: This tool does NOT perform aggregation. To aggregate raw GEDI
shots to coarser resolutions before rasterization, use gh3_aggregate first
(optionally with the --rasterize flag to do both in one step).

Author: Tiago de Conto
Package: gedih3
"""

import os
import sys
import argparse

DEBUG = False


def get_cmd_args():
    """Parse command line arguments for GEDI rasterization"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args

    p = argparse.ArgumentParser(
        description="Convert aggregated GEDI datasets to GeoTIFF raster format",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Input/output configuration
    p.add_argument("-d", "--dataset", dest="dataset", required=not DEBUG, type=str,
                   help="path to aggregated dataset (from gh3_aggregate or gh3_extract)")
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory or file path")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")
    p.add_argument("--compress", dest="compress", type=str, default='LZW',
                   choices=['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE'],
                   help="GeoTIFF compression [default=LZW]")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to rasterize (space-separated)")

    # Filtering
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering before rasterization")

    # Dask and verbosity
    add_dask_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def main():
    args = get_cmd_args()

    if DEBUG:
        args.dataset = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/gedih3_tutorial/aggregated/egi_level6'
        args.output = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/raster_test'
        args.list = ['agbd_l4a']
        args.cores = 4
        args.port = 9995

    # Import cli_exception_handler early for wrapping the main logic
    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        import json
        import glob
        import pandas as pd
        from dask.distributed import Client, progress

        import gedih3.gh3driver as gh3
        from gedih3 import raster
        from gedih3.cliutils import (parse_dask_args, setup_logging,
                                     print_banner, print_success)

        # Setup logging and print banner
        logger = setup_logging(args, __name__)
        print_banner("GEDI Rasterization Tool", logger=logger)

        # Validate input dataset exists
        if not os.path.exists(args.dataset):
            logger.error(f"Dataset not found: {args.dataset}")
            sys.exit(1)

        # Read dataset metadata to determine index type and level
        dataset_meta_path = os.path.join(args.dataset, "gedih3_dataset.json")

        if not os.path.exists(dataset_meta_path):
            logger.error(f"Dataset metadata not found: {dataset_meta_path}")
            logger.error("This tool requires a dataset produced by gh3_aggregate or gh3_extract.")
            logger.error("For raw GEDI data, use gh3_aggregate with --rasterize flag instead.")
            sys.exit(1)

        with open(dataset_meta_path, 'r') as f:
            dataset_meta = json.load(f)

        index_type = dataset_meta.get('index_type')
        index_level = dataset_meta.get('index_level')

        if not index_type or not index_level:
            logger.error("Dataset metadata missing index_type or index_level")
            sys.exit(1)

        use_egi = index_type == 'egi'

        if use_egi:
            from gedih3 import egi
            logger.info(f"Dataset type: EGI level {index_level} (~{egi.get_resolution(index_level):.0f}m)")
            rasterize_func = egi.rasterize_partition
        else:
            logger.info(f"Dataset type: H3 level {index_level}")
            rasterize_func = raster.rasterize_h3_partition

        logger.info(f"Input: {args.dataset}")

        # Collect columns to rasterize
        columns = args.list if args.list else None
        if columns:
            logger.info(f"Variables to rasterize: {columns}")

        # Build query
        query_str = args.query
        if query_str:
            logger.info(f"Query filter: {query_str}")

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            # Load the dataset
            logger.info("Loading dataset...")
            ddf = gh3.gh3_load_dataset_lazy(args.dataset, columns=columns)
            logger.info(f"  Loaded {ddf.npartitions} partitions")

            # Apply query filter if provided
            if query_str:
                logger.info("Applying filter...")
                ddf = ddf.query(query_str)

            # Rasterize
            logger.info("Rasterizing...")

            os.makedirs(args.output if not args.merge else os.path.dirname(args.output or '.'), exist_ok=True)

            # Let rasterize functions auto-detect columns from data
            raster_columns = columns

            if args.merge:
                output_path = args.output if args.output.endswith('.tif') else f"{args.output}.tif"
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

                raster.merge_and_export_rasters(
                    ddf, output_path, rasterize_func,
                    columns=raster_columns, compress=args.compress, show_progress=True
                )
                logger.info(f"Merged raster exported to {output_path}")

            else:
                os.makedirs(args.output, exist_ok=True)

                paths = raster.rasterize_and_export_partitions(
                    ddf, args.output, rasterize_func,
                    columns=raster_columns, compress=args.compress, show_progress=True
                )
                valid_paths = [p for p in paths if p]
                logger.info(f"Exported {len(valid_paths)} raster files to {args.output}")

            print_success("Rasterization complete", logger=logger)


if __name__ == '__main__':
    main()
